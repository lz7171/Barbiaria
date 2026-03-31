import os
import sys
import json
import uuid
import hashlib
import re
import unicodedata
import urllib.parse
import asyncio
import time
import logging
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import List, Optional

# ═══════════════════════════════════════════════════════
#  LOGGING — console only (Vercel captures stdout)
# ═══════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("ngbsaas")

# ═══════════════════════════════════════════════════════
#  DATABASE — MySQL via aiomysql
#  Credentials come from Vercel environment variables
# ═══════════════════════════════════════════════════════
try:
    import aiomysql
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False
    logger.warning("aiomysql not installed")

_DB_HOST     = os.environ.get("DB_HOST", "")
_DB_USER     = os.environ.get("DB_USER", "")
_DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
_DB_NAME     = os.environ.get("DB_NAME", "")
_DB_PORT     = int(os.environ.get("DB_PORT", "3306"))

_MISSING_ENV = [k for k, v in [
    ("DB_HOST", _DB_HOST), ("DB_USER", _DB_USER),
    ("DB_PASSWORD", _DB_PASSWORD), ("DB_NAME", _DB_NAME)
] if not v]

if _MISSING_ENV:
    logger.error(f"Missing environment variables: {', '.join(_MISSING_ENV)}")

DB_CONFIG = {
    "host":            _DB_HOST,
    "port":            _DB_PORT,
    "user":            _DB_USER,
    "password":        _DB_PASSWORD,
    "db":              _DB_NAME,
    "charset":         "utf8mb4",
    "autocommit":      True,
    "connect_timeout": 10,
}

ADMIN_USERNAME_ENV = os.environ.get("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD_ENV = os.environ.get("ADMIN_PASSWORD", "")

_db_pool = None
_pool_lock = asyncio.Lock()

async def _create_pool() -> bool:
    global _db_pool
    if _MISSING_ENV:
        return False
    try:
        _db_pool = await aiomysql.create_pool(
            **{k: v for k, v in DB_CONFIG.items() if k != "autocommit"},
            autocommit=True,
            minsize=1,
            maxsize=3,           # keep small for serverless
            pool_recycle=300,    # 5 min recycle — safe for serverless cold starts
        )
        logger.info("MySQL pool created")
        return True
    except Exception as e:
        logger.error(f"Failed to create MySQL pool: {e}")
        _db_pool = None
        return False

async def get_db_pool():
    global _db_pool
    if _db_pool is not None and not _db_pool.closed and HAS_MYSQL:
        return _db_pool
    if not HAS_MYSQL:
        return None
    async with _pool_lock:
        if _db_pool is not None and not _db_pool.closed:
            return _db_pool
        await _create_pool()
    return _db_pool

from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _using_mysql
    _using_mysql = await run_migrations()
    logger.info(f"Storage: {'MySQL' if _using_mysql else 'unavailable'}")
    yield
    global _db_pool
    if _db_pool and not _db_pool.closed:
        _db_pool.close()
        await _db_pool.wait_closed()

app = FastAPI(
    title="NGB SaaS Multi-Segmento",
    version="3.2.0",
    description="Sistema SaaS de agendamento multi-segmento",
    docs_url="/api/docs",
    redoc_url=None,
    lifespan=lifespan,
)

_raw_origins = os.environ.get("ALLOWED_ORIGINS", "")
ALLOWED_ORIGINS = [o.strip() for o in _raw_origins.split(",") if o.strip()] if _raw_origins else ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "Accept"],
)

# ═══════════════════════════════════════════════════════
#  DB HELPERS
# ═══════════════════════════════════════════════════════
async def db_execute(sql: str, args=None) -> int:
    pool = await get_db_pool()
    if pool is None:
        raise HTTPException(status_code=503, detail="Banco de dados temporariamente indisponível")
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, args or ())
                return cur.rowcount
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"db_execute failed: {e}")
        raise HTTPException(status_code=503, detail="Erro de banco de dados")

async def db_fetchone(sql: str, args=None) -> Optional[dict]:
    pool = await get_db_pool()
    if pool is None:
        raise HTTPException(status_code=503, detail="Banco de dados temporariamente indisponível")
    try:
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql, args or ())
                return await cur.fetchone()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"db_fetchone failed: {e}")
        raise HTTPException(status_code=503, detail="Erro de banco de dados")

async def db_fetchall(sql: str, args=None) -> list:
    pool = await get_db_pool()
    if pool is None:
        raise HTTPException(status_code=503, detail="Banco de dados temporariamente indisponível")
    try:
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql, args or ())
                return await cur.fetchall()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"db_fetchall failed: {e}")
        raise HTTPException(status_code=503, detail="Erro de banco de dados")

# ═══════════════════════════════════════════════════════
#  MIGRATIONS
# ═══════════════════════════════════════════════════════
MIGRATIONS = [
    """
    CREATE TABLE IF NOT EXISTS admin_config (
        id          INT PRIMARY KEY AUTO_INCREMENT,
        username    VARCHAR(80)  NOT NULL DEFAULT 'admin',
        password    VARCHAR(64)  NOT NULL,
        token       VARCHAR(200) NOT NULL DEFAULT '',
        token_at    DATETIME     NULL,
        updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS clients (
        id              VARCHAR(36)   PRIMARY KEY,
        slug            VARCHAR(50)   NOT NULL UNIQUE,
        nome            VARCHAR(200)  NOT NULL,
        telefone        VARCHAR(20)   NOT NULL,
        endereco        TEXT,
        descricao       TEXT,
        status          ENUM('ativo','inativo') NOT NULL DEFAULT 'ativo',
        business_type   VARCHAR(50)  NOT NULL DEFAULT 'barbearia',
        template        VARCHAR(50)  NOT NULL DEFAULT 'classic',
        cor_primaria    VARCHAR(10)  NOT NULL DEFAULT '#1a1a2e',
        cor_secundaria  VARCHAR(10)  NOT NULL DEFAULT '#c9a84c',
        logo            TEXT,
        banner          TEXT,
        meta_title      VARCHAR(200),
        meta_desc       TEXT,
        instagram       VARCHAR(100),
        slot_interval   SMALLINT UNSIGNED NOT NULL DEFAULT 30,
        booking_days    SMALLINT UNSIGNED NOT NULL DEFAULT 30,
        max_per_slot    TINYINT UNSIGNED  NOT NULL DEFAULT 1,
        confirm_msg     TEXT,
        hero_tag        VARCHAR(100),
        sobre_titulo    VARCHAR(200),
        cta_text        VARCHAR(200),
        team_label      VARCHAR(100),
        anos_exp        SMALLINT UNSIGNED NOT NULL DEFAULT 10,
        horario_json    JSON,
        servicos_json   JSON,
        barbeiros_json  JSON,
        galeria_json    JSON,
        criado_em       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        atualizado_em   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_slug   (slug),
        INDEX idx_status (status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS appointments (
        id                VARCHAR(36)  PRIMARY KEY,
        slug              VARCHAR(50)  NOT NULL,
        cliente_nome      VARCHAR(100) NOT NULL,
        cliente_telefone  VARCHAR(20)  NOT NULL,
        servico_id        VARCHAR(64)  NOT NULL,
        servico_nome      VARCHAR(100) NOT NULL,
        servico_preco     DECIMAL(10,2) NOT NULL DEFAULT 0,
        data_agend        DATE         NOT NULL,
        horario           VARCHAR(5)   NOT NULL,
        status            ENUM('confirmado','cancelado','concluido','recusado') NOT NULL DEFAULT 'confirmado',
        barbeiro_id       VARCHAR(64)  NOT NULL DEFAULT '',
        barbeiro_nome     VARCHAR(80)  NOT NULL DEFAULT '',
        criado_em         DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
        atualizado_em     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_slug_data  (slug, data_agend),
        INDEX idx_status     (status),
        INDEX idx_data       (data_agend)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
]

DEFAULT_SCHEDULE = {
    "segunda": {"aberto": True,  "inicio": "09:00", "fim": "19:00", "intervalo_inicio": "", "intervalo_fim": ""},
    "terca":   {"aberto": True,  "inicio": "09:00", "fim": "19:00", "intervalo_inicio": "", "intervalo_fim": ""},
    "quarta":  {"aberto": True,  "inicio": "09:00", "fim": "19:00", "intervalo_inicio": "", "intervalo_fim": ""},
    "quinta":  {"aberto": True,  "inicio": "09:00", "fim": "19:00", "intervalo_inicio": "", "intervalo_fim": ""},
    "sexta":   {"aberto": True,  "inicio": "09:00", "fim": "20:00", "intervalo_inicio": "", "intervalo_fim": ""},
    "sabado":  {"aberto": True,  "inicio": "08:00", "fim": "18:00", "intervalo_inicio": "", "intervalo_fim": ""},
    "domingo": {"aberto": False, "inicio": "",      "fim": "",      "intervalo_inicio": "", "intervalo_fim": ""},
}

DEFAULT_SERVICES_BY_TYPE = {
    "barbearia": [
        {"id": "s1", "nome": "Corte Masculino",  "preco": 45.0,  "duracao": 30, "descricao": "Corte clássico com acabamento impecável"},
        {"id": "s2", "nome": "Barba Completa",   "preco": 35.0,  "duracao": 20, "descricao": "Barba com navalha, toalha quente e hidratação"},
        {"id": "s3", "nome": "Corte + Barba",    "preco": 75.0,  "duracao": 45, "descricao": "Combo completo com desconto especial"},
    ],
    "outro": [
        {"id": "s1", "nome": "Serviço Padrão",   "preco": 100.0, "duracao": 60, "descricao": "Serviço personalizado"},
    ],
}

async def run_migrations():
    pool = await get_db_pool()
    if pool is None:
        return False
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                for sql in MIGRATIONS:
                    await cur.execute(sql.strip())

                # Ensure token column is wide enough
                try:
                    await cur.execute(
                        "ALTER TABLE admin_config MODIFY COLUMN token VARCHAR(200) NOT NULL DEFAULT ''"
                    )
                except Exception:
                    pass

                await cur.execute("SELECT COUNT(*) as c FROM admin_config")
                row = await cur.fetchone()
                if ADMIN_PASSWORD_ENV:
                    pw_hash = hashlib.sha256(ADMIN_PASSWORD_ENV.encode()).hexdigest()
                    if row[0] == 0:
                        await cur.execute(
                            "INSERT INTO admin_config (username, password) VALUES (%s, %s)",
                            (ADMIN_USERNAME_ENV, pw_hash)
                        )
                        logger.info(f"Admin '{ADMIN_USERNAME_ENV}' created")
                    else:
                        await cur.execute("SELECT password FROM admin_config WHERE id=1 LIMIT 1")
                        existing = await cur.fetchone()
                        if existing and existing[0] != pw_hash:
                            await cur.execute(
                                "UPDATE admin_config SET username=%s, password=%s WHERE id=1",
                                (ADMIN_USERNAME_ENV, pw_hash)
                            )
                else:
                    if row[0] == 0:
                        fallback_hash = hashlib.sha256(str(uuid.uuid4()).encode()).hexdigest()
                        await cur.execute(
                            "INSERT INTO admin_config (username, password) VALUES (%s, %s)",
                            ("admin", fallback_hash)
                        )
                        logger.warning("ADMIN_PASSWORD not set — random password generated!")

                await cur.execute("SELECT COUNT(*) as c FROM clients WHERE slug = %s", ("barbearia-demo",))
                row = await cur.fetchone()
                if row[0] == 0:
                    await _insert_demo_client(cur)

        logger.info("✓ Migrations completed")
        return True
    except Exception as e:
        logger.error(f"Migration error: {e}")
        return False

async def _insert_demo_client(cur):
    await cur.execute(
        """INSERT INTO clients
           (id, slug, nome, telefone, endereco, descricao, business_type, template,
            horario_json, servicos_json, barbeiros_json, galeria_json)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (
            "demo-001", "barbearia-demo", "Barbearia Classic", "5511999999999",
            "Rua das Tesouras, 42 - Centro, São Paulo - SP",
            "A melhor barbearia do bairro.",
            "barbearia", "classic",
            json.dumps(DEFAULT_SCHEDULE, ensure_ascii=False),
            json.dumps(DEFAULT_SERVICES_BY_TYPE["barbearia"], ensure_ascii=False),
            json.dumps([], ensure_ascii=False),
            json.dumps([], ensure_ascii=False),
        )
    )

# ═══════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════
SLUG_RESERVED = {
    "admin", "api", "health", "login", "painel", "ngbadmin",
    "www", "app", "demo", "static", "assets", "media", "uploads",
    "docs", "favicon", "robots", "sitemap", "ws", "null", "undefined",
    "true", "false", "test", "dev", "prod", "root", "system",
}
SLUG_PATTERN  = re.compile(r"^[a-z0-9][a-z0-9\-]{1,48}[a-z0-9]$")
HEX_COLOR_RE  = re.compile(r"^#[0-9a-fA-F]{3}([0-9a-fA-F]{3})?$")

MAX_STRING_LEN  = 500
MAX_DESC_LEN    = 3000
MAX_URL_LEN     = 512
MAX_PAYLOAD_KB  = 256
MAX_SERVICES    = 30
MAX_GALLERY     = 20
MAX_BARBERS     = 20

_login_attempts: dict = {}
LOGIN_MAX_ATTEMPTS = 10
LOGIN_WINDOW_SEC   = 300

DIAS_VALIDOS  = {"segunda","terca","quarta","quinta","sexta","sabado","domingo"}
DIAS_SEMANA   = ["segunda","terca","quarta","quinta","sexta","sabado","domingo"]

VALID_BUSINESS_TYPES = {
    "barbearia","salao","estetica","manicure","odontologia",
    "tatuagem","consultorio","assistencia","oficina","petshop","outro"
}
VALID_TEMPLATES = {
    "classic","dark","barber-red","barber-chrome",
    "salon-rose","salon-blush","salon-gold","salon-lilac",
    "estetica-nude","estetica-pink","estetica-spa",
    "manicure-pastel","manicure-chic","odonto-white","odonto-mint","odonto-dark",
    "tattoo-ink","tattoo-neon","clinic-clean","clinic-pro",
    "tech-slate","tech-neon","oficina-steel","pet-fresh","pet-warm",
    "minimal","law-navy","law-dark",
}

_using_mysql = False

# ═══════════════════════════════════════════════════════
#  STANDARD RESPONSES
# ═══════════════════════════════════════════════════════
def ok(data=None, message: str = "OK", status: int = 200):
    return JSONResponse({"success": True, "message": message, "data": data}, status_code=status)

def err(message: str = "Erro", status: int = 400):
    return JSONResponse({"success": False, "message": message, "data": None}, status_code=status)

# ═══════════════════════════════════════════════════════
#  SANITIZATION
# ═══════════════════════════════════════════════════════
def sanitize_str(val, max_len: int = MAX_STRING_LEN) -> str:
    if val is None:
        return ""
    s = str(val)
    s = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", s)
    s = s.replace("\x00", "")
    return s.strip()[:max_len]

def sanitize_color(val) -> str:
    s = sanitize_str(val, 10)
    return s if s and HEX_COLOR_RE.match(s) else ""

def sanitize_business_type(val: str) -> str:
    v = sanitize_str(val, 50).lower()
    return v if v in VALID_BUSINESS_TYPES else "barbearia"

def sanitize_template(val: str) -> str:
    v = sanitize_str(val, 50).lower()
    return v if v in VALID_TEMPLATES else "classic"

def sanitize_status(val: str) -> str:
    v = sanitize_str(val, 10).lower()
    return v if v in ("ativo","inativo") else "ativo"

def sanitize_url(val) -> str:
    s = sanitize_str(val, MAX_URL_LEN)
    if not s:
        return ""
    if not re.match(r"^https?://", s):
        return ""
    blocked = re.compile(r"https?://(localhost|127\.|10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.)", re.I)
    if blocked.match(s):
        return ""
    return s

def clean_phone(raw) -> str:
    return re.sub(r"\D", "", str(raw or ""))

def format_phone_display(phone: str) -> str:
    p = clean_phone(phone)
    if p.startswith("55") and len(p) in (12, 13):
        p = p[2:]
    if len(p) == 11:
        return f"({p[:2]}) {p[2:7]}-{p[7:]}"
    if len(p) == 10:
        return f"({p[:2]}) {p[2:6]}-{p[6:]}"
    return p

def build_whatsapp_link(phone: str) -> str:
    p = clean_phone(phone)
    if not p.startswith("55") and len(p) <= 11:
        p = "55" + p
    return p

def validate_phone(phone: str) -> bool:
    p = clean_phone(phone)
    return 8 <= len(p) <= 15

# ═══════════════════════════════════════════════════════
#  SLUG HELPERS
# ═══════════════════════════════════════════════════════
def slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9\s\-]", "", text)
    text = re.sub(r"[\s\-]+", "-", text)
    text = text.strip("-")
    return text[:50] if text else "estabelecimento"

def validate_slug(slug: str) -> Optional[str]:
    if not slug:
        return "Slug é obrigatório"
    if len(slug) < 3:
        return "Slug deve ter ao menos 3 caracteres"
    if len(slug) > 50:
        return "Slug deve ter no máximo 50 caracteres"
    if not SLUG_PATTERN.match(slug):
        return "Slug inválido. Use apenas letras minúsculas, números e hifens."
    if slug in SLUG_RESERVED:
        return f"Slug '{slug}' é reservado pelo sistema"
    return None

async def is_slug_taken(slug: str, exclude_id: str = "") -> bool:
    if exclude_id:
        row = await db_fetchone(
            "SELECT id FROM clients WHERE slug=%s AND id!=%s LIMIT 1", (slug, exclude_id)
        )
    else:
        row = await db_fetchone("SELECT id FROM clients WHERE slug=%s LIMIT 1", (slug,))
    return row is not None

def build_public_link(slug: str, base_url: str = "") -> str:
    frontend = os.environ.get("FRONTEND_URL", "").rstrip("/")
    if frontend:
        return f"{frontend}/?slug={slug}"
    if base_url:
        return f"{base_url.rstrip('/')}/?slug={slug}"
    return f"/?slug={slug}"

# ═══════════════════════════════════════════════════════
#  SCHEDULE VALIDATION
# ═══════════════════════════════════════════════════════
def parse_time(t: str) -> Optional[datetime]:
    try:
        return datetime.strptime(t.strip(), "%H:%M")
    except (ValueError, AttributeError):
        return None

def validate_horario_funcionamento(hf: dict) -> Optional[str]:
    if not isinstance(hf, dict):
        return "horario_funcionamento deve ser um objeto"
    for dia, info in hf.items():
        if dia not in DIAS_VALIDOS:
            return f"Dia inválido: {dia}"
        if not isinstance(info, dict):
            return f"Configuração inválida para '{dia}'"
        aberto = info.get("aberto", False)
        if not isinstance(aberto, bool):
            return f"Campo 'aberto' de '{dia}' deve ser booleano"
        if aberto:
            inicio = str(info.get("inicio", "")).strip()
            fim    = str(info.get("fim", "")).strip()
            if not inicio or not fim:
                return f"Dia '{dia}' está aberto mas faltam os horários de início/fim"
            t_inicio = parse_time(inicio)
            t_fim    = parse_time(fim)
            if not t_inicio:
                return f"Horário de início inválido em '{dia}': {inicio}"
            if not t_fim:
                return f"Horário de fim inválido em '{dia}': {fim}"
            if t_fim <= t_inicio:
                return f"Horário de fim deve ser após o de início em '{dia}'"
            iv_ini = str(info.get("intervalo_inicio", "")).strip()
            iv_fim = str(info.get("intervalo_fim", "")).strip()
            if iv_ini or iv_fim:
                if not iv_ini or not iv_fim:
                    return f"Intervalo de '{dia}' precisa ter início e fim"
                t_iv_ini = parse_time(iv_ini)
                t_iv_fim = parse_time(iv_fim)
                if not t_iv_ini:
                    return f"Início do intervalo inválido em '{dia}': {iv_ini}"
                if not t_iv_fim:
                    return f"Fim do intervalo inválido em '{dia}': {iv_fim}"
                if t_iv_fim <= t_iv_ini:
                    return f"Fim do intervalo deve ser após o início em '{dia}'"
                if t_iv_ini < t_inicio or t_iv_fim > t_fim:
                    return f"Intervalo de '{dia}' deve estar dentro do horário de funcionamento"
    return None

def normalize_horario_funcionamento(hf: dict) -> dict:
    result = {}
    for dia in DIAS_VALIDOS:
        info = hf.get(dia, {"aberto": False, "inicio": "", "fim": ""})
        result[dia] = {
            "aberto":           bool(info.get("aberto", False)),
            "inicio":           str(info.get("inicio", "")).strip(),
            "fim":              str(info.get("fim", "")).strip(),
            "intervalo_inicio": str(info.get("intervalo_inicio", "")).strip(),
            "intervalo_fim":    str(info.get("intervalo_fim", "")).strip(),
        }
    return result

def validate_barbeiros(barbeiros: list) -> Optional[str]:
    if not isinstance(barbeiros, list):
        return "barbeiros deve ser uma lista"
    if len(barbeiros) > MAX_BARBERS:
        return f"Máximo de {MAX_BARBERS} barbeiros"
    ids = set()
    for i, b in enumerate(barbeiros):
        if not isinstance(b, dict):
            return f"Barbeiro {i+1}: formato inválido"
        nome = str(b.get("nome", "")).strip()
        if not nome:
            return f"Barbeiro {i+1}: nome é obrigatório"
        if len(nome) > 80:
            return f"Barbeiro {i+1}: nome muito longo"
        bid = str(b.get("id", "")).strip()
        if not bid:
            return f"Barbeiro '{nome}': id é obrigatório"
        if bid in ids:
            return f"ID de barbeiro duplicado: {bid}"
        ids.add(bid)
        status = str(b.get("status", "ativo")).strip()
        if status not in ("ativo", "inativo"):
            return f"Barbeiro '{nome}': status deve ser 'ativo' ou 'inativo'"
    return None

def normalize_barbeiro(b: dict) -> dict:
    return {
        "id":          str(b.get("id", str(uuid.uuid4()))).strip(),
        "nome":        sanitize_str(b.get("nome", ""), 80),
        "status":      b.get("status", "ativo") if b.get("status") in ("ativo","inativo") else "ativo",
        "servicos_ids": b.get("servicos_ids", []) if isinstance(b.get("servicos_ids"), list) else [],
        "foto":        sanitize_url(b.get("foto", "")),
        "bio":         sanitize_str(b.get("bio", ""), 500),
    }

def validate_servicos(servicos: list) -> Optional[str]:
    if not isinstance(servicos, list):
        return "servicos deve ser uma lista"
    if len(servicos) > MAX_SERVICES:
        return f"Máximo de {MAX_SERVICES} serviços"
    ids = set()
    for i, s in enumerate(servicos):
        if not isinstance(s, dict):
            return f"Serviço {i+1}: formato inválido"
        nome = str(s.get("nome", "")).strip()
        if not nome:
            return f"Serviço {i+1}: nome é obrigatório"
        if len(nome) > 100:
            return f"Serviço {i+1}: nome muito longo"
        try:
            preco = float(s.get("preco", -1))
            if preco < 0:
                return f"Serviço '{nome}': preço inválido"
            if preco > 99999:
                return f"Serviço '{nome}': preço acima do limite"
        except (TypeError, ValueError):
            return f"Serviço '{nome}': preço inválido"
        try:
            dur = int(s.get("duracao", 0))
            if dur < 5:
                return f"Serviço '{nome}': duração mínima é 5 minutos"
            if dur > 480:
                return f"Serviço '{nome}': duração máxima é 480 minutos"
        except (TypeError, ValueError):
            return f"Serviço '{nome}': duração inválida"
        sid = str(s.get("id", "")).strip()
        if not sid:
            return f"Serviço '{nome}': id é obrigatório"
        if len(sid) > 64:
            return f"Serviço '{nome}': id muito longo"
        if sid in ids:
            return f"ID de serviço duplicado: {sid}"
        ids.add(sid)
    return None

# ═══════════════════════════════════════════════════════
#  REQUEST HELPERS
# ═══════════════════════════════════════════════════════
async def read_json_body(request: Request) -> Optional[dict]:
    try:
        body_bytes = await request.body()
        if len(body_bytes) > MAX_PAYLOAD_KB * 1024:
            return None
        if not body_bytes:
            return None
        data = json.loads(body_bytes)
        if not isinstance(data, dict):
            return None
        return data
    except Exception:
        return None

def get_client_ip(request: Request) -> str:
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        ip = xff.split(",")[0].strip()
        if re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", ip):
            return ip
    return request.client.host if request.client else "unknown"

def check_login_rate(ip: str) -> bool:
    now = time.time()
    attempts = [t for t in _login_attempts.get(ip, []) if now - t < LOGIN_WINDOW_SEC]
    _login_attempts[ip] = attempts
    return len(attempts) < LOGIN_MAX_ATTEMPTS

def record_login_attempt(ip: str):
    _login_attempts.setdefault(ip, []).append(time.time())

def validate_date_str(date_str: str) -> bool:
    if not date_str or not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        return False
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        if dt.year < 2020 or dt.year > datetime.now().year + 2:
            return False
        return True
    except ValueError:
        return False

# ═══════════════════════════════════════════════════════
#  SLOTS
# ═══════════════════════════════════════════════════════
def get_dia_semana(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return DIAS_SEMANA[dt.weekday()]

def generate_slots(inicio_str: str, fim_str: str, duracao_min: int = 30,
                   intervalo_ini: str = "", intervalo_fim: str = "") -> list:
    slots = []
    t   = parse_time(inicio_str)
    fim = parse_time(fim_str)
    if not t or not fim or duracao_min <= 0:
        return slots
    t_iv_ini = parse_time(intervalo_ini) if intervalo_ini else None
    t_iv_fim = parse_time(intervalo_fim) if intervalo_fim else None
    while t < fim:
        if t_iv_ini and t_iv_fim and t_iv_ini <= t < t_iv_fim:
            t += timedelta(minutes=duracao_min)
            continue
        t_end = t + timedelta(minutes=duracao_min)
        if t_end > fim:
            break
        if t_iv_ini and t_iv_fim and t < t_iv_fim and t_end > t_iv_ini:
            t += timedelta(minutes=duracao_min)
            continue
        slots.append(t.strftime("%H:%M"))
        t += timedelta(minutes=duracao_min)
    return slots

async def get_booked_slots(slug: str, date_str: str) -> list:
    rows = await db_fetchall(
        """SELECT horario FROM appointments
           WHERE slug=%s AND data_agend=%s AND status NOT IN ('cancelado','recusado')""",
        (slug, date_str)
    )
    return [r["horario"] for r in rows]

async def compute_slots_payload(client: dict, date_str: str, service_id: str = "") -> dict:
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return {"slots": [], "booked": [], "aberto": False, "dia": "", "erro": "Data inválida"}

    dia      = get_dia_semana(date_str)
    dia_info = client.get("horario_funcionamento", {}).get(dia, {})

    if not dia_info.get("aberto", False):
        return {"slots": [], "booked": [], "aberto": False, "dia": dia}

    slot_interval = max(5, int(client.get("slot_interval", 0) or 0))
    duracao = slot_interval if slot_interval > 0 else 30
    if service_id:
        svc = next((s for s in client.get("servicos", []) if s.get("id") == service_id), None)
        if svc:
            svc_dur = max(5, int(svc.get("duracao", 30)))
            duracao = slot_interval if slot_interval > 0 else svc_dur

    iv_ini = dia_info.get("intervalo_inicio", "")
    iv_fim = dia_info.get("intervalo_fim", "")

    all_slots = generate_slots(dia_info["inicio"], dia_info["fim"], duracao, iv_ini, iv_fim)
    booked    = await get_booked_slots(client["slug"], date_str)

    max_per = max(1, int(client.get("max_per_slot", 1) or 1))
    booked_count = Counter(booked)
    available = [s for s in all_slots if booked_count.get(s, 0) < max_per]

    return {
        "slots":  available,
        "booked": booked,
        "aberto": True,
        "dia":    dia,
        "total":  len(all_slots),
        "livres": len(available),
    }

def validate_horario_in_slots_sync(client: dict, date_str: str, service_id: str, horario: str, booked: list) -> bool:
    dia      = get_dia_semana(date_str)
    dia_info = client.get("horario_funcionamento", {}).get(dia, {})
    if not dia_info.get("aberto"):
        return False
    slot_interval = max(5, int(client.get("slot_interval", 0) or 0))
    duracao = slot_interval if slot_interval > 0 else 30
    svc = next((s for s in client.get("servicos", []) if s.get("id") == service_id), None)
    if svc:
        svc_dur = max(5, int(svc.get("duracao", 30)))
        duracao = slot_interval if slot_interval > 0 else svc_dur
    iv_ini = dia_info.get("intervalo_inicio", "")
    iv_fim = dia_info.get("intervalo_fim", "")
    all_slots = generate_slots(dia_info["inicio"], dia_info["fim"], duracao, iv_ini, iv_fim)
    return horario in all_slots

# ═══════════════════════════════════════════════════════
#  WHATSAPP
# ═══════════════════════════════════════════════════════
def build_whatsapp_message(appointment: dict, client: dict) -> str:
    try:
        data_fmt = datetime.strptime(appointment.get("data", ""), "%Y-%m-%d").strftime("%d/%m/%Y")
    except Exception:
        data_fmt = appointment.get("data", "")

    phone_fmt   = format_phone_display(appointment.get("cliente_telefone", ""))
    confirm_msg = (client.get("confirm_msg") or "").strip()

    if confirm_msg:
        msg = confirm_msg
        msg = msg.replace("{nome}",         appointment.get("cliente_nome", ""))
        msg = msg.replace("{servico}",      appointment.get("servico_nome", ""))
        msg = msg.replace("{data}",         data_fmt)
        msg = msg.replace("{horario}",      appointment.get("horario", ""))
        msg = msg.replace("{profissional}", appointment.get("barbeiro_nome", ""))
        msg = msg.replace("{local}",        client.get("endereco", ""))
        return msg

    linhas = [
        f"*NOVO AGENDAMENTO — {client.get('nome', '')}*",
        "",
        f"*Cliente:* {appointment.get('cliente_nome', '')}",
        f"*Telefone:* {phone_fmt}",
        f"*Serviço:* {appointment.get('servico_nome', '')}",
        f"*Valor:* R$ {float(appointment.get('servico_preco', 0)):.2f}",
        f"*Data:* {data_fmt}",
        f"*Horário:* {appointment.get('horario', '')}",
    ]
    if appointment.get("barbeiro_nome"):
        linhas.append(f"*Profissional:* {appointment['barbeiro_nome']}")
    if client.get("endereco"):
        linhas += ["", f"*Local:* {client['endereco']}"]
    linhas += ["", "_Agendamento feito pelo site_"]
    return "\n".join(linhas)

def build_whatsapp_url(phone: str, message: str) -> str:
    clean = build_whatsapp_link(phone)
    return f"https://wa.me/{clean}?text={urllib.parse.quote(message)}"

# ═══════════════════════════════════════════════════════
#  DATA LAYER — CLIENTS
# ═══════════════════════════════════════════════════════
def _row_to_client(row: dict) -> dict:
    def _parse(val, default=None):
        if default is None:
            default = []
        if val is None:
            return default
        if isinstance(val, (list, dict)):
            return val
        try:
            return json.loads(val)
        except Exception:
            return default

    return {
        "id":                  row["id"],
        "slug":                row["slug"],
        "nome":                row["nome"],
        "telefone_whatsapp":   row.get("telefone") or row.get("telefone_whatsapp", ""),
        "endereco":            row.get("endereco") or "",
        "descricao":           row.get("descricao") or "",
        "status":              row.get("status", "ativo"),
        "business_type":       row.get("business_type", "barbearia"),
        "template":            row.get("template", "classic"),
        "cor_primaria":        row.get("cor_primaria", "#1a1a2e"),
        "cor_secundaria":      row.get("cor_secundaria", "#c9a84c"),
        "logo":                row.get("logo") or "",
        "banner":              row.get("banner") or "",
        "meta_title":          row.get("meta_title") or "",
        "meta_desc":           row.get("meta_desc") or "",
        "instagram":           row.get("instagram") or "",
        "slot_interval":       int(row.get("slot_interval", 30) or 30),
        "booking_days":        int(row.get("booking_days", 30) or 30),
        "max_per_slot":        int(row.get("max_per_slot", 1) or 1),
        "confirm_msg":         row.get("confirm_msg") or "",
        "hero_tag":            row.get("hero_tag") or "",
        "sobre_titulo":        row.get("sobre_titulo") or "",
        "cta_text":            row.get("cta_text") or "",
        "team_label":          row.get("team_label") or "",
        "anos_exp":            int(row.get("anos_exp", 10) or 10),
        "horario_funcionamento": _parse(row.get("horario_json") or row.get("horario_funcionamento"), {}),
        "servicos":            _parse(row.get("servicos_json") or row.get("servicos"), []),
        "barbeiros":           _parse(row.get("barbeiros_json") or row.get("barbeiros"), []),
        "galeria":             _parse(row.get("galeria_json") or row.get("galeria"), []),
        "criado_em":           str(row["criado_em"]) if row.get("criado_em") else "",
        "atualizado_em":       str(row["atualizado_em"]) if row.get("atualizado_em") else "",
    }

async def db_list_clients() -> list:
    rows = await db_fetchall("SELECT * FROM clients ORDER BY criado_em DESC")
    return [_row_to_client(r) for r in rows]

async def db_get_client_by_slug(slug: str, only_active: bool = False) -> Optional[dict]:
    sql = "SELECT * FROM clients WHERE slug=%s"
    args = [slug]
    if only_active:
        sql += " AND status='ativo'"
    row = await db_fetchone(sql + " LIMIT 1", args)
    return _row_to_client(row) if row else None

async def db_get_client_by_id(client_id: str) -> Optional[dict]:
    row = await db_fetchone("SELECT * FROM clients WHERE id=%s LIMIT 1", (client_id,))
    return _row_to_client(row) if row else None

_CLIENT_COLS = """
    slug, nome, telefone, endereco, descricao, status, business_type, template,
    cor_primaria, cor_secundaria, logo, banner, meta_title, meta_desc, instagram,
    slot_interval, booking_days, max_per_slot, confirm_msg, hero_tag, sobre_titulo,
    cta_text, team_label, anos_exp, horario_json, servicos_json, barbeiros_json, galeria_json
"""

async def db_create_client(c: dict) -> dict:
    cid = str(uuid.uuid4())
    await db_execute(
        f"""INSERT INTO clients (id, {_CLIENT_COLS})
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (
            cid,
            c["slug"], c["nome"], c["telefone_whatsapp"], c.get("endereco",""), c.get("descricao",""),
            c.get("status","ativo"), c.get("business_type","barbearia"), c.get("template","classic"),
            c.get("cor_primaria","#1a1a2e"), c.get("cor_secundaria","#c9a84c"),
            c.get("logo",""), c.get("banner",""),
            c.get("meta_title",""), c.get("meta_desc",""), c.get("instagram",""),
            int(c.get("slot_interval",30)), int(c.get("booking_days",30)), int(c.get("max_per_slot",1)),
            c.get("confirm_msg",""), c.get("hero_tag",""), c.get("sobre_titulo",""),
            c.get("cta_text",""), c.get("team_label",""), int(c.get("anos_exp",10)),
            json.dumps(c.get("horario_funcionamento",{}), ensure_ascii=False),
            json.dumps(c.get("servicos",[]), ensure_ascii=False),
            json.dumps(c.get("barbeiros",[]), ensure_ascii=False),
            json.dumps(c.get("galeria",[]), ensure_ascii=False),
        )
    )
    return await db_get_client_by_id(cid)

async def db_update_client(client_id: str, updates: dict) -> Optional[dict]:
    fields = []
    args   = []
    simple_map = {
        "nome": "nome", "telefone_whatsapp": "telefone", "endereco": "endereco",
        "descricao": "descricao", "status": "status", "business_type": "business_type",
        "template": "template", "cor_primaria": "cor_primaria", "cor_secundaria": "cor_secundaria",
        "logo": "logo", "banner": "banner", "meta_title": "meta_title", "meta_desc": "meta_desc",
        "instagram": "instagram", "slot_interval": "slot_interval", "booking_days": "booking_days",
        "max_per_slot": "max_per_slot", "confirm_msg": "confirm_msg", "hero_tag": "hero_tag",
        "sobre_titulo": "sobre_titulo", "cta_text": "cta_text", "team_label": "team_label",
        "anos_exp": "anos_exp",
    }
    json_map = {
        "horario_funcionamento": "horario_json",
        "servicos": "servicos_json",
        "barbeiros": "barbeiros_json",
        "galeria": "galeria_json",
    }
    for k, col in simple_map.items():
        if k in updates:
            fields.append(f"{col}=%s")
            args.append(updates[k])
    for k, col in json_map.items():
        if k in updates:
            fields.append(f"{col}=%s")
            args.append(json.dumps(updates[k], ensure_ascii=False))
    if not fields:
        return await db_get_client_by_id(client_id)
    args.append(client_id)
    await db_execute(f"UPDATE clients SET {', '.join(fields)} WHERE id=%s", args)
    return await db_get_client_by_id(client_id)

async def db_delete_client(client_id: str) -> bool:
    rows = await db_execute("DELETE FROM clients WHERE id=%s", (client_id,))
    return rows > 0

# ═══════════════════════════════════════════════════════
#  DATA LAYER — APPOINTMENTS
# ═══════════════════════════════════════════════════════
def _row_to_appt(row: dict) -> dict:
    return {
        "id":               row["id"],
        "slug":             row["slug"],
        "cliente_nome":     row["cliente_nome"],
        "cliente_telefone": row["cliente_telefone"],
        "servico_id":       row["servico_id"],
        "servico_nome":     row["servico_nome"],
        "servico_preco":    float(row.get("servico_preco", 0)),
        "data":             str(row["data_agend"]) if row.get("data_agend") else "",
        "horario":          row["horario"],
        "status":           row.get("status", "confirmado"),
        "barbeiro_id":      row.get("barbeiro_id", ""),
        "barbeiro_nome":    row.get("barbeiro_nome", ""),
        "criado_em":        str(row["criado_em"]) if row.get("criado_em") else "",
        "atualizado_em":    str(row["atualizado_em"]) if row.get("atualizado_em") else "",
    }

async def db_list_appointments(status_filter="", slug_filter="", date_filter="") -> list:
    sql  = "SELECT * FROM appointments WHERE 1=1"
    args = []
    if status_filter:
        sql += " AND status=%s"; args.append(status_filter)
    if slug_filter:
        sql += " AND slug=%s"; args.append(slug_filter)
    if date_filter and validate_date_str(date_filter):
        sql += " AND data_agend=%s"; args.append(date_filter)
    sql += " ORDER BY data_agend DESC, horario DESC"
    rows = await db_fetchall(sql, args)
    return [_row_to_appt(r) for r in rows]

async def db_create_appointment(appt: dict) -> dict:
    await db_execute(
        """INSERT INTO appointments
           (id, slug, cliente_nome, cliente_telefone, servico_id, servico_nome,
            servico_preco, data_agend, horario, status, barbeiro_id, barbeiro_nome)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (
            appt["id"], appt["slug"], appt["cliente_nome"], appt["cliente_telefone"],
            appt["servico_id"], appt["servico_nome"], appt["servico_preco"],
            appt["data"], appt["horario"], appt.get("status","confirmado"),
            appt.get("barbeiro_id",""), appt.get("barbeiro_nome",""),
        )
    )
    return appt

async def db_update_appointment_status(appt_id: str, new_status: str) -> Optional[dict]:
    rows = await db_execute(
        "UPDATE appointments SET status=%s WHERE id=%s", (new_status, appt_id)
    )
    if rows == 0:
        return None
    row = await db_fetchone("SELECT * FROM appointments WHERE id=%s", (appt_id,))
    return _row_to_appt(row) if row else None

# ═══════════════════════════════════════════════════════
#  DATA LAYER — ADMIN
# ═══════════════════════════════════════════════════════
async def db_get_admin() -> Optional[dict]:
    row = await db_fetchone("SELECT * FROM admin_config LIMIT 1")
    if not row:
        return None
    return {
        "username":        row["username"],
        "password":        row["password"],
        "token":           row["token"] or "",
        "token_criado_em": str(row["token_at"]) if row.get("token_at") else "",
    }

async def db_save_admin(admin: dict) -> bool:
    token_at = None
    if admin.get("token_criado_em"):
        try:
            token_at = datetime.fromisoformat(admin["token_criado_em"])
        except Exception:
            pass
    token_val = admin.get("token", "")
    tokens_list = [t.strip() for t in token_val.split(",") if t.strip()]
    if len(",".join(tokens_list)) > 190:
        tokens_list = tokens_list[-3:]
    token_val = ",".join(tokens_list)
    rows = await db_execute(
        "UPDATE admin_config SET username=%s, password=%s, token=%s, token_at=%s WHERE id=1",
        (admin.get("username","admin"), admin["password"], token_val, token_at)
    )
    return rows > 0

# ═══════════════════════════════════════════════════════
#  AUTH
# ═══════════════════════════════════════════════════════
async def require_admin(request: Request):
    auth  = request.headers.get("Authorization", "")
    token = auth.replace("Bearer ", "").strip()
    if not token or len(token) > 36 or not re.match(r"^[0-9a-f\-]{32,36}$", token, re.IGNORECASE):
        raise HTTPException(status_code=401, detail="Não autorizado")
    admin = await db_get_admin()
    if not admin:
        raise HTTPException(status_code=401, detail="Não autorizado")
    valid_tokens = [t.strip() for t in admin.get("token", "").split(",") if t.strip()]
    if not valid_tokens or token not in valid_tokens:
        raise HTTPException(status_code=401, detail="Não autorizado")
    return token

# ═══════════════════════════════════════════════════════
#  PUBLIC ROUTES
# ═══════════════════════════════════════════════════════
@app.get("/api/config")
async def get_config():
    frontend = os.environ.get("FRONTEND_URL", "").rstrip("/")
    backend  = os.environ.get("BACKEND_URL", "").rstrip("/")
    return JSONResponse({
        "frontend_url": frontend,
        "backend_url":  backend,
        "version":      "3.2.0",
    })

@app.get("/")
async def root():
    return JSONResponse({
        "status":  "NGB SaaS API v3.2",
        "storage": "MySQL" if _using_mysql else "unavailable",
        "docs":    "/api/docs",
        "health":  "/api/health",
        "config":  "/api/config",
    })

@app.get("/api/health")
async def health():
    db_ok = False
    try:
        if _using_mysql:
            r = await db_fetchone("SELECT 1 AS ok")
            db_ok = r is not None
    except Exception:
        db_ok = False
    return ok({
        "status":  "online",
        "version": "3.2.0",
        "storage": "mysql" if _using_mysql else "unavailable",
        "ts":      datetime.now().isoformat(),
        "db_ok":   db_ok,
    })

@app.get("/api/site/{slug}")
async def get_site(slug: str):
    slug = re.sub(r"[^a-z0-9\-]", "", slug.strip().lower())[:60]
    if not slug or len(slug) < 3:
        return err("Cadastro não encontrado", 404)
    client = await db_get_client_by_slug(slug, only_active=True)
    if not client:
        return err("Cadastro não encontrado", 404)
    return ok(client)

@app.get("/api/slots/{slug}")
async def get_slots(slug: str, request: Request):
    slug       = slug.strip().lower()
    date_str   = request.query_params.get("date", "").strip()
    service_id = request.query_params.get("service_id", "").strip()[:64]

    if not date_str:
        return err("Parâmetro 'date' obrigatório")
    if not validate_date_str(date_str):
        return err("Formato de data inválido. Use YYYY-MM-DD")
    if len(slug) > 60:
        return err("Cadastro não encontrado", 404)

    client = await db_get_client_by_slug(slug, only_active=True)
    if not client:
        return err("Cadastro não encontrado", 404)

    booking_days = max(1, int(client.get("booking_days", 30) or 30))
    try:
        req_dt = datetime.strptime(date_str, "%Y-%m-%d").date()
        max_dt = datetime.now().date() + timedelta(days=booking_days)
        if req_dt > max_dt:
            return err(f"Agendamento disponível apenas para os próximos {booking_days} dias")
    except Exception:
        pass

    payload = await compute_slots_payload(client, date_str, service_id)
    return ok(payload)

@app.post("/api/appointments")
async def create_appointment(request: Request):
    body = await read_json_body(request)
    if body is None:
        return err("Body inválido ou muito grande")

    required = ["slug", "cliente_nome", "cliente_telefone", "servico_id", "data", "horario"]
    for field in required:
        val = body.get(field)
        if val is None or (isinstance(val, str) and not str(val).strip()):
            return err(f"Campo obrigatório: {field}")

    slug = sanitize_str(body["slug"]).lower()[:60]
    if not slug:
        return err("Slug inválido")

    client = await db_get_client_by_slug(slug, only_active=True)
    if not client:
        return err("Cadastro não encontrado", 404)

    date_str = sanitize_str(body["data"])
    if not validate_date_str(date_str):
        return err("Data inválida. Use YYYY-MM-DD")

    service_id = sanitize_str(body["servico_id"])[:64]
    servico    = next((s for s in client.get("servicos", []) if s.get("id") == service_id), None)
    if not servico:
        return err("Serviço não encontrado")

    horario = sanitize_str(body["horario"])[:5]
    if not re.match(r"^\d{2}:\d{2}$", horario):
        return err("Horário inválido")

    booking_days = max(1, int(client.get("booking_days", 30) or 30))
    try:
        req_dt = datetime.strptime(date_str, "%Y-%m-%d").date()
        today  = datetime.now().date()
        if req_dt < today:
            return err("Não é possível agendar em datas passadas")
        max_dt = today + timedelta(days=booking_days)
        if req_dt > max_dt:
            return err(f"Agendamento disponível apenas para os próximos {booking_days} dias")
    except Exception:
        pass

    booked = await get_booked_slots(slug, date_str)
    if not validate_horario_in_slots_sync(client, date_str, service_id, horario, booked):
        return err("Horário inválido para este serviço/dia")

    max_per = max(1, int(client.get("max_per_slot", 1) or 1))
    booked_count = Counter(booked)
    if booked_count.get(horario, 0) >= max_per:
        return err("Horário já ocupado. Escolha outro.")

    dia      = get_dia_semana(date_str)
    dia_info = client.get("horario_funcionamento", {}).get(dia, {})
    if not dia_info.get("aberto", False):
        return err("Estabelecimento fechado nesse dia")

    barbeiro_id   = sanitize_str(body.get("barbeiro_id", ""))[:64]
    barbeiro_nome = ""
    if barbeiro_id:
        barb = next(
            (b for b in client.get("barbeiros", [])
             if b.get("id") == barbeiro_id and b.get("status") == "ativo"),
            None
        )
        if not barb:
            return err("Barbeiro não encontrado ou inativo")
        barbeiro_nome = barb.get("nome", "")

    cliente_nome = sanitize_str(body["cliente_nome"])
    if not cliente_nome or len(cliente_nome) < 2:
        return err("Nome do cliente inválido")
    cliente_tel = clean_phone(body["cliente_telefone"])
    if not validate_phone(cliente_tel):
        return err("Telefone inválido")

    appointment = {
        "id":               str(uuid.uuid4()),
        "slug":             slug,
        "cliente_nome":     cliente_nome[:100],
        "cliente_telefone": cliente_tel,
        "servico_id":       service_id,
        "servico_nome":     servico["nome"],
        "servico_preco":    round(float(servico["preco"]), 2),
        "data":             date_str,
        "horario":          horario,
        "status":           "confirmado",
        "barbeiro_id":      barbeiro_id,
        "barbeiro_nome":    barbeiro_nome,
        "criado_em":        datetime.now().isoformat(),
    }

    # Race condition check
    booked_now = await get_booked_slots(slug, date_str)
    if Counter(booked_now).get(horario, 0) >= max_per:
        return err("Horário já ocupado. Escolha outro.")

    await db_create_appointment(appointment)
    logger.info(f"[APPT] New booking: {cliente_nome} → {slug} on {date_str} {horario}")

    wpp_msg = build_whatsapp_message(appointment, client)
    wpp_url = build_whatsapp_url(client.get("telefone_whatsapp", ""), wpp_msg)

    return ok({
        "appointment":  appointment,
        "whatsapp_url": wpp_url,
    }, "Agendamento realizado com sucesso!")

# ═══════════════════════════════════════════════════════
#  ADMIN — AUTH
# ═══════════════════════════════════════════════════════
@app.post("/api/admin/login")
async def admin_login(request: Request):
    ip = get_client_ip(request)
    if not check_login_rate(ip):
        logger.warning(f"[AUTH] Rate limit for IP: {ip}")
        return err("Muitas tentativas. Aguarde 5 minutos.", 429)

    body = await read_json_body(request)
    if body is None:
        return err("Body inválido")

    username = sanitize_str(body.get("username", ""))[:80]
    password = str(body.get("password", ""))[:256]

    if not username or not password:
        return err("Usuário e senha são obrigatórios")

    admin = await db_get_admin()
    if not admin:
        return err("Erro interno", 500)

    record_login_attempt(ip)

    if username != admin.get("username") or \
       hashlib.sha256(password.encode()).hexdigest() != admin["password"]:
        logger.warning(f"[AUTH] Login failed for '{username}' from {ip}")
        return err("Credenciais inválidas", 401)

    token = str(uuid.uuid4())
    existing = [t.strip() for t in admin.get("token", "").split(",") if t.strip()]
    existing.append(token)
    existing = existing[-5:]
    admin["token"]           = ",".join(existing)
    admin["token_criado_em"] = datetime.now().isoformat()
    if not await db_save_admin(admin):
        return err("Erro interno", 500)

    logger.info(f"[AUTH] Login successful: '{username}' from {ip}")
    return ok({"token": token}, "Login realizado com sucesso!")

@app.post("/api/admin/change-password")
async def admin_change_password(request: Request, _: str = Depends(require_admin)):
    body = await read_json_body(request)
    if body is None:
        return err("Body inválido")

    senha_atual = str(body.get("senha_atual", ""))
    nova_senha  = str(body.get("nova_senha", ""))

    if not senha_atual or not nova_senha:
        return err("Campos obrigatórios: senha_atual, nova_senha")
    if len(nova_senha) < 6:
        return err("Nova senha deve ter pelo menos 6 caracteres")
    if len(nova_senha) > 256:
        return err("Senha muito longa")

    admin = await db_get_admin()
    if not admin:
        return err("Erro interno", 500)

    if hashlib.sha256(senha_atual.encode()).hexdigest() != admin["password"]:
        return err("Senha atual incorreta", 401)

    admin["password"]        = hashlib.sha256(nova_senha.encode()).hexdigest()
    admin["token"]           = ""
    admin["token_criado_em"] = ""
    if not await db_save_admin(admin):
        return err("Erro ao salvar.", 500)

    return ok(None, "Senha alterada com sucesso. Faça login novamente.")

# ═══════════════════════════════════════════════════════
#  ADMIN — CLIENTS
# ═══════════════════════════════════════════════════════
@app.get("/api/admin/clients")
async def admin_list_clients(request: Request, _: str = Depends(require_admin)):
    clients = await db_list_clients()
    base = str(request.base_url).rstrip("/")
    for c in clients:
        c["link_publico"] = build_public_link(c["slug"], base)
    return ok(clients)

@app.get("/api/admin/clients/{client_id}")
async def admin_get_client(client_id: str, request: Request, _: str = Depends(require_admin)):
    if len(client_id) > 36:
        return err("ID inválido")
    client = await db_get_client_by_id(client_id)
    if not client:
        return err("Cadastro não encontrado", 404)
    base = str(request.base_url).rstrip("/")
    client["link_publico"] = build_public_link(client["slug"], base)
    return ok(client)

@app.post("/api/admin/clients")
async def admin_create_client(request: Request, _: str = Depends(require_admin)):
    body = await read_json_body(request)
    if body is None:
        return err("Body inválido ou muito grande")

    nome = sanitize_str(body.get("nome", ""))
    if not nome:
        return err("Nome é obrigatório")
    if len(nome) > 200:
        return err("Nome muito longo")

    slug = sanitize_str(body.get("slug", "")).lower()
    if not slug:
        slug = slugify(nome)
    slug_err = validate_slug(slug)
    if slug_err:
        return err(slug_err)
    if await is_slug_taken(slug):
        return err(f"Slug '{slug}' já está em uso")

    telefone = clean_phone(body.get("telefone_whatsapp", ""))
    if not validate_phone(telefone):
        return err("Telefone/WhatsApp inválido")

    biz_type = sanitize_business_type(body.get("business_type", "barbearia"))

    hf_raw = body.get("horario_funcionamento") or {}
    if not isinstance(hf_raw, dict):
        return err("horario_funcionamento deve ser um objeto")
    if not hf_raw:
        hf_raw = DEFAULT_SCHEDULE
    hf_err = validate_horario_funcionamento(hf_raw)
    if hf_err:
        return err(hf_err)

    raw_svcs = body.get("servicos") or []
    if not raw_svcs:
        raw_svcs = DEFAULT_SERVICES_BY_TYPE.get(biz_type, DEFAULT_SERVICES_BY_TYPE["outro"])
    svc_err = validate_servicos(raw_svcs)
    if svc_err:
        return err(svc_err)

    barb_err = validate_barbeiros(body.get("barbeiros", []))
    if barb_err:
        return err(barb_err)

    galeria = [sanitize_url(u) for u in (body.get("galeria") or []) if sanitize_url(u)]
    if len(galeria) > MAX_GALLERY:
        return err(f"Máximo de {MAX_GALLERY} imagens na galeria")

    tpl_raw = body.get("template", "")
    tpl = sanitize_template(tpl_raw) if tpl_raw else "classic"

    client_data = {
        "slug":                slug,
        "nome":                nome,
        "telefone_whatsapp":   telefone,
        "endereco":            sanitize_str(body.get("endereco", ""), 500),
        "descricao":           sanitize_str(body.get("descricao", ""), MAX_DESC_LEN),
        "status":              sanitize_status(body.get("status","ativo")),
        "business_type":       biz_type,
        "template":            tpl,
        "cor_primaria":        sanitize_color(body.get("cor_primaria","#1a1a2e")) or "#1a1a2e",
        "cor_secundaria":      sanitize_color(body.get("cor_secundaria","#c9a84c")) or "#c9a84c",
        "logo":                sanitize_url(body.get("logo","")),
        "banner":              sanitize_url(body.get("banner","")),
        "meta_title":          sanitize_str(body.get("meta_title",""), 200),
        "meta_desc":           sanitize_str(body.get("meta_desc",""), 500),
        "instagram":           sanitize_str(body.get("instagram",""), 100),
        "slot_interval":       max(5, min(int(body.get("slot_interval",30) or 30), 480)),
        "booking_days":        max(1, min(int(body.get("booking_days",30) or 30), 365)),
        "max_per_slot":        max(1, min(int(body.get("max_per_slot",1) or 1), 50)),
        "confirm_msg":         sanitize_str(body.get("confirm_msg",""), 1000),
        "hero_tag":            sanitize_str(body.get("hero_tag",""), 100),
        "sobre_titulo":        sanitize_str(body.get("sobre_titulo",""), 200),
        "cta_text":            sanitize_str(body.get("cta_text",""), 200),
        "team_label":          sanitize_str(body.get("team_label",""), 100),
        "anos_exp":            max(0, int(body.get("anos_exp",10) or 10)),
        "horario_funcionamento": normalize_horario_funcionamento(hf_raw),
        "servicos":            raw_svcs,
        "barbeiros":           [normalize_barbeiro(b) for b in body.get("barbeiros", [])],
        "galeria":             galeria,
    }

    created = await db_create_client(client_data)
    logger.info(f"[CLIENT] Created: {nome} / slug={slug}")
    base = str(request.base_url).rstrip("/")
    created["link_publico"] = build_public_link(created["slug"], base)
    return ok(created, "Cadastro criado com sucesso!", 201)

@app.put("/api/admin/clients/{client_id}")
async def admin_update_client(client_id: str, request: Request, _: str = Depends(require_admin)):
    if len(client_id) > 36:
        return err("ID inválido")

    client = await db_get_client_by_id(client_id)
    if not client:
        return err("Cadastro não encontrado", 404)

    body = await read_json_body(request)
    if body is None:
        return err("Body inválido")

    updates = {}

    if "nome" in body:
        nome = sanitize_str(body["nome"])
        if not nome or len(nome) > 200:
            return err("Nome inválido")
        updates["nome"] = nome

    if "slug" in body:
        new_slug = sanitize_str(body["slug"]).lower()
        slug_err = validate_slug(new_slug)
        if slug_err:
            return err(slug_err)
        if await is_slug_taken(new_slug, exclude_id=client_id):
            return err(f"Slug '{new_slug}' já está em uso")
        updates["slug"] = new_slug

    if "telefone_whatsapp" in body:
        tel = clean_phone(body["telefone_whatsapp"])
        if not validate_phone(tel):
            return err("Telefone inválido")
        updates["telefone_whatsapp"] = tel

    for field in ["endereco","descricao","meta_title","meta_desc","instagram",
                  "confirm_msg","hero_tag","sobre_titulo","cta_text","team_label"]:
        if field in body:
            updates[field] = sanitize_str(body[field], MAX_DESC_LEN if field=="descricao" else MAX_STRING_LEN)
    for field in ["cor_primaria","cor_secundaria"]:
        if field in body:
            col = sanitize_color(body[field])
            if col: updates[field] = col

    for field in ["logo","banner"]:
        if field in body:
            updates[field] = sanitize_url(body[field])

    if "status" in body:
        updates["status"] = sanitize_status(body["status"])
    if "business_type" in body:
        updates["business_type"] = sanitize_business_type(body["business_type"])
    if "template" in body:
        updates["template"] = sanitize_template(body["template"])

    for field in ["slot_interval","booking_days","max_per_slot","anos_exp"]:
        if field in body:
            try:
                v = int(body[field])
                if field == "slot_interval":
                    updates[field] = max(5, min(v, 480))
                elif field == "booking_days":
                    updates[field] = max(1, min(v, 365))
                elif field == "max_per_slot":
                    updates[field] = max(1, min(v, 50))
                elif field == "anos_exp":
                    updates[field] = max(0, min(v, 100))
            except (TypeError, ValueError):
                pass

    if "galeria" in body:
        galeria = [sanitize_url(u) for u in (body["galeria"] or []) if sanitize_url(u)]
        if len(galeria) > MAX_GALLERY:
            return err(f"Máximo de {MAX_GALLERY} imagens")
        updates["galeria"] = galeria

    if "horario_funcionamento" in body:
        hf_raw = body["horario_funcionamento"]
        if not isinstance(hf_raw, dict):
            return err("horario_funcionamento deve ser um objeto")
        hf_err = validate_horario_funcionamento(hf_raw)
        if hf_err:
            return err(hf_err)
        updates["horario_funcionamento"] = normalize_horario_funcionamento(hf_raw)

    if "servicos" in body:
        svc_err = validate_servicos(body["servicos"])
        if svc_err:
            return err(svc_err)
        updates["servicos"] = body["servicos"]

    if "barbeiros" in body:
        barb_err = validate_barbeiros(body["barbeiros"])
        if barb_err:
            return err(barb_err)
        updates["barbeiros"] = [normalize_barbeiro(b) for b in body["barbeiros"]]

    updated = await db_update_client(client_id, updates)
    if not updated:
        return err("Erro ao salvar.", 500)

    logger.info(f"[CLIENT] Updated: {client_id}")
    base = str(request.base_url).rstrip("/")
    updated["link_publico"] = build_public_link(updated["slug"], base)
    return ok(updated, "Cadastro atualizado com sucesso!")

@app.delete("/api/admin/clients/{client_id}")
async def admin_delete_client(client_id: str, _: str = Depends(require_admin)):
    if len(client_id) > 36:
        return err("ID inválido")
    deleted = await db_delete_client(client_id)
    if not deleted:
        return err("Cadastro não encontrado", 404)
    logger.info(f"[CLIENT] Deleted: {client_id}")
    return ok(None, "Cadastro excluído")

# ═══════════════════════════════════════════════════════
#  ADMIN — APPOINTMENTS
# ═══════════════════════════════════════════════════════
@app.get("/api/admin/appointments")
async def admin_list_appointments(request: Request, _: str = Depends(require_admin)):
    status_filter = request.query_params.get("status", "")[:20]
    slug_filter   = request.query_params.get("slug", "")[:60]
    date_filter   = request.query_params.get("date", "")[:10]
    valid_statuses = {"", "confirmado", "cancelado", "concluido", "recusado"}
    if status_filter not in valid_statuses:
        status_filter = ""
    appts = await db_list_appointments(status_filter, slug_filter, date_filter)
    return ok(appts)

@app.get("/api/admin/appointments/{slug}")
async def admin_appointments_by_slug(slug: str, _: str = Depends(require_admin)):
    slug = slug.strip().lower()[:60]
    appts = await db_list_appointments(slug_filter=slug)
    return ok(appts)

@app.patch("/api/admin/appointments/{appointment_id}/status")
async def admin_update_appointment_status(
    appointment_id: str, request: Request, _: str = Depends(require_admin)
):
    if len(appointment_id) > 36:
        return err("ID inválido")

    body = await read_json_body(request)
    if body is None:
        return err("Body inválido")

    new_status = sanitize_str(body.get("status", ""))
    VALID_STATUSES = {"confirmado", "cancelado", "concluido", "recusado"}
    if new_status not in VALID_STATUSES:
        return err(f"Status inválido. Use: {', '.join(sorted(VALID_STATUSES))}")

    updated = await db_update_appointment_status(appointment_id, new_status)
    if updated is None:
        return err("Agendamento não encontrado", 404)

    return ok(updated, f"Status atualizado para '{new_status}'")

# ═══════════════════════════════════════════════════════
#  ADMIN — STATS / SLUGIFY
# ═══════════════════════════════════════════════════════
@app.get("/api/admin/stats")
async def admin_stats(_: str = Depends(require_admin)):
    today = datetime.now().strftime("%Y-%m-%d")
    clients = await db_list_clients()
    appts   = await db_list_appointments()

    confirmed   = [a for a in appts if a.get("status") == "confirmado"]
    today_appts = [a for a in appts if a.get("data") == today]
    revenue     = sum(float(a.get("servico_preco", 0)) for a in confirmed)
    active      = [c for c in clients if c.get("status") == "ativo"]

    by_slug: dict = {}
    for a in appts:
        s = a.get("slug", "desconhecido")
        by_slug[s] = by_slug.get(s, 0) + 1

    return ok({
        "barbearias_total":   len(clients),
        "barbearias_ativas":  len(active),
        "agendamentos_total": len(appts),
        "agendamentos_hoje":  len(today_appts),
        "receita_estimada":   round(revenue, 2),
        "por_barbearia":      by_slug,
    })

@app.post("/api/admin/slugify")
async def admin_slugify(request: Request, _: str = Depends(require_admin)):
    body = await read_json_body(request)
    if body is None:
        return err("Body inválido")
    text = sanitize_str(body.get("text", ""))
    if not text:
        return err("Campo 'text' obrigatório")

    base = slugify(text)
    if len(base) < 3:
        base = base + "-01"

    candidate = base
    if await is_slug_taken(candidate) or validate_slug(candidate) is not None:
        for i in range(2, 50):
            candidate = f"{base}-{i}"
            if not await is_slug_taken(candidate) and validate_slug(candidate) is None:
                break

    return ok({
        "slug":       candidate,
        "disponivel": not await is_slug_taken(candidate),
        "valido":     validate_slug(candidate) is None,
    })

# ═══════════════════════════════════════════════════════
#  ERROR HANDLERS
# ═══════════════════════════════════════════════════════
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return err(exc.detail, exc.status_code)

@app.exception_handler(StarletteHTTPException)
async def starlette_exception_handler(request: Request, exc: StarletteHTTPException):
    return err("Rota não encontrada", exc.status_code)

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled error: {type(exc).__name__}: {exc}", exc_info=True)
    return err("Erro interno do servidor", 500)
