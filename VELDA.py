import discord
from discord.ext import commands, tasks
import os

# Volume persistant : DATA_DIR doit pointer vers un dossier persistant (volume Railway)
DATA_DIR = os.environ.get("DATA_DIR")
if not DATA_DIR:
    print("[ERREUR CRITIQUE] DATA_DIR non défini. Configure DATA_DIR=/data dans Railway.")
    import sys as _sys_exit; _sys_exit.exit(1)
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "velda.db")
import sys
import sqlite3
import json
import random
import asyncio
import logging
import traceback
import io
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Pour la carte de profil (image générée via Pillow)
try:
    from PIL import Image, ImageDraw, ImageFont
    import aiohttp
    PROFILE_CARD_AVAILABLE = True
except ImportError:
    PROFILE_CARD_AVAILABLE = False

# ========================= CONFIG =========================
BOT_TOKEN = os.environ.get("TOKEN")
if not BOT_TOKEN:
    print("[ERREUR CRITIQUE] La variable d'environnement TOKEN n'est pas définie.")
    print("Définis-la avant de lancer le bot (ex: export TOKEN=ton_token).")
    sys.exit(1)

PARIS_TZ = ZoneInfo("Europe/Paris")
DEFAULT_BUYER_IDS = [1312375517927706630, 1312375955737542676, 1173948561881317389, 1279358145151373352]
DEFAULT_PREFIX = "*"
MIN_BET = 100  # Mise minimum pour slots/jackpot/blackjack (évite le farm XP)
ROB_COOLDOWN = 3600  # 1h de cooldown sur *rob

# Cooldowns par jeu (secondes) - défauts, modifiables via *setcooldown
DEFAULT_GAME_COOLDOWNS = {
    "slots":    5,
    "jackpot":  5,
    "bj":       5,
    "roulette": 5,
    "des":      5,
    "pfc":      5,
}

# Gains vocaux par défaut (par tick, en Ryo)
DEFAULT_VOCAL_GAINS = {
    "base":     50,   # juste présent en voc
    "talk":     25,   # +bonus si non-mute
    "stream":   50,   # +bonus si stream
    "cam":      75,   # +bonus si cam
    "interval": 15,   # intervalle en minutes entre chaque tick
}

# Pot de jackpot initial (si le pot tombe à 0 il remonte à ça)
JACKPOT_POOL_MIN = 5000

# Prix d'un ticket de loto
LOTO_TICKET_PRICE = 1000

# Logger global (remplace les print nus)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
)
log = logging.getLogger("velda")

# Verrou global pour les opérations économiques critiques (évite les race conditions)
eco_lock = asyncio.Lock()

# Cache du prefix (évite d'ouvrir SQLite à chaque message reçu)
_prefix_cache = {"value": None}

# ========================= GIFS D'ANIMATION DES JEUX =========================
# Durée d'attente avant d'afficher le résultat (en secondes)
GAME_ANIMATION_DELAY = 2.5

# GIFs d'animation : URLs Discord CDN (signées, valides ~24h)
# Le bot les rafraîchit automatiquement via l'API Discord quand elles expirent.
# Seul l'ID du channel et l'ID du fichier sont permanents, le reste est regénéré.
GAME_GIFS = {
    "roulette": [
        "https://media.discordapp.net/attachments/1492171715554316441/1496879896469704897/roulette.gif?ex=69eb7d8a&is=69ea2c0a&hm=6c743179d92c47d029bc1243516f51808cfbfe1a824b5cf81af0bd340a193091&=",
    ],
    "slots": [
        "https://media.discordapp.net/attachments/1492171715554316441/1496879896075436164/slots.gif?ex=69eb7d89&is=69ea2c09&hm=d15ae289591af0a75f47a5a8fe02d143fb061ed072099238a5041eca4d7ed1ca&=",
    ],
    "jackpot": [
        "https://media.discordapp.net/attachments/1492171715554316441/1496879895631102184/jackpot.gif?ex=69eb7d89&is=69ea2c09&hm=6805eaac51c82cc3c246deb0bb331ff8b1a3c210c92b1b67a1876e74237f3626&=",
    ],
    "des": [
        "https://media.discordapp.net/attachments/1492171715554316441/1496879895140110377/dice.gif?ex=69eb7d89&is=69ea2c09&hm=09b1d8f77cfc68a76aa7d90f7ab082fdc9ee2b304ce6e54d4b889de98ac893c7&=",
    ],
    "pfc": [
        "https://media.discordapp.net/attachments/1492171715554316441/1496879894846771240/pfc.gif?ex=69eb7d89&is=69ea2c09&hm=629db8081e101dda7135f55aac297fa6325632ad6a92a69c722e46190070778c&=",
    ],
}

# Cache des URLs rafraîchies : {original_url: (fresh_url, expires_at_timestamp)}
_gif_url_cache = {}


async def get_fresh_gif_url(original_url):
    """
    Retourne une URL de GIF valide (non expirée).
    Si l'URL est expirée ou va expirer bientôt, demande une URL fraîche à Discord.
    Cache les résultats pour éviter de spam l'API.
    """
    import time as _t
    now = _t.time()

    # Check cache
    cached = _gif_url_cache.get(original_url)
    if cached:
        fresh_url, expires_at = cached
        # Si l'URL fraîche expire dans > 5 minutes, on la réutilise
        if expires_at - now > 300:
            return fresh_url

    # On appelle l'API Discord pour régénérer l'URL
    try:
        route = discord.http.Route("POST", "/attachments/refresh-urls")
        payload = {"attachment_urls": [original_url]}
        data = await bot.http.request(route, json=payload)
        refreshed = data.get("refreshed_urls", [])
        if refreshed and refreshed[0].get("refreshed"):
            fresh_url = refreshed[0]["refreshed"]
            # Extraire l'expiration depuis le paramètre ?ex=<hex_timestamp>
            import re
            m = re.search(r'[?&]ex=([0-9a-fA-F]+)', fresh_url)
            expires_at = int(m.group(1), 16) if m else now + 3600  # fallback 1h
            _gif_url_cache[original_url] = (fresh_url, expires_at)
            return fresh_url
    except Exception as e:
        log.warning(f"Refresh GIF URL échoué : {e}")

    # Fallback : on retourne l'URL originale (peut marcher si pas encore expirée)
    return original_url


async def pick_game_gif(game):
    """Retourne une URL de GIF valide (auto-refresh) pour un jeu donné."""
    import random as _r
    gifs = GAME_GIFS.get(game, [])
    if not gifs:
        return None
    chosen = _r.choice(gifs)
    return await get_fresh_gif_url(chosen)


# ========================= XP TABLE =========================
# Niveau i nécessite XP_TABLE[i] XP total (exponentiel)
def xp_for_level(level):
    return int(100 * (level ** 2.2))

# ========================= DATABASE =========================

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    # WAL mode: permet des lectures concurrentes pendant qu'une écriture a lieu
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()

    c.execute("""CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY, value TEXT
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS ranks (
        user_id TEXT PRIMARY KEY, rank INTEGER NOT NULL
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS economy (
        user_id TEXT PRIMARY KEY,
        hand INTEGER DEFAULT 0,
        bank INTEGER DEFAULT 0,
        fame INTEGER DEFAULT 0,
        xp INTEGER DEFAULT 0,
        level INTEGER DEFAULT 0,
        last_daily TEXT,
        last_fame TEXT,
        last_work TEXT,
        last_fish TEXT,
        last_rob TEXT,
        escrow INTEGER DEFAULT 0
    )""")

    # Migration : ajouter les nouvelles colonnes si la table existe déjà sans elles
    existing_cols = {row["name"] for row in c.execute("PRAGMA table_info(economy)").fetchall()}
    if "last_rob" not in existing_cols:
        c.execute("ALTER TABLE economy ADD COLUMN last_rob TEXT")
    if "escrow" not in existing_cols:
        c.execute("ALTER TABLE economy ADD COLUMN escrow INTEGER DEFAULT 0")

    c.execute("""CREATE TABLE IF NOT EXISTS bot_bans (
        user_id TEXT PRIMARY KEY,
        banned_by TEXT,
        banned_at TEXT
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS log_channels (
        guild_id TEXT PRIMARY KEY, channel_id TEXT NOT NULL
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS enchere_channels (
        guild_id TEXT PRIMARY KEY, channel_id TEXT NOT NULL
    )""")

    # FIX: clé primaire composée (guild_id, user_id), plus une seule ligne par guild
    c.execute("""CREATE TABLE IF NOT EXISTS active_messages (
        guild_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        message_content TEXT,
        timestamp TEXT,
        PRIMARY KEY (guild_id, user_id)
    )""")

    # Salons où le bot est autorisé par guild (les non-Sys ne peuvent utiliser le bot que là)
    c.execute("""CREATE TABLE IF NOT EXISTS allowed_channels (
        guild_id TEXT NOT NULL,
        channel_id TEXT NOT NULL,
        added_by TEXT,
        added_at TEXT,
        PRIMARY KEY (guild_id, channel_id)
    )""")

    # ===== NOUVELLES TABLES =====

    # Shop : items disponibles à l'achat
    c.execute("""CREATE TABLE IF NOT EXISTS shop_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id TEXT NOT NULL,
        name TEXT NOT NULL,
        price INTEGER NOT NULL,
        description TEXT,
        item_type TEXT NOT NULL,  -- 'role', 'temp_role', 'collectible', 'boost_xp', 'boost_vocal'
        role_id TEXT,              -- pour type 'role' ou 'temp_role'
        duration_hours INTEGER,    -- pour temp_role / boosts (durée en heures)
        multiplier REAL,           -- pour boost_xp / boost_vocal
        stock INTEGER,             -- NULL = illimité, sinon nombre restant
        created_by TEXT,
        created_at TEXT
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_shop_guild ON shop_items(guild_id)")

    # Inventaire des membres
    c.execute("""CREATE TABLE IF NOT EXISTS inventories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        guild_id TEXT NOT NULL,
        item_id INTEGER NOT NULL,
        item_name_snapshot TEXT,
        acquired_at TEXT NOT NULL,
        expires_at TEXT,           -- pour temp_role et boosts
        active INTEGER DEFAULT 1   -- 0 si boost expiré par ex
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_inv_user ON inventories(user_id, guild_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_inv_expire ON inventories(active, expires_at)")

    # Boosts actifs (pour calcul rapide)
    c.execute("""CREATE TABLE IF NOT EXISTS active_boosts (
        user_id TEXT NOT NULL,
        guild_id TEXT NOT NULL,
        boost_type TEXT NOT NULL,  -- 'xp' ou 'vocal'
        multiplier REAL NOT NULL,
        expires_at TEXT NOT NULL,
        PRIMARY KEY (user_id, guild_id, boost_type)
    )""")

    # Sessions vocales en cours (utilisateur entre dans une voc)
    c.execute("""CREATE TABLE IF NOT EXISTS vocal_sessions (
        user_id TEXT NOT NULL,
        guild_id TEXT NOT NULL,
        channel_id TEXT NOT NULL,
        joined_at TEXT NOT NULL,
        last_tick_at TEXT NOT NULL,
        PRIMARY KEY (user_id, guild_id)
    )""")

    # Stats vocales cumulées
    c.execute("""CREATE TABLE IF NOT EXISTS vocal_stats (
        user_id TEXT NOT NULL,
        guild_id TEXT NOT NULL,
        total_minutes INTEGER DEFAULT 0,
        total_earned INTEGER DEFAULT 0,
        PRIMARY KEY (user_id, guild_id)
    )""")

    # Zones vocales avec multiplicateurs
    c.execute("""CREATE TABLE IF NOT EXISTS vocal_zones (
        channel_id TEXT PRIMARY KEY,
        guild_id TEXT NOT NULL,
        multiplier REAL NOT NULL,
        set_by TEXT,
        set_at TEXT
    )""")

    # Cooldowns des jeux par user
    c.execute("""CREATE TABLE IF NOT EXISTS game_cooldowns (
        user_id TEXT NOT NULL,
        game TEXT NOT NULL,
        last_played TEXT NOT NULL,
        PRIMARY KEY (user_id, game)
    )""")

    # Stats de parties par joueur et par jeu (pour la carte de profil)
    c.execute("""CREATE TABLE IF NOT EXISTS game_stats_player (
        user_id TEXT NOT NULL,
        game TEXT NOT NULL,
        plays INTEGER DEFAULT 0,
        wins INTEGER DEFAULT 0,
        PRIMARY KEY (user_id, game)
    )""")

    # Pot du jackpot par guild (un seul rang)
    c.execute("""CREATE TABLE IF NOT EXISTS jackpot_pool (
        guild_id TEXT PRIMARY KEY,
        amount INTEGER DEFAULT 0
    )""")

    # Loterie : tickets achetés
    c.execute("""CREATE TABLE IF NOT EXISTS loto_tickets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        purchased_at TEXT NOT NULL
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_loto_guild ON loto_tickets(guild_id)")

    # Loterie : config / prochaine date de tirage par guild
    c.execute("""CREATE TABLE IF NOT EXISTS loto_config (
        guild_id TEXT PRIMARY KEY,
        next_draw_at TEXT,
        auto_interval_days INTEGER DEFAULT 7,
        last_draw_at TEXT,
        last_winner_id TEXT,
        last_prize INTEGER
    )""")

    # Migration de l'ancienne table si elle existait avec le mauvais schéma
    try:
        info = c.execute("PRAGMA table_info(active_messages)").fetchall()
        pk_cols = [r["name"] for r in info if r["pk"] > 0]
        if pk_cols != ["guild_id", "user_id"]:
            # Ancien schéma: on drop et on recrée (aucune donnée critique à conserver)
            c.execute("DROP TABLE IF EXISTS active_messages_old")
            c.execute("ALTER TABLE active_messages RENAME TO active_messages_old")
            c.execute("""CREATE TABLE active_messages (
                guild_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                message_content TEXT,
                timestamp TEXT,
                PRIMARY KEY (guild_id, user_id)
            )""")
            c.execute("DROP TABLE IF EXISTS active_messages_old")
    except sqlite3.Error as e:
        log.warning(f"Migration active_messages ignorée : {e}")

    c.execute("INSERT OR IGNORE INTO config VALUES ('prefix', ?)", (DEFAULT_PREFIX,))
    c.execute("INSERT OR IGNORE INTO config VALUES ('buyer_ids', ?)", (json.dumps([str(i) for i in DEFAULT_BUYER_IDS]),))
    c.execute("INSERT OR IGNORE INTO config VALUES ('game_cooldowns', ?)", (json.dumps(DEFAULT_GAME_COOLDOWNS),))
    c.execute("INSERT OR IGNORE INTO config VALUES ('vocal_gains', ?)", (json.dumps(DEFAULT_VOCAL_GAINS),))

    conn.commit()
    conn.close()


def get_config(key):
    conn = get_db()
    row = conn.execute("SELECT value FROM config WHERE key = ?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else None


def set_config(key, value):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO config VALUES (?, ?)", (key, str(value)))
    conn.commit()
    conn.close()
    # Invalide le cache si on touche au prefix
    if key == "prefix":
        _prefix_cache["value"] = str(value)


def get_prefix_cached():
    """Retourne le prefix depuis le cache mémoire. Charge depuis la DB au premier appel."""
    if _prefix_cache["value"] is None:
        _prefix_cache["value"] = get_config("prefix") or DEFAULT_PREFIX
    return _prefix_cache["value"]


def get_rank_db(user_id):
    buyer_ids_raw = get_config("buyer_ids")
    if buyer_ids_raw:
        buyer_ids = json.loads(buyer_ids_raw)
        if str(user_id) in buyer_ids:
            return 4
    conn = get_db()
    row = conn.execute("SELECT rank FROM ranks WHERE user_id = ?", (str(user_id),)).fetchone()
    conn.close()
    return row["rank"] if row else 0


def set_rank_db(user_id, rank):
    conn = get_db()
    if rank == 0:
        conn.execute("DELETE FROM ranks WHERE user_id = ?", (str(user_id),))
    else:
        conn.execute("INSERT OR REPLACE INTO ranks VALUES (?, ?)", (str(user_id), rank))
    conn.commit()
    conn.close()


def get_ranks_by_level(level):
    conn = get_db()
    rows = conn.execute("SELECT user_id FROM ranks WHERE rank = ?", (level,)).fetchall()
    conn.close()
    return [r["user_id"] for r in rows]


def get_economy(user_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM economy WHERE user_id = ?", (str(user_id),)).fetchone()
    if not row:
        conn.execute("INSERT OR IGNORE INTO economy (user_id) VALUES (?)", (str(user_id),))
        conn.commit()
        row = conn.execute("SELECT * FROM economy WHERE user_id = ?", (str(user_id),)).fetchone()
    conn.close()
    return dict(row)


def update_economy(user_id, **kwargs):
    eco = get_economy(user_id)
    for k, v in kwargs.items():
        eco[k] = v
    conn = get_db()
    conn.execute("""INSERT OR REPLACE INTO economy 
        (user_id, hand, bank, fame, xp, level, last_daily, last_fame, last_work, last_fish, last_rob, escrow)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (str(user_id), eco["hand"], eco["bank"], eco["fame"], eco["xp"], eco["level"],
         eco.get("last_daily"), eco.get("last_fame"), eco.get("last_work"), eco.get("last_fish"),
         eco.get("last_rob"), eco.get("escrow", 0)))
    conn.commit()
    conn.close()


def is_bot_banned(user_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM bot_bans WHERE user_id = ?", (str(user_id),)).fetchone()
    conn.close()
    return row is not None


def add_bot_ban(user_id, banned_by):
    conn = get_db()
    now = datetime.now(PARIS_TZ).strftime("%d/%m/%Y %Hh%M")
    conn.execute("INSERT OR REPLACE INTO bot_bans VALUES (?, ?, ?)", (str(user_id), str(banned_by), now))
    conn.commit()
    conn.close()


def remove_bot_ban(user_id):
    conn = get_db()
    conn.execute("DELETE FROM bot_bans WHERE user_id = ?", (str(user_id),))
    conn.commit()
    conn.close()


def get_log_channel(guild_id):
    conn = get_db()
    row = conn.execute("SELECT channel_id FROM log_channels WHERE guild_id = ?", (str(guild_id),)).fetchone()
    conn.close()
    return row["channel_id"] if row else None


def set_log_channel(guild_id, channel_id):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO log_channels VALUES (?, ?)", (str(guild_id), str(channel_id)))
    conn.commit()
    conn.close()


def get_enchere_channel(guild_id):
    conn = get_db()
    row = conn.execute("SELECT channel_id FROM enchere_channels WHERE guild_id = ?", (str(guild_id),)).fetchone()
    conn.close()
    return row["channel_id"] if row else None


def set_enchere_channel(guild_id, channel_id):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO enchere_channels VALUES (?, ?)", (str(guild_id), str(channel_id)))
    conn.commit()
    conn.close()


# ---- Allowed channels (Sys+ bypass, les autres sont restreints à ces salons) ----

def add_allowed_channel(guild_id, channel_id, added_by):
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    conn.execute(
        "INSERT OR IGNORE INTO allowed_channels (guild_id, channel_id, added_by, added_at) VALUES (?, ?, ?, ?)",
        (str(guild_id), str(channel_id), str(added_by), now)
    )
    conn.commit()
    conn.close()


def remove_allowed_channel(guild_id, channel_id):
    conn = get_db()
    cur = conn.execute(
        "DELETE FROM allowed_channels WHERE guild_id = ? AND channel_id = ?",
        (str(guild_id), str(channel_id))
    )
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    return deleted > 0


def get_allowed_channels(guild_id):
    conn = get_db()
    rows = conn.execute(
        "SELECT channel_id FROM allowed_channels WHERE guild_id = ?",
        (str(guild_id),)
    ).fetchall()
    conn.close()
    return [r["channel_id"] for r in rows]


def is_channel_allowed(guild_id, channel_id):
    conn = get_db()
    row = conn.execute(
        "SELECT 1 FROM allowed_channels WHERE guild_id = ? AND channel_id = ? LIMIT 1",
        (str(guild_id), str(channel_id))
    ).fetchone()
    conn.close()
    return row is not None


def track_message(guild_id, user_id, content):
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    # FIX: clé composée propre (guild_id, user_id)
    conn.execute(
        "INSERT OR REPLACE INTO active_messages (guild_id, user_id, message_content, timestamp) VALUES (?, ?, ?, ?)",
        (str(guild_id), str(user_id), content[:200], now)
    )
    conn.commit()
    conn.close()


def get_active_members(guild_id, limit=10):
    conn = get_db()
    cutoff = (datetime.now(PARIS_TZ) - timedelta(hours=24)).isoformat()
    # FIX: vrai filtre par guild_id (avant le filtre ne faisait rien)
    rows = conn.execute("""SELECT user_id, message_content FROM active_messages 
        WHERE guild_id = ? AND timestamp > ? ORDER BY timestamp DESC LIMIT ?""",
        (str(guild_id), cutoff, limit)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ========================= DB : SHOP =========================

def shop_add_item(guild_id, name, price, description, item_type, role_id=None,
                  duration_hours=None, multiplier=None, stock=None, created_by=None):
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    cur = conn.execute("""INSERT INTO shop_items
        (guild_id, name, price, description, item_type, role_id, duration_hours,
         multiplier, stock, created_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (str(guild_id), name, int(price), description, item_type,
         str(role_id) if role_id else None, duration_hours, multiplier, stock,
         str(created_by) if created_by else None, now))
    item_id = cur.lastrowid
    conn.commit()
    conn.close()
    return item_id


def shop_get_item(item_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM shop_items WHERE id = ?", (int(item_id),)).fetchone()
    conn.close()
    return dict(row) if row else None


def shop_list_items(guild_id):
    conn = get_db()
    rows = conn.execute("SELECT * FROM shop_items WHERE guild_id = ? ORDER BY price ASC",
                        (str(guild_id),)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def shop_remove_item(item_id):
    conn = get_db()
    cur = conn.execute("DELETE FROM shop_items WHERE id = ?", (int(item_id),))
    affected = cur.rowcount
    conn.commit()
    conn.close()
    return affected > 0


def shop_update_item(item_id, **kwargs):
    """Met à jour les champs fournis de l'item."""
    allowed_fields = {"name", "price", "description", "stock", "role_id",
                      "duration_hours", "multiplier", "item_type"}
    fields_to_update = {k: v for k, v in kwargs.items() if k in allowed_fields}
    if not fields_to_update:
        return False
    set_clauses = ", ".join(f"{k} = ?" for k in fields_to_update)
    values = list(fields_to_update.values()) + [int(item_id)]
    conn = get_db()
    cur = conn.execute(f"UPDATE shop_items SET {set_clauses} WHERE id = ?", values)
    affected = cur.rowcount
    conn.commit()
    conn.close()
    return affected > 0


def shop_decrement_stock(item_id):
    """Décrémente le stock atomiquement. Retourne True si OK, False si stock épuisé."""
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT stock FROM shop_items WHERE id = ?",
                          (int(item_id),)).fetchone()
        if not row:
            conn.rollback()
            return False
        stock = row["stock"]
        if stock is None:
            # Stock illimité : rien à faire
            conn.commit()
            return True
        if stock <= 0:
            conn.rollback()
            return False
        conn.execute("UPDATE shop_items SET stock = stock - 1 WHERE id = ?",
                    (int(item_id),))
        conn.commit()
        return True
    except sqlite3.Error as e:
        conn.rollback()
        log.error(f"shop_decrement_stock: {e}")
        return False
    finally:
        conn.close()


# ========================= DB : INVENTAIRE =========================

def inv_add(user_id, guild_id, item_id, item_name, expires_at=None):
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    conn.execute("""INSERT INTO inventories
        (user_id, guild_id, item_id, item_name_snapshot, acquired_at, expires_at, active)
        VALUES (?, ?, ?, ?, ?, ?, 1)""",
        (str(user_id), str(guild_id), int(item_id), item_name, now, expires_at))
    conn.commit()
    conn.close()


def inv_list(user_id, guild_id, active_only=True):
    conn = get_db()
    if active_only:
        rows = conn.execute("""SELECT * FROM inventories
            WHERE user_id = ? AND guild_id = ? AND active = 1
            ORDER BY acquired_at DESC""",
            (str(user_id), str(guild_id))).fetchall()
    else:
        rows = conn.execute("""SELECT * FROM inventories
            WHERE user_id = ? AND guild_id = ?
            ORDER BY acquired_at DESC""",
            (str(user_id), str(guild_id))).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def inv_get_expiring():
    """Retourne les items actifs qui ont expiré."""
    conn = get_db()
    now_iso = datetime.now(PARIS_TZ).isoformat()
    rows = conn.execute("""SELECT * FROM inventories
        WHERE active = 1 AND expires_at IS NOT NULL AND expires_at <= ?""",
        (now_iso,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def inv_deactivate(inv_id):
    conn = get_db()
    conn.execute("UPDATE inventories SET active = 0 WHERE id = ?", (int(inv_id),))
    conn.commit()
    conn.close()


# ========================= DB : BOOSTS =========================

def boost_add(user_id, guild_id, boost_type, multiplier, duration_hours):
    conn = get_db()
    expires = (datetime.now(PARIS_TZ) + timedelta(hours=duration_hours)).isoformat()
    conn.execute("""INSERT OR REPLACE INTO active_boosts
        (user_id, guild_id, boost_type, multiplier, expires_at)
        VALUES (?, ?, ?, ?, ?)""",
        (str(user_id), str(guild_id), boost_type, float(multiplier), expires))
    conn.commit()
    conn.close()


def boost_get(user_id, guild_id, boost_type):
    conn = get_db()
    row = conn.execute("""SELECT * FROM active_boosts
        WHERE user_id = ? AND guild_id = ? AND boost_type = ?""",
        (str(user_id), str(guild_id), boost_type)).fetchone()
    conn.close()
    if not row:
        return None
    # Check si expiré
    try:
        if datetime.fromisoformat(row["expires_at"]) <= datetime.now(PARIS_TZ):
            return None
    except (ValueError, TypeError):
        return None
    return dict(row)


def boost_cleanup_expired():
    conn = get_db()
    now_iso = datetime.now(PARIS_TZ).isoformat()
    conn.execute("DELETE FROM active_boosts WHERE expires_at <= ?", (now_iso,))
    conn.commit()
    conn.close()


# ========================= DB : VOCAL =========================

def vocal_start_session(user_id, guild_id, channel_id):
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    conn.execute("""INSERT OR REPLACE INTO vocal_sessions
        (user_id, guild_id, channel_id, joined_at, last_tick_at) VALUES (?, ?, ?, ?, ?)""",
        (str(user_id), str(guild_id), str(channel_id), now, now))
    conn.commit()
    conn.close()


def vocal_end_session(user_id, guild_id):
    conn = get_db()
    conn.execute("DELETE FROM vocal_sessions WHERE user_id = ? AND guild_id = ?",
                 (str(user_id), str(guild_id)))
    conn.commit()
    conn.close()


def vocal_get_session(user_id, guild_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM vocal_sessions WHERE user_id = ? AND guild_id = ?",
                       (str(user_id), str(guild_id))).fetchone()
    conn.close()
    return dict(row) if row else None


def vocal_update_tick(user_id, guild_id, channel_id):
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    conn.execute("""UPDATE vocal_sessions SET last_tick_at = ?, channel_id = ?
        WHERE user_id = ? AND guild_id = ?""",
        (now, str(channel_id), str(user_id), str(guild_id)))
    conn.commit()
    conn.close()


def vocal_add_stats(user_id, guild_id, minutes, earned):
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("""INSERT INTO vocal_stats (user_id, guild_id, total_minutes, total_earned)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id, guild_id) DO UPDATE SET
                total_minutes = total_minutes + ?,
                total_earned = total_earned + ?""",
            (str(user_id), str(guild_id), int(minutes), int(earned),
             int(minutes), int(earned)))
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        log.error(f"vocal_add_stats: {e}")
    finally:
        conn.close()


def vocal_get_stats(user_id, guild_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM vocal_stats WHERE user_id = ? AND guild_id = ?",
                       (str(user_id), str(guild_id))).fetchone()
    conn.close()
    return dict(row) if row else {"total_minutes": 0, "total_earned": 0}


# ========================= DB : ZONES VOCALES =========================

def zone_add(channel_id, guild_id, multiplier, set_by):
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    conn.execute("""INSERT OR REPLACE INTO vocal_zones
        (channel_id, guild_id, multiplier, set_by, set_at) VALUES (?, ?, ?, ?, ?)""",
        (str(channel_id), str(guild_id), float(multiplier), str(set_by), now))
    conn.commit()
    conn.close()


def zone_remove(channel_id):
    conn = get_db()
    cur = conn.execute("DELETE FROM vocal_zones WHERE channel_id = ?", (str(channel_id),))
    affected = cur.rowcount
    conn.commit()
    conn.close()
    return affected > 0


def zone_get_multiplier(channel_id):
    conn = get_db()
    row = conn.execute("SELECT multiplier FROM vocal_zones WHERE channel_id = ?",
                       (str(channel_id),)).fetchone()
    conn.close()
    return float(row["multiplier"]) if row else 1.0


def zone_list(guild_id):
    conn = get_db()
    rows = conn.execute("SELECT * FROM vocal_zones WHERE guild_id = ? ORDER BY multiplier DESC",
                        (str(guild_id),)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ========================= DB : COOLDOWNS JEUX =========================

def get_game_cooldowns():
    """Retourne le dict complet des cooldowns configurés."""
    raw = get_config("game_cooldowns")
    if not raw:
        return dict(DEFAULT_GAME_COOLDOWNS)
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return dict(DEFAULT_GAME_COOLDOWNS)


def set_game_cooldown(game, seconds):
    cds = get_game_cooldowns()
    cds[game] = int(seconds)
    set_config("game_cooldowns", json.dumps(cds))


def check_game_cooldown(user_id, game):
    """Retourne (True, None) si OK, (False, secondes_restantes) sinon."""
    cds = get_game_cooldowns()
    cd_duration = cds.get(game, 5)
    conn = get_db()
    row = conn.execute("""SELECT last_played FROM game_cooldowns
        WHERE user_id = ? AND game = ?""",
        (str(user_id), game)).fetchone()
    conn.close()
    if not row:
        return True, None
    try:
        last = datetime.fromisoformat(row["last_played"])
    except (ValueError, TypeError):
        return True, None
    elapsed = (datetime.now(PARIS_TZ) - last).total_seconds()
    if elapsed >= cd_duration:
        return True, None
    return False, int(cd_duration - elapsed)


def record_game_cooldown(user_id, game):
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    conn.execute("""INSERT OR REPLACE INTO game_cooldowns (user_id, game, last_played)
        VALUES (?, ?, ?)""",
        (str(user_id), game, now))
    conn.commit()
    conn.close()


def record_game_play(user_id, game, won=False):
    """Enregistre une partie jouée (+1 partie, +1 win si won=True)."""
    conn = get_db()
    # INSERT OR IGNORE puis UPDATE
    conn.execute("""INSERT OR IGNORE INTO game_stats_player (user_id, game, plays, wins)
        VALUES (?, ?, 0, 0)""", (str(user_id), game))
    if won:
        conn.execute("""UPDATE game_stats_player SET plays = plays + 1, wins = wins + 1
            WHERE user_id = ? AND game = ?""", (str(user_id), game))
    else:
        conn.execute("""UPDATE game_stats_player SET plays = plays + 1
            WHERE user_id = ? AND game = ?""", (str(user_id), game))
    conn.commit()
    conn.close()


def get_player_stats(user_id):
    """Retourne (total_plays, total_wins, favorite_game).
    favorite_game = le jeu le plus joué (ou None si aucune partie).
    """
    conn = get_db()
    rows = conn.execute("""SELECT game, plays, wins FROM game_stats_player
        WHERE user_id = ?""", (str(user_id),)).fetchall()
    conn.close()
    total_plays = sum(r["plays"] for r in rows)
    total_wins = sum(r["wins"] for r in rows)
    if not rows:
        return 0, 0, None
    # Jeu le plus joué
    favorite = max(rows, key=lambda r: r["plays"])
    favorite_game = favorite["game"] if favorite["plays"] > 0 else None
    return total_plays, total_wins, favorite_game


def get_user_rank(user_id):
    """Retourne (position, total_users) dans le classement or total (hand + bank)."""
    conn = get_db()
    # Position du user : combien ont plus que lui
    row = conn.execute("""SELECT (hand + bank) AS my_total FROM economy WHERE user_id = ?""",
                       (str(user_id),)).fetchone()
    if not row or row["my_total"] is None:
        my_total = 0
    else:
        my_total = row["my_total"]
    # Count des users qui ont strictement plus
    higher = conn.execute("""SELECT COUNT(*) AS c FROM economy
        WHERE (hand + bank) > ?""", (my_total,)).fetchone()
    position = (higher["c"] or 0) + 1
    total = conn.execute("""SELECT COUNT(*) AS c FROM economy
        WHERE (hand + bank) > 0""").fetchone()["c"] or 1
    conn.close()
    return position, total


# ========================= DB : VOCAL GAINS CONFIG =========================

def get_vocal_gains():
    raw = get_config("vocal_gains")
    if not raw:
        return dict(DEFAULT_VOCAL_GAINS)
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return dict(DEFAULT_VOCAL_GAINS)


def set_vocal_gains(new_gains):
    set_config("vocal_gains", json.dumps(new_gains))


# ========================= DB : JACKPOT POOL =========================

def jackpot_get(guild_id):
    conn = get_db()
    row = conn.execute("SELECT amount FROM jackpot_pool WHERE guild_id = ?",
                       (str(guild_id),)).fetchone()
    if not row:
        # Initialise à JACKPOT_POOL_MIN
        conn.execute("INSERT OR IGNORE INTO jackpot_pool (guild_id, amount) VALUES (?, ?)",
                    (str(guild_id), JACKPOT_POOL_MIN))
        conn.commit()
        amount = JACKPOT_POOL_MIN
    else:
        amount = row["amount"]
    conn.close()
    return amount


def jackpot_add(guild_id, amount):
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("""INSERT INTO jackpot_pool (guild_id, amount) VALUES (?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET amount = amount + ?""",
            (str(guild_id), int(amount), int(amount)))
        conn.commit()
    finally:
        conn.close()


def jackpot_reset(guild_id, new_amount=None):
    """Reset le pot (à JACKPOT_POOL_MIN par défaut, ou au montant spécifié)."""
    amount = new_amount if new_amount is not None else JACKPOT_POOL_MIN
    conn = get_db()
    conn.execute("""INSERT OR REPLACE INTO jackpot_pool (guild_id, amount) VALUES (?, ?)""",
                (str(guild_id), int(amount)))
    conn.commit()
    conn.close()


# ========================= DB : LOTO =========================

def loto_buy_ticket(user_id, guild_id):
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    conn.execute("""INSERT INTO loto_tickets (guild_id, user_id, purchased_at)
        VALUES (?, ?, ?)""",
        (str(guild_id), str(user_id), now))
    conn.commit()
    conn.close()


def loto_get_tickets(guild_id):
    conn = get_db()
    rows = conn.execute("SELECT * FROM loto_tickets WHERE guild_id = ?",
                        (str(guild_id),)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def loto_count_user_tickets(user_id, guild_id):
    conn = get_db()
    row = conn.execute("""SELECT COUNT(*) as c FROM loto_tickets
        WHERE guild_id = ? AND user_id = ?""",
        (str(guild_id), str(user_id))).fetchone()
    conn.close()
    return row["c"] if row else 0


def loto_clear_tickets(guild_id):
    conn = get_db()
    conn.execute("DELETE FROM loto_tickets WHERE guild_id = ?", (str(guild_id),))
    conn.commit()
    conn.close()


def loto_get_config(guild_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM loto_config WHERE guild_id = ?",
                       (str(guild_id),)).fetchone()
    if not row:
        # Init avec tirage dans 7 jours
        now = datetime.now(PARIS_TZ)
        next_draw = (now + timedelta(days=7)).isoformat()
        conn.execute("""INSERT INTO loto_config
            (guild_id, next_draw_at, auto_interval_days) VALUES (?, ?, 7)""",
            (str(guild_id), next_draw))
        conn.commit()
        row = conn.execute("SELECT * FROM loto_config WHERE guild_id = ?",
                          (str(guild_id),)).fetchone()
    conn.close()
    return dict(row)


def loto_set_next_draw(guild_id, next_draw_iso):
    conn = get_db()
    loto_get_config(guild_id)  # s'assure que la ligne existe
    conn.execute("UPDATE loto_config SET next_draw_at = ? WHERE guild_id = ?",
                (next_draw_iso, str(guild_id)))
    conn.commit()
    conn.close()


def loto_record_draw(guild_id, winner_id, prize):
    conn = get_db()
    now = datetime.now(PARIS_TZ)
    cfg = loto_get_config(guild_id)
    interval = cfg.get("auto_interval_days", 7)
    next_draw = (now + timedelta(days=interval)).isoformat()
    conn.execute("""UPDATE loto_config SET
        last_draw_at = ?, last_winner_id = ?, last_prize = ?, next_draw_at = ?
        WHERE guild_id = ?""",
        (now.isoformat(), str(winner_id) if winner_id else None,
         int(prize) if prize else 0, next_draw, str(guild_id)))
    conn.commit()
    conn.close()


# ========================= DB : LEADERBOARD =========================

def lb_top(guild_id, metric, limit=10):
    """
    Classement par métrique. Les métriques sont :
    - 'hand', 'bank', 'fame', 'xp', 'level' (dans economy)
    - 'total' (hand + bank)
    - 'vocal_time' (minutes vocales)
    - 'vocal_earned' (argent gagné en voc)
    Note : economy n'est pas par-guild, donc les top économiques sont globaux
    (ce qui colle à ton projet mono-serveur). Les stats vocales sont par-guild.
    """
    conn = get_db()
    if metric == "hand":
        rows = conn.execute("""SELECT user_id, hand as value FROM economy
            WHERE hand > 0 ORDER BY hand DESC LIMIT ?""", (limit,)).fetchall()
    elif metric == "bank":
        rows = conn.execute("""SELECT user_id, bank as value FROM economy
            WHERE bank > 0 ORDER BY bank DESC LIMIT ?""", (limit,)).fetchall()
    elif metric == "total":
        rows = conn.execute("""SELECT user_id, (hand + bank) as value FROM economy
            WHERE (hand + bank) > 0 ORDER BY value DESC LIMIT ?""", (limit,)).fetchall()
    elif metric == "fame":
        rows = conn.execute("""SELECT user_id, fame as value FROM economy
            WHERE fame > 0 ORDER BY fame DESC LIMIT ?""", (limit,)).fetchall()
    elif metric == "xp":
        rows = conn.execute("""SELECT user_id, xp as value FROM economy
            WHERE xp > 0 ORDER BY xp DESC LIMIT ?""", (limit,)).fetchall()
    elif metric == "level":
        rows = conn.execute("""SELECT user_id, level as value FROM economy
            WHERE level > 0 ORDER BY level DESC, xp DESC LIMIT ?""", (limit,)).fetchall()
    elif metric == "vocal_time":
        rows = conn.execute("""SELECT user_id, total_minutes as value FROM vocal_stats
            WHERE guild_id = ? AND total_minutes > 0 ORDER BY total_minutes DESC LIMIT ?""",
            (str(guild_id), limit)).fetchall()
    elif metric == "vocal_earned":
        rows = conn.execute("""SELECT user_id, total_earned as value FROM vocal_stats
            WHERE guild_id = ? AND total_earned > 0 ORDER BY total_earned DESC LIMIT ?""",
            (str(guild_id), limit)).fetchall()
    else:
        rows = []
    conn.close()
    return [(r["user_id"], r["value"]) for r in rows]


# ========================= HELPERS =========================

def rank_name(level):
    return {4: "Buyer", 3: "Sys", 2: "Owner", 1: "Whitelist", 0: "Aucun"}[level]


def has_min_rank(user_id, minimum):
    return get_rank_db(user_id) >= minimum


def embed_color():
    return 0x2b2d31


def success_embed(title, desc=""):
    em = discord.Embed(title=title, description=desc, color=0x43b581)
    em.set_footer(text="Velda")
    return em


def error_embed(title, desc=""):
    em = discord.Embed(title=title, description=desc, color=0xf04747)
    em.set_footer(text="Velda")
    return em


def info_embed(title, desc=""):
    em = discord.Embed(title=title, description=desc, color=embed_color())
    em.set_footer(text="Velda")
    return em


def action_embed(member, description, color=None):
    """
    Embed compact pour les actions de jeu / éco (daily, dep, rob, fame, work, fish, slots...).
    Style : titre = display_name, description = lignes compactes avec emojis, thumbnail = avatar.
    """
    em = discord.Embed(
        title=member.display_name,
        description=description,
        color=color if color is not None else embed_color(),
    )
    em.set_thumbnail(url=member.display_avatar.url)
    em.set_footer(text="Velda")
    return em


def get_french_time():
    now = datetime.now(PARIS_TZ)
    JOURS_FR = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
    MOIS_FR = ["janvier", "février", "mars", "avril", "mai", "juin",
               "juillet", "août", "septembre", "octobre", "novembre", "décembre"]
    return f"{JOURS_FR[now.weekday()]} {now.day} {MOIS_FR[now.month - 1]} {now.year} — {now.strftime('%Hh%M')}"


def format_ryo(amount):
    return f"{amount:,} Ryo".replace(",", " ")


def parse_amount(arg, hand):
    if arg.lower() == "all":
        return hand
    try:
        val = int(arg.replace(" ", "").replace(",", ""))
        # FIX: on refuse tout ce qui n'est pas strictement positif
        if val <= 0:
            return None
        return val
    except Exception:
        return None


async def resolve_member(ctx, user_input):
    """Résout un membre depuis une mention, ID, ou nom. Retourne None si échec."""
    if not user_input:
        return None
    try:
        member_id = int(user_input.strip("<@!>"))
        m = ctx.guild.get_member(member_id)
        if m:
            return m
    except (ValueError, AttributeError):
        pass
    try:
        return await commands.MemberConverter().convert(ctx, user_input)
    except commands.CommandError:
        return None


async def resolve_user_or_id(ctx, user_input):
    """
    Résout un input (mention, ID, nom) en (display_obj, user_id).
    - Si l'user est membre du serveur :                 (Member, id)
    - S'il existe globalement mais pas sur le serveur : (User, id)
    - Si seul un ID numérique est donné et qu'on ne trouve rien : (None, id)
    - Si on ne peut rien parser :                       (None, None)

    Utilisé pour les commandes de rang/ban qui doivent marcher même quand
    le user a quitté le serveur (ex: unsys sur un owner parti).
    """
    if not user_input:
        return None, None

    raw = user_input.strip()
    cleaned = raw.strip("<@!>")

    # 1) Tentative : c'est un ID numérique ?
    user_id = None
    try:
        user_id = int(cleaned)
    except ValueError:
        # Ce n'est pas un ID brut — on tente les converters par nom
        try:
            member = await commands.MemberConverter().convert(ctx, raw)
            return member, member.id
        except commands.CommandError:
            pass
        try:
            user = await commands.UserConverter().convert(ctx, raw)
            return user, user.id
        except commands.CommandError:
            return None, None

    # 2) On a un ID. Regarde d'abord dans les membres du serveur (pas d'appel API)
    if ctx.guild:
        member = ctx.guild.get_member(user_id)
        if member:
            return member, user_id

    # 3) Pas membre : tente fetch_user pour avoir au moins le nom (appel API)
    try:
        user = await bot.fetch_user(user_id)
        return user, user_id
    except discord.NotFound:
        # L'ID ne correspond à aucun compte Discord — on accepte quand même l'ID
        # pour permettre le nettoyage de la base (cas d'un compte supprimé)
        return None, user_id
    except discord.HTTPException as e:
        log.warning(f"resolve_user_or_id: échec fetch_user({user_id}) : {e}")
        return None, user_id


def format_user_display(display_obj, user_id):
    """
    Formatte un user pour affichage dans les embeds.
    Si on a un objet (Member/User) on utilise sa mention, sinon on affiche l'ID brut
    avec un marqueur indiquant qu'on est hors serveur.
    """
    if display_obj is not None:
        # Member a .mention, User aussi
        return f"{display_obj.mention} (`{display_obj.id}`)"
    return f"<@{user_id}> (`{user_id}`) *(hors serveur)*"


def atomic_transfer(from_id, to_id, amount):
    """
    Transfert atomique entre deux utilisateurs en une seule transaction SQLite.
    Renvoie True si réussi, False si fonds insuffisants.
    """
    # S'assure que les deux lignes existent
    get_economy(from_id)
    get_economy(to_id)
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT hand FROM economy WHERE user_id = ?", (str(from_id),)).fetchone()
        if not row or row["hand"] < amount:
            conn.rollback()
            return False
        conn.execute("UPDATE economy SET hand = hand - ? WHERE user_id = ?", (amount, str(from_id)))
        conn.execute("UPDATE economy SET hand = hand + ? WHERE user_id = ?", (amount, str(to_id)))
        conn.commit()
        return True
    except sqlite3.Error as e:
        conn.rollback()
        log.error(f"atomic_transfer failed: {e}")
        return False
    finally:
        conn.close()


def atomic_hand_bank(user_id, hand_delta, bank_delta):
    """Variation atomique du hand et de la bank (dépôt/retrait). Renvoie True si OK."""
    get_economy(user_id)
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT hand, bank FROM economy WHERE user_id = ?", (str(user_id),)).fetchone()
        new_hand = row["hand"] + hand_delta
        new_bank = row["bank"] + bank_delta
        if new_hand < 0 or new_bank < 0:
            conn.rollback()
            return False
        conn.execute("UPDATE economy SET hand = ?, bank = ? WHERE user_id = ?", (new_hand, new_bank, str(user_id)))
        conn.commit()
        return True
    except sqlite3.Error as e:
        conn.rollback()
        log.error(f"atomic_hand_bank failed: {e}")
        return False
    finally:
        conn.close()


def atomic_hand_delta(user_id, delta, min_hand=0):
    """Ajoute/retire delta au hand atomiquement. Renvoie True si OK, False si fonds insuffisants."""
    get_economy(user_id)
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT hand FROM economy WHERE user_id = ?", (str(user_id),)).fetchone()
        new_hand = row["hand"] + delta
        if new_hand < min_hand:
            conn.rollback()
            return False
        conn.execute("UPDATE economy SET hand = ? WHERE user_id = ?", (new_hand, str(user_id)))
        conn.commit()
        return True
    except sqlite3.Error as e:
        conn.rollback()
        log.error(f"atomic_hand_delta failed: {e}")
        return False
    finally:
        conn.close()


async def check_ban(ctx):
    if is_bot_banned(ctx.author.id):
        em = discord.Embed(color=0xf04747)
        em.set_author(name="⛔ Accès refusé — Velda", icon_url=ctx.bot.user.display_avatar.url)
        em.description = (
            "Tu as été **banni du bot Velda** suite à une infraction aux règles.\n"
            "Si tu penses que c'est une erreur, contacte un membre du staff."
        )
        em.set_footer(text="Velda")
        await ctx.send(embed=em)
        return True
    return False


async def add_xp(ctx, user_id, amount):
    eco = get_economy(user_id)
    new_xp = eco["xp"] + amount
    current_level = eco["level"]
    new_level = current_level

    # Vérif level up
    while new_level < 100 and new_xp >= xp_for_level(new_level + 1):
        new_level += 1

    bonus = (new_level - current_level) * 5000
    new_hand = eco["hand"] + bonus

    update_economy(user_id, xp=new_xp, level=new_level, hand=new_hand)

    if new_level > current_level:
        em = discord.Embed(
            title="🎉 Level Up !",
            description=f"<@{user_id}> est passé au niveau **{new_level}** !\n+{format_ryo(bonus)} en récompense !",
            color=0xffd700
        )
        em.set_footer(text="Velda")
        await ctx.send(embed=em)


# ========================= BOT SETUP =========================

init_db()
intents = discord.Intents.all()


def get_prefix(bot, message):
    # Utilise le cache mémoire au lieu d'ouvrir SQLite à chaque message
    return get_prefix_cached()


bot = commands.Bot(command_prefix=get_prefix, intents=intents, help_command=None)


# ========================= GLOBAL CHANNEL CHECK =========================

class ChannelNotAllowedError(commands.CheckFailure):
    """Levée quand un membre non-Sys utilise une commande dans un salon non autorisé."""
    pass


@bot.check
async def check_allowed_channel(ctx):
    """
    Check global : les Sys+ bypassent, les autres ne peuvent utiliser le bot que
    dans les salons explicitement autorisés via *allow.
    Les DMs sont toujours autorisés (pas de salon à filtrer).
    """
    # Sys+ bypass total
    if has_min_rank(ctx.author.id, 3):
        return True
    # DM : pas de guild, on laisse passer
    if ctx.guild is None:
        return True
    # Salon autorisé ?
    if is_channel_allowed(ctx.guild.id, ctx.channel.id):
        return True
    # Sinon : bloque silencieusement (une réaction ❌ sera ajoutée via on_command_error)
    raise ChannelNotAllowedError("Salon non autorisé pour Velda.")


# ========================= EVENTS =========================

@bot.event
async def on_ready():
    log.info(f"Velda connecté : {bot.user} ({bot.user.id})")
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name="les Ryos"))
    # Démarrage des boucles de fond
    if not vocal_tick_loop.is_running():
        vocal_tick_loop.start()
    if not boost_expire_loop.is_running():
        boost_expire_loop.start()
    if not loto_auto_loop.is_running():
        loto_auto_loop.start()
    # Au démarrage, on crée des sessions pour tous les users déjà en vocal
    for guild in bot.guilds:
        for vc in guild.voice_channels:
            for member in vc.members:
                if not member.bot:
                    vocal_start_session(member.id, guild.id, vc.id)


@bot.event
async def on_message(message):
    if message.author.bot:
        return
    if message.guild:
        track_message(message.guild.id, message.author.id, message.content)
    await bot.process_commands(message)


@bot.event
async def on_voice_state_update(member, before, after):
    """Ouvre/ferme les sessions vocales pour tracker le temps en voc."""
    if member.bot:
        return
    guild_id = member.guild.id

    # Changement de salon : maj la session avec le nouveau salon
    if before.channel is None and after.channel is not None:
        # Arrive en voc
        vocal_start_session(member.id, guild_id, after.channel.id)
    elif before.channel is not None and after.channel is None:
        # Quitte la voc : on encaisse le delta partiel et ferme la session
        await _vocal_flush_session(member, guild_id)
        vocal_end_session(member.id, guild_id)
    elif before.channel != after.channel and after.channel is not None:
        # Change de salon
        await _vocal_flush_session(member, guild_id)
        vocal_start_session(member.id, guild_id, after.channel.id)


async def _vocal_flush_session(member, guild_id):
    """Quand qqn quitte/change de salon, on paye le temps écoulé depuis le dernier tick."""
    session = vocal_get_session(member.id, guild_id)
    if not session:
        return
    try:
        last_tick = datetime.fromisoformat(session["last_tick_at"])
    except (ValueError, TypeError):
        return
    elapsed_min = (datetime.now(PARIS_TZ) - last_tick).total_seconds() / 60
    if elapsed_min <= 0.1:
        return
    # Ajoute juste les minutes, pas de gain d'argent partiel (le gain se fait par tick complet)
    vocal_add_stats(member.id, guild_id, int(elapsed_min), 0)


# ========================= BOUCLE DES GAINS VOCAUX =========================

async def _compute_vocal_gain(member, channel):
    """Calcule le gain pour un tick (en Ryo), selon l'état du membre."""
    gains = get_vocal_gains()
    # AFK channel → 0
    if member.guild.afk_channel and channel.id == member.guild.afk_channel.id:
        return 0, "AFK"
    # Seul → 0
    non_bot_members = [m for m in channel.members if not m.bot]
    if len(non_bot_members) < 2:
        return 0, "Seul"
    # Mute total (self-mute ET self-deaf) → 0
    vs = member.voice
    if vs and vs.self_mute and vs.self_deaf:
        return 0, "Mute+deaf"

    # Base
    total = gains.get("base", 50)
    # Bonus parle (non mute côté serveur, non self-mute)
    if vs and not vs.mute and not vs.self_mute:
        total += gains.get("talk", 25)
    # Stream
    if vs and vs.self_stream:
        total += gains.get("stream", 50)
    # Cam
    if vs and vs.self_video:
        total += gains.get("cam", 75)

    # Multiplicateur de zone
    zone_mult = zone_get_multiplier(channel.id)
    total = total * zone_mult

    # Boost vocal perso
    boost = boost_get(member.id, member.guild.id, "vocal")
    if boost:
        total = total * boost["multiplier"]

    return int(total), None


@tasks.loop(minutes=1)
async def vocal_tick_loop():
    """
    Check toutes les minutes mais ne paye qu'au bout de l'intervalle défini (défaut 15min).
    Chaque membre a son propre dernier tick, donc on paye qu'un seul coup par intervalle.
    """
    try:
        gains = get_vocal_gains()
        interval_min = int(gains.get("interval", 15))
        now = datetime.now(PARIS_TZ)

        # Pour chaque guild, scanner toutes les vocs
        for guild in bot.guilds:
            for vc in guild.voice_channels:
                for member in vc.members:
                    if member.bot:
                        continue
                    # Charge ou crée la session
                    session = vocal_get_session(member.id, guild.id)
                    if not session:
                        vocal_start_session(member.id, guild.id, vc.id)
                        continue
                    try:
                        last_tick = datetime.fromisoformat(session["last_tick_at"])
                    except (ValueError, TypeError):
                        vocal_update_tick(member.id, guild.id, vc.id)
                        continue
                    elapsed_min = (now - last_tick).total_seconds() / 60
                    if elapsed_min < interval_min:
                        continue
                    # Tick complet : paye le gain
                    gain, reason = await _compute_vocal_gain(member, vc)
                    vocal_update_tick(member.id, guild.id, vc.id)
                    if gain > 0:
                        async with eco_lock:
                            atomic_hand_delta(member.id, gain, 0)
                        vocal_add_stats(member.id, guild.id, int(elapsed_min), gain)
                    else:
                        # On compte quand même le temps passé mais pas d'argent
                        vocal_add_stats(member.id, guild.id, int(elapsed_min), 0)
    except Exception as e:
        log.error(f"vocal_tick_loop: erreur : {e}\n{traceback.format_exc()}")


@vocal_tick_loop.before_loop
async def _vocal_tick_before():
    await bot.wait_until_ready()


# ========================= BOUCLE DE NETTOYAGE DES BOOSTS EXPIRÉS =========================

@tasks.loop(minutes=5)
async def boost_expire_loop():
    """Nettoie les boosts expirés. Désactive aussi les items inventaire expirés."""
    try:
        boost_cleanup_expired()
        expiring = inv_get_expiring()
        for inv in expiring:
            inv_deactivate(inv["id"])
            # Pour un temp_role : retirer le rôle
            item = shop_get_item(inv["item_id"])
            if not item:
                continue
            if item["item_type"] == "temp_role" and item.get("role_id"):
                guild = bot.get_guild(int(inv["guild_id"]))
                if not guild:
                    continue
                member = guild.get_member(int(inv["user_id"]))
                role = guild.get_role(int(item["role_id"]))
                if member and role and role in member.roles:
                    try:
                        await member.remove_roles(role, reason="Expiration de l'item temp_role")
                    except (discord.Forbidden, discord.HTTPException) as e:
                        log.warning(f"boost_expire_loop: retrait rôle échoué : {e}")
    except Exception as e:
        log.error(f"boost_expire_loop: erreur : {e}\n{traceback.format_exc()}")


@boost_expire_loop.before_loop
async def _boost_expire_before():
    await bot.wait_until_ready()


# ========================= BOUCLE DE TIRAGE AUTO DE LA LOTO =========================

@tasks.loop(minutes=5)
async def loto_auto_loop():
    """Vérifie si un tirage loto est prévu et le déclenche."""
    try:
        now = datetime.now(PARIS_TZ)
        for guild in bot.guilds:
            cfg = loto_get_config(guild.id)
            if not cfg.get("next_draw_at"):
                continue
            try:
                next_draw = datetime.fromisoformat(cfg["next_draw_at"])
            except (ValueError, TypeError):
                continue
            if now >= next_draw:
                await _do_loto_draw(guild, triggered_by="auto")
    except Exception as e:
        log.error(f"loto_auto_loop: erreur : {e}\n{traceback.format_exc()}")


@loto_auto_loop.before_loop
async def _loto_auto_before():
    await bot.wait_until_ready()


async def _do_loto_draw(guild, triggered_by="auto"):
    """Effectue le tirage de la loto pour une guild."""
    tickets = loto_get_tickets(guild.id)
    if not tickets:
        loto_record_draw(guild.id, None, 0)
        log.info(f"Loto {guild.name} : aucun ticket, tirage reporté.")
        return

    winning_ticket = random.choice(tickets)
    winner_id = int(winning_ticket["user_id"])
    prize = len(tickets) * LOTO_TICKET_PRICE

    # Crédite le gagnant en banque
    async with eco_lock:
        atomic_hand_bank(winner_id, 0, prize)

    loto_clear_tickets(guild.id)
    loto_record_draw(guild.id, winner_id, prize)

    # Annonce dans le salon de log ou dans un salon autorisé
    channel = None
    log_id = get_log_channel(guild.id)
    if log_id:
        channel = guild.get_channel(int(log_id))
    if not channel:
        allowed = get_allowed_channels(guild.id)
        if allowed:
            channel = guild.get_channel(int(allowed[0]))
    if not channel and guild.text_channels:
        channel = guild.text_channels[0]

    em = discord.Embed(
        title="🎰 Tirage de la loterie !",
        description=(
            f"**{len(tickets)}** tickets en jeu\n"
            f"**Gagnant :** <@{winner_id}>\n"
            f"**Prix :** {prize:,} Ryo (crédités en banque)\n\n"
            f"*Tirage {triggered_by}. Prochaine loto : consulte `*loto` pour la date.*"
        ),
        color=0xf1c40f,
    )
    em.set_footer(text="Velda ・ Loterie")
    if channel:
        try:
            await channel.send(embed=em)
        except discord.HTTPException:
            pass
    log.info(f"Loto {guild.name} : gagnant <@{winner_id}> avec {prize} Ryo.")


# ---- ancien on_voice_state_update n'existait pas, pas besoin de suppression


# ========================= LOG =========================

async def send_log(guild, action, author, target=None, desc=None, color=0x2b2d31):
    channel_id = get_log_channel(guild.id)
    if not channel_id:
        return
    channel = guild.get_channel(int(channel_id))
    if not channel:
        return
    em = discord.Embed(title=f"📋 {action}", color=color)
    em.add_field(name="Modérateur", value=f"{author.mention} (`{author.id}`)", inline=True)
    if target:
        em.add_field(name="Cible", value=f"{target.mention} (`{target.id}`)", inline=True)
    if desc:
        em.add_field(name="Détail", value=desc, inline=False)
    em.set_footer(text=get_french_time())
    try:
        await channel.send(embed=em)
    except discord.HTTPException as e:
        log.warning(f"send_log: impossible d'envoyer dans {channel.id} : {e}")


# ========================= HELP =========================

# Structure centrale : chaque commande porte son rang minimum requis.
# Une catégorie n'apparaît dans le dropdown que si l'utilisateur a accès à au moins une de ses commandes.
# Rangs : 0 = Membre, 1 = WL, 2 = Owner, 3 = Sys, 4 = Buyer.

HELP_CATEGORIES = {
    "eco": {
        "emoji": "💰",
        "label": "Économie",
        "title": "Économie",
        "subtitle": "Gagne des Ryo, gère ta fortune, famme les autres.",
        "sections": [
            ("💵", "Gagner de l'argent", [
                ("daily / dy",                  "Récompense quotidienne",  0),
                ("work / wk",                   "Boulot (1h cooldown)",    0),
            ]),
            ("🏦", "Gérer ta fortune", [
                ("bal / b [@user]",             "Voir ta balance",         0),
                ("dep [somme/all]",             "Déposer en bank",         0),
                ("with [somme/all]",            "Retirer de la bank",      0),
            ]),
            ("⚔️", "Interagir avec les autres", [
                ("give / gv [somme] @user",     "Donner des Ryo",          0),
                ("rob / rb @user",              "Voler (5-30% main)",      0),
                ("fame / fm @user",             "Famer quelqu'un",         0),
            ]),
            ("🎴", "Ton profil", [
                ("profil / pr [@user]",         "Carte de profil visuelle",0),
            ]),
        ],
    },
    "jeux": {
        "emoji": "🎮",
        "label": "Jeux",
        "title": "Jeux",
        "subtitle": "Tente ta chance au casino, vole et pêche des Ryo.",
        "sections": [
            ("🎰", "Casino classique", [
                ("slots / sl <mise/all>",             "Machine à sous",               0),
                ("bj / blackjack <mise/all>",         "Blackjack",                    0),
                ("roulette / rl <mise> <type>",       "Roulette (rouge/noir/0-36)",   0),
            ]),
            ("🎲", "Jeux rapides", [
                ("des / dice <mise>",                 "Lance un dé contre le bot",    0),
                ("pfc / rps <mise> <choix>",          "Pierre-Feuille-Ciseaux",       0),
            ]),
            ("💰", "Cagnotte partagée", [
                ("jackpot / jp <mise/all>",           "Jackpot avec pot partagé",     0),
                ("pot",                               "Voir la cagnotte du jackpot",  0),
            ]),
            ("⏱️", "Annexes", [
                ("fish",                              "Pêche (30min cooldown)",       0),
                ("cooldowns",                         "Voir tous tes cooldowns",      0),
            ]),
        ],
    },
    "shop": {
        "emoji": "🛒",
        "label": "Shop",
        "title": "Shop",
        "subtitle": "Dépense tes Ryo pour des rôles, boosts et cosmétiques.",
        "sections": [
            ("🛍️", "Acheter", [
                ("shop",                    "Voir les items à vendre",      0),
                ("buy <id>",                "Acheter un item",              0),
            ]),
            ("🎒", "Inventaire", [
                ("inv / inventaire [@u]",   "Voir son inventaire",          0),
            ]),
        ],
    },
    "classement": {
        "emoji": "🏆",
        "label": "Classement",
        "title": "Classement",
        "subtitle": "Qui est le plus riche ? Le plus famé ? Le plus actif ?",
        "sections": [
            ("📊", "Voir les tops", [
                ("lb / leaderboard",        "Classement (menu déroulant)",  0),
            ]),
        ],
    },
    "vocal": {
        "emoji": "🎤",
        "label": "Vocal",
        "title": "Vocal",
        "subtitle": "Gains passifs en vocal selon les zones et bonus.",
        "sections": [
            ("🎧", "Stats & infos", [
                ("vocalstats [@user]",      "Tes stats vocales et gains",   0),
                ("zones",                   "Liste des zones à mult.",      0),
            ]),
            ("⚙️", "Config (WL+)", [
                ("vocalconfig",             "Config actuelle des gains",    1),
            ]),
        ],
    },
    "loto": {
        "emoji": "🎰",
        "label": "Loterie",
        "title": "Loterie",
        "subtitle": "Achète des tickets, tente de remporter le gros lot.",
        "sections": [
            ("🎟️", "Participer", [
                ("loto",                    "Voir l'état de la loterie",    0),
                ("loto ticket",             "Acheter un ticket (1000 Ryo)", 0),
            ]),
            ("🔧", "Admin (Sys+)", [
                ("loto tirage",             "Forcer un tirage",             3),
                ("lotodate <durée>",        "Modifier la date du tirage",   3),
            ]),
        ],
    },
    "speciaux": {
        "emoji": "🎁",
        "label": "Spéciaux",
        "title": "Événements spéciaux",
        "subtitle": "Animations & événements réservés aux Owner+.",
        "sections": [
            ("🏛️", "Lancer un event", [
                ("enchere @role",           "Lancer une enchère",           2),
                ("drop [somme]",            "Drop d'argent",                2),
            ]),
            ("⚙️", "Modifier (Sys+)", [
                ("encheredit <minutes>",    "Modifier durée enchère",       3),
            ]),
        ],
    },
    "shop_admin": {
        "emoji": "🛍️",
        "label": "Shop Admin",
        "title": "Shop Admin",
        "subtitle": "Gère les items disponibles dans le shop.",
        "sections": [
            ("➕", "Ajouter & modifier", [
                ("additem <nom>",               "Ajouter un item (modal)",        3),
                ("edititem <id> <champ> <val>", "Modifier un champ d'un item",    3),
            ]),
            ("📦", "Stock & retrait", [
                ("setstock <id> <n/illimite>",  "Réappro / stock illimité",       3),
                ("removeitem <id>",             "Supprimer un item",              3),
            ]),
        ],
    },
    "config": {
        "emoji": "🎚️",
        "label": "Config",
        "title": "Config (Sys+)",
        "subtitle": "Paramètres avancés du bot (gains, cooldowns, zones).",
        "sections": [
            ("🎤", "Vocal & zones", [
                ("setzone #voc <mult>",            "Zone vocale à multiplicateur",    3),
                ("unsetzone #voc",                 "Retirer une zone",                3),
                ("setvocalgain <champ> <valeur>",  "Config des gains vocaux",         3),
            ]),
            ("⏱️", "Jeux", [
                ("setcooldown <jeu> <secondes>",   "Cooldown d'un jeu",               3),
            ]),
        ],
    },
    "admin": {
        "emoji": "🔧",
        "label": "Admin",
        "title": "Administration (Sys+)",
        "subtitle": "Outils de modération et gestion économique.",
        "sections": [
            ("💸", "Économie", [
                ("addmoney @user [somme]",      "Ajouter de l'argent", 3),
                ("removemoney @user [somme]",   "Retirer de l'argent", 3),
                ("resetbal @user",              "Reset balance",       3),
            ]),
            ("✨", "XP & niveaux", [
                ("addxp @user [somme]",         "Ajouter de l'XP",     3),
                ("resetlevel @user",            "Reset niveau/XP",     3),
            ]),
            ("🚫", "Ban bot", [
                ("ban @user",                   "Bannir du bot",       3),
                ("unban @user",                 "Débannir du bot",     3),
            ]),
        ],
    },
    "perms": {
        "emoji": "👥",
        "label": "Permissions",
        "title": "Permissions",
        "subtitle": "Gère les rangs des membres (Owner+).",
        "sections": [
            ("✨", "Whitelist (Owner+)", [
                ("wl @user / unwl @user",       "Gérer la whitelist", 2),
            ]),
            ("⭐", "Owners (Sys+)", [
                ("owner @user / unowner @user", "Gérer les owners",   3),
            ]),
            ("🔧", "Sys (Buyer)", [
                ("sys @user / unsys @user",     "Gérer les sys",      4),
            ]),
        ],
    },
    "system": {
        "emoji": "⚙️",
        "label": "Système",
        "title": "Système",
        "subtitle": "Config système du bot (salons, logs, prefix).",
        "sections": [
            ("📌", "Salons (Sys+)", [
                ("allow #salon",         "Autoriser un salon pour le bot",  3),
                ("unallow #salon",       "Retirer un salon autorisé",       3),
                ("allow",                "Lister les salons autorisés",     3),
                ("setenchere #salon",    "Définir le salon des enchères",   3),
            ]),
            ("🛡️", "Buyer only", [
                ("setlog #salon",        "Définir le salon des logs",       4),
                ("prefix [nouveau]",     "Changer le prefix",               4),
            ]),
        ],
    },
    "hierarchy": {
        "emoji": "📋",
        "label": "Hiérarchie",
        "title": "Hiérarchie",
        "subtitle": "Les différents rangs du bot et leurs pouvoirs.",
        "min_rank": 2,  # Visible qu'à partir d'Owner
        "items": [],
    },
}


def accessible_items(category_key, user_rank):
    """Retourne la liste plate des (syntaxe, description) accessibles au user pour cette catégorie.
    Supporte la structure 'sections' ET l'ancienne 'items' pour rétrocompat."""
    cat = HELP_CATEGORIES.get(category_key, {})
    result = []
    # Nouvelle structure : sections
    for section in cat.get("sections", []):
        _emoji, _title, items = section
        for (syntax, desc, min_rank) in items:
            if user_rank >= min_rank:
                result.append((syntax, desc))
    # Ancienne structure : items plats
    for (syntax, desc, min_rank) in cat.get("items", []):
        if user_rank >= min_rank:
            result.append((syntax, desc))
    return result


def accessible_sections(category_key, user_rank):
    """Retourne les sections [(emoji, title, [(syntax, desc), ...])] filtrées par rang."""
    cat = HELP_CATEGORIES.get(category_key, {})
    result = []
    for section in cat.get("sections", []):
        emoji, title, items = section
        visible = [(syn, desc) for (syn, desc, min_r) in items if user_rank >= min_r]
        if visible:
            result.append((emoji, title, visible))
    return result


def category_visible(category_key, user_rank):
    """Une catégorie est visible si le user peut accéder à au moins une de ses commandes,
    ou si elle a un min_rank explicite qu'il atteint (cas de la hiérarchie)."""
    cat = HELP_CATEGORIES.get(category_key, {})
    if "min_rank" in cat:
        return user_rank >= cat["min_rank"]
    return len(accessible_items(category_key, user_rank)) > 0


def _apply_guild_thumbnail(em, ctx_or_guild):
    """Ajoute l'icône du serveur en thumbnail de l'embed si dispo."""
    guild = getattr(ctx_or_guild, "guild", None) or ctx_or_guild
    if guild and getattr(guild, "icon", None):
        try:
            em.set_thumbnail(url=guild.icon.url)
        except (AttributeError, TypeError):
            pass


def build_category_embed(category_key, user_rank, guild=None):
    """Construit un embed stylé pour la catégorie donnée, filtré au rang du user.
    Utilise la nouvelle structure 'sections' avec groupes + code blocks inline."""
    p = get_prefix_cached()
    cat = HELP_CATEGORIES[category_key]
    emoji = cat.get("emoji", "📋")
    title = cat.get("title", "Commandes")
    subtitle = cat.get("subtitle", "")

    em = discord.Embed(
        title=f"{emoji}  {title}",
        description=subtitle if subtitle else None,
        color=embed_color(),
    )

    if guild:
        _apply_guild_thumbnail(em, guild)

    sections = accessible_sections(category_key, user_rank)
    items_flat = accessible_items(category_key, user_rank)

    if not sections and not items_flat:
        em.add_field(
            name="⛔ Aucune commande accessible",
            value="Tu n'as pas les permissions pour cette catégorie.",
            inline=False,
        )
    elif sections:
        # Nouvelle structure : une field par section avec commandes en code inline
        for s_emoji, s_title, s_items in sections:
            # Formater les commandes en ligne, façon "code inline"
            # Si max 2-3 commandes courtes : sur une ligne. Sinon on fait des lignes de 1-2
            cmd_lines = []
            for syntax, desc in s_items:
                cmd_lines.append(f"`{p}{syntax}` — {desc}")
            em.add_field(
                name=f"{s_emoji} {s_title}",
                value="\n".join(cmd_lines),
                inline=False,
            )
    else:
        # Fallback ancienne structure
        lines = [f"`{p}{syntax}` — {desc}" for syntax, desc in items_flat]
        em.add_field(name="Commandes", value="\n".join(lines), inline=False)

    # Bloc astuce en bas (uniquement pour les catégories de base)
    if category_key in ("eco", "jeux"):
        em.add_field(
            name="💡 Astuce",
            value=(
                "Chaque commande a un raccourci court !\n"
                f"Ex : `{p}wk` au lieu de `{p}work`, `{p}pr` au lieu de `{p}profil`."
            ),
            inline=False,
        )

    em.set_footer(text="Made by gp ・ Velda")
    return em


def build_hierarchy_embed(user_rank, guild=None):
    """Embed hiérarchie — uniquement visible pour Owner+ (min_rank=2)."""
    em = discord.Embed(
        title="📋  Hiérarchie",
        description="Les différents rangs du bot et leurs pouvoirs.",
        color=embed_color(),
    )
    if guild:
        _apply_guild_thumbnail(em, guild)

    levels = [
        (4, "👑", "Buyer",      "Accès total au bot"),
        (3, "🔧", "Sys",        "Config, modération, admin éco"),
        (2, "⭐", "Owner",       "Événements (enchères, drops), gestion whitelist"),
        (1, "✨", "Whitelist",   "Statut privilégié"),
        (0, "👤", "Membre",      "Jeux et commandes éco de base"),
    ]
    for rank, icon, name, desc in levels:
        marker = "  ← **toi**" if rank == user_rank else ""
        em.add_field(
            name=f"{icon} {name}{marker}",
            value=desc,
            inline=False,
        )

    em.set_footer(text="Made by gp ・ Velda")
    return em


def build_home_embed(user_rank, guild=None):
    """Embed d'accueil personnalisé : ne liste que les catégories accessibles au user."""
    p = get_prefix_cached()
    em = discord.Embed(
        title="🏠  Panel d'aide — Velda",
        description=(
            f"Bienvenue sur **Velda**, le bot casino de Meira.\n"
            f"**Prefix :** `{p}` ・ **Ton rang :** {rank_name(user_rank)}\n\n"
            f"*Choisis une catégorie ci-dessous pour voir ses commandes.*"
        ),
        color=embed_color(),
    )
    if guild:
        _apply_guild_thumbnail(em, guild)

    # Liste uniquement les catégories visibles au user, regroupées par thème
    category_descriptions = {
        "eco":        "Bal, daily, dépôts, give, rob...",
        "jeux":       "Slots, BJ, jackpot, roulette, dés, PFC...",
        "shop":       "Acheter des rôles, boosts, items",
        "classement": "Leaderboard (argent, XP, vocal...)",
        "vocal":      "Stats vocales et zones",
        "loto":       "Loterie avec tirages automatiques",
        "speciaux":   "Enchères, drop",
        "shop_admin": "Créer/modifier les items du shop",
        "config":     "Config des zones, gains vocaux, cooldowns",
        "admin":      "Gérer l'argent et l'XP des membres",
        "perms":      "Attribuer les rangs",
        "system":     "Configuration du bot",
        "hierarchy":  "Qui peut faire quoi",
    }

    # Grouper en 2 fields : "Pour toi" (commandes joueur) et "Admin" (pour Sys+)
    player_keys = ["eco", "jeux", "shop", "classement", "vocal", "loto"]
    admin_keys  = ["speciaux", "shop_admin", "config", "admin", "perms", "system", "hierarchy"]

    player_lines = []
    for key in player_keys:
        if category_visible(key, user_rank):
            cat = HELP_CATEGORIES[key]
            player_lines.append(f"{cat['emoji']} **{cat['label']}** — {category_descriptions[key]}")
    if player_lines:
        em.add_field(name="🎮 Pour toi", value="\n".join(player_lines), inline=False)

    admin_lines = []
    for key in admin_keys:
        if category_visible(key, user_rank):
            cat = HELP_CATEGORIES[key]
            admin_lines.append(f"{cat['emoji']} **{cat['label']}** — {category_descriptions[key]}")
    if admin_lines:
        em.add_field(name="🛠️ Staff & Admin", value="\n".join(admin_lines), inline=False)

    em.set_footer(text=f"Made by gp ・ Velda ・ {get_french_time()}")
    return em


def build_embed_for(category_key, user_rank, guild=None):
    """Dispatcher : renvoie l'embed correspondant à la clé demandée."""
    if category_key == "home":
        return build_home_embed(user_rank, guild=guild)
    if category_key == "hierarchy":
        return build_hierarchy_embed(user_rank, guild=guild)
    return build_category_embed(category_key, user_rank, guild=guild)


class HelpDropdown(discord.ui.Select):
    def __init__(self, user_rank, guild=None):
        self.user_rank = user_rank
        self.guild = guild
        options = [discord.SelectOption(label="Accueil", emoji="🏠", value="home")]
        # Ajoute uniquement les catégories dont l'utilisateur peut voir quelque chose
        for key, cat in HELP_CATEGORIES.items():
            if category_visible(key, user_rank):
                options.append(discord.SelectOption(
                    label=cat["label"], emoji=cat["emoji"], value=key
                ))
        super().__init__(
            placeholder="📂 Choisis une catégorie...",
            min_values=1, max_values=1, options=options
        )

    async def callback(self, interaction: discord.Interaction):
        key = self.values[0]
        if key != "home" and not category_visible(key, self.user_rank):
            return await interaction.response.send_message(
                "Tu n'as pas accès à cette catégorie.", ephemeral=True
            )
        await interaction.response.edit_message(
            embed=build_embed_for(key, self.user_rank, guild=self.guild), view=self.view
        )


class HelpView(discord.ui.View):
    def __init__(self, author_id, user_rank, guild=None):
        super().__init__(timeout=120)
        self.author_id = author_id
        self.user_rank = user_rank
        self.guild = guild
        self.add_item(HelpDropdown(user_rank, guild=guild))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Seul celui qui a lancé *help peut naviguer dans son menu
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "Ce menu n'est pas à toi. Fais `*help` pour voir le tien.", ephemeral=True
            )
            return False
        return True

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


@bot.command(name="help")
async def _help(ctx):
    """Envoie le panel d'aide en DM pour ne pas polluer le chat."""
    user_rank = get_rank_db(ctx.author.id)
    view = HelpView(ctx.author.id, user_rank, guild=ctx.guild)
    home_embed = build_home_embed(user_rank, guild=ctx.guild)

    # Tentative d'envoi en DM
    try:
        await ctx.author.send(embed=home_embed, view=view)
        # Confirmation discrète dans le salon : réaction ✉️ + message auto-suppr
        try:
            await ctx.message.add_reaction("✉️")
        except discord.HTTPException:
            pass
        try:
            confirmation = await ctx.send(
                f"📬 {ctx.author.mention}, je t'ai envoyé le panel d'aide en DM."
            )
            await asyncio.sleep(5)
            await confirmation.delete()
        except discord.HTTPException:
            pass
    except discord.Forbidden:
        # DM fermé → fallback dans le salon avec un petit avertissement
        await ctx.send(
            embed=home_embed,
            view=view,
            content=(
                f"⚠️ {ctx.author.mention} je n'ai pas pu t'envoyer le help en DM "
                f"(DM fermés). Voilà ton panel directement ici :"
            ),
        )
    except discord.HTTPException as e:
        log.warning(f"Échec envoi help en DM à {ctx.author} : {e}")
        await ctx.send(embed=home_embed, view=view)


# ========================= SYSTÈME =========================

@bot.command(name="prefix")
async def _prefix(ctx, new_prefix: str = None):
    if not has_min_rank(ctx.author.id, 4):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "Seul le **Buyer** peut changer le prefix."))
    if not new_prefix:
        return await ctx.send(embed=info_embed("Prefix actuel", f"`{get_prefix_cached()}`"))
    set_config("prefix", new_prefix)
    await ctx.send(embed=success_embed("✅ Prefix modifié", f"Nouveau prefix : `{new_prefix}`"))


@bot.command(name="setlog")
async def _setlog(ctx, channel: discord.TextChannel = None):
    if not has_min_rank(ctx.author.id, 4):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "Seul le **Buyer** peut définir les logs."))
    if not channel:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un salon."))
    set_log_channel(ctx.guild.id, channel.id)
    await ctx.send(embed=success_embed("✅ Logs configurés", f"Logs dans {channel.mention}."))


@bot.command(name="setenchere")
async def _setenchere(ctx, channel: discord.TextChannel = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not channel:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un salon."))
    set_enchere_channel(ctx.guild.id, channel.id)
    await ctx.send(embed=success_embed("✅ Salon enchères configuré", f"Enchères dans {channel.mention}."))


# ========================= ALLOWED CHANNELS =========================

async def _resolve_channel(ctx, channel_input):
    """Résout un salon depuis une mention #salon, un ID, ou un nom. Retourne (channel, raw_id)."""
    clean = channel_input.strip("<#>")
    # Tentative ID direct
    try:
        cid = int(clean)
        ch = ctx.guild.get_channel(cid)
        return ch, cid
    except ValueError:
        pass
    # Tentative via converter
    try:
        ch = await commands.TextChannelConverter().convert(ctx, channel_input)
        return ch, ch.id
    except commands.CommandError:
        return None, None


@bot.command(name="allow")
async def _allow(ctx, *, channel_input: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))

    # Sans argument : liste les salons autorisés
    if channel_input is None:
        allowed = get_allowed_channels(ctx.guild.id)
        if not allowed:
            return await ctx.send(embed=info_embed(
                "📋 Aucun salon autorisé",
                "Personne ne peut utiliser le bot en dehors des **Sys+**.\n"
                f"Utilise `{get_prefix_cached()}allow #salon` pour en ajouter un."
            ))
        lines = []
        for cid in allowed:
            channel = ctx.guild.get_channel(int(cid))
            if channel:
                lines.append(f"• {channel.mention} (`{cid}`)")
            else:
                lines.append(f"• *Salon supprimé ou inaccessible* (`{cid}`)")
        return await ctx.send(embed=info_embed(
            f"📋 Salons autorisés ({len(allowed)})",
            "\n".join(lines)
        ))

    channel, raw_id = await _resolve_channel(ctx, channel_input)
    if not channel:
        return await ctx.send(embed=error_embed(
            "❌ Salon introuvable",
            "Mentionne un salon (`#salon`) ou donne son ID."
        ))

    if is_channel_allowed(ctx.guild.id, channel.id):
        return await ctx.send(embed=error_embed(
            "Déjà autorisé",
            f"{channel.mention} est déjà dans la liste des salons autorisés."
        ))

    add_allowed_channel(ctx.guild.id, channel.id, ctx.author.id)
    await ctx.send(embed=success_embed(
        "✅ Salon autorisé",
        f"{channel.mention} est maintenant un salon autorisé pour Velda."
    ))
    await send_log(
        ctx.guild, "Salon autorisé", ctx.author,
        desc=f"Salon : {channel.mention} (`{channel.id}`)",
        color=0x43b581
    )


@bot.command(name="unallow")
async def _unallow(ctx, *, channel_input: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not channel_input:
        return await ctx.send(embed=error_embed(
            "Argument manquant",
            f"Usage : `{get_prefix_cached()}unallow #salon` ou `{get_prefix_cached()}unallow [id]`"
        ))

    channel, raw_id = await _resolve_channel(ctx, channel_input)

    # Même si le salon n'existe plus (supprimé), on permet de nettoyer la DB via son ID
    if not channel:
        if raw_id is not None:
            if remove_allowed_channel(ctx.guild.id, raw_id):
                return await ctx.send(embed=success_embed(
                    "✅ Salon retiré",
                    f"Salon `{raw_id}` retiré de la liste (salon supprimé ou inaccessible)."
                ))
            return await ctx.send(embed=error_embed(
                "Pas dans la liste",
                f"Le salon `{raw_id}` n'est pas dans la liste des salons autorisés."
            ))
        return await ctx.send(embed=error_embed(
            "❌ Salon introuvable",
            "Mentionne un salon ou donne son ID."
        ))

    if not remove_allowed_channel(ctx.guild.id, channel.id):
        return await ctx.send(embed=error_embed(
            "Pas dans la liste",
            f"{channel.mention} n'est pas dans la liste des salons autorisés."
        ))

    await ctx.send(embed=success_embed(
        "✅ Salon retiré",
        f"{channel.mention} n'est plus un salon autorisé."
    ))
    await send_log(
        ctx.guild, "Salon retiré", ctx.author,
        desc=f"Salon : {channel.mention} (`{channel.id}`)",
        color=0xf04747
    )


# ========================= RANGS =========================

@bot.command(name="sys")
async def _sys(ctx, *, user_input: str = None):
    # Sans argument : liste les sys (Buyer seul)
    if user_input is None:
        if not has_min_rank(ctx.author.id, 4):
            return await ctx.send(embed=error_embed("❌ Permission refusée", "**Buyer** requis."))
        ids = get_ranks_by_level(3)
        if not ids:
            return await ctx.send(embed=info_embed("📋 Liste Sys", "Aucun sys."))
        return await ctx.send(embed=info_embed(f"📋 Liste Sys ({len(ids)})", "\n".join([f"<@{uid}>" for uid in ids])))

    if not has_min_rank(ctx.author.id, 4):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Buyer** requis."))

    display_obj, user_id = await resolve_user_or_id(ctx, user_input)
    if user_id is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Donne une mention, un nom ou un ID."))

    if get_rank_db(user_id) == 3:
        return await ctx.send(embed=error_embed("Déjà Sys", f"{format_user_display(display_obj, user_id)} est déjà sys."))
    set_rank_db(user_id, 3)
    await ctx.send(embed=success_embed("✅ Sys ajouté", f"{format_user_display(display_obj, user_id)} ajouté en **sys**."))


@bot.command(name="unsys")
async def _unsys(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 4):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Buyer** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un utilisateur ou donne son ID."))

    display_obj, user_id = await resolve_user_or_id(ctx, user_input)
    if user_id is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Donne une mention, un nom ou un ID."))

    if get_rank_db(user_id) != 3:
        return await ctx.send(embed=error_embed("Pas Sys", f"{format_user_display(display_obj, user_id)} n'est pas sys."))
    set_rank_db(user_id, 0)
    await ctx.send(embed=success_embed("✅ Sys retiré", f"{format_user_display(display_obj, user_id)} retiré des **sys**."))


@bot.command(name="owner")
async def _owner(ctx, *, user_input: str = None):
    if user_input is None:
        if not has_min_rank(ctx.author.id, 3):
            return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
        ids = get_ranks_by_level(2)
        if not ids:
            return await ctx.send(embed=info_embed("📋 Liste Owner", "Aucun owner."))
        return await ctx.send(embed=info_embed(f"📋 Liste Owner ({len(ids)})", "\n".join([f"<@{uid}>" for uid in ids])))

    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))

    display_obj, user_id = await resolve_user_or_id(ctx, user_input)
    if user_id is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Donne une mention, un nom ou un ID."))

    if get_rank_db(user_id) >= 3:
        return await ctx.send(embed=error_embed("❌ Erreur", f"{format_user_display(display_obj, user_id)} a un rang supérieur ou égal."))
    set_rank_db(user_id, 2)
    await ctx.send(embed=success_embed("✅ Owner ajouté", f"{format_user_display(display_obj, user_id)} ajouté en **owner**."))


@bot.command(name="unowner")
async def _unowner(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un utilisateur ou donne son ID."))

    display_obj, user_id = await resolve_user_or_id(ctx, user_input)
    if user_id is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Donne une mention, un nom ou un ID."))

    if get_rank_db(user_id) != 2:
        return await ctx.send(embed=error_embed("Pas Owner", f"{format_user_display(display_obj, user_id)} n'est pas owner."))
    set_rank_db(user_id, 0)
    await ctx.send(embed=success_embed("✅ Owner retiré", f"{format_user_display(display_obj, user_id)} retiré des **owners**."))


@bot.command(name="wl")
async def _wl(ctx, *, user_input: str = None):
    if user_input is None:
        if not has_min_rank(ctx.author.id, 2):
            return await ctx.send(embed=error_embed("❌ Permission refusée", "**Owner+** requis."))
        ids = get_ranks_by_level(1)
        if not ids:
            return await ctx.send(embed=info_embed("📋 Whitelist", "Aucun wl."))
        return await ctx.send(embed=info_embed(f"📋 Whitelist ({len(ids)})", "\n".join([f"<@{uid}>" for uid in ids])))

    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Owner+** requis."))

    display_obj, user_id = await resolve_user_or_id(ctx, user_input)
    if user_id is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Donne une mention, un nom ou un ID."))

    if get_rank_db(user_id) >= 2:
        return await ctx.send(embed=error_embed("❌ Erreur", f"{format_user_display(display_obj, user_id)} a un rang supérieur ou égal."))
    set_rank_db(user_id, 1)
    await ctx.send(embed=success_embed("✅ WL ajouté", f"{format_user_display(display_obj, user_id)} ajouté à la **whitelist**."))


@bot.command(name="unwl")
async def _unwl(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Owner+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un utilisateur ou donne son ID."))

    display_obj, user_id = await resolve_user_or_id(ctx, user_input)
    if user_id is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Donne une mention, un nom ou un ID."))

    if get_rank_db(user_id) != 1:
        return await ctx.send(embed=error_embed("Pas WL", f"{format_user_display(display_obj, user_id)} n'est pas wl."))
    set_rank_db(user_id, 0)
    await ctx.send(embed=success_embed("✅ WL retiré", f"{format_user_display(display_obj, user_id)} retiré de la **whitelist**."))


# ========================= BAN BOT =========================

@bot.command(name="ban")
async def _ban(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un utilisateur ou donne son ID."))

    display_obj, user_id = await resolve_user_or_id(ctx, user_input)
    if user_id is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Donne une mention, un nom ou un ID."))

    if is_bot_banned(user_id):
        return await ctx.send(embed=error_embed("Déjà banni", f"{format_user_display(display_obj, user_id)} est déjà banni du bot."))
    add_bot_ban(user_id, ctx.author.id)
    await ctx.send(embed=success_embed("⛔ Banni du bot", f"{format_user_display(display_obj, user_id)} ne peut plus utiliser **Velda**."))


@bot.command(name="unban")
async def _unban(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un utilisateur ou donne son ID."))

    display_obj, user_id = await resolve_user_or_id(ctx, user_input)
    if user_id is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Donne une mention, un nom ou un ID."))

    if not is_bot_banned(user_id):
        return await ctx.send(embed=error_embed("Pas banni", f"{format_user_display(display_obj, user_id)} n'est pas banni du bot."))
    remove_bot_ban(user_id)
    await ctx.send(embed=success_embed("✅ Débanni", f"{format_user_display(display_obj, user_id)} peut à nouveau utiliser **Velda**."))


# ========================= ADMIN ECO =========================

def _parse_admin_args(user_input):
    """
    Sépare '@user 5000' ou 'id 5000' en (user_part, amount_int).
    Retourne (user_part, None) si pas de montant, (None, None) si input vide.
    """
    if not user_input:
        return None, None
    parts = user_input.rsplit(" ", 1)
    if len(parts) == 1:
        return parts[0], None
    try:
        amount = int(parts[1].replace(",", "").replace(" ", ""))
        return parts[0], amount
    except ValueError:
        # Le dernier token n'est pas un nombre : tout l'input est le user_part
        return user_input, None


@bot.command(name="addmoney")
async def _addmoney(ctx, *, args: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    user_part, amount = _parse_admin_args(args)
    if not user_part or amount is None or amount <= 0:
        return await ctx.send(embed=error_embed("Arguments manquants", "Usage : `*addmoney @user [somme]`"))

    display_obj, user_id = await resolve_user_or_id(ctx, user_part)
    if user_id is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Donne une mention, un nom ou un ID."))

    async with eco_lock:
        atomic_hand_delta(user_id, amount, min_hand=0)
    await ctx.send(embed=success_embed("✅ Argent ajouté", f"+{format_ryo(amount)} ajouté à {format_user_display(display_obj, user_id)}."))


@bot.command(name="removemoney")
async def _removemoney(ctx, *, args: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    user_part, amount = _parse_admin_args(args)
    if not user_part or amount is None or amount <= 0:
        return await ctx.send(embed=error_embed("Arguments manquants", "Usage : `*removemoney @user [somme]`"))

    display_obj, user_id = await resolve_user_or_id(ctx, user_part)
    if user_id is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Donne une mention, un nom ou un ID."))

    async with eco_lock:
        eco = get_economy(user_id)
        new_hand = max(0, eco["hand"] - amount)
        update_economy(user_id, hand=new_hand)
    await ctx.send(embed=success_embed("✅ Argent retiré", f"-{format_ryo(amount)} retiré à {format_user_display(display_obj, user_id)}."))


@bot.command(name="resetbal")
async def _resetbal(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un utilisateur ou donne son ID."))

    display_obj, user_id = await resolve_user_or_id(ctx, user_input)
    if user_id is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Donne une mention, un nom ou un ID."))

    async with eco_lock:
        update_economy(user_id, hand=0, bank=0)
    await ctx.send(embed=success_embed("✅ Balance reset", f"La balance de {format_user_display(display_obj, user_id)} a été remise à 0."))


@bot.command(name="addxp")
async def _addxp(ctx, *, args: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    user_part, amount = _parse_admin_args(args)
    if not user_part or amount is None or amount <= 0:
        return await ctx.send(embed=error_embed("Arguments manquants", "Usage : `*addxp @user [somme]`"))

    display_obj, user_id = await resolve_user_or_id(ctx, user_part)
    if user_id is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Donne une mention, un nom ou un ID."))

    await add_xp(ctx, user_id, amount)
    await ctx.send(embed=success_embed("✅ XP ajouté", f"+{amount} XP ajouté à {format_user_display(display_obj, user_id)}."))


@bot.command(name="resetlevel")
async def _resetlevel(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un utilisateur ou donne son ID."))

    display_obj, user_id = await resolve_user_or_id(ctx, user_input)
    if user_id is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Donne une mention, un nom ou un ID."))

    async with eco_lock:
        update_economy(user_id, xp=0, level=0)
    await ctx.send(embed=success_embed("✅ Niveau reset", f"Le niveau de {format_user_display(display_obj, user_id)} a été remis à 0."))


# ========================= CARTE DE PROFIL (IMAGE GÉNÉRÉE) =========================

# URL du background (hardcodé, toujours le même pour tout le serveur)
PROFILE_BG_URL = "https://media.discordapp.net/attachments/1494413317790371862/1497672471497670736/ade86e26-5e91-4608-be63-2ec9eef506d9.png?ex=69ee5fae&is=69ed0e2e&hm=71e19b4e4e6b6858b0053d9437276109717cff74d4fa21d5d0506fb1ad353d21&=&format=webp&quality=lossless&width=550&height=310"

# Paths des fonts (Linux / Railway / VPS standard)
_FONT_BOLD_CANDIDATES = [
    "DejaVuSans-Bold.ttf",  # Racine du repo (priorité 1)
    "./DejaVuSans-Bold.ttf",
    "fonts/DejaVuSans-Bold.ttf",  # Dossier fonts/ du repo
    "./fonts/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
    "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
]
_FONT_EMOJI_CANDIDATES = [
    "NotoColorEmoji.ttf",  # Racine du repo (priorité 1)
    "./NotoColorEmoji.ttf",
    "fonts/NotoColorEmoji.ttf",  # Dossier fonts/ du repo
    "./fonts/NotoColorEmoji.ttf",
    "/usr/share/fonts/truetype/noto/NotoColorEmoji.ttf",
    "/usr/share/fonts/noto/NotoColorEmoji.ttf",
]


def _find_font(candidates):
    for p in candidates:
        if os.path.exists(p):
            return p
    return None


FONT_BOLD_PATH = _find_font(_FONT_BOLD_CANDIDATES)
FONT_EMOJI_PATH = _find_font(_FONT_EMOJI_CANDIDATES)

# Log au démarrage pour débug
if FONT_BOLD_PATH:
    log.info(f"[profil] Font principale : {FONT_BOLD_PATH}")
else:
    log.warning("[profil] ⚠️ Aucune font Bold trouvée, fallback sur default Pillow (moche)")
if FONT_EMOJI_PATH:
    log.info(f"[profil] Font emoji : {FONT_EMOJI_PATH}")
else:
    log.warning("[profil] ⚠️ Aucune font Emoji trouvée, les emojis ne s'afficheront PAS. "
                "Ajoute 'fonts/NotoColorEmoji.ttf' dans ton repo.")


# Noms stylés des jeux pour "FAVORI"
GAME_DISPLAY_NAMES = {
    "slots":    "Slots",
    "jackpot":  "Jackpot",
    "roulette": "Roulette",
    "des":      "Dés",
    "pfc":      "PFC",
    "bj":       "Blackjack",
}


def _load_font(size, bold=True):
    if FONT_BOLD_PATH:
        return ImageFont.truetype(FONT_BOLD_PATH, size)
    return ImageFont.load_default()


def _draw_emoji(card, emoji, xy, target_size):
    """Dessine un emoji couleur via la font Noto Color (109px native)."""
    if not FONT_EMOJI_PATH:
        return
    tmp = Image.new("RGBA", (128, 128), (0, 0, 0, 0))
    try:
        font = ImageFont.truetype(FONT_EMOJI_PATH, 109)
        ImageDraw.Draw(tmp).text((0, 0), emoji, embedded_color=True, font=font)
    except Exception:
        return
    tmp = tmp.resize((target_size, target_size), Image.LANCZOS)
    card.paste(tmp, xy, tmp)


def _rounded_box(size, radius, fill, border_color=None, border_width=0):
    img = Image.new("RGBA", size, (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    d.rounded_rectangle([0, 0, size[0] - 1, size[1] - 1], radius=radius, fill=fill,
                        outline=border_color, width=border_width)
    return img


def _prepare_bg(bg_bytes, w, h):
    """Charge un background depuis bytes, resize+crop pour remplir w×h."""
    bg = Image.open(io.BytesIO(bg_bytes)).convert("RGBA")
    ratio = bg.width / bg.height
    target = w / h
    if ratio > target:
        new_h = h
        new_w = int(bg.width * (h / bg.height))
        bg = bg.resize((new_w, new_h), Image.LANCZOS)
        left = (new_w - w) // 2
        bg = bg.crop((left, 0, left + w, h))
    else:
        new_w = w
        new_h = int(bg.height * (w / bg.width))
        bg = bg.resize((new_w, new_h), Image.LANCZOS)
        top = (new_h - h) // 2
        bg = bg.crop((0, top, w, top + h))
    return bg


def _draw_text_out(draw, xy, text, font, fill="white", outline="black", w=1):
    x, y = xy
    for dx in range(-w, w + 1):
        for dy in range(-w, w + 1):
            if dx or dy:
                draw.text((x + dx, y + dy), text, font=font, fill=outline)
    draw.text(xy, text, font=font, fill=fill)


async def _fetch_url_bytes(url, timeout=10):
    """Télécharge une URL et retourne les bytes. None si erreur."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=timeout) as resp:
                if resp.status == 200:
                    return await resp.read()
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        log.warning(f"fetch_url_bytes erreur {url} : {e}")
    return None


async def generate_profile_card_image(
    username, level, xp_cur, xp_need,
    rank_position, bank, hand, games, winrate, fav_game,
    fame, bg_url=None,
):
    """
    Génère la carte de profil en mémoire et retourne un BytesIO prêt à être envoyé.
    Si bg_url est None ou ne peut être chargé, utilise un background solide sombre.
    """
    W, H = 780, 440

    # Charger le background
    card = None
    if bg_url:
        bg_bytes = await _fetch_url_bytes(bg_url)
        if bg_bytes:
            try:
                card = _prepare_bg(bg_bytes, W, H)
            except Exception as e:
                log.warning(f"Parsing BG failed : {e}")
                card = None
    if card is None:
        # Fallback : fond solide sombre
        card = Image.new("RGBA", (W, H), (30, 30, 40, 255))

    # === LVL (haut gauche) ===
    box = _rounded_box((220, 70), 18, (0, 0, 0, 150), (255, 255, 255, 255), 3)
    card.paste(box, (20, 18), box)
    d = ImageDraw.Draw(card)
    d.text((40, 26), "LVL", font=_load_font(32), fill="white")
    d.text((130, 18), str(level), font=_load_font(42), fill="white")

    # === Pseudo (haut droite) ===
    box = _rounded_box((440, 70), 18, (0, 0, 0, 150), (255, 255, 255, 255), 3)
    card.paste(box, (W - 460, 18), box)
    d = ImageDraw.Draw(card)
    pf = _load_font(36)
    while True:
        bb = d.textbbox((0, 0), username, font=pf)
        if bb[2] - bb[0] <= 410 or pf.size <= 18:
            break
        pf = _load_font(pf.size - 2)
    tw = bb[2] - bb[0]
    d.text((W - 460 + (440 - tw) // 2, 24), username, font=pf, fill="white")

    # === CLASSEMENT (sous LVL) ===
    box = _rounded_box((220, 60), 16, (0, 0, 0, 150), (255, 255, 255, 255), 3)
    card.paste(box, (20, 100), box)
    _draw_emoji(card, "🏆", (32, 108), 40)
    d = ImageDraw.Draw(card)
    rank_text = f"#{rank_position}"
    rf = _load_font(28)
    bb = d.textbbox((0, 0), rank_text, font=rf)
    tw = bb[2] - bb[0]
    d.text((80 + (140 - tw) // 2, 112), rank_text, font=rf, fill=(255, 215, 0))

    # === FAME (droite sous pseudo) ===
    box = _rounded_box((180, 60), 16, (0, 0, 0, 150), (255, 255, 255, 255), 3)
    card.paste(box, (W - 200, 100), box)
    d = ImageDraw.Draw(card)
    fame_str = f"{fame:,}".replace(",", " ")
    d.text((W - 185, 110), fame_str, font=_load_font(32), fill="white")
    _draw_emoji(card, "⭐", (W - 105, 108), 40)

    # === Stats (bas gauche) ===
    box = _rounded_box((340, 200), 18, (0, 0, 0, 150), (255, 255, 255, 255), 3)
    card.paste(box, (20, H - 250), box)
    stats = [
        ("💵", "EN MAIN", f"{hand:,}".replace(",", " ")),
        ("🪙", "BANQUE", f"{bank:,}".replace(",", " ")),
        ("🎮", "PARTIES", f"{games:,}".replace(",", " ")),
        ("📊", "WINRATE", f"{winrate:.1f}%"),
    ]
    sf = _load_font(22)
    for i, (ic, lb, val) in enumerate(stats):
        y = H - 235 + i * 45
        _draw_emoji(card, ic, (38, y - 2), 30)
        d = ImageDraw.Draw(card)
        d.text((78, y), f"{lb} :", font=sf, fill="white")
        d.text((210, y), val, font=sf, fill=(255, 215, 0))

    # === Favori (bas droite) ===
    box = _rounded_box((380, 80), 16, (0, 0, 0, 150), (255, 255, 255, 255), 3)
    card.paste(box, (W - 400, H - 160), box)
    _draw_emoji(card, "🎯", (W - 385, H - 143), 36)
    d = ImageDraw.Draw(card)
    fav_display = GAME_DISPLAY_NAMES.get(fav_game, fav_game) if fav_game else "—"
    d.text((W - 340, H - 138), f"FAVORI : {fav_display}", font=_load_font(24), fill="white")

    # === Barre XP ===
    bx, by = 30, H - 45
    bw, bh = W - 60, 22
    bar = _rounded_box((bw, bh), bh // 2, (0, 0, 0, 170), (255, 255, 255, 255), 2)
    card.paste(bar, (bx, by), bar)
    pct = xp_cur / xp_need if xp_need > 0 else 0
    pct = min(1.0, max(0.0, pct))
    fw = int((bw - 4) * pct)
    if fw > bh:
        fill_b = _rounded_box((fw, bh - 4), (bh - 4) // 2, (255, 215, 0, 255))
        card.paste(fill_b, (bx + 2, by + 2), fill_b)
    elif fw > 0:
        d = ImageDraw.Draw(card)
        d.rectangle([bx + 2, by + 2, bx + 2 + fw, by + bh - 2], fill=(255, 215, 0))
    d = ImageDraw.Draw(card)
    xf = _load_font(13)
    xp_text = f"{xp_cur}/{xp_need} XP"
    bb = d.textbbox((0, 0), xp_text, font=xf)
    tw = bb[2] - bb[0]
    _draw_text_out(d, (bx + (bw - tw) // 2, by + 4), xp_text, xf, "white", "black", 1)

    # Export en bytes
    buf = io.BytesIO()
    card.convert("RGB").save(buf, format="PNG", optimize=True)
    buf.seek(0)
    return buf


@bot.command(name="profil", aliases=["profile", "pr"])
async def _profil(ctx, *, user_input: str = None):
    """Affiche la carte de profil d'un utilisateur (soi-même par défaut)."""
    if await check_ban(ctx):
        return

    if not PROFILE_CARD_AVAILABLE:
        return await ctx.send(embed=error_embed(
            "❌ Feature indisponible",
            "La carte de profil nécessite **Pillow** et **aiohttp**.\n"
            "Ajoute-les à `requirements.txt` :\n"
            "```\nPillow>=10.0.0\naiohttp>=3.9.0\n```"
        ))

    # Cible : soi-même ou quelqu'un d'autre
    if user_input:
        display_obj, user_id = await resolve_user_or_id(ctx, user_input)
        if user_id is None:
            return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))
    else:
        display_obj = ctx.author
        user_id = ctx.author.id

    # Fetch des data
    eco = get_economy(user_id)
    plays, wins, fav_game = get_player_stats(user_id)
    rank_pos, rank_total = get_user_rank(user_id)

    level = eco.get("level", 0)
    xp_cur = eco.get("xp", 0)
    xp_need = xp_for_level(level + 1) if level < 100 else xp_for_level(level)
    bank = eco.get("bank", 0)
    hand = eco.get("hand", 0)
    fame = eco.get("fame", 0)
    winrate = (wins / plays * 100) if plays > 0 else 0.0

    # Pseudo à afficher
    if display_obj and hasattr(display_obj, "display_name"):
        username = display_obj.display_name
    elif display_obj and hasattr(display_obj, "name"):
        username = display_obj.name
    else:
        username = f"User {user_id}"

    # Message d'attente (la génération peut prendre 2-3s)
    async with ctx.typing():
        try:
            img_buf = await generate_profile_card_image(
                username=username,
                level=level,
                xp_cur=xp_cur,
                xp_need=xp_need,
                rank_position=rank_pos,
                bank=bank,
                hand=hand,
                games=plays,
                winrate=winrate,
                fav_game=fav_game,
                fame=fame,
                bg_url=PROFILE_BG_URL,
            )
        except Exception as e:
            log.error(f"generate_profile_card_image erreur : {e}\n{traceback.format_exc()}")
            return await ctx.send(embed=error_embed(
                "❌ Erreur de génération",
                f"Impossible de créer la carte : `{type(e).__name__}: {e}`"
            ))

    file = discord.File(img_buf, filename=f"profil_{user_id}.png")
    await ctx.send(file=file)


# ========================= ÉCONOMIE =========================

@bot.command(name="bal", aliases=["b"])
async def _bal(ctx, *, user_input: str = None):
    if await check_ban(ctx):
        return

    target = ctx.author
    if user_input:
        resolved = await resolve_member(ctx, user_input)
        target = resolved if resolved else ctx.author

    eco = get_economy(target.id)
    xp_needed = xp_for_level(eco["level"] + 1) if eco["level"] < 100 else 0
    xp_progress = eco["xp"] - xp_for_level(eco["level"]) if eco["level"] > 0 else eco["xp"]
    xp_required = xp_needed - xp_for_level(eco["level"]) if eco["level"] > 0 else xp_needed
    xp_display = f"{xp_progress} / {xp_required}" if eco["level"] < 100 else "MAX"

    lines = [
        f"{target.mention} possède",
        "",
        f"🟡 **{format_ryo(eco['hand'])}** en poche",
        f"🏦 **{format_ryo(eco['bank'])}** en banque",
        f"⭐ **{eco['fame']}** point{'s' if eco['fame'] != 1 else ''} de fame",
        f"🎯 Niveau **{eco['level']}** / 100  ・  ✨ XP {xp_display}",
    ]
    if eco.get("escrow", 0) > 0:
        lines.append(f"🔒 **{format_ryo(eco['escrow'])}** en escrow (enchère)")

    em = discord.Embed(
        title=target.display_name,
        description="\n".join(lines),
        color=embed_color(),
    )
    em.set_thumbnail(url=target.display_avatar.url)
    em.set_footer(text="Velda")
    await ctx.send(embed=em)


@bot.command(name="daily", aliases=["dy"])
async def _daily(ctx):
    if await check_ban(ctx):
        return
    async with eco_lock:
        eco = get_economy(ctx.author.id)
        now = datetime.now(PARIS_TZ)

        if eco["last_daily"]:
            last = datetime.fromisoformat(eco["last_daily"])
            diff = now - last
            if diff.total_seconds() < 86400:
                remaining = 86400 - diff.total_seconds()
                h = int(remaining // 3600)
                m = int((remaining % 3600) // 60)
                return await ctx.send(embed=error_embed("⏰ Cooldown", f"Reviens dans **{h}h {m}min** pour ton daily."))

        amount = random.randint(10000, 30000)
        update_economy(ctx.author.id, hand=eco["hand"] + amount, last_daily=now.isoformat())
    await add_xp(ctx, ctx.author.id, 50)
    desc = (
        f"{ctx.author.mention} a récupéré son daily\n\n"
        f"🟡 **+{format_ryo(amount)}** en poche\n"
        f"✨ **+50 XP**"
    )
    await ctx.send(embed=action_embed(ctx.author, desc, color=0x43b581))


@bot.command(name="dep")
async def _dep(ctx, amount_str: str = None):
    if await check_ban(ctx):
        return
    if not amount_str:
        return await ctx.send(embed=error_embed("Argument manquant", "Usage : `*dep [somme/all]`"))
    async with eco_lock:
        eco = get_economy(ctx.author.id)
        amount = parse_amount(amount_str, eco["hand"])
        if amount is None or amount <= 0:
            return await ctx.send(embed=error_embed("Montant invalide", "Donne un montant valide ou `all`."))
        if amount > eco["hand"]:
            return await ctx.send(embed=error_embed("Fonds insuffisants", f"Tu n'as que {format_ryo(eco['hand'])} en poche."))
        if not atomic_hand_bank(ctx.author.id, -amount, +amount):
            return await ctx.send(embed=error_embed("Erreur", "Le dépôt a échoué, réessaie."))
        new_hand = eco["hand"] - amount
        new_bank = eco["bank"] + amount
    desc = (
        f"{ctx.author.mention} a déposé en banque\n\n"
        f"🏦 **+{format_ryo(amount)}** déposés\n"
        f"🟡 **{format_ryo(new_hand)}** en poche  ・  🏦 **{format_ryo(new_bank)}** en banque"
    )
    await ctx.send(embed=action_embed(ctx.author, desc, color=0x43b581))


@bot.command(name="withdraw", aliases=["with"])
async def _withdraw(ctx, amount_str: str = None):
    if await check_ban(ctx):
        return
    if not amount_str:
        return await ctx.send(embed=error_embed("Argument manquant", "Usage : `*with [somme/all]`"))
    async with eco_lock:
        eco = get_economy(ctx.author.id)
        amount = parse_amount(amount_str, eco["bank"])
        if amount is None or amount <= 0:
            return await ctx.send(embed=error_embed("Montant invalide", "Donne un montant valide ou `all`."))
        if amount > eco["bank"]:
            return await ctx.send(embed=error_embed("Fonds insuffisants", f"Tu n'as que {format_ryo(eco['bank'])} en banque."))
        if not atomic_hand_bank(ctx.author.id, +amount, -amount):
            return await ctx.send(embed=error_embed("Erreur", "Le retrait a échoué, réessaie."))
        new_hand = eco["hand"] + amount
        new_bank = eco["bank"] - amount
    desc = (
        f"{ctx.author.mention} a retiré de la banque\n\n"
        f"🟡 **+{format_ryo(amount)}** retirés\n"
        f"🟡 **{format_ryo(new_hand)}** en poche  ・  🏦 **{format_ryo(new_bank)}** en banque"
    )
    await ctx.send(embed=action_embed(ctx.author, desc, color=0x43b581))


@bot.command(name="give", aliases=["gv"])
async def _give(ctx, amount_str: str = None, *, user_input: str = None):
    if await check_ban(ctx):
        return
    if not amount_str or not user_input:
        return await ctx.send(embed=error_embed("Arguments manquants", "Usage : `*give [somme] @user`"))

    target = await resolve_member(ctx, user_input)
    if not target:
        return await ctx.send(embed=error_embed("❌ Introuvable", "Impossible de trouver cet utilisateur."))
    if target == ctx.author:
        return await ctx.send(embed=error_embed("❌ Erreur", "Tu ne peux pas te donner de l'argent à toi-même."))
    # FIX: interdire les dons aux comptes bot-banned
    if is_bot_banned(target.id):
        return await ctx.send(embed=error_embed("❌ Cible bannie", f"{target.mention} est banni du bot."))
    if target.bot:
        return await ctx.send(embed=error_embed("❌ Bot", "Tu ne peux pas donner de l'argent à un bot."))

    async with eco_lock:
        eco = get_economy(ctx.author.id)
        amount = parse_amount(amount_str, eco["hand"])
        if amount is None or amount <= 0:
            return await ctx.send(embed=error_embed("Montant invalide", "Donne un montant valide."))
        if amount > eco["hand"]:
            return await ctx.send(embed=error_embed("Fonds insuffisants", f"Tu n'as que {format_ryo(eco['hand'])} en poche."))
        # FIX: transfert atomique (évite duplication/perte en cas de concurrence)
        if not atomic_transfer(ctx.author.id, target.id, amount):
            return await ctx.send(embed=error_embed("Erreur", "Le transfert a échoué, réessaie."))
    desc = (
        f"{ctx.author.mention} a donné à {target.mention}\n\n"
        f"🟡 **{format_ryo(amount)}** transférés"
    )
    await ctx.send(embed=action_embed(ctx.author, desc, color=0x43b581))


@bot.command(name="rob", aliases=["rb"])
async def _rob(ctx, *, user_input: str = None):
    if await check_ban(ctx):
        return
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne quelqu'un ou donne son ID."))

    target = await resolve_member(ctx, user_input)
    if not target:
        return await ctx.send(embed=error_embed("❌ Introuvable", "Impossible de trouver cet utilisateur."))
    if target == ctx.author:
        return await ctx.send(embed=error_embed("❌ Erreur", "Tu ne peux pas te voler toi-même."))
    if target.bot:
        return await ctx.send(embed=error_embed("❌ Bot", "Tu ne peux pas voler un bot."))

    async with eco_lock:
        eco_author = get_economy(ctx.author.id)
        now = datetime.now(PARIS_TZ)

        # FIX: cooldown sur rob (1h) pour empêcher le spam
        if eco_author.get("last_rob"):
            last = datetime.fromisoformat(eco_author["last_rob"])
            diff = now - last
            if diff.total_seconds() < ROB_COOLDOWN:
                remaining = ROB_COOLDOWN - diff.total_seconds()
                m = int(remaining // 60)
                s = int(remaining % 60)
                return await ctx.send(embed=error_embed("⏰ Cooldown", f"Reviens dans **{m}min {s}s** pour voler à nouveau."))

        eco_target = get_economy(target.id)
        if eco_target["hand"] <= 0:
            # On met quand même le cooldown pour éviter le scan de portefeuilles
            update_economy(ctx.author.id, last_rob=now.isoformat())
            return await ctx.send(embed=error_embed("❌ Pas d'argent", f"{target.mention} n'a rien en main."))

        percent = random.randint(5, 30) / 100
        stolen = max(1, int(eco_target["hand"] * percent))

        if not atomic_transfer(target.id, ctx.author.id, stolen):
            return await ctx.send(embed=error_embed("❌ Échec", "Le vol a échoué."))
        update_economy(ctx.author.id, last_rob=now.isoformat())

    await add_xp(ctx, ctx.author.id, 20)
    desc = (
        f"{ctx.author.mention} a volé {target.mention} !\n\n"
        f"🥷 **{format_ryo(stolen)}** dérobés ({int(percent*100)}%)\n"
        f"✨ **+20 XP**"
    )
    await ctx.send(embed=action_embed(ctx.author, desc, color=0x43b581))


@bot.command(name="fame", aliases=["fm"])
async def _fame(ctx, *, user_input: str = None):
    if await check_ban(ctx):
        return
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne quelqu'un ou donne son ID."))

    target = await resolve_member(ctx, user_input)
    if not target:
        return await ctx.send(embed=error_embed("❌ Introuvable", "Impossible de trouver cet utilisateur."))
    if target == ctx.author:
        return await ctx.send(embed=error_embed("❌ Erreur", "Tu ne peux pas te famer toi-même."))
    if target.bot:
        return await ctx.send(embed=error_embed("❌ Bot", "Tu ne peux pas famer un bot."))

    async with eco_lock:
        eco = get_economy(ctx.author.id)
        now = datetime.now(PARIS_TZ)
        if eco["last_fame"]:
            last = datetime.fromisoformat(eco["last_fame"])
            diff = now - last
            if diff.total_seconds() < 7200:
                remaining = 7200 - diff.total_seconds()
                h = int(remaining // 3600)
                m = int((remaining % 3600) // 60)
                return await ctx.send(embed=error_embed("⏰ Cooldown", f"Reviens dans **{h}h {m}min** pour famer."))

        eco_target = get_economy(target.id)
        new_fame = eco_target["fame"] + 1
        update_economy(target.id, fame=new_fame)
        update_economy(ctx.author.id, last_fame=now.isoformat())
    desc = (
        f"{ctx.author.mention} a famé {target.mention}\n\n"
        f"⭐ **+1 fame**  ・  Total : **{new_fame}** point{'s' if new_fame != 1 else ''} de fame"
    )
    await ctx.send(embed=action_embed(ctx.author, desc, color=0x43b581))


# ========================= JEUX =========================

@bot.command(name="work", aliases=["wk"])
async def _work(ctx):
    if await check_ban(ctx):
        return
    async with eco_lock:
        eco = get_economy(ctx.author.id)
        now = datetime.now(PARIS_TZ)

        if eco["last_work"]:
            last = datetime.fromisoformat(eco["last_work"])
            diff = now - last
            if diff.total_seconds() < 3600:
                remaining = 3600 - diff.total_seconds()
                m = int(remaining // 60)
                return await ctx.send(embed=error_embed("⏰ Cooldown", f"Reviens dans **{m}min** pour travailler."))

        jobs = [
            ("livreur", "Tu as livré des colis sous la pluie toute la journée 📦"),
            ("cuisinier", "Tu as préparé 200 plats dans un restaurant bondé 🍳"),
            ("développeur", "Tu as corrigé des bugs jusqu'à 3h du matin 💻"),
            ("streamer", "Tu as streamé 8h d'affilée sans pause 🎮"),
            ("garde du corps", "Tu as escorté un influenceur capricieux toute la journée 🕶️"),
            ("trader", "Tu as passé la journée à fixer des graphiques 📈"),
            ("détective", "Tu as résolu une affaire de vol de baguette 🥖"),
            ("dentiste", "Tu as soigné 15 patients terrifiés 🦷"),
            ("pilote", "Tu as traversé 3 fuseaux horaires aujourd'hui ✈️"),
            ("professeur", "Tu as survécu à une journée avec 30 élèves agités 📚"),
        ]
        job, desc = random.choice(jobs)
        amount = random.randint(5000, 15000)
        update_economy(ctx.author.id, hand=eco["hand"] + amount, last_work=now.isoformat())
    await add_xp(ctx, ctx.author.id, 30)

    description = (
        f"{ctx.author.mention} a travaillé comme **{job}**\n\n"
        f"💼 *{desc}*\n\n"
        f"🟡 **+{format_ryo(amount)}** en poche\n"
        f"✨ **+30 XP**"
    )
    await ctx.send(embed=action_embed(ctx.author, description, color=0x43b581))


@bot.command(name="fish")
async def _fish(ctx):
    if await check_ban(ctx):
        return
    async with eco_lock:
        eco = get_economy(ctx.author.id)
        now = datetime.now(PARIS_TZ)

        if eco["last_fish"]:
            last = datetime.fromisoformat(eco["last_fish"])
            diff = now - last
            if diff.total_seconds() < 1800:
                remaining = 1800 - diff.total_seconds()
                m = int(remaining // 60)
                s = int(remaining % 60)
                return await ctx.send(embed=error_embed("⏰ Cooldown", f"Reviens dans **{m}min {s}s** pour pêcher."))

        catches = [
            ("🐟 Poisson commun", 500, 2000, "commun"),
            ("🐠 Poisson tropical", 3000, 8000, "peu commun"),
            ("🦈 Requin", 10000, 25000, "rare"),
            ("🦑 Calamar géant", 20000, 50000, "épique"),
            ("🐋 Baleine légendaire", 75000, 150000, "légendaire"),
            ("👢 Vieille botte", 0, 50, "déchet"),
            ("🗑️ Sac poubelle", 0, 10, "déchet"),
        ]
        weights = [40, 25, 15, 10, 3, 4, 3]
        catch = random.choices(catches, weights=weights, k=1)[0]
        name, min_val, max_val, rarity = catch
        amount = random.randint(min_val, max_val)

        update_economy(ctx.author.id, hand=eco["hand"] + amount, last_fish=now.isoformat())
    xp_gain = {"commun": 10, "peu commun": 20, "rare": 40, "épique": 70, "légendaire": 100, "déchet": 5}
    xp_amount = xp_gain.get(rarity, 10)
    await add_xp(ctx, ctx.author.id, xp_amount)

    rarity_colors = {"commun": 0x95a5a6, "peu commun": 0x2ecc71, "rare": 0x3498db, "épique": 0x9b59b6, "légendaire": 0xf1c40f, "déchet": 0x7f8c8d}
    description = (
        f"🎣 {name} *({rarity})*\n"
        f"🟡 **+{format_ryo(amount)}** en poche\n"
        f"✨ **+{xp_amount} XP**"
    )
    await ctx.send(embed=action_embed(ctx.author, description, color=rarity_colors.get(rarity, embed_color())))


@bot.command(name="slots", aliases=["sl"])
async def _slots(ctx, amount_str: str = None):
    if await check_ban(ctx):
        return
    if not amount_str:
        return await ctx.send(embed=error_embed("Argument manquant", "Usage : `*slots [somme/all]`"))

    # Cooldown
    ok, remaining = check_game_cooldown(ctx.author.id, "slots")
    if not ok:
        return await ctx.send(embed=error_embed("⏰ Cooldown", f"Attends **{remaining}s** avant de rejouer aux slots."))

    async with eco_lock:
        eco = get_economy(ctx.author.id)
        amount = parse_amount(amount_str, eco["hand"])
        if amount is None or amount <= 0:
            return await ctx.send(embed=error_embed("Montant invalide", "Donne un montant valide."))
        if amount < MIN_BET:
            return await ctx.send(embed=error_embed("Mise trop basse", f"Mise minimum : {format_ryo(MIN_BET)}"))
        if amount > eco["hand"]:
            return await ctx.send(embed=error_embed("Fonds insuffisants", f"Tu n'as que {format_ryo(eco['hand'])} en main."))

        # Probabilités ajustées - plus dur de gagner gros
        symbols = ["🍒", "🍋", "🍊", "🍇", "⭐", "💎", "7️⃣"]
        weights = [35, 28, 20, 10, 4, 2, 1]  # 7 et 💎 deviennent rares
        reels = random.choices(symbols, weights=weights, k=3)

        # Multiplicateurs nerfés
        multipliers = {"7️⃣": 15, "💎": 10, "⭐": 6, "🍇": 4, "🍊": 2.5, "🍋": 2, "🍒": 1.5}

        if reels[0] == reels[1] == reels[2]:
            mult = multipliers.get(reels[0], 2)
            winnings = int(amount * mult)
            result = f"🎉 **3x identiques x{mult}** ・ +{format_ryo(winnings)}"
            color = 0xffd700
            atomic_hand_delta(ctx.author.id, -amount + winnings, min_hand=0)
            xp_reward = 50
        elif reels[0] == reels[1] or reels[1] == reels[2] or reels[0] == reels[2]:
            # 2 identiques : gain modeste x1.5
            winnings = int(amount * 1.5)
            result = f"✅ **2 identiques x1.5** ・ +{format_ryo(winnings)}"
            color = 0x43b581
            atomic_hand_delta(ctx.author.id, -amount + winnings, min_hand=0)
            xp_reward = 12
        else:
            result = f"❌ **Perdu** ・ -{format_ryo(amount)}"
            color = 0xf04747
            atomic_hand_delta(ctx.author.id, -amount, min_hand=0)
            xp_reward = 3

    record_game_cooldown(ctx.author.id, "slots")
    record_game_play(ctx.author.id, "slots", won=(color != 0xf04747))
    await add_xp(ctx, ctx.author.id, xp_reward)

    # Animation : d'abord le GIF de suspense, puis le résultat
    gif_url = await pick_game_gif("slots")
    anim_embed = discord.Embed(
        title="🎰 Slots",
        description=f"Les rouleaux tournent...\n**Mise :** {format_ryo(amount)}",
        color=0x5865f2,
    )
    if gif_url:
        anim_embed.set_image(url=gif_url)
    anim_embed.set_author(name=ctx.author.display_name,
                          icon_url=ctx.author.display_avatar.url)

    msg = await ctx.send(embed=anim_embed)
    await asyncio.sleep(GAME_ANIMATION_DELAY)

    description = (
        f"🎰 [ {reels[0]} | {reels[1]} | {reels[2]} ]\n\n"
        f"{result}\n"
        f"Mise : **{format_ryo(amount)}**  ・  ✨ **+{xp_reward} XP**"
    )
    try:
        await msg.edit(embed=action_embed(ctx.author, description, color=color))
    except discord.HTTPException:
        await ctx.send(embed=action_embed(ctx.author, description, color=color))


@bot.command(name="jackpot", aliases=["jp"])
async def _jackpot(ctx, amount_str: str = None):
    """Nouveau jackpot : pool partagé. Ta mise alimente le pot.
    3x 7️⃣ = tu empoches tout le pot + mise x50. Autres combinaisons = gains classiques."""
    if await check_ban(ctx):
        return
    if not amount_str:
        return await ctx.send(embed=error_embed("Argument manquant", "Usage : `*jackpot [somme/all]`"))

    # Cooldown
    ok, remaining = check_game_cooldown(ctx.author.id, "jackpot")
    if not ok:
        return await ctx.send(embed=error_embed("⏰ Cooldown", f"Attends **{remaining}s** avant de rejouer au jackpot."))

    async with eco_lock:
        eco = get_economy(ctx.author.id)
        amount = parse_amount(amount_str, eco["hand"])
        if amount is None or amount <= 0:
            return await ctx.send(embed=error_embed("Montant invalide", "Donne un montant valide."))
        if amount < MIN_BET:
            return await ctx.send(embed=error_embed("Mise trop basse", f"Mise minimum : {format_ryo(MIN_BET)}"))
        if amount > eco["hand"]:
            return await ctx.send(embed=error_embed("Fonds insuffisants", f"Tu n'as que {format_ryo(eco['hand'])} en main."))

        # Tirage de 3 symboles
        symbols = ["🍒", "🍋", "🍊", "🔔", "⭐", "💎", "7️⃣"]
        weights = [32, 26, 20, 12, 6, 3, 1]  # 7️⃣ = 1% par slot, donc 3x7️⃣ ≈ 0.0001% (vraie rareté)
        reels = random.choices(symbols, weights=weights, k=3)

        pool_before = jackpot_get(ctx.guild.id)

        if reels[0] == reels[1] == reels[2] == "7️⃣":
            # MEGA JACKPOT : empoche tout le pot + mise x50
            pot_win = pool_before
            base_win = amount * 50
            total = pot_win + base_win
            atomic_hand_delta(ctx.author.id, -amount + total, min_hand=0)
            jackpot_reset(ctx.guild.id, JACKPOT_POOL_MIN)
            result = f"🎊 **MEGA JACKPOT ! 3× 7️⃣**\n💰 Pot : +{format_ryo(pot_win)}\n🎰 Mise ×50 : +{format_ryo(base_win)}"
            color = 0xffd700
            xp_reward = 300
        elif reels[0] == reels[1] == reels[2] == "💎":
            # 3 diamants : pot / 2 + mise x20
            pot_win = pool_before // 2
            base_win = amount * 20
            total = pot_win + base_win
            atomic_hand_delta(ctx.author.id, -amount + total, min_hand=0)
            jackpot_reset(ctx.guild.id, pool_before - pot_win)  # garde la moitié du pot
            result = f"💎 **3× 💎**\n💰 Demi-pot : +{format_ryo(pot_win)}\n🎰 Mise ×20 : +{format_ryo(base_win)}"
            color = 0xe91e63
            xp_reward = 120
        elif reels[0] == reels[1] == reels[2] == "⭐":
            winnings = amount * 10
            atomic_hand_delta(ctx.author.id, -amount + winnings, min_hand=0)
            # La mise alimente quand même un peu le pot même si on gagne
            jackpot_add(ctx.guild.id, amount // 4)
            result = f"⭐ **3× ⭐ x10** ・ +{format_ryo(winnings)}"
            color = 0xf1c40f
            xp_reward = 80
        elif reels[0] == reels[1] == reels[2]:
            # 3 identiques (autres) : x5
            winnings = amount * 5
            atomic_hand_delta(ctx.author.id, -amount + winnings, min_hand=0)
            jackpot_add(ctx.guild.id, amount // 4)
            result = f"🎉 **3× {reels[0]} x5** ・ +{format_ryo(winnings)}"
            color = 0xffd700
            xp_reward = 40
        elif reels[0] == reels[1] or reels[1] == reels[2] or reels[0] == reels[2]:
            # 2 identiques : x1.5
            winnings = int(amount * 1.5)
            atomic_hand_delta(ctx.author.id, -amount + winnings, min_hand=0)
            jackpot_add(ctx.guild.id, amount // 4)
            result = f"✅ **2 identiques x1.5** ・ +{format_ryo(winnings)}"
            color = 0x43b581
            xp_reward = 12
        else:
            # Perdu : la mise complète va dans le pot
            atomic_hand_delta(ctx.author.id, -amount, min_hand=0)
            jackpot_add(ctx.guild.id, amount)
            result = f"❌ **Perdu** ・ -{format_ryo(amount)}\n💰 Ta mise alimente le pot."
            color = 0xf04747
            xp_reward = 3

    record_game_cooldown(ctx.author.id, "jackpot")
    record_game_play(ctx.author.id, "jackpot", won=(color != 0xf04747))
    await add_xp(ctx, ctx.author.id, xp_reward)
    pool_after = jackpot_get(ctx.guild.id)

    # Animation
    gif_url = await pick_game_gif("jackpot")
    anim_embed = discord.Embed(
        title="💰 Jackpot",
        description=f"Tirage en cours...\n**Mise :** {format_ryo(amount)}\n**Pot actuel :** {format_ryo(pool_before)}",
        color=0xf1c40f,
    )
    if gif_url:
        anim_embed.set_image(url=gif_url)
    anim_embed.set_author(name=ctx.author.display_name,
                          icon_url=ctx.author.display_avatar.url)
    msg = await ctx.send(embed=anim_embed)
    await asyncio.sleep(GAME_ANIMATION_DELAY)

    description = (
        f"🎰 [ {reels[0]} | {reels[1]} | {reels[2]} ]\n\n"
        f"{result}\n"
        f"Mise : **{format_ryo(amount)}**  ・  ✨ **+{xp_reward} XP**\n"
        f"💰 Pot actuel : **{format_ryo(pool_after)}**"
    )
    try:
        await msg.edit(embed=action_embed(ctx.author, description, color=color))
    except discord.HTTPException:
        await ctx.send(embed=action_embed(ctx.author, description, color=color))


@bot.command(name="pot")
async def _pot(ctx):
    """Voir la cagnotte actuelle du jackpot."""
    if await check_ban(ctx):
        return
    pool = jackpot_get(ctx.guild.id)
    em = info_embed(
        "💰 Cagnotte du Jackpot",
        f"**{format_ryo(pool)}** actuellement dans le pot.\n\n"
        f"*Tire 3× 7️⃣ au `*jackpot` pour tout empocher + ta mise ×50.*"
    )
    await ctx.send(embed=em)


# ========================= NOUVEAUX JEUX =========================

@bot.command(name="roulette", aliases=["rl"])
async def _roulette(ctx, amount_str: str = None, bet_type: str = None):
    """
    Roulette européenne.
    Usage :
      *roulette <mise> rouge       x2
      *roulette <mise> noir        x2
      *roulette <mise> pair        x2
      *roulette <mise> impair      x2
      *roulette <mise> manque      x2 (1-18)
      *roulette <mise> passe       x2 (19-36)
      *roulette <mise> 17          x36 (chiffre précis 0-36)
    """
    if await check_ban(ctx):
        return
    if not amount_str or not bet_type:
        return await ctx.send(embed=error_embed(
            "Usage",
            "`*roulette <mise> <rouge|noir|pair|impair|manque|passe|0-36>`\n"
            "Ex : `*roulette 500 rouge` ou `*roulette 100 17`"
        ))

    ok, remaining = check_game_cooldown(ctx.author.id, "roulette")
    if not ok:
        return await ctx.send(embed=error_embed("⏰ Cooldown", f"Attends **{remaining}s**."))

    bet_type_lower = bet_type.lower().strip()

    # Parse le type de pari
    valid_sides = {"rouge", "noir", "pair", "impair", "manque", "passe", "red", "black", "even", "odd"}
    bet_number = None
    if bet_type_lower.isdigit():
        bet_number = int(bet_type_lower)
        if bet_number < 0 or bet_number > 36:
            return await ctx.send(embed=error_embed("❌ Chiffre invalide", "Choisis un chiffre entre **0 et 36**."))
    elif bet_type_lower not in valid_sides:
        return await ctx.send(embed=error_embed(
            "❌ Type de pari invalide",
            "Types acceptés : `rouge`, `noir`, `pair`, `impair`, `manque` (1-18), `passe` (19-36), ou un chiffre `0-36`."
        ))

    async with eco_lock:
        eco = get_economy(ctx.author.id)
        amount = parse_amount(amount_str, eco["hand"])
        if amount is None or amount <= 0:
            return await ctx.send(embed=error_embed("Montant invalide", "Donne un montant valide."))
        if amount < MIN_BET:
            return await ctx.send(embed=error_embed("Mise trop basse", f"Mise minimum : {format_ryo(MIN_BET)}"))
        if amount > eco["hand"]:
            return await ctx.send(embed=error_embed("Fonds insuffisants", f"Tu n'as que {format_ryo(eco['hand'])} en main."))

        # Tirage
        result_number = random.randint(0, 36)
        red_numbers = {1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36}
        result_color = "vert" if result_number == 0 else ("rouge" if result_number in red_numbers else "noir")

        # Détermine si gagné
        won = False
        multiplier = 0
        if bet_number is not None:
            if bet_number == result_number:
                won = True
                multiplier = 36
        else:
            if result_number == 0:
                won = False  # 0 = la banque gagne pour toutes les mises extérieures
            elif bet_type_lower in ("rouge", "red") and result_color == "rouge":
                won = True; multiplier = 2
            elif bet_type_lower in ("noir", "black") and result_color == "noir":
                won = True; multiplier = 2
            elif bet_type_lower in ("pair", "even") and result_number % 2 == 0 and result_number != 0:
                won = True; multiplier = 2
            elif bet_type_lower in ("impair", "odd") and result_number % 2 == 1:
                won = True; multiplier = 2
            elif bet_type_lower == "manque" and 1 <= result_number <= 18:
                won = True; multiplier = 2
            elif bet_type_lower == "passe" and 19 <= result_number <= 36:
                won = True; multiplier = 2

        if won:
            winnings = amount * multiplier
            atomic_hand_delta(ctx.author.id, -amount + winnings, min_hand=0)
            net = winnings - amount
            result_line = f"✅ **Gagné x{multiplier}** ・ +{format_ryo(net)}"
            color = 0x43b581
            xp_reward = multiplier * 5
        else:
            atomic_hand_delta(ctx.author.id, -amount, min_hand=0)
            result_line = f"❌ **Perdu** ・ -{format_ryo(amount)}"
            color = 0xf04747
            xp_reward = 3

    record_game_cooldown(ctx.author.id, "roulette")
    record_game_play(ctx.author.id, "roulette", won=(color != 0xf04747))
    await add_xp(ctx, ctx.author.id, xp_reward)

    # Animation : GIF de roulette qui tourne puis résultat
    gif_url = await pick_game_gif("roulette")
    anim_embed = discord.Embed(
        title="🎡 Roulette",
        description=f"La bille tourne...\n**Pari :** {bet_type} ・ **Mise :** {format_ryo(amount)}",
        color=0x5865f2,
    )
    if gif_url:
        anim_embed.set_image(url=gif_url)
    anim_embed.set_author(name=ctx.author.display_name,
                          icon_url=ctx.author.display_avatar.url)
    msg = await ctx.send(embed=anim_embed)
    await asyncio.sleep(GAME_ANIMATION_DELAY)

    color_emoji = {"rouge": "🔴", "noir": "⚫", "vert": "🟢"}[result_color]
    description = (
        f"🎡 La bille s'arrête sur : {color_emoji} **{result_number}** ({result_color})\n\n"
        f"Ton pari : **{bet_type}** ・ Mise : **{format_ryo(amount)}**\n"
        f"{result_line} ・ ✨ **+{xp_reward} XP**"
    )
    try:
        await msg.edit(embed=action_embed(ctx.author, description, color=color))
    except discord.HTTPException:
        await ctx.send(embed=action_embed(ctx.author, description, color=color))


@bot.command(name="des", aliases=["dice"])
async def _des(ctx, amount_str: str = None):
    """Lance un dé 1-6 contre le bot. Plus haut = x2, égalité = remboursé."""
    if await check_ban(ctx):
        return
    if not amount_str:
        return await ctx.send(embed=error_embed("Argument manquant", "Usage : `*des <mise>`"))

    ok, remaining = check_game_cooldown(ctx.author.id, "des")
    if not ok:
        return await ctx.send(embed=error_embed("⏰ Cooldown", f"Attends **{remaining}s**."))

    async with eco_lock:
        eco = get_economy(ctx.author.id)
        amount = parse_amount(amount_str, eco["hand"])
        if amount is None or amount <= 0:
            return await ctx.send(embed=error_embed("Montant invalide", "Donne un montant valide."))
        if amount < MIN_BET:
            return await ctx.send(embed=error_embed("Mise trop basse", f"Mise minimum : {format_ryo(MIN_BET)}"))
        if amount > eco["hand"]:
            return await ctx.send(embed=error_embed("Fonds insuffisants", f"Tu n'as que {format_ryo(eco['hand'])} en main."))

        player = random.randint(1, 6)
        bot_roll = random.randint(1, 6)

        if player > bot_roll:
            winnings = amount * 2
            atomic_hand_delta(ctx.author.id, -amount + winnings, min_hand=0)
            result_line = f"✅ **Gagné x2** ・ +{format_ryo(amount)}"
            color = 0x43b581
            xp_reward = 15
        elif player == bot_roll:
            # Remboursé
            result_line = f"➖ **Égalité** ・ mise remboursée"
            color = 0x95a5a6
            xp_reward = 5
        else:
            atomic_hand_delta(ctx.author.id, -amount, min_hand=0)
            result_line = f"❌ **Perdu** ・ -{format_ryo(amount)}"
            color = 0xf04747
            xp_reward = 3

    record_game_cooldown(ctx.author.id, "des")
    record_game_play(ctx.author.id, "des", won=(color != 0xf04747))
    await add_xp(ctx, ctx.author.id, xp_reward)

    # Animation : dés qui roulent
    gif_url = await pick_game_gif("des")
    anim_embed = discord.Embed(
        title="🎲 Dés",
        description=f"Les dés roulent...\n**Mise :** {format_ryo(amount)}",
        color=0x5865f2,
    )
    if gif_url:
        anim_embed.set_image(url=gif_url)
    anim_embed.set_author(name=ctx.author.display_name,
                          icon_url=ctx.author.display_avatar.url)
    msg = await ctx.send(embed=anim_embed)
    await asyncio.sleep(GAME_ANIMATION_DELAY)

    dice_emojis = {1: "⚀", 2: "⚁", 3: "⚂", 4: "⚃", 5: "⚄", 6: "⚅"}
    description = (
        f"🎲 Toi : {dice_emojis[player]} **{player}**  ・  Bot : {dice_emojis[bot_roll]} **{bot_roll}**\n\n"
        f"{result_line} ・ ✨ **+{xp_reward} XP**"
    )
    try:
        await msg.edit(embed=action_embed(ctx.author, description, color=color))
    except discord.HTTPException:
        await ctx.send(embed=action_embed(ctx.author, description, color=color))


@bot.command(name="pfc", aliases=["rps"])
async def _pfc(ctx, amount_str: str = None, choice: str = None):
    """Pierre-Feuille-Ciseaux contre le bot. Gagne = x2, égalité = remboursé."""
    if await check_ban(ctx):
        return
    if not amount_str or not choice:
        return await ctx.send(embed=error_embed(
            "Usage",
            "`*pfc <mise> <pierre|feuille|ciseaux>`\nEx : `*pfc 500 pierre`"
        ))

    ok, remaining = check_game_cooldown(ctx.author.id, "pfc")
    if not ok:
        return await ctx.send(embed=error_embed("⏰ Cooldown", f"Attends **{remaining}s**."))

    choice_lower = choice.lower().strip()
    valid = {"pierre": "🪨", "feuille": "📄", "ciseaux": "✂️",
             "rock": "🪨", "paper": "📄", "scissors": "✂️"}
    normalize = {"rock": "pierre", "paper": "feuille", "scissors": "ciseaux"}
    if choice_lower not in valid:
        return await ctx.send(embed=error_embed(
            "❌ Choix invalide",
            "Choix : `pierre`, `feuille` ou `ciseaux`."
        ))
    player = normalize.get(choice_lower, choice_lower)

    async with eco_lock:
        eco = get_economy(ctx.author.id)
        amount = parse_amount(amount_str, eco["hand"])
        if amount is None or amount <= 0:
            return await ctx.send(embed=error_embed("Montant invalide", "Donne un montant valide."))
        if amount < MIN_BET:
            return await ctx.send(embed=error_embed("Mise trop basse", f"Mise minimum : {format_ryo(MIN_BET)}"))
        if amount > eco["hand"]:
            return await ctx.send(embed=error_embed("Fonds insuffisants", f"Tu n'as que {format_ryo(eco['hand'])} en main."))

        bot_choice = random.choice(["pierre", "feuille", "ciseaux"])

        # Détermine victoire
        wins_against = {"pierre": "ciseaux", "ciseaux": "feuille", "feuille": "pierre"}
        if player == bot_choice:
            result_line = f"➖ **Égalité** ・ mise remboursée"
            color = 0x95a5a6
            xp_reward = 5
        elif wins_against[player] == bot_choice:
            winnings = amount * 2
            atomic_hand_delta(ctx.author.id, -amount + winnings, min_hand=0)
            result_line = f"✅ **Gagné x2** ・ +{format_ryo(amount)}"
            color = 0x43b581
            xp_reward = 15
        else:
            atomic_hand_delta(ctx.author.id, -amount, min_hand=0)
            result_line = f"❌ **Perdu** ・ -{format_ryo(amount)}"
            color = 0xf04747
            xp_reward = 3

    record_game_cooldown(ctx.author.id, "pfc")
    record_game_play(ctx.author.id, "pfc", won=(color != 0xf04747))
    await add_xp(ctx, ctx.author.id, xp_reward)

    # Animation : mains qui s'affrontent
    gif_url = await pick_game_gif("pfc")
    p_emoji = {"pierre": "🪨", "feuille": "📄", "ciseaux": "✂️"}
    anim_embed = discord.Embed(
        title="✊✋✌️ Pierre-Feuille-Ciseaux",
        description=f"Ton choix : {p_emoji[player]} **{player}**\n**Mise :** {format_ryo(amount)}\n\nLe bot réfléchit...",
        color=0x5865f2,
    )
    if gif_url:
        anim_embed.set_image(url=gif_url)
    anim_embed.set_author(name=ctx.author.display_name,
                          icon_url=ctx.author.display_avatar.url)
    msg = await ctx.send(embed=anim_embed)
    await asyncio.sleep(GAME_ANIMATION_DELAY)

    description = (
        f"{p_emoji[player]} Toi : **{player}**  ・  {p_emoji[bot_choice]} Bot : **{bot_choice}**\n\n"
        f"{result_line} ・ ✨ **+{xp_reward} XP**"
    )
    try:
        await msg.edit(embed=action_embed(ctx.author, description, color=color))
    except discord.HTTPException:
        await ctx.send(embed=action_embed(ctx.author, description, color=color))


# ========================= BLACKJACK =========================

class BlackjackView(discord.ui.View):
    def __init__(self, ctx, amount, deck, player_hand, dealer_hand):
        super().__init__(timeout=60)
        self.ctx = ctx
        self.amount = amount  # Déjà débité du hand au début (escrow)
        self.deck = deck
        self.player_hand = player_hand
        self.dealer_hand = dealer_hand
        self.finished = False
        self.message = None  # Sera set après l'envoi

    def hand_value(self, hand):
        value = 0
        aces = 0
        for card in hand:
            if card[0] in ["J", "Q", "K"]:
                value += 10
            elif card[0] == "A":
                value += 11
                aces += 1
            else:
                value += int(card[0]) if card[0] != "1" else 10
        while value > 21 and aces:
            value -= 10
            aces -= 1
        return value

    def format_hand(self, hand, hide_second=False):
        if hide_second and len(hand) > 1:
            return f"{hand[0][0]}{hand[0][1]} | 🂠"
        return " | ".join([f"{c[0]}{c[1]}" for c in hand])

    def make_embed(self, result=None, color=None):
        pv = self.hand_value(self.player_hand)
        dv = self.hand_value(self.dealer_hand)
        dealer_display = self.format_hand(self.dealer_hand, hide_second=not result)
        player_display = self.format_hand(self.player_hand)

        description = (
            f"🎩 **Dealer ({dv if result else '?'})** ・ {dealer_display}\n"
            f"🃏 **Toi ({pv})** ・ {player_display}\n"
        )
        if result:
            description += f"\n{result}"
        description += f"\n\nMise : **{format_ryo(self.amount)}**"

        em = discord.Embed(
            title=self.ctx.author.display_name,
            description=description,
            color=color if color is not None else embed_color(),
        )
        em.set_thumbnail(url=self.ctx.author.display_avatar.url)
        em.set_footer(text="Velda")
        return em

    async def on_timeout(self):
        # FIX: si le joueur laisse le timeout filer, on considère qu'il perd sa mise
        # (elle a déjà été débitée en escrow, donc on fait rien côté DB)
        if self.finished:
            return
        self.finished = True
        for item in self.children:
            item.disabled = True
        try:
            if self.message:
                await self.message.edit(
                    embed=self.make_embed(f"⏰ Timeout ! Tu as perdu ta mise de {format_ryo(self.amount)}.", 0xf04747),
                    view=self
                )
        except discord.HTTPException:
            pass
        # Tracking partie perdue par timeout
        record_game_play(self.ctx.author.id, "bj", won=False)

    @discord.ui.button(label="Tirer 🃏", style=discord.ButtonStyle.primary)
    async def hit(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user != self.ctx.author:
            return await interaction.response.send_message("Ce n'est pas ton jeu !", ephemeral=True)
        if self.finished:
            return await interaction.response.send_message("La partie est terminée.", ephemeral=True)
        self.player_hand.append(self.deck.pop())
        pv = self.hand_value(self.player_hand)
        if pv > 21:
            # Bust : on a déjà débité la mise en escrow, donc rien à faire côté DB
            self.finished = True
            for item in self.children:
                item.disabled = True
            await interaction.response.edit_message(
                embed=self.make_embed(f"💥 Bust ! Tu as {pv}. -{format_ryo(self.amount)}", 0xf04747),
                view=self
            )
            record_game_play(self.ctx.author.id, "bj", won=False)
            self.stop()
        else:
            await interaction.response.edit_message(embed=self.make_embed(), view=self)

    @discord.ui.button(label="Rester ✋", style=discord.ButtonStyle.secondary)
    async def stand(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user != self.ctx.author:
            return await interaction.response.send_message("Ce n'est pas ton jeu !", ephemeral=True)
        if self.finished:
            return await interaction.response.send_message("La partie est terminée.", ephemeral=True)
        # Dealer joue
        while self.hand_value(self.dealer_hand) < 17:
            self.dealer_hand.append(self.deck.pop())

        pv = self.hand_value(self.player_hand)
        dv = self.hand_value(self.dealer_hand)

        async with eco_lock:
            if dv > 21 or pv > dv:
                # Gain : on re-crédite la mise + les gains (mise * 2 au total)
                atomic_hand_delta(self.ctx.author.id, self.amount * 2, min_hand=0)
                result = f"🎉 Tu gagnes ! ({pv} vs {dv}) +{format_ryo(self.amount)}"
                color = 0x43b581
            elif pv == dv:
                # Égalité : on rend juste la mise
                atomic_hand_delta(self.ctx.author.id, self.amount, min_hand=0)
                result = f"🤝 Égalité ! ({pv} vs {dv}) Mise remboursée"
                color = 0xfaa61a
            else:
                # Perte : la mise était déjà débitée, rien à faire
                result = f"❌ Perdu ! ({pv} vs {dv}) -{format_ryo(self.amount)}"
                color = 0xf04747

        self.finished = True
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(embed=self.make_embed(result, color), view=self)
        # Tracking : gagné si color != rouge (donc win ou égalité compte comme non-perdu)
        record_game_play(self.ctx.author.id, "bj", won=(color != 0xf04747))
        self.stop()


@bot.command(name="bj", aliases=["blackjack"])
async def _bj(ctx, amount_str: str = None):
    if await check_ban(ctx):
        return
    if not amount_str:
        return await ctx.send(embed=error_embed("Argument manquant", "Usage : `*bj [somme/all]`"))

    # Cooldown
    ok, remaining = check_game_cooldown(ctx.author.id, "bj")
    if not ok:
        return await ctx.send(embed=error_embed("⏰ Cooldown", f"Attends **{remaining}s** avant de rejouer au blackjack."))

    async with eco_lock:
        eco = get_economy(ctx.author.id)
        amount = parse_amount(amount_str, eco["hand"])
        if amount is None or amount <= 0:
            return await ctx.send(embed=error_embed("Montant invalide", "Donne un montant valide."))
        if amount < MIN_BET:
            return await ctx.send(embed=error_embed("Mise trop basse", f"Mise minimum : {format_ryo(MIN_BET)}"))
        if amount > eco["hand"]:
            return await ctx.send(embed=error_embed("Fonds insuffisants", f"Tu n'as que {format_ryo(eco['hand'])} en main."))

        # FIX: escrow — on débite immédiatement pour qu'il ne puisse pas dépenser la mise ailleurs
        if not atomic_hand_delta(ctx.author.id, -amount, min_hand=0):
            return await ctx.send(embed=error_embed("Erreur", "Impossible de débiter la mise."))

    # Record cooldown dès que la partie démarre (même si elle n'est pas encore finie)
    record_game_cooldown(ctx.author.id, "bj")

    suits = ["♠", "♥", "♦", "♣"]
    ranks = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"]
    deck = [(r, s) for s in suits for r in ranks]
    random.shuffle(deck)

    player_hand = [deck.pop(), deck.pop()]
    dealer_hand = [deck.pop(), deck.pop()]

    view = BlackjackView(ctx, amount, deck, player_hand, dealer_hand)
    pv = view.hand_value(player_hand)

    if pv == 21:
        # Blackjack naturel : mise + gain x1.5
        winnings = int(amount * 1.5)
        async with eco_lock:
            atomic_hand_delta(ctx.author.id, amount + winnings, min_hand=0)
        em = view.make_embed(f"🃏 Blackjack ! +{format_ryo(winnings)}", 0xffd700)
        view.finished = True
        return await ctx.send(embed=em)

    msg = await ctx.send(embed=view.make_embed(), view=view)
    view.message = msg


# ========================= DROP =========================

class DropView(discord.ui.View):
    def __init__(self, amount, author_id, author_member=None, message=None):
        super().__init__(timeout=120)
        self.amount = amount
        self.author_id = author_id
        self.author_member = author_member
        self.claimed = False
        self.button_active = False
        self.message = message

    async def on_timeout(self):
        # FIX: si personne ne clique, on affiche que le drop est expiré
        if self.claimed:
            return
        for item in self.children:
            item.disabled = True
            item.label = "⏰ Drop expiré"
            item.style = discord.ButtonStyle.secondary
        try:
            if self.message:
                em = discord.Embed(
                    title="💸 Drop expiré",
                    description=(
                        f"Personne n'a réclamé les **{format_ryo(self.amount)}**...\n\n"
                        f"❌ Les fonds sont perdus"
                    ),
                    color=0xf04747,
                )
                em.set_footer(text="Velda")
                await self.message.edit(embed=em, view=self)
        except discord.HTTPException:
            pass

    @discord.ui.button(label="⏳ Attends...", style=discord.ButtonStyle.secondary, disabled=True, custom_id="drop_btn")
    async def claim(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.claimed:
            return await interaction.response.send_message("Trop tard, déjà réclamé !", ephemeral=True)
        # FIX: l'auteur ne peut pas claim son propre drop
        if interaction.user.id == self.author_id:
            return await interaction.response.send_message(
                "Tu as lancé ce drop, tu ne peux pas le réclamer toi-même !", ephemeral=True
            )
        # FIX: pas de claim pour les users bannis du bot
        if is_bot_banned(interaction.user.id):
            return await interaction.response.send_message(
                "Tu es banni de Velda, tu ne peux pas réclamer ce drop.", ephemeral=True
            )
        if interaction.user.bot:
            return await interaction.response.send_message("Les bots ne peuvent pas claim.", ephemeral=True)

        self.claimed = True
        async with eco_lock:
            atomic_hand_delta(interaction.user.id, self.amount, min_hand=0)
        button.disabled = True
        button.label = f"✅ Réclamé par {interaction.user.display_name}"
        button.style = discord.ButtonStyle.success

        em = discord.Embed(
            title=interaction.user.display_name,
            description=(
                f"{interaction.user.mention} a réclamé le drop !\n\n"
                f"💸 **+{format_ryo(self.amount)}** en poche"
            ),
            color=0x43b581,
        )
        em.set_thumbnail(url=interaction.user.display_avatar.url)
        em.set_footer(text="Velda")
        await interaction.response.edit_message(embed=em, view=self)
        self.stop()


def _drop_embed(amount, author_name, stage):
    """Construit l'embed du drop selon le stage (countdown/active). Style unifié."""
    em = discord.Embed(title="💸 DROP !", color=0xffd700)
    if stage == "active":
        em.description = (
            f"💰 **{format_ryo(amount)}** en jeu\n\n"
            f"🎯 **GO ! Clique maintenant !**\n"
            f"🏆 Le premier remporte tout"
        )
    else:
        # stage est un int = secondes restantes
        em.description = (
            f"💰 **{format_ryo(amount)}** en jeu\n\n"
            f"⏳ Bouton actif dans **{stage} seconde{'s' if stage > 1 else ''}**...\n"
            f"🏆 Le premier remporte tout"
        )
    em.set_footer(text=f"Lancé par {author_name} ・ Velda")
    return em


@bot.command(name="drop")
async def _drop(ctx, amount_str: str = None):
    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Owner+** requis."))
    if not amount_str:
        return await ctx.send(embed=error_embed("Argument manquant", "Usage : `*drop [somme]`"))

    try:
        amount = int(amount_str.replace(" ", "").replace(",", ""))
        if amount <= 0:
            return await ctx.send(embed=error_embed("Montant invalide", "Le montant doit être positif."))
    except ValueError:
        return await ctx.send(embed=error_embed("Montant invalide", "Donne un montant valide."))

    view = DropView(amount, ctx.author.id, author_member=ctx.author)
    msg = await ctx.send(embed=_drop_embed(amount, ctx.author.display_name, 10), view=view)
    view.message = msg

    # Countdown
    for i in range(10, 0, -1):
        try:
            await msg.edit(embed=_drop_embed(amount, ctx.author.display_name, i))
        except discord.HTTPException:
            pass
        await asyncio.sleep(1)

    # Active le bouton
    view.button_active = True
    for item in view.children:
        item.disabled = False
        item.label = "🎯 CLIQUER !"
        item.style = discord.ButtonStyle.danger

    try:
        await msg.edit(embed=_drop_embed(amount, ctx.author.display_name, "active"), view=view)
    except discord.HTTPException:
        pass


# ========================= ENCHÈRES =========================

class BidModal(discord.ui.Modal, title="Placer une enchère"):
    def __init__(self, view):
        super().__init__()
        self.view_ref = view
        self.amount_input = discord.ui.TextInput(
            label="Montant de l'enchère (Ryo)",
            placeholder=f"Minimum : {view.min_next_bid()}",
            required=True,
            max_length=20,
        )
        self.add_item(self.amount_input)

    async def on_submit(self, interaction: discord.Interaction):
        await self.view_ref.process_bid(interaction, self.amount_input.value)


class EnchereView(discord.ui.View):
    def __init__(self, role, starting_bid, author, duration_seconds=3600):
        super().__init__(timeout=duration_seconds)
        self.role = role
        self.current_bid = starting_bid
        self.current_winner = None
        self.author = author
        self.message = None  # Set après l'envoi
        self.end_time = datetime.now(PARIS_TZ) + timedelta(seconds=duration_seconds)
        self.lock = asyncio.Lock()  # Empêche deux enchères simultanées de se marcher dessus
        self.closed = False

    def min_next_bid(self):
        return self.current_bid + max(1000, int(self.current_bid * 0.05))

    def make_embed(self):
        end_ts = int(self.end_time.timestamp())
        if self.current_winner:
            description = (
                f"🎪 Enchère en cours pour {self.role.mention}\n\n"
                f"💰 Mise actuelle : **{format_ryo(self.current_bid)}**\n"
                f"🏆 Meilleur enchérisseur : {self.current_winner.mention}\n"
                f"⏰ Fin : <t:{end_ts}:R>\n"
                f"🎁 Récompense : le rôle pour **48h**\n\n"
                f"Mise minimum : **{format_ryo(self.min_next_bid())}**"
            )
            color = 0xffd700
        else:
            description = (
                f"🎪 Nouvelle enchère pour {self.role.mention}\n\n"
                f"💰 Mise de départ : **{format_ryo(self.current_bid)}**\n"
                f"⏰ Fin : <t:{end_ts}:R>\n"
                f"🎁 Récompense : le rôle pour **48h**\n\n"
                f"Clique sur **Enchérir** pour participer"
            )
            color = 0xffd700
        em = discord.Embed(
            title=f"Enchère — @{self.role.name}",
            description=description,
            color=color,
        )
        em.set_footer(text=f"Lancé par {self.author.display_name} ・ Velda")
        return em

    async def process_bid(self, interaction: discord.Interaction, raw_value: str):
        # Appelé par le BidModal. Toute la logique critique est sous lock.
        try:
            bid_amount = int(raw_value.replace(" ", "").replace(",", ""))
        except ValueError:
            return await interaction.response.send_message(
                "Montant invalide, donne un nombre entier.", ephemeral=True
            )

        if self.closed:
            return await interaction.response.send_message("L'enchère est terminée.", ephemeral=True)

        async with self.lock, eco_lock:
            if self.closed:
                return await interaction.response.send_message("L'enchère est terminée.", ephemeral=True)

            min_bid = self.min_next_bid()
            if bid_amount < min_bid:
                return await interaction.response.send_message(
                    f"Mise trop basse ! Minimum : {format_ryo(min_bid)}.", ephemeral=True
                )
            if interaction.user == self.current_winner:
                return await interaction.response.send_message(
                    "Tu es déjà en tête de cette enchère.", ephemeral=True
                )

            # FIX: escrow — on débite immédiatement le nouvel enchérisseur
            if not atomic_hand_delta(interaction.user.id, -bid_amount, min_hand=0):
                return await interaction.response.send_message(
                    f"Fonds insuffisants ! Il te faut {format_ryo(bid_amount)} en main.", ephemeral=True
                )

            # Rembourse l'ancien enchérisseur s'il y en avait un
            if self.current_winner is not None:
                atomic_hand_delta(self.current_winner.id, self.current_bid, min_hand=0)

            self.current_bid = bid_amount
            self.current_winner = interaction.user

            # Anti-snipe : si bid dans les 60 dernières secondes, prolonge de 60s
            remaining = (self.end_time - datetime.now(PARIS_TZ)).total_seconds()
            snipe_extend = False
            if remaining < 60:
                self.end_time = datetime.now(PARIS_TZ) + timedelta(seconds=60)
                snipe_extend = True

        # Édition du message original (FIX: avant on envoyait un nouveau message)
        try:
            if self.message:
                await self.message.edit(embed=self.make_embed(), view=self)
        except discord.HTTPException:
            pass

        msg = f"Mise placée : {format_ryo(bid_amount)} !"
        if snipe_extend:
            msg += " ⏰ Enchère prolongée de 60s (anti-snipe)."
        await interaction.response.send_message(msg, ephemeral=True)

    @discord.ui.button(label="Enchérir 💰", style=discord.ButtonStyle.primary)
    async def bid(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.closed:
            return await interaction.response.send_message("L'enchère est terminée.", ephemeral=True)
        if is_bot_banned(interaction.user.id):
            return await interaction.response.send_message(
                "Tu es banni de Velda.", ephemeral=True
            )
        if interaction.user.bot:
            return await interaction.response.send_message("Les bots ne peuvent pas enchérir.", ephemeral=True)
        if interaction.user.id == self.author.id:
            return await interaction.response.send_message(
                "Tu as lancé cette enchère, tu ne peux pas y participer.", ephemeral=True
            )

        eco = get_economy(interaction.user.id)
        min_bid = self.min_next_bid()
        if eco["hand"] < min_bid:
            return await interaction.response.send_message(
                f"Il te faut au moins {format_ryo(min_bid)} en main pour enchérir !", ephemeral=True
            )

        await interaction.response.send_modal(BidModal(self))


async def run_enchere_lifecycle(view: EnchereView, channel, role):
    """Gère la fin de l'enchère avec prolongation possible (anti-snipe)."""
    # Attente jusqu'à la fin avec check périodique (pour gérer les extensions)
    while True:
        now = datetime.now(PARIS_TZ)
        remaining = (view.end_time - now).total_seconds()
        if remaining <= 0:
            break
        await asyncio.sleep(min(remaining, 5))

    view.closed = True
    for item in view.children:
        item.disabled = True
    try:
        if view.message:
            await view.message.edit(view=view)
    except discord.HTTPException:
        pass

    # Fin de l'enchère
    if view.current_winner:
        # L'argent est déjà débité (escrow), donc on ne touche plus à l'économie
        try:
            await view.current_winner.add_roles(role, reason="Gagnant enchère Velda")
        except (discord.Forbidden, discord.HTTPException) as e:
            log.warning(f"Impossible d'attribuer le rôle {role.name} : {e}")

        em_end = discord.Embed(
            title=view.current_winner.display_name,
            description=(
                f"{view.current_winner.mention} a remporté l'enchère !\n\n"
                f"🏆 Rôle obtenu : {role.mention}\n"
                f"💰 Mise finale : **{format_ryo(view.current_bid)}**\n"
                f"⏳ Durée : **48h**"
            ),
            color=0x43b581,
        )
        em_end.set_thumbnail(url=view.current_winner.display_avatar.url)
        em_end.set_footer(text="Velda")
        try:
            await channel.send(embed=em_end)
        except discord.HTTPException:
            pass

        # Retirer le rôle après 48h
        await asyncio.sleep(172800)
        try:
            await view.current_winner.remove_roles(role, reason="Fin enchère Velda 48h")
        except (discord.Forbidden, discord.HTTPException) as e:
            log.warning(f"Impossible de retirer le rôle {role.name} : {e}")
    else:
        em_end = discord.Embed(
            title="Enchère terminée",
            description=(
                f"❌ Aucun participant pour {role.mention}\n\n"
                f"L'enchère est close sans gagnant"
            ),
            color=0xf04747,
        )
        em_end.set_footer(text="Velda")
        try:
            await channel.send(embed=em_end)
        except discord.HTTPException:
            pass


@bot.command(name="enchere")
async def _enchere(ctx, role: discord.Role = None):
    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Owner+** requis."))
    if not role:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un rôle."))

    # Sanity check : le bot doit pouvoir assigner ce rôle
    if role >= ctx.guild.me.top_role:
        return await ctx.send(embed=error_embed(
            "❌ Rôle trop haut",
            "Ce rôle est au-dessus du mien dans la hiérarchie, je ne peux pas l'attribuer."
        ))
    if role.managed:
        return await ctx.send(embed=error_embed("❌ Rôle géré", "Ce rôle est géré par une intégration."))

    channel_id = get_enchere_channel(ctx.guild.id)
    channel = ctx.guild.get_channel(int(channel_id)) if channel_id else ctx.channel
    if not channel:
        channel = ctx.channel

    view = EnchereView(role, 1000, ctx.author, duration_seconds=3600)
    msg = await channel.send(embed=view.make_embed(), view=view)
    view.message = msg
    _active_enchere_views.append(view)

    # Lance la gestion du cycle de vie en tâche de fond
    await run_enchere_lifecycle(view, channel, role)
    # Nettoie après fin
    if view in _active_enchere_views:
        _active_enchere_views.remove(view)


# ========================= ENCHÈRE : DATE MODIFIABLE =========================

# Registre des vues d'enchères actives (pour *encheredit)
_active_enchere_views = []


@bot.command(name="encheredit", aliases=["enchere_edit"])
async def _encheredit(ctx, duration_min: int = None):
    """Modifier le temps restant d'une enchère en cours (ex : *encheredit 30 = fin dans 30min)."""
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if duration_min is None or duration_min <= 0:
        return await ctx.send(embed=error_embed(
            "Argument manquant",
            f"Usage : `*encheredit <minutes>`\nEx : `*encheredit 30` pour finir dans 30min."
        ))

    # Cherche une enchère active dans le salon
    active_view = None
    # On nettoie les closed pendant qu'on y est
    global _active_enchere_views
    _active_enchere_views = [v for v in _active_enchere_views if not v.closed]
    for view in _active_enchere_views:
        if view.message and view.message.channel.id == ctx.channel.id:
            active_view = view
            break

    if not active_view:
        return await ctx.send(embed=error_embed(
            "❌ Aucune enchère",
            "Aucune enchère n'est active dans ce salon."
        ))

    new_end_time = datetime.now(PARIS_TZ) + timedelta(minutes=duration_min)
    active_view.end_time = new_end_time
    # Refresh visuel du message
    try:
        if active_view.message:
            await active_view.message.edit(embed=active_view.make_embed(), view=active_view)
    except discord.HTTPException as e:
        log.warning(f"encheredit: refresh échoué : {e}")

    ts = int(new_end_time.timestamp())
    await ctx.send(embed=success_embed(
        "✅ Enchère modifiée",
        f"Nouvelle fin : <t:{ts}:F> (<t:{ts}:R>)"
    ))
    await send_log(ctx.guild, "Enchère modifiée", ctx.author,
                   desc=f"Nouvelle durée : {duration_min} min", color=0xfaa61a)


# ========================= LEADERBOARD =========================

LB_METRICS = {
    "hand":         {"emoji": "💰", "label": "Argent en main",  "suffix": "Ryo",     "guild_only": False},
    "bank":         {"emoji": "🏦", "label": "Argent en banque","suffix": "Ryo",     "guild_only": False},
    "total":        {"emoji": "💎", "label": "Total (main + bank)", "suffix": "Ryo", "guild_only": False},
    "fame":         {"emoji": "⭐", "label": "Fame",             "suffix": "fame",    "guild_only": False},
    "xp":           {"emoji": "✨", "label": "XP",               "suffix": "XP",      "guild_only": False},
    "level":        {"emoji": "📈", "label": "Niveau",           "suffix": "",        "guild_only": False},
    "vocal_time":   {"emoji": "🎤", "label": "Temps en vocal",   "suffix": "minutes", "guild_only": True},
    "vocal_earned": {"emoji": "💵", "label": "Gains vocaux",     "suffix": "Ryo",     "guild_only": True},
}


def build_lb_embed(guild, metric_key):
    meta = LB_METRICS[metric_key]
    top = lb_top(guild.id, metric_key, limit=10)
    em = discord.Embed(
        title=f"{meta['emoji']} Classement — {meta['label']}",
        color=embed_color(),
    )
    if not top:
        em.description = "*Aucun classement pour l'instant.*"
    else:
        medals = ["🥇", "🥈", "🥉"]
        lines = []
        for i, (uid, val) in enumerate(top):
            marker = medals[i] if i < 3 else f"**{i+1}.**"
            if meta["suffix"] == "Ryo":
                value_display = f"**{format_ryo(val)}**"
            else:
                value_display = f"**{val}** {meta['suffix']}"
            lines.append(f"{marker} <@{uid}> ・ {value_display}")
        em.description = "\n".join(lines)
    em.set_footer(text="Velda ・ Utilise le menu pour changer de classement")
    return em


class LbDropdown(discord.ui.Select):
    def __init__(self, guild):
        self.guild = guild
        options = []
        for key, meta in LB_METRICS.items():
            options.append(discord.SelectOption(
                label=meta["label"], emoji=meta["emoji"], value=key
            ))
        super().__init__(placeholder="📊 Choisis un classement...",
                         min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            embed=build_lb_embed(self.guild, self.values[0]), view=self.view
        )


class LbView(discord.ui.View):
    def __init__(self, author_id, guild):
        super().__init__(timeout=120)
        self.author_id = author_id
        self.add_item(LbDropdown(guild))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "Ce menu n'est pas à toi. Fais `*lb` pour voir le tien.", ephemeral=True
            )
            return False
        return True

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


@bot.command(name="lb", aliases=["leaderboard", "classement"])
async def _lb(ctx):
    if await check_ban(ctx):
        return
    view = LbView(ctx.author.id, ctx.guild)
    await ctx.send(embed=build_lb_embed(ctx.guild, "total"), view=view)


# ========================= VOCAL STATS =========================

@bot.command(name="vocalstats", aliases=["vstats"])
async def _vocalstats(ctx, member: discord.Member = None):
    target = member or ctx.author
    stats = vocal_get_stats(target.id, ctx.guild.id)
    total_min = stats.get("total_minutes", 0)
    total_earned = stats.get("total_earned", 0)

    # Durée lisible
    if total_min < 60:
        time_display = f"{total_min}min"
    elif total_min < 1440:
        h = total_min // 60
        m = total_min % 60
        time_display = f"{h}h{m:02d}"
    else:
        d = total_min // 1440
        h = (total_min % 1440) // 60
        time_display = f"{d}j{h}h"

    em = discord.Embed(
        title=f"🎤 Stats vocales — {target.display_name}",
        color=embed_color(),
    )
    em.set_thumbnail(url=target.display_avatar.url)
    em.add_field(name="⏱️ Temps cumulé", value=f"**{time_display}**", inline=True)
    em.add_field(name="💰 Gains totaux", value=f"**{format_ryo(total_earned)}**", inline=True)

    # Si en vocal actuellement, afficher la session en cours
    if target.voice and target.voice.channel:
        session = vocal_get_session(target.id, ctx.guild.id)
        if session:
            try:
                joined = datetime.fromisoformat(session["joined_at"])
                session_min = int((datetime.now(PARIS_TZ) - joined).total_seconds() / 60)
                em.add_field(
                    name="🟢 En cours",
                    value=f"{target.voice.channel.mention} ・ **{session_min}min**",
                    inline=False,
                )
            except (ValueError, TypeError):
                pass

    # Boost vocal actif ?
    boost = boost_get(target.id, ctx.guild.id, "vocal")
    if boost:
        try:
            until = datetime.fromisoformat(boost["expires_at"])
            ts = int(until.timestamp())
            em.add_field(
                name="⚡ Boost vocal actif",
                value=f"×{boost['multiplier']} jusqu'à <t:{ts}:R>",
                inline=False,
            )
        except (ValueError, TypeError):
            pass

    em.set_footer(text="Velda ・ Meira")
    await ctx.send(embed=em)


# ========================= ZONES VOCALES =========================

@bot.command(name="setzone")
async def _setzone(ctx, channel: discord.VoiceChannel = None, multiplier: float = None):
    """Définir un multiplicateur pour une voc. Ex : *setzone #voc-x2 2"""
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if channel is None or multiplier is None:
        return await ctx.send(embed=error_embed(
            "Usage",
            "`*setzone #voc-channel <multiplicateur>`\nEx : `*setzone #voc-x2 2`"
        ))
    if multiplier < 0.1 or multiplier > 10:
        return await ctx.send(embed=error_embed(
            "❌ Multiplicateur invalide",
            "Le multiplicateur doit être entre **0.1** et **10**."
        ))

    zone_add(channel.id, ctx.guild.id, multiplier, ctx.author.id)
    await ctx.send(embed=success_embed(
        "✅ Zone vocale définie",
        f"{channel.mention} → multiplicateur **×{multiplier}**"
    ))
    await send_log(ctx.guild, "Zone vocale définie", ctx.author,
                   desc=f"{channel.name} ×{multiplier}", color=0x43b581)


@bot.command(name="unsetzone")
async def _unsetzone(ctx, channel: discord.VoiceChannel = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if channel is None:
        return await ctx.send(embed=error_embed("Argument manquant", "Usage : `*unsetzone #voc-channel`"))
    if not zone_remove(channel.id):
        return await ctx.send(embed=error_embed("Pas de zone", f"{channel.mention} n'est pas une zone."))
    await ctx.send(embed=success_embed("✅ Zone retirée", f"{channel.mention} n'a plus de multiplicateur."))
    await send_log(ctx.guild, "Zone vocale retirée", ctx.author,
                   desc=channel.name, color=0xfaa61a)


@bot.command(name="zones")
async def _zones(ctx):
    """Liste les zones vocales configurées."""
    zones = zone_list(ctx.guild.id)
    if not zones:
        return await ctx.send(embed=info_embed(
            "🎤 Zones vocales",
            "Aucune zone configurée.\n"
            f"Utilise `*setzone #voc <mult>` pour en créer une."
        ))
    lines = []
    for z in zones:
        ch = ctx.guild.get_channel(int(z["channel_id"]))
        ch_display = ch.mention if ch else f"*Salon supprimé* (`{z['channel_id']}`)"
        lines.append(f"• {ch_display} → ×**{z['multiplier']}**")
    em = info_embed(f"🎤 Zones vocales ({len(zones)})", "\n".join(lines))
    em.set_footer(text="Velda ・ Multiplicateurs appliqués aux gains vocaux")
    await ctx.send(embed=em)


# ========================= CONFIG GAINS VOCAUX =========================

@bot.command(name="setvocalgain")
async def _setvocalgain(ctx, field: str = None, value: str = None):
    """Configurer les gains vocaux. Champs : base, talk, stream, cam, interval"""
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))

    if not field or value is None:
        return await ctx.send(embed=error_embed(
            "Usage",
            f"`*setvocalgain <champ> <valeur>`\n\n"
            f"**Champs possibles :**\n"
            f"• `base` — gain de base en voc\n"
            f"• `talk` — bonus si non-mute\n"
            f"• `stream` — bonus si stream\n"
            f"• `cam` — bonus si caméra\n"
            f"• `interval` — intervalle entre 2 ticks (en minutes)\n\n"
            f"Voir la config actuelle : `*vocalconfig`"
        ))

    field = field.lower().strip()
    valid = {"base", "talk", "stream", "cam", "interval"}
    if field not in valid:
        return await ctx.send(embed=error_embed(
            "❌ Champ invalide",
            f"Champs valides : `{'`, `'.join(valid)}`"
        ))

    try:
        int_value = int(value)
    except ValueError:
        return await ctx.send(embed=error_embed("❌ Valeur invalide", "Entier positif requis."))

    if int_value < 0:
        return await ctx.send(embed=error_embed("❌ Valeur invalide", "Doit être ≥ 0."))
    if field == "interval" and (int_value < 1 or int_value > 1440):
        return await ctx.send(embed=error_embed("❌ Interval invalide", "Entre 1 et 1440 minutes."))

    gains = get_vocal_gains()
    gains[field] = int_value
    set_vocal_gains(gains)

    await ctx.send(embed=success_embed(
        "✅ Config vocale mise à jour",
        f"**{field}** = **{int_value}**"
    ))
    await send_log(ctx.guild, "Gains vocaux modifiés", ctx.author,
                   desc=f"{field} = {int_value}", color=0x43b581)


@bot.command(name="vocalconfig")
async def _vocalconfig(ctx):
    """Affiche la config actuelle des gains vocaux."""
    if not has_min_rank(ctx.author.id, 1):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Whitelist+** requis."))
    gains = get_vocal_gains()
    desc = (
        f"```\n"
        f"base     = {gains.get('base', 50)} Ryo\n"
        f"talk     = +{gains.get('talk', 25)} Ryo (non-mute)\n"
        f"stream   = +{gains.get('stream', 50)} Ryo\n"
        f"cam      = +{gains.get('cam', 75)} Ryo\n"
        f"interval = {gains.get('interval', 15)} min\n"
        f"```\n"
        f"**Exemple de gain max** (talk + stream + cam) : "
        f"**{gains.get('base',50) + gains.get('talk',25) + gains.get('stream',50) + gains.get('cam',75)} Ryo** "
        f"toutes les {gains.get('interval', 15)}min.\n\n"
        f"*Modifie via `*setvocalgain <champ> <valeur>` (Sys+)*"
    )
    em = info_embed("🎤 Configuration des gains vocaux", desc)
    await ctx.send(embed=em)


# ========================= CONFIG COOLDOWNS =========================

@bot.command(name="setcooldown")
async def _setcooldown(ctx, game: str = None, seconds: int = None):
    """Modifier le cooldown d'un jeu. Ex : *setcooldown slots 10"""
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not game or seconds is None:
        return await ctx.send(embed=error_embed(
            "Usage",
            "`*setcooldown <jeu> <secondes>`\n"
            f"Jeux : `{'`, `'.join(DEFAULT_GAME_COOLDOWNS.keys())}`"
        ))
    game = game.lower().strip()
    if game not in DEFAULT_GAME_COOLDOWNS:
        return await ctx.send(embed=error_embed(
            "❌ Jeu inconnu",
            f"Jeux valides : `{'`, `'.join(DEFAULT_GAME_COOLDOWNS.keys())}`"
        ))
    if seconds < 0 or seconds > 3600:
        return await ctx.send(embed=error_embed("❌ Valeur invalide", "Entre 0 et 3600 secondes."))

    set_game_cooldown(game, seconds)
    await ctx.send(embed=success_embed(
        "✅ Cooldown modifié",
        f"Cooldown de **{game}** : **{seconds}s**"
    ))


@bot.command(name="cooldowns")
async def _cooldowns(ctx):
    """Affiche les cooldowns actuels des jeux."""
    cds = get_game_cooldowns()
    lines = [f"• `{game}` → **{sec}s**" for game, sec in sorted(cds.items())]
    desc = "\n".join(lines) + "\n\n*Les Sys+ peuvent modifier via `*setcooldown <jeu> <secondes>`*"
    em = info_embed("⏰ Cooldowns des jeux", desc)
    await ctx.send(embed=em)


# ========================= LOTO =========================

@bot.command(name="loto", aliases=["loterie"])
async def _loto(ctx, action: str = None):
    """*loto = voir état. *loto ticket = acheter. *loto tirage (Sys+). *loto date <date>."""
    if await check_ban(ctx):
        return

    cfg = loto_get_config(ctx.guild.id)
    tickets = loto_get_tickets(ctx.guild.id)
    pot = len(tickets) * LOTO_TICKET_PRICE

    if action is None:
        # Affichage état
        my_tickets = loto_count_user_tickets(ctx.author.id, ctx.guild.id)
        next_draw_str = "non définie"
        if cfg.get("next_draw_at"):
            try:
                next_draw = datetime.fromisoformat(cfg["next_draw_at"])
                ts = int(next_draw.timestamp())
                next_draw_str = f"<t:{ts}:F> (<t:{ts}:R>)"
            except (ValueError, TypeError):
                pass

        em = discord.Embed(
            title="🎰 Loterie",
            color=0xf1c40f,
        )
        em.add_field(name="🎟️ Tickets vendus", value=f"**{len(tickets)}**", inline=True)
        em.add_field(name="💰 Cagnotte", value=f"**{format_ryo(pot)}**", inline=True)
        em.add_field(name="🎫 Prix du ticket", value=f"**{format_ryo(LOTO_TICKET_PRICE)}**", inline=True)
        em.add_field(name="⏰ Prochain tirage", value=next_draw_str, inline=False)
        em.add_field(name="🎟️ Tes tickets", value=f"**{my_tickets}**", inline=True)
        if cfg.get("last_winner_id"):
            em.add_field(
                name="🏆 Dernier gagnant",
                value=f"<@{cfg['last_winner_id']}> ・ {format_ryo(cfg.get('last_prize', 0))}",
                inline=False,
            )
        em.set_footer(text=f"Velda ・ *loto ticket pour participer")
        return await ctx.send(embed=em)

    action = action.lower().strip()

    if action in ("ticket", "buy", "acheter"):
        # Achat d'un ticket
        async with eco_lock:
            eco = get_economy(ctx.author.id)
            if eco["hand"] < LOTO_TICKET_PRICE:
                return await ctx.send(embed=error_embed(
                    "Fonds insuffisants",
                    f"Il te faut **{format_ryo(LOTO_TICKET_PRICE)}** en main."
                ))
            if not atomic_hand_delta(ctx.author.id, -LOTO_TICKET_PRICE, min_hand=0):
                return await ctx.send(embed=error_embed("Erreur", "Impossible de débiter."))
        loto_buy_ticket(ctx.author.id, ctx.guild.id)

        my_tickets = loto_count_user_tickets(ctx.author.id, ctx.guild.id)
        await ctx.send(embed=success_embed(
            "🎟️ Ticket acheté",
            f"Tu as maintenant **{my_tickets}** ticket(s) pour la prochaine loto."
        ))
        return

    if action in ("tirage", "draw"):
        # Tirage manuel (Sys+)
        if not has_min_rank(ctx.author.id, 3):
            return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis pour forcer un tirage."))
        if not tickets:
            return await ctx.send(embed=error_embed("❌ Aucun ticket", "Personne n'a de ticket, pas de tirage possible."))
        await _do_loto_draw(ctx.guild, triggered_by="manuel")
        await ctx.send(embed=success_embed("✅ Tirage effectué", "Voir l'annonce."))
        return

    if action in ("date", "setdate"):
        return await ctx.send(embed=error_embed(
            "Usage",
            "Pour modifier la date : `*lotodate <durée>`\n"
            "Ex : `*lotodate 7j` pour reporter à +7 jours\n"
            "Ex : `*lotodate 2h30m` pour un tirage dans 2h30"
        ))

    await ctx.send(embed=error_embed("❌ Action inconnue",
        "Actions : `ticket`, `tirage` (Sys+), ou juste `*loto` pour voir l'état."))


@bot.command(name="lotodate")
async def _lotodate(ctx, *, duration: str = None):
    """Modifier la date du prochain tirage. Ex : *lotodate 7j, *lotodate 2h30m"""
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not duration:
        return await ctx.send(embed=error_embed(
            "Usage",
            "`*lotodate <durée>`\nEx : `*lotodate 7j`, `*lotodate 2h`, `*lotodate 30m`"
        ))

    # Parse la durée (format : 7j, 2h, 30m, ou combinaisons 2h30m, 1j12h)
    import re as _re
    total_seconds = 0
    for match in _re.finditer(r"(\d+)\s*(j|jour|jours|d|day|h|hour|hours|m|min|minute|minutes|s|sec)?", duration.lower()):
        val = int(match.group(1))
        unit = match.group(2) or "m"
        if unit in ("s", "sec"):
            total_seconds += val
        elif unit in ("m", "min", "minute", "minutes"):
            total_seconds += val * 60
        elif unit in ("h", "hour", "hours"):
            total_seconds += val * 3600
        elif unit in ("j", "jour", "jours", "d", "day"):
            total_seconds += val * 86400

    if total_seconds <= 0:
        return await ctx.send(embed=error_embed("❌ Durée invalide", "Format attendu : `7j`, `2h`, `30m`, etc."))

    next_draw = datetime.now(PARIS_TZ) + timedelta(seconds=total_seconds)
    loto_set_next_draw(ctx.guild.id, next_draw.isoformat())

    ts = int(next_draw.timestamp())
    await ctx.send(embed=success_embed(
        "✅ Date du tirage modifiée",
        f"Prochain tirage : <t:{ts}:F> (<t:{ts}:R>)"
    ))
    await send_log(ctx.guild, "Date loto modifiée", ctx.author,
                   desc=f"Tirage dans {duration}", color=0xf1c40f)


# ========================= SHOP =========================

ITEM_TYPES = {
    "role":         {"emoji": "🎭", "label": "Rôle permanent"},
    "temp_role":    {"emoji": "⏳", "label": "Rôle temporaire"},
    "collectible":  {"emoji": "🎁", "label": "Item collection"},
    "boost_xp":     {"emoji": "✨", "label": "Boost XP"},
    "boost_vocal":  {"emoji": "🎤", "label": "Boost vocal"},
}


def build_shop_embed(guild):
    items = shop_list_items(guild.id)
    em = discord.Embed(title="🛒 Shop", color=embed_color())
    if not items:
        em.description = "*Le shop est vide pour le moment.*\n\n*Les Sys peuvent ajouter des items via `*additem <nom>`.*"
    else:
        lines = []
        for item in items:
            t = ITEM_TYPES.get(item["item_type"], {"emoji": "📦", "label": item["item_type"]})
            stock_txt = "♾️ illimité" if item["stock"] is None else f"📦 {item['stock']} restants"
            extras = ""
            if item["item_type"] == "temp_role" and item.get("duration_hours"):
                extras = f" ・ {item['duration_hours']}h"
            elif item["item_type"] in ("boost_xp", "boost_vocal") and item.get("multiplier"):
                extras = f" ・ ×{item['multiplier']} pendant {item.get('duration_hours', 24)}h"
            lines.append(
                f"**#{item['id']}** ・ {t['emoji']} **{item['name']}** ・ "
                f"**{format_ryo(item['price'])}** ・ {stock_txt}{extras}\n"
                f"   ↳ *{item.get('description') or 'Sans description'}*"
            )
        em.description = "\n\n".join(lines)
    em.set_footer(text="Velda ・ *buy <id> pour acheter")
    return em


@bot.command(name="shop")
async def _shop(ctx):
    if await check_ban(ctx):
        return
    await ctx.send(embed=build_shop_embed(ctx.guild))


@bot.command(name="buy")
async def _buy(ctx, item_id: int = None):
    """Acheter un item du shop par son ID."""
    if await check_ban(ctx):
        return
    if item_id is None:
        return await ctx.send(embed=error_embed("Argument manquant", "Usage : `*buy <id>`"))

    item = shop_get_item(item_id)
    if not item or str(item["guild_id"]) != str(ctx.guild.id):
        return await ctx.send(embed=error_embed("❌ Item introuvable", f"Aucun item `#{item_id}` dans ce shop."))

    # Stock ?
    if item["stock"] is not None and item["stock"] <= 0:
        return await ctx.send(embed=error_embed("❌ Rupture de stock", f"**{item['name']}** n'est plus disponible."))

    price = int(item["price"])

    async with eco_lock:
        eco = get_economy(ctx.author.id)
        # On accepte le paiement depuis la main ET la banque combinés
        total_money = eco["hand"] + eco["bank"]
        if total_money < price:
            return await ctx.send(embed=error_embed(
                "Fonds insuffisants",
                f"Il te faut **{format_ryo(price)}** (tu as **{format_ryo(total_money)}** au total).\n"
                f"Hand : {format_ryo(eco['hand'])} ・ Bank : {format_ryo(eco['bank'])}"
            ))

        # On débite en priorité la main, sinon la banque
        if eco["hand"] >= price:
            ok = atomic_hand_delta(ctx.author.id, -price, min_hand=0)
        else:
            # débiter main complète + reste sur bank
            hand_part = eco["hand"]
            bank_part = price - hand_part
            ok = atomic_hand_bank(ctx.author.id, -hand_part, -bank_part)
        if not ok:
            return await ctx.send(embed=error_embed("Erreur", "Impossible de débiter."))

    # Décrémente stock atomique
    if not shop_decrement_stock(item_id):
        # Rollback : re-créditer
        async with eco_lock:
            atomic_hand_delta(ctx.author.id, price, min_hand=0)
        return await ctx.send(embed=error_embed("❌ Rupture", "Stock épuisé entre temps, remboursé."))

    # Appliquer l'effet
    item_type = item["item_type"]
    result_desc = ""
    expires_at = None

    if item_type == "role":
        role = ctx.guild.get_role(int(item["role_id"])) if item.get("role_id") else None
        if role:
            try:
                await ctx.author.add_roles(role, reason=f"Achat shop #{item_id}")
                result_desc = f"🎭 Rôle **{role.name}** ajouté."
            except discord.Forbidden:
                result_desc = "⚠️ Rôle non ajouté (permissions manquantes). Contacte un admin."
            except discord.HTTPException as e:
                result_desc = f"⚠️ Erreur rôle : {e}"
        else:
            result_desc = "⚠️ Rôle associé introuvable. Contacte un admin."

    elif item_type == "temp_role":
        role = ctx.guild.get_role(int(item["role_id"])) if item.get("role_id") else None
        if role:
            duration = item.get("duration_hours", 24)
            try:
                await ctx.author.add_roles(role, reason=f"Achat temp_role shop #{item_id}")
                expires_at = (datetime.now(PARIS_TZ) + timedelta(hours=duration)).isoformat()
                ts = int(datetime.fromisoformat(expires_at).timestamp())
                result_desc = f"⏳ Rôle **{role.name}** ajouté (expire <t:{ts}:R>)."
            except discord.Forbidden:
                result_desc = "⚠️ Rôle non ajouté (permissions manquantes)."

    elif item_type == "boost_xp":
        mult = item.get("multiplier") or 2.0
        duration = item.get("duration_hours", 24)
        boost_add(ctx.author.id, ctx.guild.id, "xp", mult, duration)
        expires_at = (datetime.now(PARIS_TZ) + timedelta(hours=duration)).isoformat()
        result_desc = f"✨ Boost XP ×{mult} actif pendant **{duration}h**."

    elif item_type == "boost_vocal":
        mult = item.get("multiplier") or 2.0
        duration = item.get("duration_hours", 24)
        boost_add(ctx.author.id, ctx.guild.id, "vocal", mult, duration)
        expires_at = (datetime.now(PARIS_TZ) + timedelta(hours=duration)).isoformat()
        result_desc = f"🎤 Boost vocal ×{mult} actif pendant **{duration}h**."

    elif item_type == "collectible":
        result_desc = f"🎁 **{item['name']}** ajouté à ton inventaire."

    # Enregistre dans l'inventaire
    inv_add(ctx.author.id, ctx.guild.id, item_id, item["name"], expires_at)

    em = success_embed(
        f"🛒 Achat : {item['name']}",
        f"💰 Payé : **{format_ryo(price)}**\n{result_desc}"
    )
    await ctx.send(embed=em)
    await send_log(ctx.guild, "Achat shop", ctx.author,
                   desc=f"Item #{item_id} `{item['name']}` ・ {format_ryo(price)}",
                   color=0x43b581)


@bot.command(name="inventaire", aliases=["inv", "inventory"])
async def _inventaire(ctx, member: discord.Member = None):
    if await check_ban(ctx):
        return
    target = member or ctx.author
    inv = inv_list(target.id, ctx.guild.id, active_only=True)
    if not inv:
        return await ctx.send(embed=info_embed(
            f"🎁 Inventaire — {target.display_name}",
            "*Inventaire vide.*"
        ))
    lines = []
    for i in inv[:25]:
        item = shop_get_item(i["item_id"])
        t_meta = ITEM_TYPES.get(item["item_type"], {"emoji": "📦"}) if item else {"emoji": "📦"}
        expires_part = ""
        if i["expires_at"]:
            try:
                ts = int(datetime.fromisoformat(i["expires_at"]).timestamp())
                expires_part = f" ・ expire <t:{ts}:R>"
            except (ValueError, TypeError):
                pass
        lines.append(f"{t_meta['emoji']} **{i['item_name_snapshot']}**{expires_part}")

    em = discord.Embed(
        title=f"🎁 Inventaire — {target.display_name}",
        description="\n".join(lines),
        color=embed_color(),
    )
    em.set_thumbnail(url=target.display_avatar.url)
    if len(inv) > 25:
        em.set_footer(text=f"Velda ・ {len(inv)} items, 25 affichés")
    else:
        em.set_footer(text=f"Velda ・ {len(inv)} item(s)")
    await ctx.send(embed=em)


# ========================= SHOP ADMIN =========================

class AddItemModal(discord.ui.Modal, title="Ajouter un item au shop"):
    def __init__(self, item_type, item_name):
        super().__init__()
        self.item_type = item_type
        self.item_name = item_name

        self.price_input = discord.ui.TextInput(
            label="Prix (en Ryo)",
            placeholder="Ex : 50000",
            required=True,
            max_length=12,
        )
        self.desc_input = discord.ui.TextInput(
            label="Description",
            placeholder="Description affichée dans le shop",
            required=False,
            max_length=200,
            style=discord.TextStyle.paragraph,
        )
        self.stock_input = discord.ui.TextInput(
            label="Stock (vide = illimité, ou un nombre)",
            placeholder="Ex : 5 ou laisse vide",
            required=False,
            max_length=6,
        )
        self.extra_input = discord.ui.TextInput(
            label=self._extra_label(),
            placeholder=self._extra_placeholder(),
            required=self.item_type in ("role", "temp_role", "boost_xp", "boost_vocal"),
            max_length=30,
        )
        self.extra2_input = discord.ui.TextInput(
            label=self._extra2_label(),
            placeholder=self._extra2_placeholder(),
            required=self.item_type in ("temp_role", "boost_xp", "boost_vocal"),
            max_length=6,
        )

        self.add_item(self.price_input)
        self.add_item(self.desc_input)
        self.add_item(self.stock_input)
        self.add_item(self.extra_input)
        self.add_item(self.extra2_input)

    def _extra_label(self):
        if self.item_type in ("role", "temp_role"):
            return "ID du rôle Discord à donner"
        if self.item_type == "boost_xp":
            return "Multiplicateur XP (ex : 2.0)"
        if self.item_type == "boost_vocal":
            return "Multiplicateur vocal (ex : 2.0)"
        return "Non utilisé (laisse vide)"

    def _extra_placeholder(self):
        if self.item_type in ("role", "temp_role"):
            return "Clique droit sur le rôle → Copier l'ID"
        if self.item_type in ("boost_xp", "boost_vocal"):
            return "2.0 = double XP/gain vocal"
        return "Laisse vide"

    def _extra2_label(self):
        if self.item_type == "temp_role":
            return "Durée du rôle (en heures)"
        if self.item_type in ("boost_xp", "boost_vocal"):
            return "Durée du boost (en heures)"
        return "Non utilisé (laisse vide)"

    def _extra2_placeholder(self):
        if self.item_type == "temp_role":
            return "Ex : 24 pour 1 jour"
        if self.item_type in ("boost_xp", "boost_vocal"):
            return "Ex : 24 pour 1 jour"
        return "Laisse vide"

    async def on_submit(self, interaction: discord.Interaction):
        try:
            price = int(self.price_input.value.strip())
            if price < 0:
                raise ValueError
        except ValueError:
            return await interaction.response.send_message(
                "❌ Prix invalide (entier positif requis).", ephemeral=True
            )

        description = self.desc_input.value.strip() or None
        stock_raw = self.stock_input.value.strip()
        stock = None
        if stock_raw:
            try:
                stock = int(stock_raw)
                if stock < 0:
                    raise ValueError
            except ValueError:
                return await interaction.response.send_message(
                    "❌ Stock invalide (entier positif ou vide pour illimité).", ephemeral=True
                )

        role_id = None
        multiplier = None
        duration_hours = None

        if self.item_type in ("role", "temp_role"):
            try:
                role_id = int(self.extra_input.value.strip())
            except ValueError:
                return await interaction.response.send_message(
                    "❌ ID de rôle invalide.", ephemeral=True
                )
            role = interaction.guild.get_role(role_id)
            if not role:
                return await interaction.response.send_message(
                    f"❌ Rôle `{role_id}` introuvable sur ce serveur.", ephemeral=True
                )

        if self.item_type in ("boost_xp", "boost_vocal"):
            try:
                multiplier = float(self.extra_input.value.strip())
                if multiplier < 0.1 or multiplier > 10:
                    raise ValueError
            except ValueError:
                return await interaction.response.send_message(
                    "❌ Multiplicateur invalide (entre 0.1 et 10).", ephemeral=True
                )

        if self.item_type in ("temp_role", "boost_xp", "boost_vocal"):
            try:
                duration_hours = int(self.extra2_input.value.strip())
                if duration_hours < 1 or duration_hours > 8760:
                    raise ValueError
            except ValueError:
                return await interaction.response.send_message(
                    "❌ Durée invalide (1 à 8760 heures).", ephemeral=True
                )

        item_id = shop_add_item(
            interaction.guild.id, self.item_name, price, description,
            self.item_type, role_id=role_id, duration_hours=duration_hours,
            multiplier=multiplier, stock=stock, created_by=interaction.user.id,
        )
        stock_display = "♾️ illimité" if stock is None else f"{stock}"
        extras = ""
        if role_id:
            role = interaction.guild.get_role(role_id)
            extras = f"\nRôle : {role.mention if role else role_id}"
        if multiplier:
            extras += f"\nMultiplicateur : ×{multiplier}"
        if duration_hours:
            extras += f"\nDurée : {duration_hours}h"

        await interaction.response.send_message(
            embed=success_embed(
                f"✅ Item ajouté : {self.item_name}",
                f"**ID :** `#{item_id}`\n"
                f"**Prix :** {format_ryo(price)}\n"
                f"**Stock :** {stock_display}"
                f"{extras}"
            )
        )


class AddItemTypeSelect(discord.ui.Select):
    def __init__(self, item_name):
        self.item_name = item_name
        options = [
            discord.SelectOption(
                label=meta["label"], value=key, emoji=meta["emoji"]
            )
            for key, meta in ITEM_TYPES.items()
        ]
        super().__init__(
            placeholder="Choisis le type d'item...",
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        item_type = self.values[0]
        modal = AddItemModal(item_type, self.item_name)
        await interaction.response.send_modal(modal)


class AddItemTypeView(discord.ui.View):
    def __init__(self, author_id, item_name):
        super().__init__(timeout=120)
        self.author_id = author_id
        self.add_item(AddItemTypeSelect(item_name))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.author_id


@bot.command(name="additem")
async def _additem(ctx, *, item_name: str = None):
    """Ajouter un item au shop. Ouvre un menu pour choisir le type puis un formulaire."""
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not item_name:
        return await ctx.send(embed=error_embed(
            "Usage",
            "`*additem <nom de l'item>`\nEx : `*additem Rôle VIP mensuel`"
        ))
    item_name = item_name.strip()[:100]
    em = info_embed(
        "🛒 Ajouter un item au shop",
        f"Item : **{item_name}**\n\nChoisis le type ci-dessous :"
    )
    view = AddItemTypeView(ctx.author.id, item_name)
    await ctx.send(embed=em, view=view)


@bot.command(name="edititem")
async def _edititem(ctx, item_id: int = None, field: str = None, *, value: str = None):
    """Modifier un champ d'un item. Champs : name, price, description, stock, multiplier, duration_hours"""
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if item_id is None or not field or value is None:
        return await ctx.send(embed=error_embed(
            "Usage",
            "`*edititem <id> <champ> <valeur>`\n"
            "Champs : `name`, `price`, `description`, `stock`, `multiplier`, `duration_hours`"
        ))

    item = shop_get_item(item_id)
    if not item or str(item["guild_id"]) != str(ctx.guild.id):
        return await ctx.send(embed=error_embed("❌ Item introuvable", f"Aucun item `#{item_id}`."))

    field = field.lower().strip()
    allowed = {"name", "price", "description", "stock", "multiplier", "duration_hours"}
    if field not in allowed:
        return await ctx.send(embed=error_embed(
            "❌ Champ invalide",
            f"Champs valides : `{'`, `'.join(allowed)}`"
        ))

    # Parse valeur selon champ
    parsed = value.strip()
    if field in ("price", "stock", "duration_hours"):
        if field == "stock" and parsed.lower() in ("illimite", "unlimited", "∞", "none", "null"):
            parsed = None
        else:
            try:
                parsed = int(parsed)
                if parsed < 0:
                    raise ValueError
            except ValueError:
                return await ctx.send(embed=error_embed("❌ Valeur invalide", "Entier positif requis."))
    elif field == "multiplier":
        try:
            parsed = float(parsed)
            if parsed < 0.1 or parsed > 10:
                raise ValueError
        except ValueError:
            return await ctx.send(embed=error_embed("❌ Valeur invalide", "Entre 0.1 et 10."))

    if not shop_update_item(item_id, **{field: parsed}):
        return await ctx.send(embed=error_embed("❌ Erreur", "Impossible de mettre à jour."))

    await ctx.send(embed=success_embed(
        "✅ Item modifié",
        f"Item `#{item_id}` ・ **{field}** = `{parsed}`"
    ))


@bot.command(name="removeitem", aliases=["delitem"])
async def _removeitem(ctx, item_id: int = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if item_id is None:
        return await ctx.send(embed=error_embed("Usage", "`*removeitem <id>`"))
    item = shop_get_item(item_id)
    if not item or str(item["guild_id"]) != str(ctx.guild.id):
        return await ctx.send(embed=error_embed("❌ Item introuvable", f"Aucun item `#{item_id}`."))
    if not shop_remove_item(item_id):
        return await ctx.send(embed=error_embed("❌ Erreur", "Impossible de supprimer."))
    await ctx.send(embed=success_embed("✅ Item supprimé", f"`{item['name']}` retiré du shop."))


@bot.command(name="setstock")
async def _setstock(ctx, item_id: int = None, stock: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if item_id is None or stock is None:
        return await ctx.send(embed=error_embed(
            "Usage",
            "`*setstock <id> <nombre>`\nOu : `*setstock <id> illimite` pour retirer la limite"
        ))
    item = shop_get_item(item_id)
    if not item or str(item["guild_id"]) != str(ctx.guild.id):
        return await ctx.send(embed=error_embed("❌ Item introuvable", f"Aucun item `#{item_id}`."))

    if stock.lower() in ("illimite", "unlimited", "∞", "none", "null"):
        parsed = None
    else:
        try:
            parsed = int(stock)
            if parsed < 0:
                raise ValueError
        except ValueError:
            return await ctx.send(embed=error_embed("❌ Valeur invalide", "Entier positif ou `illimite`."))

    shop_update_item(item_id, stock=parsed)
    stock_display = "♾️ illimité" if parsed is None else f"**{parsed}**"
    await ctx.send(embed=success_embed("✅ Stock modifié", f"Item `#{item_id}` : stock = {stock_display}"))


# ========================= ERROR HANDLING =========================

@bot.event
async def on_command_error(ctx, error):
    # Unwrap les CommandInvokeError
    if isinstance(error, commands.CommandInvokeError):
        error = error.original

    # FIX: salon non autorisé → réaction ❌ discrète, pas de message pour éviter le spam
    if isinstance(error, ChannelNotAllowedError):
        try:
            await ctx.message.add_reaction("❌")
        except discord.HTTPException:
            pass
        return

    if isinstance(error, (commands.MemberNotFound, commands.UserNotFound)):
        await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Impossible de trouver cet utilisateur."))
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=error_embed(
            "❌ Argument manquant",
            f"Tu as oublié l'argument : `{error.param.name}`."
        ))
    elif isinstance(error, commands.BadArgument):
        await ctx.send(embed=error_embed("❌ Argument invalide", str(error)))
    elif isinstance(error, commands.CommandOnCooldown):
        await ctx.send(embed=error_embed(
            "⏰ Cooldown",
            f"Reviens dans {int(error.retry_after)}s."
        ))
    elif isinstance(error, commands.NoPrivateMessage):
        await ctx.send(embed=error_embed("❌ DM non supporté", "Cette commande ne marche qu'en serveur."))
    elif isinstance(error, commands.CommandNotFound):
        pass  # Silencieux pour éviter le bruit
    else:
        # FIX: vrai logging avec stack trace + notification à l'utilisateur
        log.error(
            f"Erreur non gérée dans la commande '{ctx.command}' par {ctx.author} : {error}\n"
            + "".join(traceback.format_exception(type(error), error, error.__traceback__))
        )
        try:
            await ctx.send(embed=error_embed(
                "❌ Erreur interne",
                "Une erreur inattendue est survenue. Les logs ont été générés."
            ))
        except discord.HTTPException:
            pass


# ========================= RUN =========================
if __name__ == "__main__":
    try:
        log.info("Démarrage de Velda...")
        bot.run(BOT_TOKEN, log_handler=None)  # log_handler=None : on garde notre logger
    except KeyboardInterrupt:
        log.info("Arrêt demandé par l'utilisateur.")
    except Exception as e:
        log.error(f"Erreur fatale au démarrage : {e}", exc_info=True)
        sys.exit(1)
