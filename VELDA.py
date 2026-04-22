import discord
from discord.ext import commands, tasks
import os
import sys
import sqlite3
import json
import random
import asyncio
import logging
import traceback
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ========================= CONFIG =========================
BOT_TOKEN = os.environ.get("TOKEN")
if not BOT_TOKEN:
    print("[ERREUR CRITIQUE] La variable d'environnement TOKEN n'est pas définie.")
    print("Définis-la avant de lancer le bot (ex: export TOKEN=ton_token).")
    sys.exit(1)

PARIS_TZ = ZoneInfo("Europe/Paris")
DEFAULT_BUYER_IDS = [1312375517927706630]
DEFAULT_PREFIX = "*"
MIN_BET = 100  # Mise minimum pour slots/jackpot/blackjack (évite le farm XP)
ROB_COOLDOWN = 3600  # 1h de cooldown sur *rob

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

# ========================= XP TABLE =========================
# Niveau i nécessite XP_TABLE[i] XP total (exponentiel)
def xp_for_level(level):
    return int(100 * (level ** 2.2))

# ========================= DATABASE =========================

def get_db():
    conn = sqlite3.connect("velda.db", timeout=30)
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


@bot.event
async def on_message(message):
    if message.author.bot:
        return
    if message.guild:
        track_message(message.guild.id, message.author.id, message.content)
    await bot.process_commands(message)


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
        "title": "💰  Économie",
        "items": [
            # (syntaxe, description, min_rank)
            ("bal [@user]",        "Balance d'un membre",     0),
            ("daily / dy",         "Récompense quotidienne",  0),
            ("dep [somme/all]",    "Déposer en bank",         0),
            ("with [somme/all]",   "Retirer de la bank",      0),
            ("give [somme] @user", "Donner des Ryo",          0),
            ("rob @user",          "Voler (5-30% main)",      0),
            ("fame @user",         "Famer quelqu'un",         0),
        ],
    },
    "jeux": {
        "emoji": "🎮",
        "label": "Jeux",
        "title": "🎮  Jeux",
        "items": [
            ("slots [somme/all]",   "Machine à sous", 0),
            ("bj [somme/all]",      "Blackjack",      0),
            ("jackpot [somme/all]", "Jackpot",        0),
            ("fish",                "Pêche (30min)",  0),
            ("work",                "Boulot (1h)",    0),
        ],
    },
    "speciaux": {
        "emoji": "🏆",
        "label": "Spéciaux",
        "title": "🏆  Spéciaux",
        "items": [
            ("enchere @role", "Lancer une enchère", 2),
            ("drop [somme]",  "Drop d'argent",      2),
            ("enquete",       "Lancer une enquête", 3),
        ],
    },
    "admin": {
        "emoji": "🔧",
        "label": "Admin",
        "title": "🔧  Admin",
        "items": [
            ("addmoney @user [somme]",    "Ajouter de l'argent", 3),
            ("removemoney @user [somme]", "Retirer de l'argent", 3),
            ("resetbal @user",            "Reset balance",       3),
            ("addxp @user [somme]",       "Ajouter de l'XP",     3),
            ("resetlevel @user",          "Reset niveau/XP",     3),
            ("ban @user",                 "Bannir du bot",       3),
            ("unban @user",               "Débannir du bot",     3),
        ],
    },
    "perms": {
        "emoji": "👥",
        "label": "Permissions",
        "title": "👥  Permissions",
        "items": [
            ("wl @user / unwl @user",       "Gérer la whitelist", 2),
            ("owner @user / unowner @user", "Gérer les owners",   3),
            ("sys @user / unsys @user",     "Gérer les sys",      4),
        ],
    },
    "system": {
        "emoji": "⚙️",
        "label": "Système",
        "title": "⚙️  Système",
        "items": [
            ("allow #salon",      "Autoriser un salon pour le bot", 3),
            ("unallow #salon",    "Retirer un salon autorisé",      3),
            ("allow",             "Lister les salons autorisés",    3),
            ("setenchere #salon", "Définir le salon des enchères",  3),
            ("setlog #salon",     "Définir le salon des logs",      4),
            ("prefix [nouveau]",  "Changer le prefix",              4),
        ],
    },
    "hierarchy": {
        "emoji": "📋",
        "label": "Hiérarchie",
        "title": "📋  Hiérarchie",
        "min_rank": 2,  # Cette catégorie n'est visible qu'à partir d'Owner
        "items": [],    # Contenu statique géré dans build_hierarchy_embed
    },
}


def accessible_items(category_key, user_rank):
    """Retourne la liste des (syntaxe, description) accessibles au user pour cette catégorie."""
    cat = HELP_CATEGORIES.get(category_key, {})
    return [(syntax, desc) for (syntax, desc, min_rank) in cat.get("items", []) if user_rank >= min_rank]


def category_visible(category_key, user_rank):
    """Une catégorie est visible si le user peut accéder à au moins une de ses commandes,
    ou si elle a un min_rank explicite qu'il atteint (cas de la hiérarchie)."""
    cat = HELP_CATEGORIES.get(category_key, {})
    if "min_rank" in cat:
        return user_rank >= cat["min_rank"]
    return len(accessible_items(category_key, user_rank)) > 0


def build_category_embed(category_key, user_rank):
    """Construit un embed pour la catégorie donnée, filtré au rang du user."""
    p = get_prefix_cached()
    cat = HELP_CATEGORIES[category_key]
    em = discord.Embed(title=cat["title"], color=embed_color())

    items = accessible_items(category_key, user_rank)
    if not items:
        em.description = "*Aucune commande accessible à ton rang.*"
    else:
        # Aligne les syntaxes pour un rendu code-block propre
        max_syntax = max(len(f"{p}{syntax}") for syntax, _ in items)
        lines = [
            f"{p}{syntax}".ljust(max_syntax + 2) + f"→ {desc}"
            for syntax, desc in items
        ]
        em.description = "```\n" + "\n".join(lines) + "\n```"

    em.set_footer(text="Made by gp ・ Velda")
    return em


def build_hierarchy_embed(user_rank):
    """Embed hiérarchie — uniquement visible pour Owner+ (min_rank=2)."""
    em = discord.Embed(title="📋  Hiérarchie", color=embed_color())
    lines = ["```\nBuyer > Sys > Owner > Whitelist > Tout le monde\n```\n"]
    # On affiche chaque niveau, mais on marque celui du user
    levels = [
        (4, "👑 **Buyer**",     "Accès total, `*prefix`, `*setlog`, `*sys`/`*unsys`"),
        (3, "🔧 **Sys**",       "`*allow`/`*unallow`, `*enquete`, `*setenchere`, `*ban`/`*unban`, `*owner`/`*unowner`, admin éco"),
        (2, "⭐ **Owner**",      "`*enchere`, `*drop`, `*wl`/`*unwl`"),
        (1, "✨ **Whitelist**",  "Statut privilégié"),
        (0, "👤 **Tout le monde**", "Jeux et commandes éco"),
    ]
    for rank, name, desc in levels:
        marker = " ← **toi**" if rank == user_rank else ""
        lines.append(f"> {name} — {desc}{marker}")
    em.description = "\n".join(lines)
    em.set_footer(text="Made by gp ・ Velda")
    return em


def build_home_embed(user_rank):
    """Embed d'accueil personnalisé : ne liste que les catégories accessibles au user."""
    p = get_prefix_cached()
    em = discord.Embed(color=embed_color())
    em.set_author(name="Velda - Bot Casino")

    rank_label = rank_name(user_rank)
    intro = (
        f"```\n🕐  {get_french_time()}\n```\n"
        f"Bienvenue sur **Velda**.\n\n"
        f"**Prefix :** `{p}` ・ **Ton rang :** {rank_label}\n\n"
    )

    # Liste uniquement les catégories visibles au user (hors Accueil lui-même)
    category_descriptions = {
        "eco":       "Bal, daily, dépôts, give, rob...",
        "jeux":      "Slots, BJ, Jackpot, Fish, Work",
        "speciaux":  "Enchères, Drop, Enquête",
        "admin":     "Gérer l'argent et l'XP des membres",
        "perms":     "Attribuer les rangs",
        "system":    "Configuration du bot",
        "hierarchy": "Qui peut faire quoi",
    }
    visible_lines = []
    for key, label in category_descriptions.items():
        if category_visible(key, user_rank):
            cat = HELP_CATEGORIES[key]
            visible_lines.append(f"> {cat['emoji']} **{cat['label']}** — {label}")

    em.description = intro + "\n".join(visible_lines) if visible_lines else intro
    em.set_footer(text="Made by gp ・ Velda")
    return em


def build_embed_for(category_key, user_rank):
    """Dispatcher : renvoie l'embed correspondant à la clé demandée."""
    if category_key == "home":
        return build_home_embed(user_rank)
    if category_key == "hierarchy":
        return build_hierarchy_embed(user_rank)
    return build_category_embed(category_key, user_rank)


class HelpDropdown(discord.ui.Select):
    def __init__(self, user_rank):
        self.user_rank = user_rank
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
        # Double-check : on n'affiche que ce que le user peut voir (au cas où)
        key = self.values[0]
        if key != "home" and not category_visible(key, self.user_rank):
            return await interaction.response.send_message(
                "Tu n'as pas accès à cette catégorie.", ephemeral=True
            )
        await interaction.response.edit_message(
            embed=build_embed_for(key, self.user_rank), view=self.view
        )


class HelpView(discord.ui.View):
    def __init__(self, author_id, user_rank):
        super().__init__(timeout=120)
        self.author_id = author_id
        self.user_rank = user_rank
        self.add_item(HelpDropdown(user_rank))

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
    user_rank = get_rank_db(ctx.author.id)
    view = HelpView(ctx.author.id, user_rank)
    await ctx.send(embed=build_home_embed(user_rank), view=view)


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


# ========================= ÉCONOMIE =========================

@bot.command(name="bal")
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

    em = discord.Embed(title=f"💰 Balance — {target.display_name}", color=embed_color())
    em.set_thumbnail(url=target.display_avatar.url)
    em.add_field(name="👜 En main", value=format_ryo(eco["hand"]), inline=True)
    em.add_field(name="🏦 En bank", value=format_ryo(eco["bank"]), inline=True)
    em.add_field(name="⭐ Fame", value=str(eco["fame"]), inline=True)
    em.add_field(name="🎯 Niveau", value=f"**{eco['level']}** / 100", inline=True)
    em.add_field(name="✨ XP", value=f"{xp_progress} / {xp_required if eco['level'] < 100 else 'MAX'}", inline=True)
    # Affiche l'escrow (argent bloqué dans les enchères actives) si > 0
    if eco.get("escrow", 0) > 0:
        em.add_field(name="🔒 En escrow", value=format_ryo(eco["escrow"]), inline=True)
    em.set_footer(text=f"Velda ・ {get_french_time()}")
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
    await ctx.send(embed=success_embed("✅ Daily récupéré !", f"+{format_ryo(amount)} en main !"))


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
            return await ctx.send(embed=error_embed("Fonds insuffisants", f"Tu n'as que {format_ryo(eco['hand'])} en main."))
        if not atomic_hand_bank(ctx.author.id, -amount, +amount):
            return await ctx.send(embed=error_embed("Erreur", "Le dépôt a échoué, réessaie."))
    await ctx.send(embed=success_embed("🏦 Dépôt effectué", f"+{format_ryo(amount)} déposés en bank."))


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
            return await ctx.send(embed=error_embed("Fonds insuffisants", f"Tu n'as que {format_ryo(eco['bank'])} en bank."))
        if not atomic_hand_bank(ctx.author.id, +amount, -amount):
            return await ctx.send(embed=error_embed("Erreur", "Le retrait a échoué, réessaie."))
    await ctx.send(embed=success_embed("👜 Retrait effectué", f"+{format_ryo(amount)} retirés en main."))


@bot.command(name="give")
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
            return await ctx.send(embed=error_embed("Fonds insuffisants", f"Tu n'as que {format_ryo(eco['hand'])} en main."))
        # FIX: transfert atomique (évite duplication/perte en cas de concurrence)
        if not atomic_transfer(ctx.author.id, target.id, amount):
            return await ctx.send(embed=error_embed("Erreur", "Le transfert a échoué, réessaie."))
    await ctx.send(embed=success_embed("✅ Don effectué", f"{ctx.author.mention} a donné {format_ryo(amount)} à {target.mention}."))


@bot.command(name="rob")
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
    await ctx.send(embed=success_embed("🥷 Vol réussi !", f"Tu as volé {format_ryo(stolen)} ({int(percent*100)}%) à {target.mention} !"))


@bot.command(name="fame")
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
        update_economy(target.id, fame=eco_target["fame"] + 1)
        update_economy(ctx.author.id, last_fame=now.isoformat())
    await ctx.send(embed=success_embed("⭐ Fame !", f"{ctx.author.mention} a famé {target.mention} ! ({eco_target['fame'] + 1} fame)"))


# ========================= JEUX =========================

@bot.command(name="work")
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

    em = discord.Embed(title=f"💼 Boulot — {job.capitalize()}", color=embed_color())
    em.description = f"{desc}\n\n**+{format_ryo(amount)}** gagnés !"
    em.set_footer(text="Velda")
    await ctx.send(embed=em)


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
    await add_xp(ctx, ctx.author.id, xp_gain.get(rarity, 10))

    rarity_colors = {"commun": 0x95a5a6, "peu commun": 0x2ecc71, "rare": 0x3498db, "épique": 0x9b59b6, "légendaire": 0xf1c40f, "déchet": 0x7f8c8d}
    em = discord.Embed(title=f"🎣 Pêche — {rarity.upper()}", color=rarity_colors.get(rarity, embed_color()))
    em.description = f"Tu as pêché un **{name}** !\n\n+**{format_ryo(amount)}** gagnés !"
    em.set_footer(text="Velda")
    await ctx.send(embed=em)


@bot.command(name="slots")
async def _slots(ctx, amount_str: str = None):
    if await check_ban(ctx):
        return
    if not amount_str:
        return await ctx.send(embed=error_embed("Argument manquant", "Usage : `*slots [somme/all]`"))
    async with eco_lock:
        eco = get_economy(ctx.author.id)
        amount = parse_amount(amount_str, eco["hand"])
        if amount is None or amount <= 0:
            return await ctx.send(embed=error_embed("Montant invalide", "Donne un montant valide."))
        # FIX: mise minimum pour éviter le farm XP avec 1 Ryo
        if amount < MIN_BET:
            return await ctx.send(embed=error_embed("Mise trop basse", f"Mise minimum : {format_ryo(MIN_BET)}"))
        if amount > eco["hand"]:
            return await ctx.send(embed=error_embed("Fonds insuffisants", f"Tu n'as que {format_ryo(eco['hand'])} en main."))

        symbols = ["🍒", "🍋", "🍊", "🍇", "⭐", "💎", "7️⃣"]
        weights = [30, 25, 20, 12, 7, 4, 2]
        reels = random.choices(symbols, weights=weights, k=3)

        multipliers = {"💎": 20, "7️⃣": 15, "⭐": 10, "🍇": 5, "🍊": 3, "🍋": 2, "🍒": 1.5}

        if reels[0] == reels[1] == reels[2]:
            mult = multipliers.get(reels[0], 2)
            winnings = int(amount * mult)
            result = f"JACKPOT ! x{mult} — +{format_ryo(winnings)}"
            color = 0xffd700
            atomic_hand_delta(ctx.author.id, -amount + winnings, min_hand=0)
            xp_reward = 50
        elif reels[0] == reels[1] or reels[1] == reels[2]:
            winnings = int(amount * 1.5)
            result = f"Deux identiques ! x1.5 — +{format_ryo(winnings)}"
            color = 0x43b581
            atomic_hand_delta(ctx.author.id, -amount + winnings, min_hand=0)
            xp_reward = 15
        else:
            result = f"Perdu — -{format_ryo(amount)}"
            color = 0xf04747
            atomic_hand_delta(ctx.author.id, -amount, min_hand=0)
            xp_reward = 5

    await add_xp(ctx, ctx.author.id, xp_reward)
    em = discord.Embed(title="🎰 Machine à sous", color=color)
    em.description = f"[ {reels[0]} | {reels[1]} | {reels[2]} ]\n\n{result}"
    em.set_footer(text="Velda")
    await ctx.send(embed=em)


@bot.command(name="jackpot")
async def _jackpot(ctx, amount_str: str = None):
    if await check_ban(ctx):
        return
    if not amount_str:
        return await ctx.send(embed=error_embed("Argument manquant", "Usage : `*jackpot [somme/all]`"))
    async with eco_lock:
        eco = get_economy(ctx.author.id)
        amount = parse_amount(amount_str, eco["hand"])
        if amount is None or amount <= 0:
            return await ctx.send(embed=error_embed("Montant invalide", "Donne un montant valide."))
        if amount < MIN_BET:
            return await ctx.send(embed=error_embed("Mise trop basse", f"Mise minimum : {format_ryo(MIN_BET)}"))
        if amount > eco["hand"]:
            return await ctx.send(embed=error_embed("Fonds insuffisants", f"Tu n'as que {format_ryo(eco['hand'])} en main."))

        roll = random.randint(1, 100)
        if roll <= 2:
            mult = 50
            result_text = f"🎊 **MEGA JACKPOT x50 !** (tirage : {roll}/100)"
            color = 0xffd700
        elif roll <= 10:
            mult = 10
            result_text = f"🎉 **Jackpot x10 !** (tirage : {roll}/100)"
            color = 0xf1c40f
        elif roll <= 30:
            mult = 2
            result_text = f"✅ **Gain x2** (tirage : {roll}/100)"
            color = 0x43b581
        else:
            mult = 0
            result_text = f"❌ **Perdu** (tirage : {roll}/100)"
            color = 0xf04747

        if mult > 0:
            winnings = int(amount * mult)
            atomic_hand_delta(ctx.author.id, -amount + winnings, min_hand=0)
            xp_reward = mult * 10
            result_text += f"\n\n+{format_ryo(winnings)} !"
        else:
            atomic_hand_delta(ctx.author.id, -amount, min_hand=0)
            xp_reward = 5
            result_text += f"\n\n-{format_ryo(amount)}"

    await add_xp(ctx, ctx.author.id, xp_reward)
    em = discord.Embed(title="🎲 Jackpot", color=color)
    em.description = result_text
    em.set_footer(text="Velda")
    await ctx.send(embed=em)


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
        em = discord.Embed(title="🃏 Blackjack", color=color or embed_color())
        em.add_field(name=f"Dealer ({dv if result else '?'})", value=self.format_hand(self.dealer_hand, hide_second=not result), inline=False)
        em.add_field(name=f"Toi ({pv})", value=self.format_hand(self.player_hand), inline=False)
        if result:
            em.add_field(name="Résultat", value=result, inline=False)
        em.set_footer(text=f"Mise : {format_ryo(self.amount)} ・ Velda")
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
        self.stop()


@bot.command(name="bj")
async def _bj(ctx, amount_str: str = None):
    if await check_ban(ctx):
        return
    if not amount_str:
        return await ctx.send(embed=error_embed("Argument manquant", "Usage : `*bj [somme/all]`"))

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
    def __init__(self, amount, author_id, message=None):
        super().__init__(timeout=120)
        self.amount = amount
        self.author_id = author_id
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
                await self.message.edit(embed=discord.Embed(
                    title="💸 Drop expiré",
                    description=f"Personne n'a réclamé les **{format_ryo(self.amount)}**...",
                    color=0xf04747
                ), view=self)
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
        await interaction.response.edit_message(embed=discord.Embed(
            title="💸 Drop réclamé !",
            description=f"{interaction.user.mention} a réclamé **{format_ryo(self.amount)}** !",
            color=0x43b581
        ), view=self)
        self.stop()


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

    view = DropView(amount, ctx.author.id)

    em = discord.Embed(title="💸 DROP !", color=0xffd700)
    em.description = (
        f"**{format_ryo(amount)}** sont en jeu !\n\n"
        f"⏳ Le bouton s'active dans **10 secondes**...\n"
        f"🏆 Le premier à cliquer remporte tout !"
    )
    em.set_footer(text=f"Drop lancé par {ctx.author.display_name} ・ Velda")

    msg = await ctx.send(embed=em, view=view)
    view.message = msg

    # Countdown
    for i in range(10, 0, -1):
        em.description = (
            f"**{format_ryo(amount)}** sont en jeu !\n\n"
            f"⏳ Le bouton s'active dans **{i} seconde{'s' if i > 1 else ''}**...\n"
            f"🏆 Le premier à cliquer remporte tout !"
        )
        try:
            await msg.edit(embed=em)
        except discord.HTTPException:
            pass
        await asyncio.sleep(1)

    # Active le bouton
    view.button_active = True
    for item in view.children:
        item.disabled = False
        item.label = "🎯 CLIQUER !"
        item.style = discord.ButtonStyle.danger

    em.description = (
        f"**{format_ryo(amount)}** sont en jeu !\n\n"
        f"🎯 **GO ! Clique maintenant !**\n"
        f"🏆 Le premier remporte tout !"
    )
    try:
        await msg.edit(embed=em, view=view)
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
        em = discord.Embed(title=f"🎪 Enchère — @{self.role.name}", color=0xffd700)
        if self.current_winner:
            em.description = (
                f"**Mise actuelle :** {format_ryo(self.current_bid)}\n"
                f"**Meilleur enchérisseur :** {self.current_winner.mention}\n"
                f"**Fin :** <t:{end_ts}:R>\n"
                f"**Prix :** Le rôle {self.role.mention} pour **48h** !\n\n"
                f"Clique sur **Enchérir** pour surenchérir ! (min : {format_ryo(self.min_next_bid())})"
            )
        else:
            em.description = (
                f"**Mise de départ :** {format_ryo(self.current_bid)}\n"
                f"**Fin :** <t:{end_ts}:R>\n"
                f"**Prix :** Le rôle {self.role.mention} pour **48h** !\n\n"
                f"Clique sur **Enchérir** pour participer !"
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

        em_end = discord.Embed(title=f"🏆 Enchère terminée — @{role.name}", color=0x43b581)
        em_end.description = (
            f"**Gagnant :** {view.current_winner.mention}\n"
            f"**Mise finale :** {format_ryo(view.current_bid)}\n"
            f"Le rôle est attribué pour **48h** !"
        )
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
        em_end = discord.Embed(title=f"❌ Enchère terminée — Aucun participant", color=0xf04747)
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

    # Lance la gestion du cycle de vie en tâche de fond
    await run_enchere_lifecycle(view, channel, role)


# ========================= ENQUÊTE =========================

SCENARIOS = [
    {
        "crime": "vol d'une somme importante de Ryo dans la banque du serveur",
        "lieux": ["la salle des coffres", "le couloir de surveillance", "le bureau du directeur"],
        "indices_templates": [
            "Un témoin a vu {suspect2} sortir précipitamment de {lieu} à 23h47.",
            "Des traces de {suspect1} ont été retrouvées près de {lieu}.",
            "La caméra de surveillance a filmé quelqu'un ressemblant à {coupable} entrer dans {lieu} avec une sacoche.",
        ]
    },
    {
        "crime": "disparition mystérieuse du cristal de puissance du serveur",
        "lieux": ["la chambre secrète", "le laboratoire", "le jardin enchanté"],
        "indices_templates": [
            "{suspect1} a été vu en train de discuter avec {suspect2} la nuit du crime près de {lieu}.",
            "Un message chiffré adressé à {coupable} a été trouvé dans {lieu}.",
            "Des empreintes de {suspect2} ont été découvertes à {lieu}, mais elles menaient dans la mauvaise direction...",
        ]
    },
    {
        "crime": "sabotage du tournoi de jeux organisé sur le serveur",
        "lieux": ["la salle d'arcade", "les coulisses", "la loge des joueurs"],
        "indices_templates": [
            "Un arbitre affirme avoir vu {suspect1} fouiller dans {lieu} avant le tournoi.",
            "Le matériel saboteur retrouvé dans {lieu} porte les initiales de {coupable}.",
            "{suspect2} prétend avoir un alibi mais des témoins le contredisent pour la période autour de {lieu}.",
        ]
    }
]


@bot.command(name="enquete")
async def _enquete(ctx):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))

    # Récupère des membres actifs
    active = get_active_members(ctx.guild.id, limit=20)
    active_members = []
    for a in active:
        m = ctx.guild.get_member(int(a["user_id"]))
        if m and not m.bot and m.id != ctx.author.id:
            active_members.append(m)

    # Déduplique
    seen = set()
    unique_members = []
    for m in active_members:
        if m.id not in seen:
            seen.add(m.id)
            unique_members.append(m)

    if len(unique_members) < 3:
        return await ctx.send(embed=error_embed("❌ Pas assez de membres actifs", "Il faut au moins 3 membres actifs récents."))

    # Choisit 3 à 5 personnes
    count = random.randint(3, min(5, len(unique_members)))
    chosen = random.sample(unique_members, count)
    roles_list = ["coupable", "témoin", "complice", "victime", "suspect"]
    random.shuffle(roles_list)

    coupable = chosen[0]
    scenario = random.choice(SCENARIOS)
    lieu = random.choice(scenario["lieux"])

    # Envoie les rôles en DM
    role_assignments = {}
    for i, member in enumerate(chosen):
        role = roles_list[i] if i < len(roles_list) else "suspect"
        role_assignments[member.id] = role
        try:
            em_dm = discord.Embed(title="🕵️ Enquête — Ton rôle secret", color=embed_color())
            em_dm.description = (
                f"Tu as été sélectionné dans l'enquête sur **{ctx.guild.name}** !\n\n"
                f"**Ton rôle : {role.upper()}**\n\n"
            )
            if role == "coupable":
                em_dm.description += "Tu es le **coupable**. Fais tout pour ne pas te faire démasquer !"
            elif role == "victime":
                em_dm.description += "Tu es la **victime**. Tu peux témoigner mais certains indices te mettront en cause à tort."
            elif role == "témoin":
                em_dm.description += "Tu es **témoin**. Tu as vu des choses... mais tu n'es pas obligé de tout dire."
            elif role == "complice":
                em_dm.description += "Tu es le **complice**. Aide le coupable sans te faire remarquer."
            else:
                em_dm.description += "Tu es **suspect**. Tous les regards se tournent vers toi, mais tu es innocent."
            em_dm.set_footer(text="Velda ・ Enquête")
            await member.send(embed=em_dm)
        except discord.Forbidden:
            log.info(f"Enquête : DM fermés pour {member} ({member.id})")
        except discord.HTTPException as e:
            log.warning(f"Enquête : échec DM à {member} : {e}")

    # Construit les indices
    suspect1 = chosen[1].display_name if len(chosen) > 1 else "Inconnu"
    suspect2 = chosen[2].display_name if len(chosen) > 2 else "Inconnu"
    indices = [t.format(
        coupable=coupable.display_name,
        suspect1=suspect1,
        suspect2=suspect2,
        lieu=lieu
    ) for t in scenario["indices_templates"]]

    # Lance l'enquête
    participants_str = ", ".join([m.mention for m in chosen])
    em = discord.Embed(title="🕵️ ENQUÊTE OUVERTE", color=0x3498db)
    em.description = (
        f"**Crime :** {scenario['crime']}\n\n"
        f"**Personnes impliquées :** {participants_str}\n\n"
        f"*Les rôles ont été envoyés en DM aux personnes concernées.*\n\n"
        f"**3 indices vont être révélés progressivement...**\n"
        f"Pour désigner le coupable : tapez le nom du suspect dans le chat !"
    )
    em.set_footer(text="Velda ・ Enquête")
    await ctx.send(embed=em)

    # Révèle les indices progressivement
    for i, indice in enumerate(indices):
        await asyncio.sleep(30)
        em_indice = discord.Embed(title=f"🔍 Indice {i+1}/3", color=0x3498db)
        em_indice.description = indice
        em_indice.set_footer(text="Velda ・ Enquête")
        await ctx.send(embed=em_indice)

    await asyncio.sleep(20)

    # Attente des réponses
    em_guess = discord.Embed(title="⏰ Dernière chance !", color=0xffa500)
    em_guess.description = "Tapez le **nom du coupable** dans le chat ! Vous avez 30 secondes !"
    em_guess.set_footer(text="Velda ・ Enquête")
    await ctx.send(embed=em_guess)

    correct_guessers = []

    def check(m):
        return (not m.author.bot and
                m.channel == ctx.channel and
                coupable.display_name.lower() in m.content.lower())

    try:
        while True:
            msg = await bot.wait_for("message", check=check, timeout=30)
            correct_guessers.append(msg.author)
            if len(correct_guessers) >= 3:
                break
    except asyncio.TimeoutError:
        pass

    # Résultat
    reward = 15000
    em_result = discord.Embed(title="📋 Résultat de l'enquête", color=0x43b581 if correct_guessers else 0xf04747)
    em_result.description = f"**Le coupable était : {coupable.mention} ({role_assignments[coupable.id]})**\n\n"

    if correct_guessers:
        em_result.description += f"**🏆 Détectives gagnants :**\n"
        for guesser in correct_guessers:
            eco = get_economy(guesser.id)
            update_economy(guesser.id, hand=eco["hand"] + reward)
            await add_xp(ctx, guesser.id, 100)
            em_result.description += f"{guesser.mention} +{format_ryo(reward)}\n"
    else:
        em_result.description += "Personne n'a trouvé le coupable à temps !"

    em_result.set_footer(text="Velda ・ Enquête")
    await ctx.send(embed=em_result)


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
