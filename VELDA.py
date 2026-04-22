import discord
from discord.ext import commands, tasks
import os
import sqlite3
import json
import random
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ========================= CONFIG =========================
BOT_TOKEN = os.environ["TOKEN"]
PARIS_TZ = ZoneInfo("Europe/Paris")
DEFAULT_BUYER_IDS = [1312375517927706630]
DEFAULT_PREFIX = "*"

# ========================= XP TABLE =========================
# Niveau i nécessite XP_TABLE[i] XP total (exponentiel)
def xp_for_level(level):
    return int(100 * (level ** 2.2))

# ========================= DATABASE =========================

def get_db():
    conn = sqlite3.connect("velda.db")
    conn.row_factory = sqlite3.Row
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
        last_fish TEXT
    )""")

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

    c.execute("""CREATE TABLE IF NOT EXISTS active_messages (
        guild_id TEXT PRIMARY KEY,
        user_id TEXT,
        message_content TEXT,
        timestamp TEXT
    )""")

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
        (user_id, hand, bank, fame, xp, level, last_daily, last_fame, last_work, last_fish)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (str(user_id), eco["hand"], eco["bank"], eco["fame"], eco["xp"], eco["level"],
         eco["last_daily"], eco["last_fame"], eco["last_work"], eco["last_fish"]))
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


def track_message(guild_id, user_id, content):
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    conn.execute("INSERT OR REPLACE INTO active_messages VALUES (?, ?, ?, ?)",
                 (str(guild_id) + "_" + str(user_id), str(user_id), content[:200], now))
    conn.commit()
    conn.close()


def get_active_members(guild_id, limit=10):
    conn = get_db()
    cutoff = (datetime.now(PARIS_TZ) - timedelta(hours=24)).isoformat()
    rows = conn.execute("""SELECT user_id, message_content FROM active_messages 
        WHERE user_id LIKE ? AND timestamp > ? ORDER BY timestamp DESC LIMIT ?""",
        (f"%", cutoff, limit)).fetchall()
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
        return val
    except:
        return None


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
        em = discord.Embed(color=0xffd700)
        em.set_author(name="🎉 Level Up !", icon_url=ctx.bot.user.display_avatar.url)
        em.description = (
            f"👤 **Joueur**\n<@{user_id}>\n\n"
            f"🎯 **Nouveau niveau**\n{new_level} / 100\n\n"
            f"💰 **Récompense**\n+{format_ryo(bonus)}"
        )
        em.set_footer(text="Velda")
        await ctx.send(embed=em)


# ========================= BOT SETUP =========================

init_db()
intents = discord.Intents.all()


def get_prefix(bot, message):
    return get_config("prefix") or DEFAULT_PREFIX


bot = commands.Bot(command_prefix=get_prefix, intents=intents, help_command=None)


# ========================= EVENTS =========================

@bot.event
async def on_ready():
    print(f"[OK] Velda connecté : {bot.user} ({bot.user.id})")
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
    except:
        pass


# ========================= HELP =========================

def home_embed():
    p = get_config("prefix") or DEFAULT_PREFIX
    em = discord.Embed(color=embed_color())
    em.set_author(name="Velda ─ Panel d'aide")
    em.description = (
        f"```\n🕐  {get_french_time()}\n```\n"
        f"Bienvenue sur **Velda**.\n\n"
        f"**Prefix :** `{p}`\n\n"
        f"> 💰 **Économie** — Bal, daily, dépôts...\n"
        f"> 🎮 **Jeux** — Slots, BJ, Jackpot...\n"
        f"> 🏆 **Spéciaux** — Enchères, Drop, Enquête\n"
        f"> 👥 **Permissions** — Rangs\n"
        f"> 📋 **Hiérarchie** — Pouvoirs"
    )
    em.set_footer(text="Made by gp ・ Velda")
    return em


def eco_embed():
    p = get_config("prefix") or DEFAULT_PREFIX
    em = discord.Embed(title="💰  Économie", color=embed_color())
    em.description = (
        f"```\n"
        f"{p}bal [@user]          → Balance\n"
        f"{p}daily / {p}dy         → Récompense quotidienne\n"
        f"{p}dep [somme/all]      → Déposer en bank\n"
        f"{p}with [somme/all]     → Retirer de la bank\n"
        f"{p}give [somme] @user   → Donner des Ryo\n"
        f"{p}rob @user            → Voler (5-30% main)\n"
        f"{p}fame @user           → Famer quelqu'un\n"
        f"```"
    )
    em.set_footer(text="Made by gp ・ Velda")
    return em


def jeux_embed():
    p = get_config("prefix") or DEFAULT_PREFIX
    em = discord.Embed(title="🎮  Jeux", color=embed_color())
    em.description = (
        f"```\n"
        f"{p}slots [somme/all]    → Machine à sous\n"
        f"{p}bj [somme/all]       → Blackjack\n"
        f"{p}jackpot [somme/all]  → Jackpot\n"
        f"{p}fish                 → Pêche (30min)\n"
        f"{p}work                 → Boulot (1h)\n"
        f"```"
    )
    em.set_footer(text="Made by gp ・ Velda")
    return em


def speciaux_embed():
    p = get_config("prefix") or DEFAULT_PREFIX
    em = discord.Embed(title="🏆  Spéciaux", color=embed_color())
    em.description = (
        f"```\n"
        f"{p}enchere @role        → Lancer une enchère (Owner+)\n"
        f"{p}drop [somme]         → Drop d'argent (Owner+)\n"
        f"{p}enquete              → Lancer une enquête (Sys+)\n"
        f"```"
    )
    em.set_footer(text="Made by gp ・ Velda")
    return em


def perms_embed():
    p = get_config("prefix") or DEFAULT_PREFIX
    em = discord.Embed(title="👥  Permissions", color=embed_color())
    em.description = (
        f"**Whitelist**\n```\n{p}wl @user / {p}unwl @user\n```\n"
        f"**Owner**\n```\n{p}owner @user / {p}unowner @user\n```\n"
        f"**Sys**\n```\n{p}sys @user / {p}unsys @user\n```"
    )
    em.set_footer(text="Made by gp ・ Velda")
    return em


def hierarchy_embed():
    em = discord.Embed(title="📋  Hiérarchie", color=embed_color())
    em.description = (
        "```\nBuyer > Sys > Owner > Whitelist > Tout le monde\n```\n\n"
        "> 👑 **Buyer** — Accès total, `*prefix`, `*setlog`, `*sys`/`*unsys`\n"
        "> 🔧 **Sys** — `*enquete`, `*setenchere`, `*ban`/`*unban`, `*owner`/`*unowner`, `*addmoney`, `*removemoney`, `*resetbal`, `*addxp`, `*resetlevel`\n"
        "> ⭐ **Owner** — `*enchere`, `*drop`, `*wl`/`*unwl`\n"
        "> 👤 **Tout le monde** — Tous les jeux et commandes éco\n"
    )
    em.set_footer(text="Made by gp ・ Velda")
    return em


class HelpDropdown(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label="Accueil", emoji="🏠", value="home"),
            discord.SelectOption(label="Économie", emoji="💰", value="eco"),
            discord.SelectOption(label="Jeux", emoji="🎮", value="jeux"),
            discord.SelectOption(label="Spéciaux", emoji="🏆", value="speciaux"),
            discord.SelectOption(label="Permissions", emoji="👥", value="perms"),
            discord.SelectOption(label="Hiérarchie", emoji="📋", value="hierarchy"),
        ]
        super().__init__(placeholder="📂 Choisis une catégorie...", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        embeds = {
            "home": home_embed, "eco": eco_embed, "jeux": jeux_embed,
            "speciaux": speciaux_embed, "perms": perms_embed, "hierarchy": hierarchy_embed,
        }
        await interaction.response.edit_message(embed=embeds[self.values[0]](), view=self.view)


class HelpView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=120)
        self.add_item(HelpDropdown())

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


@bot.command(name="help")
async def _help(ctx):
    await ctx.send(embed=home_embed(), view=HelpView())


# ========================= SYSTÈME =========================

@bot.command(name="prefix")
async def _prefix(ctx, new_prefix: str = None):
    if not has_min_rank(ctx.author.id, 4):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "Seul le **Buyer** peut changer le prefix."))
    if not new_prefix:
        return await ctx.send(embed=info_embed("Prefix actuel", f"`{get_config('prefix')}`"))
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


# ========================= RANGS =========================

@bot.command(name="sys")
async def _sys(ctx, member: discord.Member = None):
    if member is None:
        if not has_min_rank(ctx.author.id, 4):
            return await ctx.send(embed=error_embed("❌ Permission refusée", "**Buyer** requis."))
        ids = get_ranks_by_level(3)
        if not ids:
            return await ctx.send(embed=info_embed("📋 Liste Sys", "Aucun sys."))
        return await ctx.send(embed=info_embed(f"📋 Liste Sys ({len(ids)})", "\n".join([f"<@{uid}>" for uid in ids])))
    if not has_min_rank(ctx.author.id, 4):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Buyer** requis."))
    if get_rank_db(member.id) == 3:
        return await ctx.send(embed=error_embed("Déjà Sys", f"{member.mention} est déjà sys."))
    set_rank_db(member.id, 3)
    await ctx.send(embed=success_embed("✅ Sys ajouté", f"{member.mention} ajouté en **sys**."))


@bot.command(name="unsys")
async def _unsys(ctx, member: discord.Member = None):
    if not has_min_rank(ctx.author.id, 4):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Buyer** requis."))
    if not member:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un utilisateur."))
    if get_rank_db(member.id) != 3:
        return await ctx.send(embed=error_embed("Pas Sys", f"{member.mention} n'est pas sys."))
    set_rank_db(member.id, 0)
    await ctx.send(embed=success_embed("✅ Sys retiré", f"{member.mention} retiré des **sys**."))


@bot.command(name="owner")
async def _owner(ctx, member: discord.Member = None):
    if member is None:
        if not has_min_rank(ctx.author.id, 3):
            return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
        ids = get_ranks_by_level(2)
        if not ids:
            return await ctx.send(embed=info_embed("📋 Liste Owner", "Aucun owner."))
        return await ctx.send(embed=info_embed(f"📋 Liste Owner ({len(ids)})", "\n".join([f"<@{uid}>" for uid in ids])))
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if get_rank_db(member.id) >= 3:
        return await ctx.send(embed=error_embed("❌ Erreur", f"{member.mention} a un rang supérieur ou égal."))
    set_rank_db(member.id, 2)
    await ctx.send(embed=success_embed("✅ Owner ajouté", f"{member.mention} ajouté en **owner**."))


@bot.command(name="unowner")
async def _unowner(ctx, member: discord.Member = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not member:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un utilisateur."))
    if get_rank_db(member.id) != 2:
        return await ctx.send(embed=error_embed("Pas Owner", f"{member.mention} n'est pas owner."))
    set_rank_db(member.id, 0)
    await ctx.send(embed=success_embed("✅ Owner retiré", f"{member.mention} retiré des **owners**."))


@bot.command(name="wl")
async def _wl(ctx, member: discord.Member = None):
    if member is None:
        if not has_min_rank(ctx.author.id, 2):
            return await ctx.send(embed=error_embed("❌ Permission refusée", "**Owner+** requis."))
        ids = get_ranks_by_level(1)
        if not ids:
            return await ctx.send(embed=info_embed("📋 Whitelist", "Aucun wl."))
        return await ctx.send(embed=info_embed(f"📋 Whitelist ({len(ids)})", "\n".join([f"<@{uid}>" for uid in ids])))
    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Owner+** requis."))
    if get_rank_db(member.id) >= 2:
        return await ctx.send(embed=error_embed("❌ Erreur", f"{member.mention} a un rang supérieur ou égal."))
    set_rank_db(member.id, 1)
    await ctx.send(embed=success_embed("✅ WL ajouté", f"{member.mention} ajouté à la **whitelist**."))


@bot.command(name="unwl")
async def _unwl(ctx, member: discord.Member = None):
    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Owner+** requis."))
    if not member:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un utilisateur."))
    if get_rank_db(member.id) != 1:
        return await ctx.send(embed=error_embed("Pas WL", f"{member.mention} n'est pas wl."))
    set_rank_db(member.id, 0)
    await ctx.send(embed=success_embed("✅ WL retiré", f"{member.mention} retiré de la **whitelist**."))


# ========================= BAN BOT =========================

@bot.command(name="ban")
async def _ban(ctx, member: discord.Member = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not member:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un utilisateur."))
    if is_bot_banned(member.id):
        return await ctx.send(embed=error_embed("Déjà banni", f"{member.mention} est déjà banni du bot."))
    add_bot_ban(member.id, ctx.author.id)
    await ctx.send(embed=success_embed("⛔ Banni du bot", f"{member.mention} ne peut plus utiliser **Velda**."))


@bot.command(name="unban")
async def _unban(ctx, member: discord.Member = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not member:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un utilisateur."))
    if not is_bot_banned(member.id):
        return await ctx.send(embed=error_embed("Pas banni", f"{member.mention} n'est pas banni du bot."))
    remove_bot_ban(member.id)
    await ctx.send(embed=success_embed("✅ Débanni", f"{member.mention} peut à nouveau utiliser **Velda**."))


# ========================= ADMIN ECO =========================

@bot.command(name="addmoney")
async def _addmoney(ctx, member: discord.Member = None, amount: int = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not member or not amount:
        return await ctx.send(embed=error_embed("Arguments manquants", "Usage : `*addmoney @user [somme]`"))
    eco = get_economy(member.id)
    update_economy(member.id, hand=eco["hand"] + amount)
    await ctx.send(embed=success_embed("✅ Argent ajouté", f"+{format_ryo(amount)} ajouté à {member.mention}."))


@bot.command(name="removemoney")
async def _removemoney(ctx, member: discord.Member = None, amount: int = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not member or not amount:
        return await ctx.send(embed=error_embed("Arguments manquants", "Usage : `*removemoney @user [somme]`"))
    eco = get_economy(member.id)
    new_hand = max(0, eco["hand"] - amount)
    update_economy(member.id, hand=new_hand)
    await ctx.send(embed=success_embed("✅ Argent retiré", f"-{format_ryo(amount)} retiré à {member.mention}."))


@bot.command(name="resetbal")
async def _resetbal(ctx, member: discord.Member = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not member:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un utilisateur."))
    update_economy(member.id, hand=0, bank=0)
    await ctx.send(embed=success_embed("✅ Balance reset", f"La balance de {member.mention} a été remise à 0."))


@bot.command(name="addxp")
async def _addxp(ctx, member: discord.Member = None, amount: int = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not member or not amount:
        return await ctx.send(embed=error_embed("Arguments manquants", "Usage : `*addxp @user [somme]`"))
    await add_xp(ctx, member.id, amount)
    await ctx.send(embed=success_embed("✅ XP ajouté", f"+{amount} XP ajouté à {member.mention}."))


@bot.command(name="resetlevel")
async def _resetlevel(ctx, member: discord.Member = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not member:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un utilisateur."))
    update_economy(member.id, xp=0, level=0)
    await ctx.send(embed=success_embed("✅ Niveau reset", f"Le niveau de {member.mention} a été remis à 0."))


# ========================= ÉCONOMIE =========================

@bot.command(name="bal")
async def _bal(ctx, *, user_input: str = None):
    if await check_ban(ctx):
        return

    target = ctx.author
    if user_input:
        try:
            member_id = int(user_input.strip("<@!>"))
            target = ctx.guild.get_member(member_id)
        except:
            try:
                target = await commands.MemberConverter().convert(ctx, user_input)
            except:
                target = ctx.author

    eco = get_economy(target.id)
    xp_current = eco["xp"] - xp_for_level(eco["level"]) if eco["level"] > 0 else eco["xp"]
    xp_next = xp_for_level(eco["level"] + 1) - xp_for_level(eco["level"]) if eco["level"] < 100 else 0
    total = eco["hand"] + eco["bank"]

    # Barre de progression XP
    if eco["level"] < 100 and xp_next > 0:
        filled = int((xp_current / xp_next) * 10)
        bar = "█" * filled + "░" * (10 - filled)
        xp_str = f"`{bar}` {xp_current}/{xp_next}"
    else:
        xp_str = "`██████████` MAX"

    em = discord.Embed(color=embed_color())
    em.set_author(name=f"Balance de {target.display_name}", icon_url=target.display_avatar.url)
    em.description = (
        f"👜 **En main**\n{format_ryo(eco['hand'])}\n\n"
        f"🏦 **En bank**\n{format_ryo(eco['bank'])}\n\n"
        f"💰 **Total**\n{format_ryo(total)}\n\n"
        f"⭐ **Fame**\n{eco['fame']} point{'s' if eco['fame'] > 1 else ''}\n\n"
        f"🎯 **Niveau {eco['level']} / 100**\n{xp_str}"
    )
    em.set_footer(text=f"Velda ・ {get_french_time()}")
    await ctx.send(embed=em)


@bot.command(name="daily", aliases=["dy"])
async def _daily(ctx):
    if await check_ban(ctx):
        return
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

    em = discord.Embed(color=0x43b581)
    em.set_author(name="Daily récupéré !", icon_url=ctx.author.display_avatar.url)
    em.description = (
        f"✅ **Récompense du jour**\n+{format_ryo(amount)}\n\n"
        f"👜 **Nouveau solde en main**\n{format_ryo(eco['hand'] + amount)}"
    )
    em.set_footer(text="Velda ・ Reviens demain !")
    await ctx.send(embed=em)


@bot.command(name="dep")
async def _dep(ctx, amount_str: str = None):
    if await check_ban(ctx):
        return
    if not amount_str:
        return await ctx.send(embed=error_embed("Argument manquant", "Usage : `*dep [somme/all]`"))
    eco = get_economy(ctx.author.id)
    amount = parse_amount(amount_str, eco["hand"])
    if amount is None or amount <= 0:
        return await ctx.send(embed=error_embed("Montant invalide", "Donne un montant valide ou `all`."))
    if amount > eco["hand"]:
        return await ctx.send(embed=error_embed("Fonds insuffisants", f"Tu n'as que {format_ryo(eco['hand'])} en main."))
    update_economy(ctx.author.id, hand=eco["hand"] - amount, bank=eco["bank"] + amount)
    em = discord.Embed(color=0x3498db)
    em.set_author(name="Dépôt effectué", icon_url=ctx.author.display_avatar.url)
    em.description = (
        f"🏦 **Déposé en bank**\n+{format_ryo(amount)}\n\n"
        f"👜 **En main**\n{format_ryo(eco['hand'] - amount)}\n\n"
        f"🏦 **En bank**\n{format_ryo(eco['bank'] + amount)}"
    )
    em.set_footer(text="Velda")
    await ctx.send(embed=em)


@bot.command(name="withdraw", aliases=["with"])
async def _withdraw(ctx, amount_str: str = None):
    if await check_ban(ctx):
        return
    if not amount_str:
        return await ctx.send(embed=error_embed("Argument manquant", "Usage : `*with [somme/all]`"))
    eco = get_economy(ctx.author.id)
    amount = parse_amount(amount_str, eco["bank"])
    if amount is None or amount <= 0:
        return await ctx.send(embed=error_embed("Montant invalide", "Donne un montant valide ou `all`."))
    if amount > eco["bank"]:
        return await ctx.send(embed=error_embed("Fonds insuffisants", f"Tu n'as que {format_ryo(eco['bank'])} en bank."))
    update_economy(ctx.author.id, hand=eco["hand"] + amount, bank=eco["bank"] - amount)
    em = discord.Embed(color=0x3498db)
    em.set_author(name="Retrait effectué", icon_url=ctx.author.display_avatar.url)
    em.description = (
        f"👜 **Retiré en main**\n+{format_ryo(amount)}\n\n"
        f"👜 **En main**\n{format_ryo(eco['hand'] + amount)}\n\n"
        f"🏦 **En bank**\n{format_ryo(eco['bank'] - amount)}"
    )
    em.set_footer(text="Velda")
    await ctx.send(embed=em)


@bot.command(name="give")
async def _give(ctx, amount_str: str = None, *, user_input: str = None):
    if await check_ban(ctx):
        return
    if not amount_str or not user_input:
        return await ctx.send(embed=error_embed("Arguments manquants", "Usage : `*give [somme] @user`"))

    target = None
    try:
        member_id = int(user_input.strip("<@!>"))
        target = ctx.guild.get_member(member_id)
    except:
        try:
            target = await commands.MemberConverter().convert(ctx, user_input)
        except:
            pass
    if not target:
        return await ctx.send(embed=error_embed("❌ Introuvable", "Impossible de trouver cet utilisateur."))
    if target == ctx.author:
        return await ctx.send(embed=error_embed("❌ Erreur", "Tu ne peux pas te donner de l'argent à toi-même."))

    eco = get_economy(ctx.author.id)
    amount = parse_amount(amount_str, eco["hand"])
    if amount is None or amount <= 0:
        return await ctx.send(embed=error_embed("Montant invalide", "Donne un montant valide."))
    if amount > eco["hand"]:
        return await ctx.send(embed=error_embed("Fonds insuffisants", f"Tu n'as que {format_ryo(eco['hand'])} en main."))

    eco_target = get_economy(target.id)
    update_economy(ctx.author.id, hand=eco["hand"] - amount)
    update_economy(target.id, hand=eco_target["hand"] + amount)

    em = discord.Embed(color=0x43b581)
    em.set_author(name="✅ Don effectué", icon_url=ctx.author.display_avatar.url)
    em.description = (
        f"👤 **De**\n{ctx.author.mention}\n\n"
        f"🎯 **À**\n{target.mention}\n\n"
        f"💸 **Montant**\n{format_ryo(amount)}\n\n"
        f"👜 **Ta main maintenant**\n{format_ryo(eco['hand'] - amount)}"
    )
    em.set_footer(text="Velda")
    await ctx.send(embed=em)


@bot.command(name="rob")
async def _rob(ctx, *, user_input: str = None):
    if await check_ban(ctx):
        return
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne quelqu'un ou donne son ID."))

    target = None
    try:
        member_id = int(user_input.strip("<@!>"))
        target = ctx.guild.get_member(member_id)
    except:
        try:
            target = await commands.MemberConverter().convert(ctx, user_input)
        except:
            pass
    if not target:
        return await ctx.send(embed=error_embed("❌ Introuvable", "Impossible de trouver cet utilisateur."))
    if target == ctx.author:
        return await ctx.send(embed=error_embed("❌ Erreur", "Tu ne peux pas te voler toi-même."))

    eco_target = get_economy(target.id)
    if eco_target["hand"] <= 0:
        return await ctx.send(embed=error_embed("❌ Pas d'argent", f"{target.mention} n'a rien en main."))

    percent = random.randint(5, 30) / 100
    stolen = int(eco_target["hand"] * percent)
    if stolen <= 0:
        stolen = 1

    eco_author = get_economy(ctx.author.id)
    update_economy(target.id, hand=eco_target["hand"] - stolen)
    update_economy(ctx.author.id, hand=eco_author["hand"] + stolen)
    await add_xp(ctx, ctx.author.id, 20)

    em = discord.Embed(color=0xf04747)
    em.set_author(name="🥷 Vol réussi !", icon_url=ctx.author.display_avatar.url)
    em.description = (
        f"🎯 **Cible**\n{target.mention}\n\n"
        f"💸 **Volé**\n{format_ryo(stolen)} ({int(percent*100)}% de sa main)\n\n"
        f"👜 **Ta main maintenant**\n{format_ryo(eco_author['hand'] + stolen)}"
    )
    em.set_footer(text="Velda")
    await ctx.send(embed=em)


@bot.command(name="fame")
async def _fame(ctx, *, user_input: str = None):
    if await check_ban(ctx):
        return
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne quelqu'un ou donne son ID."))

    target = None
    try:
        member_id = int(user_input.strip("<@!>"))
        target = ctx.guild.get_member(member_id)
    except:
        try:
            target = await commands.MemberConverter().convert(ctx, user_input)
        except:
            pass
    if not target:
        return await ctx.send(embed=error_embed("❌ Introuvable", "Impossible de trouver cet utilisateur."))
    if target == ctx.author:
        return await ctx.send(embed=error_embed("❌ Erreur", "Tu ne peux pas te famer toi-même."))

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

    em = discord.Embed(color=0xffd700)
    em.set_author(name="⭐ Fame !", icon_url=ctx.author.display_avatar.url)
    em.description = (
        f"👤 **Famé par**\n{ctx.author.mention}\n\n"
        f"🎯 **Cible**\n{target.mention}\n\n"
        f"⭐ **Total fame de {target.display_name}**\n{eco_target['fame'] + 1} point{'s' if eco_target['fame'] + 1 > 1 else ''}"
    )
    em.set_footer(text="Velda ・ Cooldown 2h")
    await ctx.send(embed=em)


# ========================= JEUX =========================

@bot.command(name="work")
async def _work(ctx):
    if await check_ban(ctx):
        return
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

    em = discord.Embed(color=embed_color())
    em.set_author(name=f"💼 {job.capitalize()}", icon_url=ctx.author.display_avatar.url)
    em.description = (
        f"📝 **Mission**\n{desc}\n\n"
        f"💸 **Salaire**\n+{format_ryo(amount)}\n\n"
        f"👜 **En main maintenant**\n{format_ryo(eco['hand'] + amount)}"
    )
    em.set_footer(text="Velda ・ Cooldown 1h")
    await ctx.send(embed=em)


@bot.command(name="fish")
async def _fish(ctx):
    if await check_ban(ctx):
        return
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
    em = discord.Embed(color=rarity_colors.get(rarity, embed_color()))
    em.set_author(name=f"🎣 Pêche — {rarity.upper()}", icon_url=ctx.author.display_avatar.url)
    em.description = (
        f"🐟 **Prise**\n{name}\n\n"
        f"💸 **Valeur**\n+{format_ryo(amount)}\n\n"
        f"👜 **En main maintenant**\n{format_ryo(eco['hand'] + amount)}"
    )
    em.set_footer(text="Velda ・ Cooldown 30min")
    await ctx.send(embed=em)


@bot.command(name="slots")
async def _slots(ctx, amount_str: str = None):
    if await check_ban(ctx):
        return
    if not amount_str:
        return await ctx.send(embed=error_embed("Argument manquant", "Usage : `*slots [somme/all]`"))
    eco = get_economy(ctx.author.id)
    amount = parse_amount(amount_str, eco["hand"])
    if amount is None or amount <= 0:
        return await ctx.send(embed=error_embed("Montant invalide", "Donne un montant valide."))
    if amount > eco["hand"]:
        return await ctx.send(embed=error_embed("Fonds insuffisants", f"Tu n'as que {format_ryo(eco['hand'])} en main."))

    symbols = ["🍒", "🍋", "🍊", "🍇", "⭐", "💎", "7️⃣"]
    weights = [30, 25, 20, 12, 7, 4, 2]
    reels = random.choices(symbols, weights=weights, k=3)

    multipliers = {"💎": 20, "7️⃣": 15, "⭐": 10, "🍇": 5, "🍊": 3, "🍋": 2, "🍒": 1.5}

    if reels[0] == reels[1] == reels[2]:
        mult = multipliers.get(reels[0], 2)
        winnings = int(amount * mult)
        result = f"🎊 JACKPOT ! x{mult}"
        color = 0xffd700
        new_hand = eco["hand"] - amount + winnings
        update_economy(ctx.author.id, hand=new_hand)
        await add_xp(ctx, ctx.author.id, 50)
    elif reels[0] == reels[1] or reels[1] == reels[2]:
        winnings = int(amount * 1.5)
        result = f"✅ Deux identiques ! x1.5"
        color = 0x43b581
        new_hand = eco["hand"] - amount + winnings
        update_economy(ctx.author.id, hand=new_hand)
        await add_xp(ctx, ctx.author.id, 15)
    else:
        winnings = 0
        result = f"❌ Perdu"
        color = 0xf04747
        new_hand = eco["hand"] - amount
        update_economy(ctx.author.id, hand=new_hand)
        await add_xp(ctx, ctx.author.id, 5)

    gain_str = f"+{format_ryo(winnings)}" if winnings > 0 else f"-{format_ryo(amount)}"

    em = discord.Embed(color=color)
    em.set_author(name="🎰 Machine à sous", icon_url=ctx.author.display_avatar.url)
    em.description = (
        f"🎰 **Rouleaux**\n`[ {reels[0]}  {reels[1]}  {reels[2]} ]`\n\n"
        f"📊 **Résultat**\n{result} ({gain_str})\n\n"
        f"💰 **Mise**\n{format_ryo(amount)}\n\n"
        f"👜 **En main maintenant**\n{format_ryo(new_hand)}"
    )
    em.set_footer(text="Velda")
    await ctx.send(embed=em)


@bot.command(name="jackpot")
async def _jackpot(ctx, amount_str: str = None):
    if await check_ban(ctx):
        return
    if not amount_str:
        return await ctx.send(embed=error_embed("Argument manquant", "Usage : `*jackpot [somme/all]`"))
    eco = get_economy(ctx.author.id)
    amount = parse_amount(amount_str, eco["hand"])
    if amount is None or amount <= 0:
        return await ctx.send(embed=error_embed("Montant invalide", "Donne un montant valide."))
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
        new_hand = eco["hand"] - amount + winnings
        update_economy(ctx.author.id, hand=new_hand)
        await add_xp(ctx, ctx.author.id, mult * 10)
        gain_str = f"+{format_ryo(winnings)}"
    else:
        winnings = 0
        new_hand = eco["hand"] - amount
        update_economy(ctx.author.id, hand=new_hand)
        await add_xp(ctx, ctx.author.id, 5)
        gain_str = f"-{format_ryo(amount)}"

    em = discord.Embed(color=color)
    em.set_author(name="🎲 Jackpot", icon_url=ctx.author.display_avatar.url)
    em.description = (
        f"🎲 **Tirage**\n{result_text}\n\n"
        f"📊 **Gain/Perte**\n{gain_str}\n\n"
        f"💰 **Mise**\n{format_ryo(amount)}\n\n"
        f"👜 **En main maintenant**\n{format_ryo(new_hand)}"
    )
    em.set_footer(text="Velda")
    await ctx.send(embed=em)


# ========================= BLACKJACK =========================

class BlackjackView(discord.ui.View):
    def __init__(self, ctx, amount, deck, player_hand, dealer_hand):
        super().__init__(timeout=60)
        self.ctx = ctx
        self.amount = amount
        self.deck = deck
        self.player_hand = player_hand
        self.dealer_hand = dealer_hand

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

    @discord.ui.button(label="Tirer 🃏", style=discord.ButtonStyle.primary)
    async def hit(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user != self.ctx.author:
            return await interaction.response.send_message("Ce n'est pas ton jeu !", ephemeral=True)
        self.player_hand.append(self.deck.pop())
        pv = self.hand_value(self.player_hand)
        if pv > 21:
            eco = get_economy(self.ctx.author.id)
            update_economy(self.ctx.author.id, hand=eco["hand"] - self.amount)
            for item in self.children:
                item.disabled = True
            await interaction.response.edit_message(embed=self.make_embed(f"💥 Bust ! Tu as {pv}. -${format_ryo(self.amount)}", 0xf04747), view=self)
            self.stop()
        else:
            await interaction.response.edit_message(embed=self.make_embed(), view=self)

    @discord.ui.button(label="Rester ✋", style=discord.ButtonStyle.secondary)
    async def stand(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user != self.ctx.author:
            return await interaction.response.send_message("Ce n'est pas ton jeu !", ephemeral=True)
        # Dealer joue
        while self.hand_value(self.dealer_hand) < 17:
            self.dealer_hand.append(self.deck.pop())

        pv = self.hand_value(self.player_hand)
        dv = self.hand_value(self.dealer_hand)
        eco = get_economy(self.ctx.author.id)

        if dv > 21 or pv > dv:
            winnings = self.amount
            update_economy(self.ctx.author.id, hand=eco["hand"] + winnings)
            result = f"🎉 Tu gagnes ! ({pv} vs {dv}) +{format_ryo(winnings)}"
            color = 0x43b581
        elif pv == dv:
            result = f"🤝 Égalité ! ({pv} vs {dv}) Mise remboursée"
            color = 0xfaa61a
        else:
            update_economy(self.ctx.author.id, hand=eco["hand"] - self.amount)
            result = f"❌ Perdu ! ({pv} vs {dv}) -{format_ryo(self.amount)}"
            color = 0xf04747

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
    eco = get_economy(ctx.author.id)
    amount = parse_amount(amount_str, eco["hand"])
    if amount is None or amount <= 0:
        return await ctx.send(embed=error_embed("Montant invalide", "Donne un montant valide."))
    if amount > eco["hand"]:
        return await ctx.send(embed=error_embed("Fonds insuffisants", f"Tu n'as que {format_ryo(eco['hand'])} en main."))

    suits = ["♠", "♥", "♦", "♣"]
    ranks = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"]
    deck = [(r, s) for s in suits for r in ranks]
    random.shuffle(deck)

    player_hand = [deck.pop(), deck.pop()]
    dealer_hand = [deck.pop(), deck.pop()]

    view = BlackjackView(ctx, amount, deck, player_hand, dealer_hand)
    pv = view.hand_value(player_hand)

    if pv == 21:
        winnings = int(amount * 1.5)
        update_economy(ctx.author.id, hand=eco["hand"] + winnings)
        em = view.make_embed(f"🃏 Blackjack ! +{format_ryo(winnings)}", 0xffd700)
        return await ctx.send(embed=em)

    await ctx.send(embed=view.make_embed(), view=view)


# ========================= DROP =========================

class DropView(discord.ui.View):
    def __init__(self, amount, author_id):
        super().__init__(timeout=70)
        self.amount = amount
        self.author_id = author_id
        self.claimed = False
        self.button_active = False

    @discord.ui.button(label="⏳ Attends...", style=discord.ButtonStyle.secondary, disabled=True, custom_id="drop_btn")
    async def claim(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.claimed:
            return await interaction.response.send_message("Trop tard, déjà réclamé !", ephemeral=True)
        self.claimed = True
        eco = get_economy(interaction.user.id)
        update_economy(interaction.user.id, hand=eco["hand"] + self.amount)
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
    except:
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

    # Countdown
    for i in range(10, 0, -1):
        em.description = (
            f"**{format_ryo(amount)}** sont en jeu !\n\n"
            f"⏳ Le bouton s'active dans **{i} seconde{'s' if i > 1 else ''}**...\n"
            f"🏆 Le premier à cliquer remporte tout !"
        )
        await msg.edit(embed=em)
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
    await msg.edit(embed=em, view=view)


# ========================= ENCHÈRES =========================

class EnchereView(discord.ui.View):
    def __init__(self, role, starting_bid, author):
        super().__init__(timeout=3600)
        self.role = role
        self.current_bid = starting_bid
        self.current_winner = None
        self.author = author

    @discord.ui.button(label="Enchérir 💰", style=discord.ButtonStyle.primary)
    async def bid(self, interaction: discord.Interaction, button: discord.ui.Button):
        eco = get_economy(interaction.user.id)
        min_bid = self.current_bid + max(1000, int(self.current_bid * 0.05))

        if eco["hand"] < min_bid:
            return await interaction.response.send_message(
                f"Il te faut au moins {format_ryo(min_bid)} en main pour enchérir !", ephemeral=True)

        # Demander le montant
        await interaction.response.send_message(
            f"Combien veux-tu miser ? (minimum {format_ryo(min_bid)})\nRéponds dans ce salon.", ephemeral=True)

        def check(m):
            return m.author == interaction.user and m.channel == interaction.channel

        try:
            msg = await bot.wait_for("message", check=check, timeout=30)
            bid_amount = int(msg.content.replace(" ", ""))
            if bid_amount < min_bid:
                return await interaction.channel.send(
                    embed=error_embed("Mise trop basse", f"Minimum : {format_ryo(min_bid)}"))
            if bid_amount > eco["hand"]:
                return await interaction.channel.send(
                    embed=error_embed("Fonds insuffisants", f"Tu n'as que {format_ryo(eco['hand'])} en main."))

            self.current_bid = bid_amount
            self.current_winner = interaction.user

            em = discord.Embed(title=f"🎪 Enchère — @{self.role.name}", color=0xffd700)
            em.description = (
                f"**Mise actuelle :** {format_ryo(self.current_bid)}\n"
                f"**Meilleur enchérisseur :** {self.current_winner.mention}\n\n"
                f"Clique sur **Enchérir** pour surenchérir !"
            )
            em.set_footer(text=f"Lancé par {self.author.display_name} ・ Velda")
            await interaction.channel.send(embed=em)
            try:
                await msg.delete()
            except:
                pass
        except:
            pass


@bot.command(name="enchere")
async def _enchere(ctx, role: discord.Role = None):
    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Owner+** requis."))
    if not role:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un rôle."))

    channel_id = get_enchere_channel(ctx.guild.id)
    channel = ctx.guild.get_channel(int(channel_id)) if channel_id else ctx.channel

    view = EnchereView(role, 1000, ctx.author)

    em = discord.Embed(title=f"🎪 Enchère — @{role.name}", color=0xffd700)
    em.description = (
        f"**Mise de départ :** {format_ryo(1000)}\n"
        f"**Durée :** 1 heure\n"
        f"**Prix :** Le rôle {role.mention} pour **48h** !\n\n"
        f"Clique sur **Enchérir** pour participer !"
    )
    em.set_footer(text=f"Lancé par {ctx.author.display_name} ・ Velda")

    msg = await channel.send(embed=em, view=view)
    await view.wait()

    # Fin de l'enchère
    if view.current_winner:
        eco = get_economy(view.current_winner.id)
        update_economy(view.current_winner.id, hand=eco["hand"] - view.current_bid)

        try:
            await view.current_winner.add_roles(role, reason="Gagnant enchère Velda")
        except:
            pass

        em_end = discord.Embed(title=f"🏆 Enchère terminée — @{role.name}", color=0x43b581)
        em_end.description = (
            f"**Gagnant :** {view.current_winner.mention}\n"
            f"**Mise finale :** {format_ryo(view.current_bid)}\n"
            f"Le rôle est attribué pour **48h** !"
        )
        em_end.set_footer(text="Velda")
        await channel.send(embed=em_end)

        # Retirer le rôle après 48h
        await asyncio.sleep(172800)
        try:
            await view.current_winner.remove_roles(role, reason="Fin enchère Velda 48h")
        except:
            pass
    else:
        em_end = discord.Embed(title=f"❌ Enchère terminée — Aucun participant", color=0xf04747)
        await channel.send(embed=em_end)


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
        except:
            pass

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
    if isinstance(error, commands.MemberNotFound) or isinstance(error, commands.UserNotFound):
        await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Impossible de trouver cet utilisateur."))
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=error_embed("❌ Argument manquant", "Tu as oublié un argument."))
    elif isinstance(error, commands.CommandNotFound):
        pass
    else:
        print(f"Erreur: {error}")


# ========================= RUN =========================
try:
    print("[...] Démarrage de Velda...")
    bot.run(BOT_TOKEN)
except Exception as e:
    print(f"\n[ERREUR] {e}")
    input("\nAppuie sur Entrée pour fermer...")
