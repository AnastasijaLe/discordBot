import discord
from discord.ext import commands, tasks
import sqlite3
from datetime import date, datetime, timedelta
import aiohttp
import asyncio
import os
from PIL import Image
from io import BytesIO
from reportlab.lib.pagesizes import landscape, A4
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
import tempfile
import time
import aiofiles

# ====== НАСТРОЙКА ======
TOKEN = os.environ.get('DISCORD_TOKEN')
GUILD_ID = int(os.environ.get('GUILD_ID'))
CHANNEL_APPROVAL_ID = int(os.environ.get('CHANNEL_APPROVAL_ID'))
CHANNEL_MAIN_ID = int(os.environ.get('CHANNEL_MAIN_ID'))
CHANNEL_WEEKLY_STATS_ID = int(os.environ.get('CHANNEL_WEEKLY_STATS_ID'))
CHANNEL_WEEKLY_STATS_MAIN_ID = int(os.environ.get('CHANNEL_WEEKLY_STATS_MAIN_ID'))
ROLE_TEST_ID = int(os.environ.get('ROLE_TEST_ID'))
ROLE_MAIN_ID = int(os.environ.get('ROLE_MAIN_ID'))
ROLE_REC_ID = int(os.environ.get('ROLE_REC_ID'))
ROLE_HIGH_ID = int(os.environ.get('ROLE_HIGH_ID'))
ROLE_TIR3_ID = int(os.environ.get('ROLE_TIR3_ID'))
WEEKLY_STATS_MESSAGE_ID = int(os.environ.get('WEEKLY_STATS_MESSAGE_ID', 0))
DEFAULT_THRESHOLD = int(os.environ.get('DEFAULT_THRESHOLD', 15))
INACTIVE_DAYS_THRESHOLD = int(os.environ.get('INACTIVE_DAYS_THRESHOLD', 3))
MAX_PDF_IMAGES = int(os.environ.get('MAX_PDF_IMAGES', 50))
CHANNEL_REPORTS_ID = int(os.environ.get('CHANNEL_REPORTS_ID'))

# Настройка бота и БД
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix='!', intents=intents)

SAVE_DIR = "/mnt/data/screenshots"

# Инициализация базы данных
def init_db():
    os.makedirs(SAVE_DIR, exist_ok=True)
    db = sqlite3.connect('/mnt/data/screenshots.db', check_same_thread=False)
    cursor = db.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        screenshots_total INTEGER DEFAULT 0,
        screenshots_weekly INTEGER DEFAULT 0,
        last_screenshot_date TEXT,
        join_date TEXT,
        discord_join_date TEXT,
        approved INTEGER DEFAULT 0,
        required_screens INTEGER DEFAULT 0,
        days_in_faction INTEGER DEFAULT 0,
        last_reminder_date TEXT
    )
    ''')
    
    cursor.execute('''
        DROP TABLE IF EXISTS users_main;
    ''')
    cursor.execute('''
        CREATE TABLE users_main (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        screenshots_total INTEGER DEFAULT 0,
        screenshots_weekly INTEGER DEFAULT 0,
        last_screenshot_date TEXT,
        discord_join_date TEXT,
        join_date TEXT,
        days_in_faction INTEGER DEFAULT 0,
        last_reminder_date TEXT
    );
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS screenshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        message_id INTEGER,
        path TEXT,
        date TEXT,
        FOREIGN KEY (user_id) REFERENCES users (user_id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS weekly_stats (
        week_start TEXT PRIMARY KEY,
        message_id INTEGER
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS weekly_stats_main (
        week_start TEXT PRIMARY KEY,
        message_id INTEGER
    )
    ''')
    
    cursor.execute('''
    DROP TABLE IF EXISTS daily_stats;
    ''')
    
    db.commit()
    return db, cursor

# Инициализация базы данных
db, cursor = init_db()

async def save_attachment(attachment, user_id):
    os.makedirs(SAVE_DIR, exist_ok=True)
    filename = f"{user_id}_{attachment.id}.png"
    path = os.path.join(SAVE_DIR, filename)
    async with aiohttp.ClientSession() as session:
        async with session.get(attachment.url) as resp:
            if resp.status == 200:
                async with aiofiles.open(path, mode='wb') as f:
                    await f.write(await resp.read())
                return path
    return None

def delete_user_files(user_id):
    cursor.execute("SELECT path FROM screenshots WHERE user_id = ?", (user_id,))
    for row in cursor.fetchall():
        file_path = row[0]
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception as e:
                print(f"⚠️ Не удалось удалить файл {file_path}: {e}")
    cursor.execute("DELETE FROM screenshots WHERE user_id = ?", (user_id,))
    db.commit()

# ========== КНОПКИ ПОДТВЕРЖДЕНИЯ / ОТКАЗА ==========
class ApprovalButtons(discord.ui.View):
    def __init__(self, target_user_id):
        super().__init__(timeout=None)
        self.target_user_id = target_user_id

    @discord.ui.button(label="Подтвердить", style=discord.ButtonStyle.success, custom_id="approve_confirm")
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        target_user = guild.get_member(self.target_user_id)
        role_test = guild.get_role(ROLE_TEST_ID)
        role_main = guild.get_role(ROLE_MAIN_ID)
        role_tir3 = guild.get_role(ROLE_TIR3_ID)
        
        if not target_user:
            return await interaction.response.send_message("Пользователь не найден.", ephemeral=True)
        
        if role_main or role_tir3:
            roles_to_add = [r for r in (role_main, role_tir3) if r]
            await target_user.add_roles(*roles_to_add)
        if role_test:
            await target_user.remove_roles(role_test)
        
        cursor.execute("UPDATE users SET approved = 1 WHERE user_id = ?", (self.target_user_id,))
        discord_join_date = target_user.joined_at.date().isoformat() if target_user.joined_at else date.today().isoformat()
        cursor.execute(
            "INSERT OR IGNORE INTO users_main (user_id, username, join_date, discord_join_date) VALUES (?, ?, ?, ?)",
            (self.target_user_id, target_user.name, date.today().isoformat(), discord_join_date)
        )
        db.commit()
        
        try:
            await target_user.send("🎉 Поздравляем! Ваш перевод на **Мейн** был одобрен!")
        except discord.Forbidden:
            print(f"⚠️ Не удалось отправить сообщение {target_user.name} (закрытые ЛС)")
        
        embed = interaction.message.embeds[0]
        embed.color = discord.Color.green()
        embed.set_footer(text=f"✅ Одобрено {interaction.user.display_name}")
        await interaction.message.edit(view=None, embed=embed)
        await interaction.response.send_message(f"Игрок {target_user.mention} успешно переведен!", ephemeral=True)
        await update_weekly_stats()
        await update_weekly_stats_main()

    @discord.ui.button(label="Отклонить", style=discord.ButtonStyle.danger, custom_id="approve_deny")
    async def deny(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = ReasonModal(self.target_user_id)
        await interaction.response.send_modal(modal)

class ReasonModal(discord.ui.Modal, title="Отклонение перевода"):
    def __init__(self, target_user_id):
        super().__init__()
        self.target_user_id = target_user_id
        
        self.required_screens = discord.ui.TextInput(
            label="Сколько доп. скринов нужно?",
            placeholder="Введите число",
            required=True
        )
        
        self.reason = discord.ui.TextInput(
            label="Причина",
            style=discord.TextStyle.paragraph,
            required=True
        )
        
        self.add_item(self.required_screens)
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        target_user = guild.get_member(self.target_user_id)
        
        try:
            required = int(self.required_screens.value)
        except ValueError:
            return await interaction.response.send_message("Нужно ввести число!", ephemeral=True)
        
        cursor.execute(
            "UPDATE users SET required_screens = ?, screenshots_total = 0, screenshots_weekly = 0 WHERE user_id = ?",
            (required, self.target_user_id)
        )
        delete_user_files(self.target_user_id)
        db.commit()
        
        try:
            await target_user.send(
                f"❌ Ваш перевод на **Мейн** был отклонен.\n"
                f"**Причина:** {self.reason.value}\n"
                f"**Требуется дополнительно скринов:** {required}\n"
            )
        except discord.Forbidden:
            print(f"⚠️ Не удалось отправить сообщение {target_user.name} (закрытые ЛС)")
        
        embed = interaction.message.embeds[0]
        embed.color = discord.Color.red()
        embed.set_footer(text=f"❌ Отклонено {interaction.user.display_name} | Нужно ещё {required} скринов")
        await interaction.message.edit(view=None, embed=embed)
        await interaction.response.send_message(
            f"Перевод отклонен. {target_user.mention} должен отправить ещё {required} скринов.",
            ephemeral=True
        )
        
        await update_weekly_stats()

# ========== УЛУЧШЕННАЯ PDF ГЕНЕРАЦИЯ (ТОЛЬКО ДЛЯ TEST) ==========
async def generate_pdf(user_id: int, paths: list[str]) -> str:
    pdf_path = f"screenshots_{user_id}_{int(time.time())}.pdf"
    c = canvas.Canvas(pdf_path, pagesize=landscape(A4))
    width, height = landscape(A4)
    
    print(f"🔧 Начинаем генерацию PDF для пользователя {user_id} с {len(paths)} скринами")
    successful_images = 0
    
    for i, path in enumerate(paths[:MAX_PDF_IMAGES], start=1):
        try:
            if not os.path.exists(path):
                print(f"❌ Файл не найден: {path}")
                continue
                
            with open(path, 'rb') as f:
                image_data = f.read()
            
            img = Image.open(BytesIO(image_data))
            if img.mode in ("RGBA", "P"):
                img = img.convert("RGB")
            
            with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as temp_file:
                temp_path = temp_file.name
                try:
                    img.save(temp_path, "JPEG", quality=85)
                    c.drawImage(temp_path, 0, 0, width, height, preserveAspectRatio=True)
                    c.showPage()
                    successful_images += 1
                    print(f"✅ Добавлено изображение {i} в PDF")
                finally:
                    try:
                        os.unlink(temp_path)
                    except:
                        pass
        except Exception as e:
            print(f"❌ Ошибка обработки изображения {i} ({path}): {e}")
            continue
    
    if successful_images > 0:
        c.save()
        print(f"✅ PDF создан успешно с {successful_images} изображениями")
        return pdf_path
    else:
        print("❌ Не удалось добавить ни одного изображения в PDF")
        return None

# ========== НЕДЕЛЬНАЯ СТАТИСТИКА ДЛЯ TEST ==========
async def update_weekly_stats():
    channel = bot.get_channel(CHANNEL_WEEKLY_STATS_ID)
    if not channel:
        return
    
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    week_start_str = week_start.isoformat()
    
    guild = bot.get_guild(GUILD_ID)
    role_test = guild.get_role(ROLE_TEST_ID)
    test_users = []
    
    for member in guild.members:
        if role_test in member.roles:
            cursor.execute(
                "SELECT screenshots_weekly, join_date, discord_join_date, days_in_faction FROM users WHERE user_id = ?",
                (member.id,)
            )
            row = cursor.fetchone()
            
            if row:
                screens_weekly, join_date, discord_join_date, days_in_faction = row
                if not discord_join_date:
                    discord_join_date = member.joined_at.date().isoformat() if member.joined_at else date.today().isoformat()
                    cursor.execute(
                        "UPDATE users SET discord_join_date = ? WHERE user_id = ?",
                        (discord_join_date, member.id)
                    )
            else:
                join_date = date.today().isoformat()
                discord_join_date = member.joined_at.date().isoformat() if member.joined_at else date.today().isoformat()
                days_in_faction = 0
                screens_weekly = 0
                cursor.execute(
                    "INSERT INTO users (user_id, username, join_date, discord_join_date, days_in_faction, screenshots_weekly) VALUES (?, ?, ?, ?, ?, ?)",
                    (member.id, member.name, join_date, discord_join_date, days_in_faction, screens_weekly)
                )
            
            if discord_join_date:
                join_date_obj = datetime.strptime(discord_join_date, '%Y-%m-%d').date()
                days_in_discord = (today - join_date_obj).days
            else:
                days_in_discord = 0
            
            test_users.append({
                'id': member.id,
                'name': member.name,
                'screens_weekly': screens_weekly,
                'days_in_discord': days_in_discord,
                'days_in_faction': days_in_faction
            })
    
    db.commit()
    test_users.sort(key=lambda x: x['screens_weekly'], reverse=True)
    
    embed = discord.Embed(
        title=f"📈 Недельная статистика TEST (неделя с {week_start_str})",
        color=discord.Color.gold()
    )
    
    def chunk_text(text: str, limit: int = 1024) -> list[str]:
        chunks = []
        while len(text) > limit:
            split_index = text.rfind("\n", 0, limit)
            if split_index == -1:
                split_index = limit
            chunks.append(text[:split_index])
            text = text[split_index:].lstrip("\n")
        if text:
            chunks.append(text)
        return chunks
    
    def add_zone_fields(zone_name, users, emoji):
        if not users:
            return
        full_text = "\n".join(
            f"{emoji} <@{u['id']}>: {u['screens_weekly']} скринов (дней в Discord: {u['days_in_discord']})"
            for u in users
        )
        for i, chunk in enumerate(chunk_text(full_text)):
            name = zone_name if i == 0 else f"{zone_name} (продолжение {i})"
            embed.add_field(name=name, value=chunk, inline=False)
    
    green_zone = [u for u in test_users if u['screens_weekly'] >= 10]
    yellow_zone = [u for u in test_users if 5 <= u['screens_weekly'] < 10]
    red_zone = [u for u in test_users if u['screens_weekly'] < 5]
    
    add_zone_fields("🟢 Активные", green_zone, "✅")
    add_zone_fields("🟡 Средний актив", yellow_zone, "⚠️")
    add_zone_fields("🔴 Маленький актив", red_zone, "❌")
    
    cursor.execute("SELECT message_id FROM weekly_stats WHERE week_start = ?", (week_start_str,))
    row = cursor.fetchone()
    
    if row and row[0]:
        try:
            message = await channel.fetch_message(row[0])
            await message.edit(embed=embed)
        except:
            message = await channel.send(embed=embed)
            cursor.execute("UPDATE weekly_stats SET message_id = ? WHERE week_start = ?", (message.id, week_start_str))
    else:
        message = await channel.send(embed=embed)
        cursor.execute(
            "INSERT OR REPLACE INTO weekly_stats (week_start, message_id) VALUES (?, ?)",
            (week_start_str, message.id)
        )
    
    db.commit()
    
    cursor.execute("SELECT week_start, message_id FROM weekly_stats ORDER BY week_start DESC")
    all_rows = cursor.fetchall()
    
    if len(all_rows) > 2:
        for old_week, old_message_id in all_rows[2:]:
            try:
                old_msg = await channel.fetch_message(old_message_id)
                await old_msg.delete()
            except discord.NotFound:
                pass
            except Exception as e:
                print(f"⚠️ Не удалось удалить старое недельное сообщение {old_message_id}: {e}")

# ========== НЕДЕЛЬНАЯ СТАТИСТИКА ДЛЯ MAIN ==========
async def update_weekly_stats_main():
    channel = bot.get_channel(CHANNEL_WEEKLY_STATS_MAIN_ID)
    if not channel:
        return
    
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    week_start_str = week_start.isoformat()
    
    guild = bot.get_guild(GUILD_ID)
    role_main = guild.get_role(ROLE_MAIN_ID)
    main_users = []
    
    for member in guild.members:
        if role_main in member.roles:
            cursor.execute(
                "SELECT screenshots_weekly, discord_join_date, days_in_faction FROM users_main WHERE user_id = ?",
                (member.id,)
            )
            row = cursor.fetchone()
            if row:
                screens_weekly, discord_join_date, days_in_faction = row
            else:
                screens_weekly = 0
                discord_join_date = member.joined_at.date().isoformat() if member.joined_at else date.today().isoformat()
                days_in_faction = 0
                cursor.execute(
                    "INSERT INTO users_main (user_id, username, discord_join_date, days_in_faction, screenshots_weekly) VALUES (?, ?, ?, ?, ?)",
                    (member.id, member.name, discord_join_date, days_in_faction, screens_weekly)
                )
            if discord_join_date:
                join_date_obj = datetime.strptime(discord_join_date, '%Y-%m-%d').date()
                days_in_discord = (today - join_date_obj).days
            else:
                days_in_discord = 0
            main_users.append({
                'id': member.id,
                'name': member.name,
                'screens_weekly': screens_weekly,
                'days_in_discord': days_in_discord,
                'days_in_faction': days_in_faction
            })
    
    db.commit()
    main_users.sort(key=lambda x: x['screens_weekly'], reverse=True)
    
    full_text = "\n".join(f"🔹 <@{u['id']}>: {u['screens_weekly']} скринов (дней в Discord: {u['days_in_discord']})" for u in main_users)
    
    def chunk_text(text: str, limit: int = 4000) -> list[str]:
        chunks = []
        while len(text) > limit:
            split_index = text.rfind("\n", 0, limit)
            if split_index == -1:
                split_index = limit
            chunks.append(text[:split_index])
            text = text[split_index:].lstrip("\n")
        if text:
            chunks.append(text)
        return chunks
    
    pages = chunk_text(full_text)
    
    cursor.execute("SELECT message_id FROM weekly_stats_main WHERE week_start = ?", (week_start_str,))
    row = cursor.fetchone()
    if row and row[0]:
        try:
            old_message = await channel.fetch_message(row[0])
            await old_message.delete()
        except:
            pass
    
    if pages:
        embed = discord.Embed(
            title=f"📈 Недельная статистика MAIN (неделя с {week_start_str})" + (f" (стр. 1/{len(pages)})" if len(pages) > 1 else ""),
            description=pages[0],
            color=discord.Color.purple()
        )
        view = WeeklyStatsPaginator(pages) if len(pages) > 1 else None
        message = await channel.send(embed=embed, view=view)
        cursor.execute("INSERT OR REPLACE INTO weekly_stats_main (week_start, message_id) VALUES (?, ?)", (week_start_str, message.id))
        db.commit()
    
    cursor.execute("SELECT week_start, message_id FROM weekly_stats_main ORDER BY week_start DESC")
    all_rows = cursor.fetchall()
    
    if len(all_rows) > 2:
        for old_week, old_message_id in all_rows[2:]:
            try:
                old_msg = await channel.fetch_message(old_message_id)
                await old_msg.delete()
            except discord.NotFound:
                pass
            except Exception as e:
                print(f"⚠️ Не удалось удалить старое недельное сообщение MAIN {old_message_id}: {e}")

class WeeklyStatsPaginator(discord.ui.View):
    def __init__(self, pages):
        super().__init__(timeout=None)
        self.pages = pages
        self.current_page = 0
    
    async def update_embed(self, interaction):
        embed = discord.Embed(
            title=f"📈 Недельная статистика MAIN (неделя с {(date.today() - timedelta(days=date.today().weekday())).isoformat()}) (стр. {self.current_page + 1}/{len(self.pages)})",
            description=self.pages[self.current_page],
            color=discord.Color.purple()
        )
        await interaction.response.edit_message(embed=embed, view=self)
    
    @discord.ui.button(label="⬅️", style=discord.ButtonStyle.secondary)
    async def previous(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page > 0:
            self.current_page -= 1
            await self.update_embed(interaction)
        else:
            await interaction.response.defer()
    
    @discord.ui.button(label="➡️", style=discord.ButtonStyle.secondary)
    async def next(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page < len(self.pages) - 1:
            self.current_page += 1
            await self.update_embed(interaction)
        else:
            await interaction.response.defer()

# ========== НАПОМИНАНИЯ О НЕАКТИВНОСТИ ДЛЯ TEST ==========
async def check_inactive_users():
    guild = bot.get_guild(GUILD_ID)
    role_test = guild.get_role(ROLE_TEST_ID)
    today = date.today()
    
    for member in guild.members:
        if role_test not in member.roles:
            continue
        
        cursor.execute(
            "SELECT last_screenshot_date, last_reminder_date, join_date FROM users WHERE user_id = ?",
            (member.id,)
        )
        row = cursor.fetchone()
        if not row:
            continue
        
        last_screenshot_date_str, last_reminder_date_str, join_date_str = row
        should_send_reminder = False
        custom_message = None
        
        if last_screenshot_date_str:
            last_screenshot_date = datetime.strptime(last_screenshot_date_str, '%Y-%m-%d').date()
            days_inactive = (today - last_screenshot_date).days
            
            if days_inactive >= INACTIVE_DAYS_THRESHOLD:
                should_send_reminder = True
                custom_message = (
                    f"⚠️ **Напоминание**\n"
                    f"Вы не отправляли скриншоты уже {days_inactive} дней.\n"
                    f"Пожалуйста, отправьте скриншоты в канал для отчётов, чтобы избежать исключения."
                )
        else:
            should_send_reminder = True
            custom_message = (
                f"⚠️ **Напоминание**\n"
                f"Вы ещё не отправили ни одного скриншота на повышение в канал <#{CHANNEL_REPORTS_ID}>.\n"
                f"Пожалуйста, не забывайте о повышении, чтобы избежать проблем!"
            )
        
        if last_reminder_date_str:
            last_reminder_date = datetime.strptime(last_reminder_date_str, '%Y-%m-%d').date()
            if (today - last_reminder_date).days < INACTIVE_DAYS_THRESHOLD:
                should_send_reminder = False
        
        if should_send_reminder and custom_message:
            try:
                await member.send(custom_message)
                cursor.execute(
                    "UPDATE users SET last_reminder_date = ? WHERE user_id = ?",
                    (today.isoformat(), member.id)
                )
                db.commit()
                print(f"📩 Напоминание отправлено {member.name} (TEST)")
            except discord.Forbidden:
                print(f"⚠️ Не удалось отправить напоминание {member.name} (TEST) (закрытые ЛС)")
            except Exception as e:
                print(f"❌ Ошибка при отправке напоминания {member.name} (TEST): {e}")

# ========== НАПОМИНАНИЯ О НЕАКТИВНОСТИ ДЛЯ MAIN ==========
async def check_inactive_users_main():
    guild = bot.get_guild(GUILD_ID)
    role_main = guild.get_role(ROLE_MAIN_ID)
    today = date.today()
    
    for member in guild.members:
        if role_main not in member.roles:
            continue
        
        cursor.execute(
            "SELECT last_screenshot_date, last_reminder_date, join_date FROM users_main WHERE user_id = ?",
            (member.id,)
        )
        row = cursor.fetchone()
        if not row:
            continue
        
        last_screenshot_date_str, last_reminder_date_str, join_date_str = row
        should_send_reminder = False
        custom_message = None
        
        if last_screenshot_date_str:
            last_screenshot_date = datetime.strptime(last_screenshot_date_str, '%Y-%m-%d').date()
            days_inactive = (today - last_screenshot_date).days
            
            if days_inactive >= INACTIVE_DAYS_THRESHOLD:
                should_send_reminder = True
                custom_message = (
                    f"⚠️ **Напоминание**\n"
                    f"Вы не отправляли скриншоты уже {days_inactive} дней.\n"
                    f"Пожалуйста, отправьте скриншоты в канал для отчётов, чтобы поддерживать активность."
                )
        else:
            should_send_reminder = True
            custom_message = (
                f"⚠️ **Напоминание**\n"
                f"Вы ещё не отправили ни одного скриншота в канал <#{CHANNEL_MAIN_ID}>.\n"
                f"Пожалуйста, отправляйте скриншоты для поддержания активности!"
            )
        
        if last_reminder_date_str:
            last_reminder_date = datetime.strptime(last_reminder_date_str, '%Y-%m-%d').date()
            if (today - last_reminder_date).days < INACTIVE_DAYS_THRESHOLD:
                should_send_reminder = False
        
        if should_send_reminder and custom_message:
            try:
                await member.send(custom_message)
                cursor.execute(
                    "UPDATE users_main SET last_reminder_date = ? WHERE user_id = ?",
                    (today.isoformat(), member.id)
                )
                db.commit()
                print(f"📩 Напоминание отправлено {member.name} (MAIN)")
            except discord.Forbidden:
                print(f"⚠️ Не удалось отправить напоминание {member.name} (MAIN) (закрытые ЛС)")
            except Exception as e:
                print(f"❌ Ошибка при отправке напоминания {member.name} (MAIN): {e}")

# ========== СОБЫТИЯ ==========
@bot.event
async def on_ready():
    print(f'Бот {bot.user} запущен!')
    await initialize_discord_join_dates()
    weekly_tasks.start()
    inactive_check.start()
    await update_weekly_stats()
    await update_weekly_stats_main()

async def initialize_discord_join_dates():
    guild = bot.get_guild(GUILD_ID)
    for member in guild.members:
        if member.joined_at:
            discord_join_date = member.joined_at.date().isoformat()
            cursor.execute(
                "SELECT discord_join_date FROM users WHERE user_id = ?",
                (member.id,)
            )
            row = cursor.fetchone()
            if not row or not row[0]:
                cursor.execute(
                    "INSERT OR REPLACE INTO users (user_id, username, discord_join_date) VALUES (?, ?, ?)",
                    (member.id, member.name, discord_join_date)
                )
            cursor.execute(
                "SELECT discord_join_date FROM users_main WHERE user_id = ?",
                (member.id,)
            )
            row = cursor.fetchone()
            if not row or not row[0]:
                cursor.execute(
                    "INSERT OR REPLACE INTO users_main (user_id, username, discord_join_date) VALUES (?, ?, ?)",
                    (member.id, member.name, discord_join_date)
                )
    db.commit()

@bot.event
async def on_member_update(before, after):
    role_test = after.guild.get_role(ROLE_TEST_ID)
    role_main = after.guild.get_role(ROLE_MAIN_ID)
    
    if role_test not in before.roles and role_test in after.roles:
        join_date = date.today().isoformat()
        discord_join_date = after.joined_at.date().isoformat() if after.joined_at else date.today().isoformat()
        cursor.execute(
            "INSERT OR IGNORE INTO users (user_id, username, join_date, discord_join_date) VALUES (?, ?, ?, ?)",
            (after.id, after.name, join_date, discord_join_date)
        )
        db.commit()
        await update_weekly_stats()
    
    if role_test in before.roles and role_test not in after.roles:
        cursor.execute("SELECT approved FROM users WHERE user_id = ?", (after.id,))
        row = cursor.fetchone()
        if row and row[0] == 0:
            cursor.execute("DELETE FROM users WHERE user_id = ?", (after.id,))
            delete_user_files(after.id)
            db.commit()
            await update_weekly_stats()
    
    if role_main not in before.roles and role_main in after.roles:
        join_date = date.today().isoformat()
        discord_join_date = after.joined_at.date().isoformat() if after.joined_at else date.today().isoformat()
        cursor.execute(
            "INSERT OR IGNORE INTO users_main (user_id, username, join_date, discord_join_date) VALUES (?, ?, ?, ?)",
            (after.id, after.name, join_date, discord_join_date)
        )
        db.commit()
        await update_weekly_stats_main()
    
    if role_main in before.roles and role_main not in after.roles:
        cursor.execute("DELETE FROM users_main WHERE user_id = ?", (after.id,))
        db.commit()
        await update_weekly_stats_main()

@bot.event
async def on_member_remove(member):
    role_test = member.guild.get_role(ROLE_TEST_ID)
    role_main = member.guild.get_role(ROLE_MAIN_ID)
    
    if role_test in member.roles:
        cursor.execute("DELETE FROM users WHERE user_id = ?", (member.id,))
        delete_user_files(member.id)
        db.commit()
        await update_weekly_stats()
    
    if role_main in member.roles:
        cursor.execute("DELETE FROM users_main WHERE user_id = ?", (member.id,))
        db.commit()
        await update_weekly_stats_main()

@bot.event
async def on_message(message):
    if message.author.bot or isinstance(message.channel, discord.DMChannel):
        return
    
    if message.content.lower() in ["!статистика", "!stats"] and message.channel.permissions_for(message.author).administrator:
        await update_weekly_stats()
        await update_weekly_stats_main()
        await message.channel.send("✅ Статистика обновлена!", delete_after=10)
        return
    
    if message.content.lower() in ["!totals", "!статистика_всех"]:
        await handle_totals_command(message, role_type="TEST")
        return
    
    if message.content.lower() in ["!totals_main", "!статистика_мейн"]:
        await handle_totals_command(message, role_type="MAIN")
        return
    
    if message.content.lower() in ["!fix_dates", "!исправить_даты"] and message.channel.permissions_for(message.author).administrator:
        await initialize_discord_join_dates()
        await message.channel.send("✅ Даты вступления в Discord обновлены!", delete_after=10)
        return
    
    if message.channel.id in [CHANNEL_REPORTS_ID, CHANNEL_MAIN_ID]:
        await handle_screenshots(message)

async def handle_screenshots(message):
    role_test = message.guild.get_role(ROLE_TEST_ID)
    role_main = message.guild.get_role(ROLE_MAIN_ID)
    today = date.today().isoformat()
    user_id = message.author.id
    username = message.author.name
    
    if not message.attachments:
        return
    
    discord_join_date = message.author.joined_at.date().isoformat() if message.author.joined_at else date.today().isoformat()
    
    screenshot_count = sum(1 for attachment in message.attachments if attachment.content_type and attachment.content_type.startswith('image/'))
    if screenshot_count == 0:
        return
    
    if role_test in message.author.roles and message.channel.id == CHANNEL_REPORTS_ID:
        cursor.execute(
            "INSERT OR IGNORE INTO users (user_id, username, discord_join_date) VALUES (?, ?, ?)",
            (user_id, username, discord_join_date)
        )
        cursor.execute(
            "UPDATE users SET username = ?, discord_join_date = ? WHERE user_id = ?",
            (username, discord_join_date, user_id)
        )
        
        for attachment in message.attachments:
            if attachment.content_type and attachment.content_type.startswith('image/'):
                path = await save_attachment(attachment, user_id)
                if path:
                    cursor.execute(
                        "INSERT INTO screenshots (user_id, message_id, path, date) VALUES (?, ?, ?, ?)",
                        (user_id, message.id, path, today)
                    )
        
        cursor.execute('''
        INSERT INTO users (user_id, username, screenshots_total, screenshots_weekly, last_screenshot_date)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username = excluded.username,
            screenshots_total = screenshots_total + ?,
            screenshots_weekly = screenshots_weekly + ?,
            last_screenshot_date = excluded.last_screenshot_date
        ''', (user_id, username, screenshot_count, screenshot_count, today, screenshot_count, screenshot_count))
        
        total_screens = cursor.execute('SELECT screenshots_total FROM users WHERE user_id = ?', (user_id,)).fetchone()[0]
        weekly_screens = cursor.execute('SELECT screenshots_weekly FROM users WHERE user_id = ?', (user_id,)).fetchone()[0]
        try:
            await message.reply(f"📸 {message.author.mention}, скриншоты приняты! Всего скринов: {total_screens}, за неделю: {weekly_screens}", delete_after=10)
        except discord.Forbidden:
            print(f"⚠️ Не удалось ответить {message.author.name} в канале {message.channel.id} (отсутствуют права)")
        except Exception as e:
            print(f"❌ Ошибка при отправке ответа {message.author.name}: {e}")
        
        cursor.execute("SELECT screenshots_total, required_screens FROM users WHERE user_id = ?", (user_id,))
        row = cursor.fetchone()
        if row:
            total, required = row
            if (required and total >= required) or (not required and total >= DEFAULT_THRESHOLD):
                cursor.execute("SELECT path FROM screenshots WHERE user_id = ? ORDER BY date DESC, id DESC", (user_id,))
                paths = [row[0] for row in cursor.fetchall()]
                asyncio.create_task(process_approval_request(message.author, total, user_id, paths))
    
    elif role_main in message.author.roles and message.channel.id == CHANNEL_MAIN_ID:
        cursor.execute(
            "INSERT OR IGNORE INTO users_main (user_id, username, discord_join_date) VALUES (?, ?, ?)",
            (user_id, username, discord_join_date)
        )
        cursor.execute(
            "UPDATE users_main SET username = ?, discord_join_date = ? WHERE user_id = ?",
            (username, discord_join_date, user_id)
        )
        
        cursor.execute('''
        INSERT INTO users_main (user_id, username, screenshots_total, screenshots_weekly, last_screenshot_date)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username = excluded.username,
            screenshots_total = screenshots_total + ?,
            screenshots_weekly = screenshots_weekly + ?,
            last_screenshot_date = excluded.last_screenshot_date
        ''', (user_id, username, screenshot_count, screenshot_count, today, screenshot_count, screenshot_count))
        
        total_screens = cursor.execute('SELECT screenshots_total FROM users_main WHERE user_id = ?', (user_id,)).fetchone()[0]
        weekly_screens = cursor.execute('SELECT screenshots_weekly FROM users_main WHERE user_id = ?', (user_id,)).fetchone()[0]
        try:
            await message.reply(f"📸 {message.author.mention}, скриншоты приняты! Всего скринов: {total_screens}, за неделю: {weekly_screens}", delete_after=10)
        except discord.Forbidden:
            print(f"⚠️ Не удалось ответить {message.author.name} в канале {message.channel.id} (отсутствуют права)")
        except Exception as e:
            print(f"❌ Ошибка при отправке ответа {message.author.name}: {e}")
    
    db.commit()
    await update_weekly_stats()
    await update_weekly_stats_main()

async def process_approval_request(user, total_screens, user_id, paths):
    try:
        pdf_path = await generate_pdf(user_id, paths)
        if pdf_path:
            await send_approval_request(user, total_screens, pdf_path)
        else:
            await send_approval_request_without_pdf(user, total_screens)
    except Exception as e:
        print(f"❌ Ошибка при обработке запроса подтверждения: {e}")
        await send_approval_request_without_pdf(user, total_screens)

async def send_approval_request_without_pdf(user, total_screens):
    channel_approval = bot.get_channel(CHANNEL_APPROVAL_ID)
    guild = channel_approval.guild
    role_rec = guild.get_role(ROLE_REC_ID)
    role_high = guild.get_role(ROLE_HIGH_ID)
    mentions = " ".join([r.mention for r in (role_rec, role_high) if r])
    embed = discord.Embed(
        title="🎯 Запрос на перевод игрока (БЕЗ PDF)",
        description=f"Игрок {user.mention} ({user}) отправил {total_screens} скриншотов.\n\n⚠️ **Не удалось сгенерировать PDF файл!**",
        color=discord.Color.orange()
    )
    embed.set_thumbnail(url=user.avatar.url)
    view = ApprovalButtons(user.id)
    await channel_approval.send(content=mentions, embed=embed, view=view)

async def send_approval_request(user, total_screens, pdf_path):
    channel_approval = bot.get_channel(CHANNEL_APPROVAL_ID)
    guild = channel_approval.guild
    role_rec = guild.get_role(ROLE_REC_ID)
    role_high = guild.get_role(ROLE_HIGH_ID)
    mentions = " ".join([r.mention for r in (role_rec, role_high) if r])
    embed = discord.Embed(
        title="🎯 Запрос на перевод игрока",
        description=f"Игрок {user.mention} ({user}) отправил {total_screens} скриншотов.",
        color=discord.Color.orange()
    )
    embed.set_thumbnail(url=user.avatar.url)
    view = ApprovalButtons(user.id)
    
    file_size = os.path.getsize(pdf_path) / (1024 * 1024)
    if file_size > 25:
        await channel_approval.send(content=mentions, embed=embed, view=view)
    else:
        await channel_approval.send(content=mentions, embed=embed, view=view, file=discord.File(pdf_path))
    
    try:
        os.remove(pdf_path)
    except:
        pass

# ========== ТАСКИ ==========
@tasks.loop(hours=24)
async def weekly_tasks():
    print(f"⏰ weekly_tasks запущен: {datetime.now().isoformat()}")
    guild = bot.get_guild(GUILD_ID)
    role_test = guild.get_role(ROLE_TEST_ID)
    role_main = guild.get_role(ROLE_MAIN_ID)
    
    for member in guild.members:
        if role_test in member.roles:
            cursor.execute(
                "UPDATE users SET days_in_faction = days_in_faction + 1 WHERE user_id = ?",
                (member.id,)
            )
        if role_main in member.roles:
            cursor.execute(
                "UPDATE users_main SET days_in_faction = days_in_faction + 1 WHERE user_id = ?",
                (member.id,)
            )
    
    today = date.today()
    if today.weekday() == 0:
        cursor.execute("UPDATE users SET screenshots_weekly = 0")
        cursor.execute("UPDATE users_main SET screenshots_weekly = 0")
    
    db.commit()
    await update_weekly_stats()
    await update_weekly_stats_main()

@tasks.loop(hours=12)
async def inactive_check():
    await check_inactive_users()
    await check_inactive_users_main()

@weekly_tasks.before_loop
async def before_weekly_tasks():
    await bot.wait_until_ready()
    now = datetime.now()
    next_run = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if next_run <= now:
        next_run += timedelta(days=1)
    await discord.utils.sleep_until(next_run)

# ========== ОБРАБОТКА КОМАНД ==========
async def handle_totals_command(message, role_type="TEST"):
    guild = message.guild
    role_id = ROLE_TEST_ID if role_type == "TEST" else ROLE_MAIN_ID
    role = guild.get_role(role_id)
    table = "users" if role_type == "TEST" else "users_main"
    field = "screenshots_total"
    title = "TEST" if role_type == "TEST" else "MAIN"
    
    if message.mentions:
        member = message.mentions[0]
        if role not in member.roles:
            return await message.channel.send(f"❌ Пользователь {member.mention} не имеет роли {title}.", delete_after=10)
        
        cursor.execute(
            f"SELECT {field}, discord_join_date FROM {table} WHERE user_id = ?",
            (member.id,)
        )
        row = cursor.fetchone()
        if not row:
            return await message.channel.send(f"❌ Пользователь {member.mention} не найден в базе.", delete_after=10)
        
        total, discord_join_date = row
        try:
            days_in_discord = (date.today() - datetime.strptime(discord_join_date, "%Y-%m-%d").date()).days if discord_join_date else 0
        except ValueError:
            days_in_discord = 0
        
        embed = discord.Embed(
            title=f"📊 Статистика {member.display_name} ({title})",
            description=f"**Пользователь:** {member.mention}\n**Скриншотов:** {total}\n**Дней в Discord:** {days_in_discord}",
            color=discord.Color.blue()
        )
        return await message.channel.send(embed=embed, delete_after=30)
    
    lines = []
    for member in guild.members:
        if role in member.roles:
            cursor.execute(
                f"SELECT {field}, discord_join_date FROM {table} WHERE user_id = ?",
                (member.id,)
            )
            row = cursor.fetchone()
            
            if row:
                total, discord_join_date = row
                try:
                    days_in_discord = (date.today() - datetime.strptime(discord_join_date, "%Y-%m-%d").date()).days if discord_join_date else 0
                except ValueError:
                    days_in_discord = 0
                lines.append(f"{member.mention}: {total} скринов ({days_in_discord} дней в Discord)")
    
    if not lines:
        return await message.channel.send(f"Нет данных о пользователях с ролью {title}.", delete_after=10)
    
    def sort_key(line):
        import re
        match = re.search(r': (\d+) скринов', line)
        return int(match.group(1)) if match else 0
    
    lines.sort(key=sort_key, reverse=True)
    
    pages = []
    current_page = ""
    for line in lines:
        if len(current_page) + len(line) + 1 > 4000:
            pages.append(current_page)
            current_page = ""
        current_page += line + "\n"
    if current_page:
        pages.append(current_page)
    
    view = TotalsPaginator(pages)
    embed = discord.Embed(
        title=f"📊 Статистика пользователей с ролью {title} (стр. 1/{len(pages)})",
        description=pages[0],
        color=discord.Color.blue()
    )
    await message.channel.send(embed=embed, view=view, delete_after=120)

class TotalsPaginator(discord.ui.View):
    def __init__(self, pages):
        super().__init__(timeout=120)
        self.pages = pages
        self.current_page = 0
    
    async def update_message(self, interaction):
        embed = discord.Embed(
            title=f"📊 Общая статистика (стр. {self.current_page + 1}/{len(self.pages)}) — листай ⬅️➡️",
            description=self.pages[self.current_page],
            color=discord.Color.blue()
        )
        await interaction.response.edit_message(embed=embed, view=self)
    
    @discord.ui.button(label="⬅️", style=discord.ButtonStyle.secondary)
    async def previous(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page > 0:
            self.current_page -= 1
            await self.update_message(interaction)
        else:
            await interaction.response.defer()
    
    @discord.ui.button(label="➡️", style=discord.ButtonStyle.secondary)
    async def next(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page < len(self.pages) - 1:
            self.current_page += 1
            await self.update_message(interaction)
        else:
            await interaction.response.defer()

@bot.command(name='delete_user')
@commands.has_permissions(administrator=True)
async def delete_user_command(ctx, member: discord.Member):
    user_id = member.id
    role_test = ctx.guild.get_role(ROLE_TEST_ID)
    role_main = ctx.guild.get_role(ROLE_MAIN_ID)
    
    if role_test in member.roles:
        cursor.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
        delete_user_files(user_id)
    if role_main in member.roles:
        cursor.execute("DELETE FROM users_main WHERE user_id = ?", (user_id,))
    
    db.commit()
    await update_weekly_stats()
    await update_weekly_stats_main()
    await ctx.send(f"✅ Данные пользователя {member.mention} удалены из базы.")

# ========== СТАРТ ==========
if __name__ == "__main__":
    bot.run(TOKEN)