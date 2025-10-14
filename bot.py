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
CHANNEL_WEEKLY_STATS_ID = int(os.environ.get('CHANNEL_WEEKLY_STATS_ID'))
ROLE_TEST_ID = int(os.environ.get('ROLE_TEST_ID'))
ROLE_MAIN_ID = int(os.environ.get('ROLE_MAIN_ID'))
ROLE_REC_ID = int(os.environ.get('ROLE_REC_ID'))
ROLE_HIGH_ID = int(os.environ.get('ROLE_HIGH_ID'))
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
        CREATE TABLE IF NOT EXISTS users_main (
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
        stats_type TEXT PRIMARY KEY,
        message_id INTEGER,
        week_start TEXT
    )
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
        #role_tir3 = guild.get_role(ROLE_TIR3_ID)
        
        if not target_user:
            return await interaction.response.send_message("Пользователь не найден.", ephemeral=True)
        
        # Только роль MAIN, TIR3 больше не выдаем автоматически
        if role_main:
            await target_user.add_roles(role_main)
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

# ========== ОБНОВЛЕННАЯ НЕДЕЛЬНАЯ СТАТИСТИКА ==========
async def initialize_weekly_stats():
    """Инициализирует два фиксированных сообщения для статистики"""
    channel = bot.get_channel(CHANNEL_WEEKLY_STATS_ID)
    if not channel:
        return None, None
    
    # Проверяем существующие сообщения в базе
    cursor.execute("SELECT message_id, stats_type FROM weekly_stats")
    existing_messages = cursor.fetchall()
    
    test_message_id = None
    main_message_id = None
    
    for msg_id, stats_type in existing_messages:
        try:
            message = await channel.fetch_message(msg_id)
            if stats_type == "TEST":
                test_message_id = msg_id
            elif stats_type == "MAIN":
                main_message_id = msg_id
        except discord.NotFound:
            # Сообщение было удалено, удаляем из базы
            cursor.execute("DELETE FROM weekly_stats WHERE message_id = ?", (msg_id,))
            continue
    
    # Создаем недостающие сообщения
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    week_start_str = week_start.isoformat()
    
    if not test_message_id:
        test_embed = discord.Embed(
            title=f"📈 Недельная статистика TEST (неделя с {week_start_str})",
            description="*Загрузка статистики...*",
            color=discord.Color.gold()
        )
        test_message = await channel.send(embed=test_embed)
        test_message_id = test_message.id
        cursor.execute(
            "INSERT OR REPLACE INTO weekly_stats (stats_type, message_id, week_start) VALUES (?, ?, ?)",
            ("TEST", test_message_id, week_start_str)
        )
    
    if not main_message_id:
        main_embed = discord.Embed(
            title=f"📈 Недельная статистика MAIN (неделя с {week_start_str})",
            description="*Загрузка статистики...*",
            color=discord.Color.purple()
        )
        main_message = await channel.send(embed=main_embed)
        main_message_id = main_message.id
        cursor.execute(
            "INSERT OR REPLACE INTO weekly_stats (stats_type, message_id, week_start) VALUES (?, ?, ?)",
            ("MAIN", main_message_id, week_start_str)
        )
    
    db.commit()
    return test_message_id, main_message_id

async def update_weekly_stats():
    """Обновляет существующие сообщения статистики"""
    channel = bot.get_channel(CHANNEL_WEEKLY_STATS_ID)
    if not channel:
        return
    
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    week_start_str = week_start.isoformat()
    
    # Получаем ID сообщений из базы
    cursor.execute("SELECT message_id, stats_type FROM weekly_stats")
    messages_data = cursor.fetchall()
    
    test_message_id = None
    main_message_id = None
    
    for msg_id, stats_type in messages_data:
        if stats_type == "TEST":
            test_message_id = msg_id
        elif stats_type == "MAIN":
            main_message_id = msg_id
    
    # Если сообщений нет - инициализируем
    if not test_message_id or not main_message_id:
        test_message_id, main_message_id = await initialize_weekly_stats()
    
    guild = bot.get_guild(GUILD_ID)
    role_test = guild.get_role(ROLE_TEST_ID)
    role_main = guild.get_role(ROLE_MAIN_ID)
    
    # Собираем статистику для TEST - ВСЕХ пользователей с ролью
    test_users = []
    for member in guild.members:
        if role_test in member.roles:
            cursor.execute(
                "SELECT screenshots_weekly, discord_join_date, days_in_faction FROM users WHERE user_id = ?",
                (member.id,)
            )
            row = cursor.fetchone()
            
            if row:
                screens_weekly, discord_join_date, days_in_faction = row
                if not discord_join_date:
                    discord_join_date = member.joined_at.date().isoformat() if member.joined_at else date.today().isoformat()
                    cursor.execute(
                        "UPDATE users SET discord_join_date = ? WHERE user_id = ?",
                        (discord_join_date, member.id)
                    )
            else:
                # Если пользователя нет в базе, добавляем
                discord_join_date = member.joined_at.date().isoformat() if member.joined_at else date.today().isoformat()
                screens_weekly = 0
                days_in_faction = 0
                cursor.execute(
                    "INSERT INTO users (user_id, username, discord_join_date, screenshots_weekly, days_in_faction) VALUES (?, ?, ?, ?, ?)",
                    (member.id, member.name, discord_join_date, screens_weekly, days_in_faction)
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
    
    # Собираем статистику для MAIN - ВСЕХ пользователей с ролью
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
                # Если пользователя нет в базе, добавляем
                discord_join_date = member.joined_at.date().isoformat() if member.joined_at else date.today().isoformat()
                screens_weekly = 0
                days_in_faction = 0
                cursor.execute(
                    "INSERT INTO users_main (user_id, username, discord_join_date, screenshots_weekly, days_in_faction) VALUES (?, ?, ?, ?, ?)",
                    (member.id, member.name, discord_join_date, screens_weekly, days_in_faction)
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
    
    # Создаем страницы для сообщений
    test_pages = create_stats_pages(test_users, "TEST")
    main_pages = create_stats_pages(main_users, "MAIN")
    
    # Обновляем сообщение TEST
    if test_message_id and test_pages:
        try:
            test_message = await channel.fetch_message(test_message_id)
            test_embed = discord.Embed(
                title=f"📈 Недельная статистика TEST (неделя с {week_start_str})" + (f" (стр. 1/{len(test_pages)})" if len(test_pages) > 1 else ""),
                description=test_pages[0],  # Брать первую страницу, а не весь список
                color=discord.Color.gold()
            )
            test_view = WeeklyStatsPaginator(test_pages, "TEST", test_message_id) if len(test_pages) > 1 else None
            
            await test_message.edit(embed=test_embed, view=test_view)
        except discord.NotFound:
            # Сообщение было удалено, создаем новое
            test_embed = discord.Embed(
                title=f"📈 Недельная статистика TEST (неделя с {week_start_str})" + (f" (стр. 1/{len(test_pages)})" if len(test_pages) > 1 else ""),
                description=test_pages[0],  # Брать первую страницу, а не весь список
                color=discord.Color.gold()
            )
            test_view = WeeklyStatsPaginator(test_pages, "TEST") if len(test_pages) > 1 else None
            test_message = await channel.send(embed=test_embed, view=test_view)
            test_message_id = test_message.id
            
            # Обновляем в базе
            cursor.execute(
                "UPDATE weekly_stats SET message_id = ? WHERE stats_type = 'TEST'",
                (test_message_id,)
            )
    
    # Обновляем сообщение MAIN
    if main_message_id and main_pages:
        try:
            main_message = await channel.fetch_message(main_message_id)
            main_embed = discord.Embed(
                title=f"📈 Недельная статистика MAIN (неделя с {week_start_str})" + (f" (стр. 1/{len(main_pages)})" if len(main_pages) > 1 else ""),
                description=main_pages[0],  # Брать первую страницу, а не весь список
                color=discord.Color.purple()
            )
            main_view = WeeklyStatsPaginator(main_pages, "MAIN", main_message_id) if len(main_pages) > 1 else None
            
            await main_message.edit(embed=main_embed, view=main_view)
        except discord.NotFound:
            # Сообщение было удалено, создаем новое
            main_embed = discord.Embed(
                title=f"📈 Недельная статистика MAIN (неделя с {week_start_str})" + (f" (стр. 1/{len(main_pages)})" if len(main_pages) > 1 else ""),
                description=main_pages[0],  # Брать первую страницу, а не весь список
                color=discord.Color.purple()
            )
            main_view = WeeklyStatsPaginator(main_pages, "MAIN") if len(main_pages) > 1 else None
            main_message = await channel.send(embed=main_embed, view=main_view)
            main_message_id = main_message.id
            
            # Обновляем в базе
            cursor.execute(
                "UPDATE weekly_stats SET message_id = ? WHERE stats_type = 'MAIN'",
                (main_message_id,)
            )
    
    db.commit()

def create_stats_pages(users, stats_type):
    """Создает страницы статистики с зонами активности"""
    if not users:
        return ["Нет пользователей с этой ролью"]
    
    # Сортируем сначала по скриншотам (по убыванию), потом по дням в Discord (по убыванию)
    users.sort(key=lambda x: (-x['screens_weekly'], -x['days_in_discord']))
    
    # Разделяем на зоны
    if stats_type == "TEST":
        green_zone = [u for u in users if u['screens_weekly'] >= 10]
        yellow_zone = [u for u in users if 5 <= u['screens_weekly'] < 10]
        red_zone = [u for u in users if u['screens_weekly'] < 5]
    else:  # MAIN
        green_zone = [u for u in users if u['screens_weekly'] >= 7]
        yellow_zone = [u for u in users if 3 <= u['screens_weekly'] < 7]
        red_zone = [u for u in users if u['screens_weekly'] < 3]
    
    # Формируем текст
    full_text = ""
    
    if green_zone:
        full_text += "🟢 **Активные:**\n" + "\n".join(
            f"✅ <@{u['id']}>: {u['screens_weekly']} скринов (в Discord: {u['days_in_discord']}д)"
            for u in green_zone
        ) + "\n\n"
    
    if yellow_zone:
        full_text += "🟡 **Средний актив:**\n" + "\n".join(
            f"⚠️ <@{u['id']}>: {u['screens_weekly']} скринов (в Discord: {u['days_in_discord']}д)"
            for u in yellow_zone
        ) + "\n\n"
    
    if red_zone:
        full_text += "🔴 **Маленький актив:**\n" + "\n".join(
            f"❌ <@{u['id']}>: {u['screens_weekly']} скринов (в Discord: {u['days_in_discord']}д)"
            for u in red_zone
        )
    
    if not full_text.strip():
        return ["Нет данных для отображения"]
    
    # Разбиваем на страницы
    def chunk_text(text: str, limit: int = 4000) -> list[str]:
        if len(text) <= limit:
            return [text]
        
        chunks = []
        while len(text) > limit:
            split_index = text.rfind("\n", 0, limit)
            if split_index == -1:
                split_index = limit
            chunks.append(text[:split_index])
            text = text[split_index:].lstrip("\n")
        if text.strip():
            chunks.append(text)
        return chunks
    
    return chunk_text(full_text)

class WeeklyStatsPaginator(discord.ui.View):
    def __init__(self, pages, stats_type, message_id=None):
        super().__init__(timeout=None)
        self.pages = pages
        self.stats_type = stats_type
        self.current_page = 0
        self.message_id = message_id
    
    async def update_embed(self, interaction):
        today = date.today()
        week_start = today - timedelta(days=today.weekday())
        
        embed = discord.Embed(
            title=f"📈 Недельная статистика {self.stats_type} (неделя с {week_start.isoformat()}) (стр. {self.current_page + 1}/{len(self.pages)})",
            description=self.pages[self.current_page],
            color=discord.Color.gold() if self.stats_type == "TEST" else discord.Color.purple()
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
                    f"Пожалуйста, отправьте скриншоты в канал <#{CHANNEL_REPORTS_ID}>, чтобы избежать исключения."
                )
        else:
            should_send_reminder = True
            custom_message = (
                f"⚠️ **Напоминание**\n"
                f"Вы ещё не отправили ни одного скриншота в канал <#{CHANNEL_REPORTS_ID}>.\n"
                f"Пожалуйста, не забывайте об активности, чтобы избежать проблем!"
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
                    f"Пожалуйста, отправьте скриншоты в канал <#{CHANNEL_REPORTS_ID}>, чтобы поддерживать активность."
                )
        else:
            should_send_reminder = True
            custom_message = (
                f"⚠️ **Напоминание**\n"
                f"Вы ещё не отправили ни одного скриншота в канал <#{CHANNEL_REPORTS_ID}>n"
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
    await initialize_weekly_stats()  # Инициализируем сообщения статистики
    weekly_tasks.start()
    inactive_check.start()
    await update_weekly_stats()  # Обновляем контент

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
        await update_weekly_stats()
    
    if role_main in before.roles and role_main not in after.roles:
        cursor.execute("DELETE FROM users_main WHERE user_id = ?", (after.id,))
        db.commit()
        await update_weekly_stats()

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
        await update_weekly_stats()

@bot.event
async def on_message(message):
    if message.author.bot or isinstance(message.channel, discord.DMChannel):
        return
    
    if message.content.lower() in ["!статистика", "!stats"] and message.channel.permissions_for(message.author).administrator:
        await update_weekly_stats()
        await message.channel.send("✅ Статистика обновлена!", delete_after=10)
        return
    
    if message.content.lower() in ["!totals_test", "!статистика_всех"]:
        await handle_totals_command(message, role_type="TEST")
        return
    
    if message.content.lower() in ["!totals_main", "!статистика_мейн"]:
        await handle_totals_command(message, role_type="MAIN")
        return
    
    if message.content.lower() in ["!fix_dates", "!исправить_даты"] and message.channel.permissions_for(message.author).administrator:
        await initialize_discord_join_dates()
        await message.channel.send("✅ Даты вступления в Discord обновлены!", delete_after=10)
        return
    
    # Обработка скриншотов в одном канале с проверкой роли
    if message.channel.id == CHANNEL_REPORTS_ID:
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
    
    # Проверяем роль пользователя и обрабатываем соответственно
    if role_test in message.author.roles:
        # Обработка для TEST (с отчетом)
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
    
    elif role_main in message.author.roles:
        # Обработка для MAIN (без отчета)
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

@bot.event
async def on_reaction_add(reaction, user):
    # Игнорируем реакции бота
    if user.bot:
        return
    if reaction.message.channel.id != CHANNEL_REPORTS_ID:
        return
    if str(reaction.emoji) != "❌":
        return
    
    message = reaction.message
    author = message.author

    # Проверяем, что это пользователь с ролью TEST или MAIN
    guild = message.guild
    role_test = guild.get_role(ROLE_TEST_ID)
    role_main = guild.get_role(ROLE_MAIN_ID)

    member = guild.get_member(author.id)
    if not member or (role_test not in member.roles and role_main not in member.roles):
        return

    # Ищем скриншот в базе по message_id
    cursor.execute("SELECT path FROM screenshots WHERE message_id = ?", (message.id,))
    row = cursor.fetchone()
    if not row:
        return

    # Удаляем файл
    file_path = row[0]
    if file_path and os.path.exists(file_path):
        try:
            os.remove(file_path)
        except Exception as e:
            print(f"⚠️ Не удалось удалить файл {file_path}: {e}")

    # Удаляем из базы
    cursor.execute("DELETE FROM screenshots WHERE message_id = ?", (message.id,))

    # Уменьшаем счетчики у TEST
    if role_test in member.roles:
        cursor.execute('''
            UPDATE users SET
                screenshots_total = CASE WHEN screenshots_total > 0 THEN screenshots_total - 1 ELSE 0 END,
                screenshots_weekly = CASE WHEN screenshots_weekly > 0 THEN screenshots_weekly - 1 ELSE 0 END
            WHERE user_id = ?
        ''', (author.id,))

    # Уменьшаем счетчики у MAIN
    if role_main in member.roles:
        cursor.execute('''
            UPDATE users_main SET
                screenshots_total = CASE WHEN screenshots_total > 0 THEN screenshots_total - 1 ELSE 0 END,
                screenshots_weekly = CASE WHEN screenshots_weekly > 0 THEN screenshots_weekly - 1 ELSE 0 END
            WHERE user_id = ?
        ''', (author.id,))

    db.commit()

    # Обновляем недельную статистику
    await update_weekly_stats()

    # Можно отправить уведомление
    try:
        await message.channel.send(f"⚠️ Скриншот {author.mention} был удалён по реакции ❌", delete_after=10)
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
                lines.append({
                    'mention': member.mention,
                    'total': total,
                    'days_in_discord': days_in_discord
                })
    
    if not lines:
        return await message.channel.send(f"Нет данных о пользователях с ролью {title}.", delete_after=10)
    
    # Сортируем сначала по общему количеству скринов (по убыванию), потом по дням в Discord (по убыванию)
    lines.sort(key=lambda x: (-x['total'], -x['days_in_discord']))
    
    # Форматируем строки после сортировки
    formatted_lines = [f"{line['mention']}: {line['total']} скринов ({line['days_in_discord']} дней в Discord)" for line in lines]
    
    pages = []
    current_page = ""
    for line in formatted_lines:
        if len(current_page) + len(line) + 1 > 4000:
            pages.append(current_page)
            current_page = ""
        current_page += line + "\n"
    if current_page:
        pages.append(current_page)
    
    view = TotalsPaginator(pages, title)
    embed = discord.Embed(
        title=f"📊 Общая статистика {title} (стр. 1/{len(pages)})",
        description=pages[0],
        color=discord.Color.gold() if role_type == "TEST" else discord.Color.purple()
    )
    await message.channel.send(embed=embed, view=view, delete_after=120)

class TotalsPaginator(discord.ui.View):
    def __init__(self, pages, stats_type):
        super().__init__(timeout=120)
        self.pages = pages
        self.current_page = 0
        self.stats_type = stats_type
    
    async def update_message(self, interaction):
        embed = discord.Embed(
            title=f"📊 Общая статистика {self.stats_type} (стр. {self.current_page + 1}/{len(self.pages)})",
            description=self.pages[self.current_page],
            color=discord.Color.gold() if self.stats_type == "TEST" else discord.Color.purple()
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
    await ctx.send(f"✅ Данные пользователя {member.mention} удалены из базы.")

# ========== СТАРТ ==========
if __name__ == "__main__":
    bot.run(TOKEN)