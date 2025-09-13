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
#from dotenv import load_dotenv

#load_dotenv()

# ====== НАСТРОЙКА ======
TOKEN = os.environ.get('DISCORD_TOKEN')
GUILD_ID = int(os.environ.get('GUILD_ID'))
CHANNEL_REPORTS_ID = int(os.environ.get('CHANNEL_REPORTS_ID'))
CHANNEL_APPROVAL_ID = int(os.environ.get('CHANNEL_APPROVAL_ID'))
CHANNEL_DAILY_STATS_ID = int(os.environ.get('CHANNEL_DAILY_STATS_ID'))
CHANNEL_WEEKLY_STATS_ID = int(os.environ.get('CHANNEL_WEEKLY_STATS_ID'))
ROLE_TEST_ID = int(os.environ.get('ROLE_TEST_ID'))
ROLE_MAIN_ID = int(os.environ.get('ROLE_MAIN_ID'))
DAILY_STATS_MESSAGE_ID = int(os.environ.get('DAILY_STATS_MESSAGE_ID', 0))  # 0 означает, что сообщение будет создано
WEEKLY_STATS_MESSAGE_ID = int(os.environ.get('WEEKLY_STATS_MESSAGE_ID', 0))  # 0 означает, что сообщение будет создано
DEFAULT_THRESHOLD = int(os.environ.get('DEFAULT_THRESHOLD', 15))
INACTIVE_DAYS_THRESHOLD = int(os.environ.get('INACTIVE_DAYS_THRESHOLD', 3))
MAX_PDF_IMAGES = int(os.environ.get('MAX_PDF_IMAGES', 50))

# Настройка бота и БД
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix='!', intents=intents)

# Инициализация базы данных
def init_db():
    db = sqlite3.connect('/mnt/data/screenshots.db', check_same_thread=False)
    #db = sqlite3.connect('screenshots.db', check_same_thread=False)
    cursor = db.cursor()

    # Создаем таблицы
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        screenshots_total INTEGER DEFAULT 0,
        screenshots_daily INTEGER DEFAULT 0,
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
    CREATE TABLE IF NOT EXISTS screenshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        message_id INTEGER,
        url TEXT,
        date TEXT,
        FOREIGN KEY (user_id) REFERENCES users (user_id)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_stats (
        date TEXT PRIMARY KEY,
        message_id INTEGER
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS weekly_stats (
        week_start TEXT PRIMARY KEY,
        message_id INTEGER
    )
    ''')

    db.commit()
    return db, cursor

# Инициализация базы данных
db, cursor = init_db()

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

        if not target_user:
            return await interaction.response.send_message("Пользователь не найден.", ephemeral=True)

        if role_main:
            await target_user.add_roles(role_main)
        if role_test:
            await target_user.remove_roles(role_test)

        cursor.execute("UPDATE users SET approved = 1 WHERE user_id = ?", (self.target_user_id,))
        db.commit()

        try:
            await target_user.send("🎉 Поздравляем! Ваш перевод на **Мейн** был одобрен!")
        except discord.Forbidden:
            pass

        embed = interaction.message.embeds[0]
        embed.color = discord.Color.green()
        embed.set_footer(text=f"✅ Одобрено {interaction.user.display_name}")
        await interaction.message.edit(view=None, embed=embed)
        await interaction.response.send_message(f"Игрок {target_user.mention} успешно переведен!", ephemeral=True)

    @discord.ui.button(label="Отклонить", style=discord.ButtonStyle.danger, custom_id="approve_deny")
    async def deny(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = ReasonModal(self.target_user_id)
        await interaction.response.send_modal(modal)

class ReasonModal(discord.ui.Modal, title="Отклонение перевода"):
    def __init__(self, target_user_id):
        super().__init__()
        self.target_user_id = target_user_id

    required_screens = discord.ui.TextInput(
        label="Сколько доп. скринов нужно?",
        placeholder="Введите число",
        required=True
    )
    reason = discord.ui.TextInput(
        label="Причина",
        style=discord.TextStyle.paragraph,
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        target_user = guild.get_member(self.target_user_id)

        try:
            required = int(self.required_screens.value)
        except ValueError:
            return await interaction.response.send_message("Нужно ввести число!", ephemeral=True)

        # Полный сброс скринов при отклонении
        cursor.execute(
            "UPDATE users SET required_screens = ?, screenshots_total = 0, screenshots_daily = 0, screenshots_weekly = 0 WHERE user_id = ?",
            (required, self.target_user_id)
        )
        cursor.execute("DELETE FROM screenshots WHERE user_id = ?", (self.target_user_id,))
        db.commit()

        try:
            await target_user.send(
                f"❌ Ваш перевод на **Мейн** был отклонен.\n"
                f"**Причина:** {self.reason.value}\n"
                f"**Требуется дополнительно скринов:** {required}\n"
            )
        except discord.Forbidden:
            pass

        embed = interaction.message.embeds[0]
        embed.color = discord.Color.red()
        embed.set_footer(text=f"❌ Отклонено {interaction.user.display_name} | Нужно ещё {required} скринов")
        await interaction.message.edit(view=None, embed=embed)
        await interaction.response.send_message(
            f"Перевод отклонен. {target_user.mention} должен отправить ещё {required} скринов.",
            ephemeral=True
        )

        # Обновляем статистику после сброса
        await update_daily_stats()
        await update_weekly_stats()

# ========== УЛУЧШЕННАЯ PDF ГЕНЕРАЦИЯ ==========
async def download_image(session, url, retries=3):
    """Асинхронная загрузка изображения с повторными попытками"""
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    return await response.read()
                else:
                    print(f"❌ Ошибка загрузки {url}: статус {response.status}")
        except Exception as e:
            print(f"❌ Попытка {attempt + 1} ошибка загрузки {url}: {e}")
            await asyncio.sleep(1)
    return None

async def generate_pdf(user_id: int, screenshots: list[str]) -> str:
    """Создаёт PDF со скриншотами и возвращает путь к файлу"""
    pdf_path = f"screenshots_{user_id}_{int(time.time())}.pdf"
    c = canvas.Canvas(pdf_path, pagesize=landscape(A4))
    width, height = landscape(A4)
    
    print(f"🔧 Начинаем генерацию PDF для пользователя {user_id} с {len(screenshots)} скринами")
    
    successful_images = 0
    async with aiohttp.ClientSession() as session:
        for i, url in enumerate(screenshots[:MAX_PDF_IMAGES], start=1):
            try:
                print(f"📥 Загружаем изображение {i}/{len(screenshots)}")
                image_data = await download_image(session, url)
                
                if not image_data:
                    print(f"❌ Не удалось загрузить изображение {i}")
                    continue
                
                # Используем временный файл для обработки
                with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as temp_file:
                    temp_path = temp_file.name
                
                try:
                    # Открываем и обрабатываем изображение
                    img = Image.open(BytesIO(image_data))
                    
                    # Конвертируем в RGB если нужно
                    if img.mode in ("RGBA", "P"):
                        img = img.convert("RGB")
                    
                    # Сохраняем во временный файл
                    img.save(temp_path, "JPEG", quality=85)
                    
                    # Добавляем в PDF
                    c.drawImage(temp_path, 0, 0, width, height, preserveAspectRatio=True)
                    c.showPage()
                    
                    successful_images += 1
                    print(f"✅ Добавлено изображение {i} в PDF")
                    
                finally:
                    # Удаляем временный файл
                    try:
                        os.unlink(temp_path)
                    except:
                        pass
                        
            except Exception as e:
                print(f"❌ Ошибка обработки изображения {i}: {e}")
                continue
    
    if successful_images > 0:
        c.save()
        print(f"✅ PDF создан успешно с {successful_images} изображениями")
        return pdf_path
    else:
        print("❌ Не удалось добавить ни одного изображения в PDF")
        return None

# ========== СТАТИСТИКА ==========
async def update_daily_stats():
    """Обновляет дневную статистику"""
    channel = bot.get_channel(CHANNEL_DAILY_STATS_ID)
    if not channel:
        return
    
    today = date.today().isoformat()
    guild = bot.get_guild(GUILD_ID)
    role_test = guild.get_role(ROLE_TEST_ID)
    
    # Собираем всех пользователей с ролью тест
    users_data = []
    for member in guild.members:
        if role_test in member.roles:
            cursor.execute(
                "SELECT screenshots_daily FROM users WHERE user_id = ?",
                (member.id,)
            )
            row = cursor.fetchone()
            if row:
                screenshots_daily = row[0]
            else:
                screenshots_daily = 0
            users_data.append((member, screenshots_daily))
    
    # Сортируем по количеству скринов
    users_data.sort(key=lambda x: x[1], reverse=True)
    
    embed = discord.Embed(
        title=f"📊 Статистика за сегодня ({today})",
        color=discord.Color.blue()
    )
    
    if users_data:
        stats_text = ""
        for member, screens_daily in users_data:
            stats_text += f"**{member.mention}**: {screens_daily} скринов\n"
        embed.description = stats_text
    else:
        embed.description = "Сегодня ещё никто не отправлял скрины"
    
    # Проверяем, есть ли уже сообщение за сегодня
    cursor.execute("SELECT message_id FROM daily_stats WHERE date = ?", (today,))
    row = cursor.fetchone()
    
    if row and row[0]:
        try:
            message = await channel.fetch_message(row[0])
            await message.edit(embed=embed)
        except:
            # Если сообщение не найдено, создаем новое
            message = await channel.send(embed=embed)
            cursor.execute("UPDATE daily_stats SET message_id = ? WHERE date = ?", (message.id, today))
    else:
        message = await channel.send(embed=embed)
        cursor.execute("INSERT OR REPLACE INTO daily_stats (date, message_id) VALUES (?, ?)", (today, message.id))
    
    db.commit()


async def update_weekly_stats():
    """Обновляет недельную статистику"""
    channel = bot.get_channel(CHANNEL_WEEKLY_STATS_ID)
    if not channel:
        return
    
    # Определяем начало недели (понедельник)
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    week_start_str = week_start.isoformat()
    
    # Получаем всех пользователей с ролью тест
    guild = bot.get_guild(GUILD_ID)
    role_test = guild.get_role(ROLE_TEST_ID)
    
    test_users = []
    for member in guild.members:
        if role_test in member.roles:
            # Получаем данные из БД
            cursor.execute(
                "SELECT screenshots_weekly, join_date, discord_join_date, days_in_faction FROM users WHERE user_id = ?",
                (member.id,)
            )
            row = cursor.fetchone()
            
            if row:
                screens_weekly, join_date, discord_join_date, days_in_faction = row
                # Если дата вступления в Discord не сохранена, сохраняем ее
                if not discord_join_date:
                    discord_join_date = member.joined_at.date().isoformat() if member.joined_at else date.today().isoformat()
                    cursor.execute(
                        "UPDATE users SET discord_join_date = ? WHERE user_id = ?",
                        (discord_join_date, member.id)
                    )
            else:
                # Если пользователя нет в БД, добавляем
                join_date = date.today().isoformat()
                discord_join_date = member.joined_at.date().isoformat() if member.joined_at else date.today().isoformat()
                days_in_faction = 0
                screens_weekly = 0
                cursor.execute(
                    "INSERT INTO users (user_id, username, join_date, discord_join_date, days_in_faction, screenshots_weekly) VALUES (?, ?, ?, ?, ?, ?)",
                    (member.id, member.name, join_date, discord_join_date, days_in_faction, screens_weekly)
                )
            
            # Рассчитываем количество дней в Discord
            if discord_join_date:
                join_date_obj = datetime.strptime(discord_join_date, '%Y-%m-%d').date()
                days_in_discord = (date.today() - join_date_obj).days
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
    
    # Сортируем по количеству скринов
    test_users.sort(key=lambda x: x['screens_weekly'], reverse=True)
    
    # Создаем embed
    embed = discord.Embed(
        title=f"📈 Недельная статистика (неделя с {week_start_str})",
        color=discord.Color.gold()
    )
    
    # Разделяем на зоны
    green_zone = []
    yellow_zone = []
    red_zone = []
    
    for user in test_users:
        if user['screens_weekly'] >= 10:
            green_zone.append(user)
        elif user['screens_weekly'] >= 5:
            yellow_zone.append(user)
        else:
            red_zone.append(user)
    
    # Добавляем поля в embed
    if green_zone:
        green_text = "\n".join([f"✅ <@{u['id']}>: {u['screens_weekly']} скринов (дней в Discord: {u['days_in_discord']})" for u in green_zone])
        embed.add_field(name="🟢 Активные", value=green_text, inline=False)
    
    if yellow_zone:
        yellow_text = "\n".join([f"⚠️ <@{u['id']}>: {u['screens_weekly']} скринов (дней в Discord: {u['days_in_discord']})" for u in yellow_zone])
        embed.add_field(name="🟡 Средний актив", value=yellow_text, inline=False)
    
    if red_zone:
        red_text = "\n".join([f"❌ <@{u['id']}>: {u['screens_weekly']} скринов (дней в Discord: {u['days_in_discord']})" for u in red_zone])
        embed.add_field(name="🔴 Маленький актив", value=red_text, inline=False)
    
    # Проверяем, есть ли уже сообщение за эту неделю
    cursor.execute("SELECT message_id FROM weekly_stats WHERE week_start = ?", (week_start_str,))
    row = cursor.fetchone()
    
    if row and row[0]:
        try:
            message = await channel.fetch_message(row[0])
            await message.edit(embed=embed)
        except:
            # Если сообщение не найдено, создаем новое
            message = await channel.send(embed=embed)
            cursor.execute("UPDATE weekly_stats SET message_id = ? WHERE week_start = ?", (message.id, week_start_str))
    else:
        message = await channel.send(embed=embed)
        cursor.execute("INSERT OR REPLACE INTO weekly_stats (week_start, message_id) VALUES (?, ?)", (week_start_str, message.id))
    
    db.commit()

# ========== НАПОМИНАНИЯ О НЕАКТИВНОСТИ ==========
async def check_inactive_users():
    """Проверяет неактивных пользователей и отправляет напоминания"""
    guild = bot.get_guild(GUILD_ID)
    role_test = guild.get_role(ROLE_TEST_ID)
    today = date.today()
    
    for member in guild.members:
        if role_test in member.roles:
            cursor.execute(
                "SELECT last_screenshot_date, last_reminder_date FROM users WHERE user_id = ?",
                (member.id,)
            )
            row = cursor.fetchone()
            
            if row:
                last_screenshot_date_str, last_reminder_date_str = row
                
                if last_screenshot_date_str:
                    last_screenshot_date = datetime.strptime(last_screenshot_date_str, '%Y-%m-%d').date()
                    days_inactive = (today - last_screenshot_date).days
                    
                    # Проверяем, нужно ли отправлять напоминание
                    should_send_reminder = False
                    if last_reminder_date_str:
                        last_reminder_date = datetime.strptime(last_reminder_date_str, '%Y-%m-%d').date()
                        if (today - last_reminder_date).days >= 1:  # Не чаще 1 раза в день
                            should_send_reminder = True
                    else:
                        should_send_reminder = True
                    
                    if days_inactive >= INACTIVE_DAYS_THRESHOLD and should_send_reminder:
                        try:
                            await member.send(
                                f"⚠️ **Напоминание**\n"
                                f"Вы не отправляли скриншоты уже {days_inactive} или больше дней.\n"
                                f"Пожалуйста, отправьте скриншоты в канал для отчетов, чтобы избежать исключения."
                            )
                            # Обновляем дату последнего напоминания
                            cursor.execute(
                                "UPDATE users SET last_reminder_date = ? WHERE user_id = ?",
                                (today.isoformat(), member.id)
                            )
                            db.commit()
                        except discord.Forbidden:
                            print(f"Не удалось отправить напоминание пользователю {member.name} (закрытые ЛС)")
                        except Exception as e:
                            print(f"Ошибка при отправке напоминания: {e}")

# ========== СОБЫТИЯ ==========
@bot.event
async def on_ready():
    print(f'Бот {bot.user} запущен!')
    # Инициализируем даты вступления в Discord для всех пользователей
    await initialize_discord_join_dates()
    daily_tasks.start()
    inactive_check.start()
    await update_daily_stats()
    await update_weekly_stats()

async def initialize_discord_join_dates():
    """Инициализирует даты вступления в Discord для всех пользователей"""
    guild = bot.get_guild(GUILD_ID)
    for member in guild.members:
        if member.joined_at:
            cursor.execute(
                "SELECT discord_join_date FROM users WHERE user_id = ?",
                (member.id,)
            )
            row = cursor.fetchone()
            if not row or not row[0]:
                discord_join_date = member.joined_at.date().isoformat()
                cursor.execute(
                    "INSERT OR REPLACE INTO users (user_id, username, discord_join_date) VALUES (?, ?, ?)",
                    (member.id, member.name, discord_join_date)
                )
    db.commit()

@bot.event
async def on_member_update(before, after):
    role_test = after.guild.get_role(ROLE_TEST_ID)
    
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
            cursor.execute("DELETE FROM screenshots WHERE user_id = ?", (after.id,))
            db.commit()
            await update_weekly_stats()

@bot.event
async def on_message(message):
    if message.author.bot or message.channel.id != CHANNEL_REPORTS_ID:
        return

    role_test = message.guild.get_role(ROLE_TEST_ID)
    if role_test not in message.author.roles:
        return

    if not message.attachments:
        return

    today = date.today().isoformat()
    user_id = message.author.id
    username = message.author.name

    # Обновляем имя пользователя и дату вступления в Discord
    discord_join_date = message.author.joined_at.date().isoformat() if message.author.joined_at else date.today().isoformat()
    cursor.execute(
        "UPDATE users SET username = ?, discord_join_date = ? WHERE user_id = ?",
        (username, discord_join_date, user_id)
    )
    
    # Добавляем скриншоты
    screenshot_count = 0
    for attachment in message.attachments:
        if attachment.content_type and attachment.content_type.startswith('image/'):
            cursor.execute(
                "INSERT INTO screenshots (user_id, message_id, url, date) VALUES (?, ?, ?, ?)",
                (user_id, message.id, attachment.url, today)
            )
            screenshot_count += 1

    if screenshot_count == 0:
        return

    # Обновляем счетчики
    cursor.execute('''
        INSERT INTO users (user_id, username, screenshots_total, screenshots_daily, screenshots_weekly, last_screenshot_date)
        VALUES (?, ?, 1, 1, 1, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username = excluded.username,
            screenshots_total = screenshots_total + ?,
            screenshots_daily = screenshots_daily + ?,
            screenshots_weekly = screenshots_weekly + ?,
            last_screenshot_date = excluded.last_screenshot_date
    ''', (user_id, username, today, screenshot_count, screenshot_count, screenshot_count))
    db.commit()

    # Отправляем подтверждение
    total_screens = cursor.execute('SELECT screenshots_total FROM users WHERE user_id = ?', (user_id,)).fetchone()[0]
    try:
        await message.reply(f"📸 {message.author.mention}, скриншоты приняты! Всего скринов: {total_screens}", delete_after=10)
    except:
        pass

    # Обновляем статистику
    await update_daily_stats()
    await update_weekly_stats()

    # Проверяем, нужно ли отправить на подтверждение
    cursor.execute("SELECT screenshots_total, required_screens FROM users WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    if row:
        total, required = row
        if (required and total >= required) or (not required and total >= DEFAULT_THRESHOLD):
            cursor.execute("SELECT url FROM screenshots WHERE user_id = ? ORDER BY date DESC, id DESC", (user_id,))
            urls = [row[0] for row in cursor.fetchall()]
            # Запускаем генерацию PDF в фоновой задаче
            asyncio.create_task(process_approval_request(message.author, total, user_id, urls))

async def process_approval_request(user, total_screens, user_id, urls):
    """Обрабатывает запрос на подтверждение в фоновом режиме"""
    try:
        pdf_path = await generate_pdf(user_id, urls)
        if pdf_path:
            await send_approval_request(user, total_screens, pdf_path)
        else:
            await send_approval_request_without_pdf(user, total_screens)
    except Exception as e:
        print(f"❌ Ошибка при обработке запроса подтверждения: {e}")
        await send_approval_request_without_pdf(user, total_screens)

async def send_approval_request_without_pdf(user, total_screens):
    """Отправляет заявку без PDF файла"""
    channel_approval = bot.get_channel(CHANNEL_APPROVAL_ID)
    embed = discord.Embed(
        title="🎯 Запрос на перевод игрока (БЕЗ PDF)",
        description=f"Игрок {user.mention} (`{user}`) отправил {total_screens} скриншотов.\n\n⚠️ **Не удалось сгенерировать PDF файл!**",
        color=discord.Color.orange()
    )
    embed.set_thumbnail(url=user.avatar.url)
    view = ApprovalButtons(user.id)
    await channel_approval.send(embed=embed, view=view)

async def send_approval_request(user, total_screens, pdf_path):
    """Отправляет заявку с PDF файлом"""
    channel_approval = bot.get_channel(CHANNEL_APPROVAL_ID)
    embed = discord.Embed(
        title="🎯 Запрос на перевод игрока",
        description=f"Игрок {user.mention} (`{user}`) отправил {total_screens} скриншотов.",
        color=discord.Color.orange()
    )
    embed.set_thumbnail(url=user.avatar.url)
    view = ApprovalButtons(user.id)
    
    # Проверяем размер файла
    file_size = os.path.getsize(pdf_path) / (1024 * 1024)
    if file_size > 25:
        await channel_approval.send(embed=embed, view=view)
    else:
        await channel_approval.send(embed=embed, view=view, file=discord.File(pdf_path))
    
    # Удаляем временный PDF файл
    try:
        os.remove(pdf_path)
    except:
        pass

# ========== ТАСКИ ==========
@tasks.loop(hours=24)
async def daily_tasks():
    """Ежедневные задачи"""
    cursor.execute("UPDATE users SET screenshots_daily = 0")
    
    guild = bot.get_guild(GUILD_ID)
    role_test = guild.get_role(ROLE_TEST_ID)
    
    for member in guild.members:
        if role_test in member.roles:
            cursor.execute(
                "UPDATE users SET days_in_faction = days_in_faction + 1 WHERE user_id = ?",
                (member.id,)
            )
    
    db.commit()
    await update_daily_stats()
    await update_weekly_stats()

@tasks.loop(hours=12)
async def inactive_check():
    await check_inactive_users()

@daily_tasks.before_loop
async def before_daily_tasks():
    await bot.wait_until_ready()
    now = datetime.now()
    if now.hour > 0 or now.minute > 0:
        next_run = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        await discord.utils.sleep_until(next_run)

# ========== КОМАНДЫ ==========
@bot.command()
@commands.has_permissions(administrator=True)
async def stats(ctx):
    await update_daily_stats()
    await update_weekly_stats()
    await ctx.send("✅ Статистика обновлена!")

@bot.command()
@commands.has_permissions(administrator=True)
async def fix_dates(ctx):
    """Исправляет даты вступления в Discord для всех пользователей"""
    await initialize_discord_join_dates()
    await ctx.send("✅ Даты вступления в Discord обновлены!")

# ========== СТАРТ ==========
if __name__ == "__main__":
    bot.run(TOKEN)