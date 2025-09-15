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

# ====== НАСТРОЙКА ======
TOKEN = os.environ.get('DISCORD_TOKEN')
GUILD_ID = int(os.environ.get('GUILD_ID'))
CHANNEL_REPORTS_ID = int(os.environ.get('CHANNEL_REPORTS_ID'))
CHANNEL_APPROVAL_ID = int(os.environ.get('CHANNEL_APPROVAL_ID'))
CHANNEL_DAILY_STATS_ID = int(os.environ.get('CHANNEL_DAILY_STATS_ID'))
CHANNEL_WEEKLY_STATS_ID = int(os.environ.get('CHANNEL_WEEKLY_STATS_ID'))
ROLE_TEST_ID = int(os.environ.get('ROLE_TEST_ID'))
ROLE_MAIN_ID = int(os.environ.get('ROLE_MAIN_ID'))
DAILY_STATS_MESSAGE_ID = int(os.environ.get('DAILY_STATS_MESSAGE_ID', 0))
WEEKLY_STATS_MESSAGE_ID = int(os.environ.get('WEEKLY_STATS_MESSAGE_ID', 0))
DEFAULT_THRESHOLD = int(os.environ.get('DEFAULT_THRESHOLD', 15))
INACTIVE_DAYS_THRESHOLD = int(os.environ.get('INACTIVE_DAYS_THRESHOLD', 3))
MAX_PDF_IMAGES = int(os.environ.get('MAX_PDF_IMAGES', 50))

# Настройка бота и БД
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

# Убираем префикс команд и используем проверку сообщений
bot = commands.Bot(command_prefix='!', intents=intents)

# Инициализация базы данных
def init_db():
    db = sqlite3.connect('/mnt/data/screenshots.db', check_same_thread=False)
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

# ========== УЛУЧШЕННАЯ PDF ГЕНЕРАЦИА ==========
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
    """Обновляет дневную статистику и удаляет старые сообщения из канала"""
    channel = bot.get_channel(CHANNEL_DAILY_STATS_ID)
    if not channel:
        return
    
    today = date.today().isoformat()
    guild = bot.get_guild(GUILD_ID)
    role_test = guild.get_role(ROLE_TEST_ID)
    
    # Собираем данные по пользователям
    users_data = []
    for member in guild.members:
        if role_test in member.roles:
            cursor.execute(
                "SELECT screenshots_daily FROM users WHERE user_id = ?",
                (member.id,)
            )
            row = cursor.fetchone()
            screenshots_daily = row[0] if row else 0
            users_data.append((member, screenshots_daily))
    
    users_data.sort(key=lambda x: x[1], reverse=True)
    
    embed = discord.Embed(
        title=f"📊 Статистика за сегодня ({today})",
        color=discord.Color.blue()
    )
    
    embed.description = (
        "\n".join(f"**{member.mention}**: {screens_daily} скринов" for member, screens_daily in users_data)
        if users_data else "Сегодня ещё никто не отправлял скрины"
    )
    
    # === Находим и удаляем старые сообщения, но не трогаем базу ===
    cursor.execute("SELECT date, message_id FROM daily_stats ORDER BY date DESC")
    all_rows = cursor.fetchall()
    
    if len(all_rows) > 5:  # Если больше 5 дней есть сообщения
        for old_date, old_message_id in all_rows[5:]:  # Все, кроме последних 5
            try:
                old_msg = await channel.fetch_message(old_message_id)
                await old_msg.delete()
            except discord.NotFound:
                pass  # Сообщение уже удалено вручную — игнорируем
            except Exception as e:
                print(f"⚠️ Не удалось удалить старое сообщение {old_message_id}: {e}")
    
    # === Обновляем или создаем сообщение за сегодня ===
    cursor.execute("SELECT message_id FROM daily_stats WHERE date = ?", (today,))
    row = cursor.fetchone()
    
    if row and row[0]:
        try:
            message = await channel.fetch_message(row[0])
            await message.edit(embed=embed)
        except discord.NotFound:
            message = await channel.send(embed=embed)
            cursor.execute("UPDATE daily_stats SET message_id = ? WHERE date = ?", (message.id, today))
    else:
        message = await channel.send(embed=embed)
        cursor.execute("INSERT OR REPLACE INTO daily_stats (date, message_id) VALUES (?, ?)", (today, message.id))
    
    db.commit()

async def update_weekly_stats():
    """Обновляет недельную статистику с безопасным разбиением текста по лимиту Discord"""
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
    test_users.sort(key=lambda x: x['screens_weekly'], reverse=True)
    
    embed = discord.Embed(
        title=f"📈 Недельная статистика (неделя с {week_start_str})",
        color=discord.Color.gold()
    )
    
    def chunk_text(text: str, limit: int = 1024) -> list[str]:
        """Разбивает текст на куски по limit символов, стараясь резать по переносам"""
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
    
    # === Удаляем старые сообщения из канала, но не трогаем базу ===
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

# ========== НАПОМИНАНИЯ О НЕАКТИВНОСТИ ==========
async def check_inactive_users():
    """Проверяет неактивных пользователей и отправляет напоминания, включая тех, кто не отправлял скрины"""
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
            # Есть хотя бы один скрин — проверяем обычную неактивность
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
            # Нет ни одного скрина — отправляем твоё сообщение
            should_send_reminder = True
            custom_message = (
                f"⚠️ **Напоминание**\n"
                f"Вы ещё не отправили ни одного скриншота на повышение в канал <#{CHANNEL_REPORTS_ID}>.\n"
                f"Пожалуйста, не забывайте о повышении, чтобы избежать проблем!"
            )
        
        # Проверка интервала между напоминаниями (раз в N дней)
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
                print(f"📩 Напоминание отправлено {member.name}")
            except discord.Forbidden:
                print(f"⚠️ Не удалось отправить напоминание {member.name} (закрытые ЛС)")
            except Exception as e:
                print(f"❌ Ошибка при отправке напоминания {member.name}: {e}")

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
    # Игнорируем сообщения от ботов и личные сообщения
    if message.author.bot or isinstance(message.channel, discord.DMChannel):
        return
    
    # Обработка команд через ключевые слова
    if message.content.lower() in ["!статистика", "!stats"] and message.channel.permissions_for(message.author).administrator:
        await update_daily_stats()
        await update_weekly_stats()
        await message.channel.send("✅ Статистика обновлена!", delete_after=10)
        return
    
    if message.content.lower() in ["!totals", "!статистика_всех"]:
        await handle_totals_command(message)
        return
    
    if message.content.lower() in ["!fix_dates", "!исправить_даты"] and message.channel.permissions_for(message.author).administrator:
        await initialize_discord_join_dates()
        await message.channel.send("✅ Даты вступления в Discord обновлены!", delete_after=10)
        return
    
    # Обработка скриншотов (только в канале отчетов)
    if message.channel.id == CHANNEL_REPORTS_ID:
        await handle_screenshots(message)

async def handle_screenshots(message):
    """Обрабатывает отправку скриншотов"""
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
        "INSERT OR IGNORE INTO users (user_id, username, discord_join_date) VALUES (?, ?, ?)",
        (user_id, username, discord_join_date)
    )
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
        description=f"Игрок {user.mention} ({user}) отправил {total_screens} скриншотов.\n\n⚠️ **Не удалось сгенерировать PDF файл!**",
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
        description=f"Игрок {user.mention} ({user}) отправил {total_screens} скриншотов.",
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
    print(f"⏰ daily_tasks запущен: {datetime.now().isoformat()}")
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
    
    # === СБРОС НЕДЕЛЬНОЙ СТАТИСТИКИ ПО ПОНЕДЕЛЬНИКАМ ===
    today = date.today()
    if today.weekday() == 0:  # Понедельник
        cursor.execute("UPDATE users SET screenshots_weekly = 0")
    
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
    # всегда считаем следующее обновление на ближайшую полночь
    next_run = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if next_run <= now:
        next_run += timedelta(days=1)
    await discord.utils.sleep_until(next_run)

# ========== ОБРАБОТКА КОМАНД ==========
async def handle_totals_command(message):
    """Обрабатывает команду totals"""
    # Проверяем, упоминается ли конкретный пользователь
    if message.mentions:
        member = message.mentions[0]
        cursor.execute(
            "SELECT screenshots_total, discord_join_date FROM users WHERE user_id = ?",
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
            title=f"📊 Статистика {member.display_name}",
            description=f"**Скриншотов:** {total}\n**Дней в Discord:** {days_in_discord}",
            color=discord.Color.blue()
        )
        return await message.channel.send(embed=embed, delete_after=30)
    
    # Показываем статистику всех пользователей
    cursor.execute(
        "SELECT user_id, username, screenshots_total, discord_join_date FROM users ORDER BY screenshots_total DESC"
    )
    rows = cursor.fetchall()
    
    if not rows:
        return await message.channel.send("Нет данных о пользователях.", delete_after=10)
    
    lines = []
    for user_id, username, total, discord_join_date in rows:
        try:
            days_in_discord = (date.today() - datetime.strptime(discord_join_date, "%Y-%m-%d").date()).days if discord_join_date else 0
        except ValueError:
            days_in_discord = 0
        lines.append(f"**{username}** — {total} скринов ({days_in_discord} дней в Discord)")
    
    # Разбиваем на страницы
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
        title=f"📊 Общая статистика всех пользователей (стр. 1/{len(pages)})",
        description=pages[0],
        color=discord.Color.blue()
    )
    await message.channel.send(embed=embed, view=view, delete_after=120)

class TotalsPaginator(discord.ui.View):
    def __init__(self, pages):
        super().__init__(timeout=120)  # кнопки живут 2 минуты
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

# ========== СТАРТ ==========
if __name__ == "__main__":
    bot.run(TOKEN)