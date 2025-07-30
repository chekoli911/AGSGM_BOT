import os
import logging
import random
import re
import tempfile
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes,
    MessageHandler, filters, ConversationHandler
)
import firebase_admin
from firebase_admin import credentials, db

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("telegram.client").setLevel(logging.WARNING)
logging.getLogger("telegram.vendor.ptb_urllib3").setLevel(logging.WARNING)
logging.getLogger("telegram.ext.updater").setLevel(logging.WARNING)

GITHUB_RAW_URL = 'https://github.com/chekoli911/AGSGM_BOT/raw/main/store-8370478-Vse_igri-202507290225_fixed.xlsx'

df = pd.read_excel(BytesIO(requests.get(GITHUB_RAW_URL).content), usecols=['Title', 'Url'])

advice_texts = [
    "Вот отличный вариант для твоего досуга:",
    "Попробуй сыграть в эту игру — она классная!",
    "Эта игра точно не разочарует тебя:",
    "Если хочешь чего-то нового — вот игра для тебя!",
    "Настоятельно рекомендую эту игру:",
    "Идеальная игра для расслабления:",
    "Не проходи мимо, посмотри на эту игру:",
    "Для разнообразия рекомендую эту игру:",
    "Вот игра, которая может тебя заинтересовать:",
    "Отличный выбор для сегодняшнего вечера:",
    "Эта игра поднимет настроение:",
    "Если нужен совет, то вот он:",
    "Отличный способ провести время — эта игра:",
    "Обрати внимание на эту игру:",
    "Попробуй эту игру, она заслуживает внимания:",
    "Для настроения — рекомендую эту игру:",
    "Не пропусти эту классную игру:",
    "Эта игра стоит твоего внимания:"
]

advice_triggers = [
    "совет",
    "во что поиграть",
    "?",
    "??",
    "порекомендуй",
    "рекомендация",
    "дай совет",
    "что поиграть",
    "посоветуй",
    "игра на сегодня",
]

ASKING_IF_WANT_NEW = 1
user_last_game = {}

def normalize_text(text):
    text = text.lower().strip()
    text = re.sub(r'[?]+', '', text)  # удаляем все знаки вопроса
    return text

def pick_random_game():
    row = df.sample(n=1).iloc[0]
    return row['Title'], row['Url']

# --- Firebase инициализация ---
firebase_json_str = os.getenv('FIREBASE_CREDENTIALS_JSON')
if not firebase_json_str:
    raise RuntimeError("Переменная окружения FIREBASE_CREDENTIALS_JSON не установлена")

with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.json') as temp_file:
    temp_file.write(firebase_json_str)
    temp_filename = temp_file.name

cred = credentials.Certificate(temp_filename)
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://agsgm-search-default-rtdb.firebaseio.com/'  # замени на свой URL Realtime DB
})

def log_user_visit(user_id):
    ref = db.reference(f'users/{user_id}')
    now_iso = datetime.utcnow().isoformat()
    user_data = ref.get()
    if not user_data:
        ref.set({'first_visit': now_iso, 'last_visit': now_iso, 'visits_count': 1})
    else:
        visits = user_data.get('visits_count', 1) + 1
        ref.update({'last_visit': now_iso, 'visits_count': visits})

async def notify_admin(app, text: str):
    admin_chat_id = -1002773793511  # chat_id твоего канала
    await app.bot.send_message(chat_id=admin_chat_id, text=text)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    log_user_visit(user_id)

    logging.info(f"Команда /start от пользователя {user_id}")
    await update.message.reply_text(
        "Привет! Напиши название игры или её часть, и я пришлю ссылку на сайт с этой игрой."
    )

async def greet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info(f"Пользователь {update.effective_user.id} поздоровался")
    await update.message.reply_text(
        "Здравствуйте! Я бот для поиска игр на PlayStation. "
        "Напишите название игры для поиска или напишите 'совет', '?' или 'во что поиграть', "
        "чтобы получить случайную рекомендацию."
    )

async def send_advice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "no_username"
    log_user_visit(user_id)

    logging.info(f"Пользователь {user_id} (@{username}) запросил совет")

    advice = random.choice(advice_texts)
    title, url = pick_random_game()

    user_last_game[user_id] = (title, url)

    message = (
        f"{advice}\n{title}\n{url}\n\n"
        "Если подходит этот вариант, то напиши в ответе мне \"Спасибо\" — мне будет приятно)\n"
        "Если хочешь другой вариант, просто скажи 'Уже играл', 'Уже прошел' или 'Неинтересно'."
    )
    await update.message.reply_text(message)

    return ASKING_IF_WANT_NEW

async def handle_response(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.lower().strip()
    log_user_visit(user_id)

    if text in ['да', 'конечно', 'давай'] or text in ['уже играл', 'уже прошел', 'неинтересно']:
        title, url = pick_random_game()
        user_last_game[user_id] = (title, url)
        advice = random.choice(advice_texts)
        message = (
            f"{advice}\n{title}\n{url}\n\n"
            "Если подходит этот вариант, то напиши в ответе мне \"Спасибо\" — мне будет приятно)\n"
            "Если хочешь другой вариант, скажи 'Уже играл', 'Уже прошел' или 'Неинтересно'."
        )
        await update.message.reply_text(message)
        return ASKING_IF_WANT_NEW

    if 'спасибо' in text:
        title, url = user_last_game.get(user_id, ("неизвестная игра", ""))
        message = (
            f"Рад помочь! Если хочешь оформить игру без комиссии, переходи по ссылке:\n{url}\n"
            "Выдача игр после оплаты занимает примерно 15 минут."
        )
        await update.message.reply_text(message)
        return ConversationHandler.END

    if text == 'нет':
        await update.message.reply_text("Отлично. Спасибо, что написал. Я буду здесь, если понадоблюсь.")
        return ConversationHandler.END

    await update.message.reply_text(
        "Не понял тебя. Напиши 'Да', если хочешь другой вариант, или 'Нет', если не нужно."
    )
    return ASKING_IF_WANT_NEW

async def search_game(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "no_username"
    raw_text = update.message.text
    text = normalize_text(raw_text)
    log_user_visit(user_id)
    logging.info(f"Получено сообщение: {raw_text} от пользователя {user_id} (@{username})")

    await notify_admin(context.application, f"Пользователь {user_id} (@{username}) написал запрос: {raw_text}")

    if text == "привет":
        await greet(update, context)
        return ConversationHandler.END

    if text in advice_triggers:
        return await send_advice(update, context)

    logging.info(f"[Поиск] Пользователь {user_id} (@{username}) ищет: {text}")

    results = df[df['Title'].str.lower().str.contains(text, na=False)]

    if results.empty:
        await update.message.reply_text("Игра не найдена, попробуй другое название.")
        return ConversationHandler.END

    for _, row in results.head(25).iterrows():
        message = f"{row['Title']}\n{row['Url']}"
        await update.message.reply_text(message)

    return ConversationHandler.END

if __name__ == '__main__':
    TOKEN = os.getenv('BOT_TOKEN')

    from telegram.ext import ConversationHandler

    app = ApplicationBuilder().token(TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.TEXT & (~filters.COMMAND), search_game)],
        states={
            ASKING_IF_WANT_NEW: [MessageHandler(filters.TEXT & (~filters.COMMAND), handle_response)],
        },
        fallbacks=[]
    )

    app.add_handler(CommandHandler('start', start))
    app.add_handler(conv_handler)

    logging.info("Бот запущен...")
    app.run_polling()
