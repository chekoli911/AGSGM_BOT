import os
import logging
import random
import pandas as pd
import requests
from io import BytesIO
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes,
    MessageHandler, filters, ConversationHandler
)

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

ASKING_IF_WANT_NEW = 1
user_last_game = {}

def pick_random_game():
    row = df.sample(n=1).iloc[0]
    return row['Title'], row['Url']

async def notify_admin(app, text: str):
    admin_chat_id = -1002773793511  # твой chat_id канала
    await app.bot.send_message(chat_id=admin_chat_id, text=text)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info(f"Команда /start от пользователя {update.effective_user.id}")
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
    logging.info(f"Пользователь {user_id} (@{username}) запросил совет")

    advice = random.choice(advice_texts)
    title, url = pick_random_game()

    user_last_game[user_id] = (title, url)

    message = f"{advice}\n{title}\n{url}\n\nЕсли хочешь другой вариант, просто скажи 'Уже играл', 'Уже прошел' или 'Неинтересно'."
    await update.message.reply_text(message)

    return ASKING_IF_WANT_NEW

async def handle_response(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.lower().strip()

    # Добавлено условие для «уже играл» и подобных
    if text in ['да', 'конечно', 'давай'] or text in ['уже играл', 'уже прошел', 'неинтересно']:
        title, url = pick_random_game()
        user_last_game[user_id] = (title, url)
        advice = random.choice(advice_texts)
        message = f"{advice}\n{title}\n{url}\n\nЕсли хочешь другой вариант, скажи 'Уже играл', 'Уже прошел' или 'Неинтересно'."
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
    text = update.message.text.lower().strip()
    logging.info(f"Получено сообщение: {text} от пользователя {user_id} (@{username})")

    if text == "привет":
        await greet(update, context)
        return ConversationHandler.END

    if text in ["во что поиграть", "?", "совет"]:
        return await send_advice(update, context)

    logging.info(f"[Поиск] Пользователь {user_id} (@{username}) ищет: {text}")
    await notify_admin(context.application, f"Пользователь {user_id} (@{username}) сделал запрос: {text}")

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
