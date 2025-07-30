import os
import logging
import pandas as pd
import requests
from io import BytesIO
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

GITHUB_RAW_URL = 'https://github.com/chekoli911/AGSGM_BOT/raw/main/store-8370478-Vse_igri-202507290225_fixed.xlsx'

df = pd.read_excel(BytesIO(requests.get(GITHUB_RAW_URL).content), usecols=['Title', 'Url'])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info(f"Команда /start от пользователя {update.effective_user.id}")
    await update.message.reply_text(
        "Привет! Напиши название игры или её часть, и я пришлю ссылку на сайт с этой игрой."
    )

async def search_game(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "no_username"
    logging.info(f"Получено сообщение: {update.message.text} от пользователя {user_id} (@{username})")

    query = update.message.text.lower().strip()
    logging.info(f"[Поиск] Пользователь {user_id} (@{username}) ищет: {query}")

    results = df[df['Title'].str.lower().str.contains(query, na=False)]

    if results.empty:
        await update.message.reply_text("Игра не найдена, попробуй другое название.")
    else:
        response_lines = [f"{row['Title']}\n{row['Url']}" for _, row in results.head(25).iterrows()]
        response = '\n\n'.join(response_lines)
        await update.message.reply_text(response)

if __name__ == '__main__':
    TOKEN = os.getenv('BOT_TOKEN')

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler('start', start))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), search_game))

    logging.info("Бот запущен...")
    app.run_polling()
