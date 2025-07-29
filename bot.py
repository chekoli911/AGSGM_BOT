import os
import tempfile
import pandas as pd
import requests
from io import BytesIO
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

import firebase_admin
from firebase_admin import credentials, db

GITHUB_RAW_URL = 'https://github.com/chekoli911/AGSGM_BOT/raw/main/store-8370478-Vse_igri-202507290225_fixed.xlsx'

firebase_json_str = os.getenv('FIREBASE_CREDENTIALS_JSON')
if not firebase_json_str:
    raise RuntimeError("Переменная окружения FIREBASE_CREDENTIALS_JSON не установлена")

with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.json') as temp_file:
    temp_file.write(firebase_json_str)
    temp_filename = temp_file.name

firebase_cred = credentials.Certificate(temp_filename)
firebase_admin.initialize_app(firebase_cred, {
    'databaseURL': 'https://agsgm-search-default-rtdb.firebaseio.com/'  # Замените на свой URL Realtime DB
})

df = pd.read_excel(BytesIO(requests.get(GITHUB_RAW_URL).content), usecols=['Title', 'Url'])

def log_search(user_id, username, query):
    ref = db.reference('search_logs')
    ref.push({
        'user_id': user_id,
        'username': username,
        'query': query,
        'timestamp': db.ServerValue.TIMESTAMP
    })

    users_ref = db.reference('unique_users')
    users_ref.update({str(user_id): True})

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Напиши название игры или её часть, и я пришлю ссылку на сайт с этой игрой."
    )

async def search_game(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text.lower().strip()
    user_id = update.effective_user.id
    username = update.effective_user.username

    log_search(user_id, username, query)

    results = df[df['Title'].str.lower().str.contains(query, na=False)]

    if results.empty:
        await update.message.reply_text("Игра не найдена, попробуй другое название.")
    else:
        response_lines = [
            f"{row['Title']}\n{row['Url']}" for _, row in results.head(25).iterrows()
        ]
        response = '\n\n'.join(response_lines)
        await update.message.reply_text(response)

if __name__ == '__main__':
    TOKEN = os.getenv('BOT_TOKEN')
    if not TOKEN:
        raise RuntimeError("Переменная окружения BOT_TOKEN не установлена")

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler('start', start))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), search_game))

    print("Бот запущен...")
    app.run_polling()
