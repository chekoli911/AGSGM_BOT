import os
import json
import tempfile
import pandas as pd
import requests
from io import BytesIO
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

import firebase_admin
from firebase_admin import credentials, db

GITHUB_RAW_URL = 'https://github.com/chekoli911/AGSGM_BOT/raw/main/store-8370478-Vse_igri-202507290225_fixed.xlsx'

def load_excel_from_github(url):
    response = requests.get(url)
    response.raise_for_status()
    file_data = BytesIO(response.content)
    df = pd.read_excel(file_data, usecols=['Title', 'Url'])
    return df

df = load_excel_from_github(GITHUB_RAW_URL)

# Получаем JSON из переменной окружения
firebase_json_str = os.getenv('FIREBASE_CREDENTIALS_JSON')
if not firebase_json_str:
    raise RuntimeError("Переменная окружения FIREBASE_CREDENTIALS_JSON не установлена")

# Создаем временный файл с ключом
with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.json') as temp_file:
    temp_file.write(firebase_json_str)
    temp_filename = temp_file.name

# Инициализация Firebase
firebase_cred = credentials.Certificate(temp_filename)
firebase_admin.initialize_app(firebase_cred, {
    'databaseURL': 'https://agsgm-search-default-rtdb.firebaseio.com/'  # <- замени на свой URL Realtime DB
})

def log_search_to_firebase(user_id, username, query):
    ref = db.reference('search_logs')
    new_log_ref = ref.push()
    new_log_ref.set({
        'user_id': user_id,
        'username': username,
        'query': query,
        'timestamp': db.SERVER_TIMESTAMP
    })

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Напиши название игры или её часть, и я пришлю ссылку на сайт с этой игрой."
    )

async def search_game(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"Получено сообщение: {update.message.text} от пользователя {update.effective_user.id} (@{update.effective_user.username})")

    query = update.message.text.lower().strip()

    # Логируем запрос в Firebase
    log_search_to_firebase(
        user_id=update.effective_user.id,
        username=update.effective_user.username,
        query=query
    )

    # Логируем в консоль
    print(f"[Поиск] Пользователь {update.effective_user.id} (@{update.effective_user.username}) ищет: {query}")

    results = df[df['Title'].str.lower().str.contains(query, na=False)]

    if results.empty:
        await update.message.reply_text("Игра не найдена, попробуй другое название.")
    else:
        response_lines = []
        for _, row in results.head(25).iterrows():
            response_lines.append(f"{row['Title']}\n{row['Url']}")
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
