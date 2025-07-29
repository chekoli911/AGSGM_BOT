import os
import pandas as pd
import requests
from io import BytesIO
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

GITHUB_RAW_URL = 'https://github.com/chekoli911/AGSGM_BOT/raw/main/store-8370478-Vse_igri-202507290225_fixed.xlsx'

def load_excel_from_github(url):
    response = requests.get(url)
    response.raise_for_status()
    file_data = BytesIO(response.content)
    df = pd.read_excel(file_data, usecols=['Title', 'Url'])
    return df

df = load_excel_from_github(GITHUB_RAW_URL)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Напиши название игры или её часть, и я пришлю ссылку на сайт с этой игрой."
    )

async def search_game(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text.lower().strip()
    results = df[df['Title'].str.lower().str.contains(query, na=False)]

    if results.empty:
        await update.message.reply_text("Игра не найдена, попробуй другое название как в PS Store.")
    else:
        response_lines = []
        for _, row in results.head(25).iterrows():
            response_lines.append(f"{row['Title']}\n{row['Url']}")
        response = '\n\n'.join(response_lines)
        await update.message.reply_text(response)

if __name__ == '__main__':
    TOKEN = os.getenv('BOT_TOKEN')

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler('start', start))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), search_game))

    print("Бот запущен...")
    app.run_polling()
