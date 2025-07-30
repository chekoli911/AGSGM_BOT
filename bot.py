import os
import logging
import pandas as pd
import requests
from io import BytesIO
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes,
    MessageHandler, filters, ConversationHandler
)
import firebase_admin
from firebase_admin import credentials, db
from datetime import datetime, timezone
import random
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

firebase_json = os.getenv('FIREBASE_CREDENTIALS_JSON')
if not firebase_json:
    raise RuntimeError("FIREBASE_CREDENTIALS_JSON env var is missing")

firebase_cred = credentials.Certificate(eval(firebase_json))
firebase_admin.initialize_app(firebase_cred, {
    'databaseURL': 'https://ag-searh-default-rtdb.firebaseio.com/'
})

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
    'совет', 'во что поиграть', '?', '??', 'порекомендуй', 'рекомендация',
    'дай совет', 'что поиграть', 'посоветуй', 'игра на сегодня'
]

ASKING_IF_WANT_NEW = 1

def add_game_mark(user_id: int, game_title: str, mark_type: str):
    ref = db.reference(f'users/{user_id}/{mark_type}')
    ref.update({game_title: True})

def check_game_mark(user_id: int, game_title: str, mark_type: str):
    ref = db.reference(f'users/{user_id}/{mark_type}/{game_title}')
    return ref.get() is not None

def get_marked_games(user_id: int, mark_type: str):
    ref = db.reference(f'users/{user_id}/{mark_type}')
    data = ref.get()
    return list(data.keys()) if data else []

def log_user_query(user_id: int, username: str, query: str):
    ref = db.reference(f'users/{user_id}/queries')
    now_iso = datetime.now(timezone.utc).isoformat()
    ref.push({
        'query': query,
        'timestamp': now_iso,
        'username': username
    })

def normalize_text(text):
    text = text.lower().strip()
    text = re.sub(r'[?]+', '', text)
    return text

def pick_random_game(exclude_titles=set()):
    available = df[~df['Title'].isin(exclude_titles)]
    if available.empty:
        return None, None
    row = available.sample(1).iloc[0]
    return row['Title'], row['Url']

async def notify_admin(app, text: str):
    admin_chat_id = -1002773793511
    await app.bot.send_message(chat_id=admin_chat_id, text=text)

async def greet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info(f"Пользователь {update.effective_user.id} поздоровался")
    await update.message.reply_text(
        "Привет! Я искусственный интеллект для поиска игр на PlayStation. "
        "Напиши название игры для поиска в нашей базе аренды или напиши 'Совет', или 'Во что поиграть?', "
        "чтобы получить случайную рекомендацию."
    )

async def send_advice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "no_username"
    log_user_query(user_id, username, "requested advice")

    completed_games = set(get_marked_games(user_id, 'completed_games'))
    title, url = pick_random_game(exclude_titles=completed_games)
    if not title:
        await update.message.reply_text("Все игры из базы у вас уже пройдены!")
        return ConversationHandler.END

    advice = random.choice(advice_texts)
    context.user_data['last_recommended_game'] = title
    msg = (f"{advice}\n{title}\n{url}\n\n"
           'Если подходит, напиши "Спасибо". Если хочешь другой вариант, скажи "Уже играл", "Уже прошел" или "Неинтересно" я это запомню и по команде "Пройденные" будет видно твою библиотеку.\n'
           'Если хочешь получить ещё рекомендацию — напиши "Еще".')
    await update.message.reply_text(msg)
    return ASKING_IF_WANT_NEW

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    logging.info(f"Команда /start от пользователя {user_id}")
    await update.message.reply_text(
        "Привет! Напиши название игры или её часть, и я пришлю ссылку на сайт с этой игрой."
    )

async def search_game(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "no_username"
    raw_text = update.message.text
    text = normalize_text(raw_text)

    logging.info(f"Получено сообщение: {raw_text} от пользователя {user_id} (@{username})")
    log_user_query(user_id, username, raw_text.lower())
    await notify_admin(context.application, f"Пользователь {user_id} (@{username}) написал запрос: {raw_text}")

    # Обработка слова "пока"
    if text == 'пока':
        await update.message.reply_text(
            "До встречи! У нас можно не только арендовать игры, но и купить их навсегда по выгодным ценам.\n"
            "Сайт - https://arenapsgm.ru/P2P3\n"
            "Группа - @StorePSGM"
        )
        return ConversationHandler.END

    # Обработка запроса "еще"
    if text == 'еще':
        return await send_advice(update, context)

    # Вопросы про вход в аккаунт
    account_phrases = [
        "как войти в аккаунт",
        "как войти в акаунт",
        "как зайти в аккаунт",
        "инструкция входа в аккаунт",
        "вход в аккаунт",
    ]
    if any(phrase in text for phrase in account_phrases):
        await update.message.reply_text(
            "Сделать это очень просто, вот инструкция:\nhttp://arenapsgm.ru/vhodps5"
        )
        return ConversationHandler.END

    # Приветствия
    if text in ['привет', 'здравствуй', 'добрый день', 'доброе утро', 'добрый вечер']:
        return await greet(update, context)

    # Запрос совета — вызываем отдельную функцию
    if text in advice_triggers:
        return await send_advice(update, context)

    # Запрос списка пройденных игр
    if text == 'пройденные':
        completed = get_marked_games(user_id, 'completed_games')
        if completed:
            response = "Вот список ваших пройденных игр:\n" + "\n".join(completed)
        else:
            response = "Вы пока не отметили ни одной пройденной игры."
        await update.message.reply_text(response)
        return ConversationHandler.END

    # Ответы на рекомендации
    last_game = context.user_data.get('last_recommended_game')
    if text in ['уже прошел', 'уже играл', 'неинтересно'] and last_game:
        if text == 'уже прошел':
            add_game_mark(user_id, last_game, 'completed_games')
        elif text == 'уже играл':
            add_game_mark(user_id, last_game, 'played_games')
        else:
            add_game_mark(user_id, last_game, 'not_interested_games')
        await update.message.reply_text("Хорошо, понял. Хочешь новую рекомендацию?")
        return ASKING_IF_WANT_NEW

    if text in ['да', 'конечно', 'давай']:
        context.user_data['last_recommended_game'] = None
        return await send_advice(update, context)

    if text == 'спасибо':
        await update.message.reply_text(
            "Рад помочь! Если хочешь оформить игру без комиссии, переходи по ссылке.\n"
            "Выдача игр после оплаты занимает примерно 15 минут."
        )
        return ConversationHandler.END

    if text == 'нет':
        await update.message.reply_text("Отлично. Спасибо, что написал. Я буду здесь, если понадоблюсь.")
        return ConversationHandler.END

    # Поиск игр по названию
    results = df[df['Title'].str.lower().str.contains(text, na=False)]
    if results.empty:
        await update.message.reply_text("Игра не найдена, попробуй другое название.")
        return ConversationHandler.END

    for _, row in results.head(25).iterrows():
        await update.message.reply_text(f"{row['Title']}\n{row['Url']}")

    return ConversationHandler.END

if __name__ == '__main__':
    TOKEN = os.getenv('BOT_TOKEN')

    conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.TEXT & (~filters.COMMAND), search_game)],
        states={
            ASKING_IF_WANT_NEW: [MessageHandler(filters.TEXT & (~filters.COMMAND), search_game)],
        },
        fallbacks=[]
    )

    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('greet', greet))
    app.add_handler(conv_handler)

    logging.info("Бот запущен...")
    app.run_polling()
