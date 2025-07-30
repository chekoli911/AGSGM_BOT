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

passed_triggers = ['пройденные', 'пройденное', 'пройдено', 'пройденные игры']
played_triggers = ['уже играл', 'сыграл', 'played']
not_interested_triggers = ['неинтересно', 'не интересно', 'неинтересные игры']

ASKING_IF_WANT_NEW = 1

TEST_USER_IDS = [6280405854, 291987661, 5381215134]

def add_game_mark(user_id: int, game_title: str, mark_type: str):
    ref = db.reference(f'users/{user_id}/{mark_type}')
    ref.update({game_title: True})

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
        "Привет! Я искусственный интеллект. Для поиска игр на PlayStation. "
        "Напиши название игры и я найду её в нашей базе аренды. А если не знаешь во что поиграть, напиши 'Совет' или 'Во что поиграть?', "
        "я подумаю и пришлю случайную рекомендацию. Ещё я могу показать некоторые команды, если напишешь вот такую палочку - /"
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
           'Если подходит, напиши "Спасибо". Если хочешь другой вариант, скажи "Уже играл", "Уже прошел" или "Неинтересно" — я это запомню и по команде "Пройденные" будет видно твою библиотеку.\n'
           'Если хочешь получить ещё рекомендацию — напиши "Еще".')
    await update.message.reply_text(msg)
    return ASKING_IF_WANT_NEW

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    logging.info(f"Команда /start от пользователя {user_id}")
    await update.message.reply_text(
        "Привет! Напиши название игры или её часть, и я пришлю ссылку на сайт с этой игрой."
    )

async def passed_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    completed = get_marked_games(user_id, 'completed_games')
    if completed:
        response = "Вот список ваших пройденных игр:\n" + "\n".join(completed)
    else:
        response = "Вы пока не отметили ни одной пройденной игры."
    await update.message.reply_text(response)

async def played_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    played = get_marked_games(user_id, 'played_games')
    if played:
        response = "Вот список игр, в которые вы уже играли:\n" + "\n".join(played)
    else:
        response = "Вы пока не отметили ни одной игры как сыгранной."
    await update.message.reply_text(response)

async def not_interested_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    not_interested = get_marked_games(user_id, 'not_interested_games')
    if not_interested:
        response = "Вот список игр, которые вы отметили как неинтересные:\n" + "\n".join(not_interested)
    else:
        response = "Вы пока не отметили ни одной игры как неинтересную."
    await update.message.reply_text(response)

async def whattoplay_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await send_advice(update, context)

async def send_test_image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    image_url = "https://image.api.playstation.com/pr/bam-art/214/754/fc50c647-5854-443c-886f-37377576c781.png"
    caption = "Это тестовое сообщение с картинкой от ИИ."

    sent_count = 0
    failed_count = 0

    for chat_id in TEST_USER_IDS:
        try:
            await context.bot.send_photo(chat_id=chat_id, photo=image_url, caption=caption)
            sent_count += 1
        except Exception as e:
            logging.error(f"Ошибка при отправке пользователю {chat_id}: {e}")
            failed_count += 1

    await update.message.reply_text(f"Рассылка завершена.\nУспешно: {sent_count}\nОшибок: {failed_count}")

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

    # Запрос списка пройденных игр (текстовые варианты)
    if text in passed_triggers:
        await passed_command(update, context)
        return ConversationHandler.END

    # Запрос списка сыгранных игр
    if text in played_triggers:
        await played_command(update, context)
        return ConversationHandler.END

    # Запрос списка неинтересных игр
    if text in not_interested_triggers:
        await not_interested_command(update, context)
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
    app.add_handler(CommandHandler('passed', passed_command))
    app.add_handler(CommandHandler('played', played_command))
    app.add_handler(CommandHandler('notinterested', not_interested_command))
    app.add_handler(CommandHandler('whattoplay', whattoplay_command))
    app.add_handler(CommandHandler('sendtestimage', send_test_image))
    app.add_handler(conv_handler)

    logging.info("Бот запущен...")
    app.run_polling()
