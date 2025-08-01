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
from datetime import datetime, timezone, timedelta
import random
import re
import asyncio

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

CHANNEL_CHAT_ID = -1002773793511  # ID канала для сообщений пользователей
ADMIN_IDS = {5381215134}  # Множество админов

ASKING_IF_WANT_NEW = 1

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

async def notify_channel(app, text: str):
    # Отправка сообщений пользователей в канал
    await app.bot.send_message(chat_id=CHANNEL_CHAT_ID, text=text)

async def notify_admin(app, text: str):
    # Отправка операционных сообщений всем админам
    for admin_id in ADMIN_IDS:
        await app.bot.send_message(chat_id=admin_id, text=text)

async def greet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info(f"Пользователь {update.effective_user.id} поздоровался")
    await update.message.reply_text(
        "Привет! 👋\n"
        "Я помогу найти игры для PlayStation: просто напиши название игры или её часть, и я пришлю ссылку на аренду или покупку.\n"
        "Кроме того, я могу:\n"
        "🎮 Посоветовать интересные игры, если не знаешь, во что поиграть — просто напиши «Совет» или «Во что поиграть?»\n"
        "📚 Хранить твою библиотеку пройденных и сыгранных игр, чтобы не советовать их повторно\n"
        "🆕 Показывать последние новинки — их всегда можно арендовать у нас!\n\n"
        "Пиши любое название или запрос — я помогу подобрать игру!"
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
           'Если хочешь получить новую рекомендацию, напиши "Еще".\n\n'
           '"Играл",\n'
           '"Уже прошел",\n'
           '"Неинтересно" — я это запомню и по команде "Пройденные" будет видна твоя библиотека.\n\n'
           'Если подходит, напиши "Спасибо".\n\n'
           'Если пора прощаться, напиши "Пока".')
    await update.message.reply_text(msg)
    return ASKING_IF_WANT_NEW

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

async def new_releases_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    last_25 = df.tail(25)
    messages = [f"{row['Title']}\n{row['Url']}" for _, row in last_25.iterrows()]
    for msg in messages:
        await update.message.reply_text(msg)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    logging.info(f"Команда /start от пользователя {user_id}")
    await update.message.reply_text(
        "Привет! 👋\n"
        "Я помогу найти игры для PlayStation: просто напиши название игры или её часть, и я пришлю ссылку на аренду или покупку.\n"
        "Кроме того, я могу:\n"
        "🎮 Посоветовать интересные игры, если не знаешь, во что поиграть — просто напиши «Совет» или «Во что поиграть?»\n"
        "📚 Хранить твою библиотеку пройденных и сыгранных игр, чтобы не советовать их повторно\n"
        "🆕 Показывать последние новинки — их всегда можно арендовать у нас! Просто напиши «Новинки»\n\n"
        "Пиши любое название или запрос — я помогу подобрать игру!"
    )

async def search_game(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "no_username"
    raw_text = update.message.text
    text = normalize_text(raw_text)

    logging.info(f"Получено сообщение: {raw_text} от пользователя {user_id} (@{username})")
    log_user_query(user_id, username, raw_text.lower())

    # Отправляем текст запроса пользователя в канал
    await notify_channel(context.application, f"Пользователь {user_id} (@{username}) написал запрос:\n{raw_text}")

    if text == 'пока':
        await update.message.reply_text(
            "До встречи! У нас можно не только арендовать игры, но и купить их навсегда по выгодным ценам.\n"
            "Сайт - https://arenapsgm.ru/P2P3\n"
            "Группа - @StorePSGM"
        )
        return ConversationHandler.END

    if text == 'еще':
        return await send_advice(update, context)

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

    if text in ['привет', 'здравствуй', 'добрый день', 'доброе утро', 'добрый вечер']:
        return await greet(update, context)

    if text in advice_triggers:
        return await send_advice(update, context)

    if text in passed_triggers:
        await passed_command(update, context)
        return ConversationHandler.END

    if text in played_triggers:
        await played_command(update, context)
        return ConversationHandler.END

    last_game = context.user_data.get('last_recommended_game')
    if text in not_interested_triggers and not last_game:
        await not_interested_command(update, context)
        return ConversationHandler.END

    if text == 'новинки':
        await new_releases_command(update, context)
        return ConversationHandler.END

    if last_game:
        if text == 'неинтересно':
            add_game_mark(user_id, last_game, 'not_interested_games')
            await update.message.reply_text("Понял, отмечаю эту игру как неинтересную. Вот новая рекомендация:")
            return await send_advice(update, context)
        elif text in ['уже играл', 'играл']:
            add_game_mark(user_id, last_game, 'played_games')
            await update.message.reply_text("Отлично, отметил как сыгранную. Вот новая рекомендация:")
            return await send_advice(update, context)
        elif text == 'уже прошел':
            add_game_mark(user_id, last_game, 'completed_games')
            await update.message.reply_text("Отлично, отметил как пройденную. Вот новая рекомендация:")
            return await send_advice(update, context)

    mark_patterns = {
        'completed_games': ['пройдено', 'пройденные', 'пройденное', 'пройден'],
        'played_games': ['сыграл', 'уже играл', 'играл'],
        'not_interested_games': ['неинтересно', 'не интересно', 'не интересна', 'не интересные']
    }

    for mark_type, keywords in mark_patterns.items():
        for keyword in keywords:
            if text.startswith(keyword):
                game_title = text[len(keyword):].strip()
                if not game_title:
                    await update.message.reply_text(f"Пожалуйста, укажи название игры после слова '{keyword}'.")
                    return ConversationHandler.END

                results = df[df['Title'].str.lower().str.startswith(game_title)]
                if results.empty:
                    await update.message.reply_text("Игра не найдена в базе. Проверь правильность написания.")
                    return ConversationHandler.END

                add_game_mark(user_id, results.iloc[0]['Title'], mark_type)
                await update.message.reply_text(f"Игра '{results.iloc[0]['Title']}' отмечена как {mark_type.replace('_', ' ')}.")
                return ConversationHandler.END

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

    results = df[df['Title'].str.lower().str.contains(text, na=False)]
    if results.empty:
        await update.message.reply_text("Игра не найдена, попробуй другое название.")
        return ConversationHandler.END

    for _, row in results.head(25).iterrows():
        await update.message.reply_text(f"{row['Title']}\n{row['Url']}")

    return ConversationHandler.END

# --- Команда /sendto ---
async def sendto_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("У тебя нет прав для этой команды.")
        return

    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Использование: /sendto <user_id> <сообщение>")
        return

    try:
        target_user_id = int(args[0])
    except ValueError:
        await update.message.reply_text("Неверный формат user_id. Он должен быть числом.")
        return

    message_text = " ".join(args[1:])
    try:
        await context.application.bot.send_message(chat_id=target_user_id, text=message_text)
        await update.message.reply_text(f"Сообщение успешно отправлено пользователю {target_user_id}.")
        await notify_admin(context.application, f"✅ Сообщение отправлено пользователю {target_user_id} админом {user_id}.")
    except Exception as e:
        await update.message.reply_text(f"Ошибка при отправке сообщения: {e}")

# --- Команда /schedule ---
def convert_utc3_to_unix_timestamp(date_str: str) -> int:
    dt_naive = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
    dt_aware = dt_naive.replace(tzinfo=timezone(timedelta(hours=3)))
    dt_utc = dt_aware.astimezone(timezone.utc)
    return int(dt_utc.timestamp())

async def schedule_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("У тебя нет прав для этой команды.")
        return

    args = context.args
    if len(args) < 4:
        await update.message.reply_text("Использование: /schedule <user_id> <YYYY-MM-DD> <HH:MM> <текст сообщения>")
        return

    try:
        target_user_id = int(args[0])
        date_part = args[1]
        time_part = args[2]
        date_time_str = f"{date_part} {time_part}"
        send_at_timestamp = convert_utc3_to_unix_timestamp(date_time_str)
    except Exception as e:
        await update.message.reply_text(f"Ошибка при обработке даты и времени: {e}")
        return

    message_text = " ".join(args[3:])
    ref = db.reference('scheduled_messages')
    ref.push({
        'target_user_id': target_user_id,
        'message_text': message_text,
        'send_at': send_at_timestamp,
        'status': 'pending'
    })

    await update.message.reply_text(f"Сообщение запланировано для пользователя {target_user_id} на {date_time_str} по UTC+3.")
    await notify_admin(context.application, f"✅ Сообщение добавлено в расписание для пользователя {target_user_id} админом {user_id}.")

# --- Фоновый воркер отправки отложенных сообщений ---
async def scheduled_messages_worker(app):
    while True:
        try:
            ref = db.reference('scheduled_messages')
            all_messages = ref.order_by_child('status').equal_to('pending').get()
            if all_messages:
                now_ts = int(datetime.now(timezone.utc).timestamp())
                for key, msg_data in all_messages.items():
                    send_at = msg_data.get('send_at', 0)
                    if send_at <= now_ts:
                        target_user_id = msg_data.get('target_user_id')
                        message_text = msg_data.get('message_text')
                        try:
                            await app.bot.send_message(chat_id=target_user_id, text=message_text)
                            ref.child(key).update({'status': 'sent'})
                            logging.info(f"Отложенное сообщение отправлено пользователю {target_user_id}")
                            # Отправляем операционное уведомление админам
                            await notify_admin(app, f"✅ Отложенное сообщение отправлено пользователю {target_user_id}")
                        except Exception as e:
                            logging.error(f"Ошибка при отправке отложенного сообщения: {e}")
            await asyncio.sleep(30)
        except Exception as e:
            logging.error(f"Ошибка в воркере отложенных сообщений: {e}")
            await asyncio.sleep(30)

async def on_startup(app):
    app.create_task(scheduled_messages_worker(app))

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
    app.add_handler(CommandHandler('newreleases', new_releases_command))
    app.add_handler(CommandHandler('sendto', sendto_command))
    app.add_handler(CommandHandler('schedule', schedule_command))
    app.add_handler(conv_handler)

    app.post_init = on_startup

    logging.info("Бот запущен...")
    app.run_polling()
