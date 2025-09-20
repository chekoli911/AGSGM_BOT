import os
import logging
import time
import pandas as pd
import requests
from io import BytesIO
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, InputMediaPhoto
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes,
    MessageHandler, filters, ConversationHandler, CallbackQueryHandler
)
import firebase_admin
from firebase_admin import credentials, db
from datetime import datetime, timezone, timedelta
import random
import re
import asyncio

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

firebase_json = os.getenv('FIREBASE_CREDENTIALS_JSON')
if firebase_json:
    try:
        firebase_cred = credentials.Certificate(eval(firebase_json))
        firebase_admin.initialize_app(firebase_cred, {
            'databaseURL': 'https://ag-searh-default-rtdb.firebaseio.com/'
        })
        logging.info("Firebase подключен успешно")
    except Exception as e:
        logging.error(f"Ошибка подключения к Firebase: {e}")
        logging.info("Бот будет работать без Firebase")
else:
    logging.warning("FIREBASE_CREDENTIALS_JSON не установлен, бот будет работать без Firebase")

GITHUB_RAW_URL = 'https://github.com/chekoli911/AGSGM_BOT/raw/main/store-8370478-Vse_igri-202507290225_fixed.xlsx'
df = pd.read_excel(BytesIO(requests.get(GITHUB_RAW_URL).content), usecols=['Title', 'Url'])

CHANNEL_CHAT_ID = -1002773793511  # ID канала для сообщений пользователей
ADMIN_IDS = {5381215134, 6280405854}  # Множество админов

ASKING_IF_WANT_NEW = 1
WAITING_FOR_ACCOUNT_DATA = 2

# Функции для создания клавиатур
def get_main_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("🏠 Аренда"), KeyboardButton("🛒 Покупка")],
        [KeyboardButton("📚 Мои игры"), KeyboardButton("🎮 Во что поиграть?")],
        [KeyboardButton("⚙️ Функции бота"), KeyboardButton("🆕 Новинки")],
        [KeyboardButton("❓ Помощь")]
    ], resize_keyboard=True, is_persistent=True)

def get_search_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔍 Поиск игры", callback_data="search_game")]
    ])

def get_library_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Пройденные игры", callback_data="completed")],
        [InlineKeyboardButton("🎯 Сыгранные игры", callback_data="played")],
        [InlineKeyboardButton("❌ Неинтересные игры", callback_data="not_interested")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
    ])

def get_completed_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎯 Сыгранные игры", callback_data="played")],
        [InlineKeyboardButton("❌ Неинтересные игры", callback_data="not_interested")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
    ])

def get_played_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Пройденные игры", callback_data="completed")],
        [InlineKeyboardButton("❌ Неинтересные игры", callback_data="not_interested")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
    ])

def get_not_interested_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎯 Сыгранные игры", callback_data="played")],
        [InlineKeyboardButton("✅ Пройденные игры", callback_data="completed")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
    ])

def get_advice_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Еще совет", callback_data="advice")],
        [InlineKeyboardButton("✅ Уже играл", callback_data="advice_played")],
        [InlineKeyboardButton("🏆 Уже прошел", callback_data="advice_completed")],
        [InlineKeyboardButton("❌ Неинтересно", callback_data="advice_not_interested")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
    ])

def get_new_advice_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Еще совет", callback_data="advice")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
    ])

def get_rental_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎮 Арендовать игру", callback_data="rent_game")],
        [InlineKeyboardButton("🎯 Арендовать PS Plus", callback_data="rent_ps_plus")],
        [InlineKeyboardButton("✅ Продлить аренду профиля", callback_data="extend_rental_profile")],
        [InlineKeyboardButton("✅ Завершить аренду", callback_data="end_rental")],
        [InlineKeyboardButton("🔐 Получить код 2FA", callback_data="get_2fa")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
    ])

def get_purchase_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎮 Купить игры", callback_data="buy_games")],
        [InlineKeyboardButton("📱 Купить Подписку", callback_data="buy_subscription")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
    ])

def get_buy_games_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 Купить дешевле", callback_data="buy_cheaper")],
        [InlineKeyboardButton("💎 Полная покупка", callback_data="buy_full")],
        [InlineKeyboardButton("🔙 Назад", callback_data="purchase")]
    ])

def get_buy_full_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔥 Распродажа", callback_data="buy_sale")],
        [InlineKeyboardButton("🎯 Игра вне распродажи", callback_data="buy_outside_sale")],
        [InlineKeyboardButton("🔙 Назад", callback_data="buy_games")]
    ])

def get_buy_subscription_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎮 Купить PS Plus", callback_data="buy_ps_plus")],
        [InlineKeyboardButton("🎯 Купить EA Play", callback_data="buy_ea_play")],
        [InlineKeyboardButton("🔙 Назад", callback_data="purchase")]
    ])

def get_end_rental_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏰ Закончился срок", callback_data="rental_expired")],
        [InlineKeyboardButton("📤 Сдать игру досрочно", callback_data="early_return")],
        [InlineKeyboardButton("💳 Продлить игру со скидкой", callback_data="extend_rental")],
        [InlineKeyboardButton("🔙 Назад", callback_data="rental")]
    ])

def get_console_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎮 У меня PS4", callback_data="ps4_guide")],
        [InlineKeyboardButton("🎮 У меня PS5", callback_data="ps5_guide")],
        [InlineKeyboardButton("🔙 Назад", callback_data="end_rental")]
    ])

def get_early_return_confirm_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Понял(а)", callback_data="early_return_confirm")],
        [InlineKeyboardButton("🔙 Назад", callback_data="end_rental")]
    ])

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
        "🎮 Посоветовать интересные игры, если не знаешь, во что поиграть\n"
        "📚 Хранить твою библиотеку пройденных и сыгранных игр, чтобы не советовать их повторно\n"
        "🆕 Показывать последние новинки — их всегда можно арендовать у нас!\n\n"
        "Выбери действие или напиши любое название игры:",
        reply_markup=get_main_keyboard()
    )

async def send_advice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "no_username"
    log_user_query(user_id, username, "requested advice")

    completed_games = set(get_marked_games(user_id, 'completed_games'))
    title, url = pick_random_game(exclude_titles=completed_games)
    if not title:
        if update.callback_query:
            await update.callback_query.edit_message_text("Все игры из базы у вас уже пройдены!", reply_markup=get_main_keyboard())
        else:
            await update.message.reply_text("Все игры из базы у вас уже пройдены!")
        return ConversationHandler.END

    advice = random.choice(advice_texts)
    context.user_data['last_recommended_game'] = title
    msg = f"{advice}\n{title}\n{url}\n\nЧто думаешь об этой игре?"
    
    if update.callback_query:
        await update.callback_query.edit_message_text(msg, reply_markup=get_advice_keyboard())
    else:
        await update.message.reply_text(msg, reply_markup=get_advice_keyboard())
    return ASKING_IF_WANT_NEW

async def passed_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    completed = get_marked_games(user_id, 'completed_games')
    if completed:
        response = "✅ **Пройденные игры:**\n\n" + "\n".join(f"• {game}" for game in completed)
    else:
        response = "Вы пока не отметили ни одной пройденной игры."
    
    if update.callback_query:
        await update.callback_query.edit_message_text(response, reply_markup=get_completed_keyboard())
    else:
        await update.message.reply_text(response, reply_markup=get_completed_keyboard())

async def played_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    played = get_marked_games(user_id, 'played_games')
    if played:
        response = "🎯 **Сыгранные игры:**\n\n" + "\n".join(f"• {game}" for game in played)
    else:
        response = "Вы пока не отметили ни одной игры как сыгранной."
    
    if update.callback_query:
        await update.callback_query.edit_message_text(response, reply_markup=get_played_keyboard())
    else:
        await update.message.reply_text(response, reply_markup=get_played_keyboard())

async def not_interested_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    not_interested = get_marked_games(user_id, 'not_interested_games')
    if not_interested:
        response = "❌ **Неинтересные игры:**\n\n" + "\n".join(f"• {game}" for game in not_interested)
    else:
        response = "Вы пока не отметили ни одной игры как неинтересную."
    
    if update.callback_query:
        await update.callback_query.edit_message_text(response, reply_markup=get_not_interested_keyboard())
    else:
        await update.message.reply_text(response, reply_markup=get_not_interested_keyboard())

async def whattoplay_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await send_advice(update, context)

async def new_releases_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    last_25 = df.tail(25)
    messages = [f"{row['Title']}\n{row['Url']}" for _, row in last_25.iterrows()]
    
    if update.callback_query:
        await update.callback_query.edit_message_text("🆕 **Последние новинки:**\n\nОтправляю список...")
        for msg in messages:
            await update.callback_query.message.reply_text(msg)
    else:
        for msg in messages:
            await update.message.reply_text(msg)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    logging.info(f"Команда /start от пользователя {user_id}")
    await update.message.reply_text(
        "Привет! 👋\n"
        "Я помогу найти игры для PlayStation: просто напиши название игры или её часть, и я пришлю ссылку на аренду или покупку.\n"
        "Кроме того, я могу:\n"
        "🎮 Посоветовать интересные игры, если не знаешь, во что поиграть\n"
        "📚 Хранить твою библиотеку пройденных и сыгранных игр, чтобы не советовать их повторно\n"
        "🆕 Показывать последние новинки — их всегда можно арендовать у нас!\n\n"
        "Выбери действие или напиши любое название игры:",
        reply_markup=get_main_keyboard()
    )

async def handle_button_press(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатий на закрепленные кнопки"""
    text = update.message.text
    user_id = update.effective_user.id
    
    if text == "🆕 Новинки":
        await new_releases_command(update, context)
    elif text == "🎮 Во что поиграть?":
        context.user_data['last_recommended_game'] = None
        await send_advice(update, context)
    elif text == "📚 Мои игры":
        await update.message.reply_text(
            "📚 **Моя библиотека игр**\n\nВыбери категорию:",
            reply_markup=get_library_keyboard()
        )
    elif text == "❓ Помощь":
        await update.message.reply_text(
            "❓ **Помощь:**\n\n"
            "🎮 **Во что поиграть?** - получить персональную рекомендацию игры\n"
            "🆕 **Новинки** - показать последние 25 игр\n"
            "📚 **Мои игры** - посмотреть пройденные, сыгранные и неинтересные игры\n"
            "🏠 **Аренда** - аренда игр, PS Plus, продление аренды\n"
            "🛒 **Покупка** - купить игры и подписки навсегда\n"
            "⚙️ **Функции бота** - описание всех возможностей\n\n"
            "💡 **Советы по использованию:**\n"
            "• Напиши название игры для поиска\n"
            "• Используй кнопки для быстрой навигации\n"
            "• Отмечай игры, чтобы получать более точные рекомендации\n"
            "• В разделе 'Аренда' можно продлить игру промокодом ARENALOVE\n"
            "• В разделе 'Покупка' можно купить игры дешевле или навсегда\n\n"
            "🔗 **Полезные ссылки:**\n"
            "• Купить навсегда: https://arenapsgm.ru/P2P3\n"
            "• Группа покупки: @StorePSGM\n"
            "• Группа аренды: @ArenaPSGMrent\n"
            "• По вопросам: @ArenaPSGMadmin"
        )
    elif text == "⚙️ Функции бота":
        await update.message.reply_text(
            "Привет! 👋\n"
            "Я помогу найти игры для PlayStation: просто напиши название игры или её часть, и я пришлю ссылку на аренду или покупку.\n"
            "Кроме того, я могу:\n"
            "🎮 Посоветовать интересные игры, если не знаешь, во что поиграть\n"
            "📚 Хранить твою библиотеку пройденных и сыгранных игр, чтобы не советовать их повторно\n"
            "🆕 Показывать последние новинки — их всегда можно арендовать у нас!\n\n"
            "Выбери действие или напиши любое название игры:"
        )
    elif text == "🏠 Аренда":
        await update.message.reply_text(
            "🏠 **Аренда игр**\n\nВыбери действие:",
            reply_markup=get_rental_keyboard()
        )
    elif text == "🛒 Покупка":
        await update.message.reply_text(
            "🛒 **Покупка игр**\n\nВыбери категорию:",
            reply_markup=get_purchase_keyboard()
        )
    else:
        # Если это не кнопка, обрабатываем как поиск игры
        await search_game(update, context)

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
        await update.message.reply_text("Использование: /sendto <user_id> <сообщение> [photo_url]")
        return

    try:
        target_user_id = int(args[0])
    except ValueError:
        await update.message.reply_text("Неверный формат user_id. Он должен быть числом.")
        return

    message_text = " ".join(args[1:])
    photo_url = None
    
    # Проверяем, есть ли URL в сообщении (может быть в любом месте)
    import re
    url_pattern = r'https?://[^\s]+'
    url_match = re.search(url_pattern, message_text)
    
    if url_match:
        photo_url = url_match.group(0)
        # Убираем URL из текста сообщения
        message_text = re.sub(url_pattern, '', message_text).strip()
        # Убираем лишние пробелы
        message_text = re.sub(r'\s+', ' ', message_text)

    try:
        if photo_url:
            await context.application.bot.send_photo(chat_id=target_user_id, photo=photo_url, caption=message_text)
            await update.message.reply_text(f"Сообщение с фото успешно отправлено пользователю {target_user_id}.")
            await notify_admin(context.application, f"✅ Сообщение с фото отправлено пользователю {target_user_id} админом {user_id}.")
        else:
            await context.application.bot.send_message(chat_id=target_user_id, text=message_text)
            await update.message.reply_text(f"Сообщение успешно отправлено пользователю {target_user_id}.")
            await notify_admin(context.application, f"✅ Сообщение отправлено пользователю {target_user_id} админом {user_id}.")
    except Exception as e:
        await update.message.reply_text(f"Ошибка при отправке сообщения: {e}")

# --- Команда /sendtoall ---
async def sendtoall_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("У тебя нет прав для этой команды.")
        return

    args = context.args
    if len(args) < 1:
        await update.message.reply_text("Использование: /sendtoall <сообщение> [photo_url]")
        return

    message_text = " ".join(args)
    photo_url = None
    
    # Проверяем, есть ли URL в сообщении
    if message_text.startswith('http'):
        parts = message_text.split(' ', 1)
        if len(parts) == 2:
            photo_url = parts[0]
            message_text = parts[1]
        else:
            photo_url = message_text
            message_text = ""

    try:
        # Получаем список всех пользователей из Firebase
        ref = db.reference('users')
        users = ref.get()
        
        if not users:
            await update.message.reply_text("Пользователи не найдены в базе данных.")
            return

        sent_count = 0
        failed_count = 0
        
        for user_id_key in users.keys():
            try:
                target_user_id = int(user_id_key)
                if photo_url:
                    await context.application.bot.send_photo(chat_id=target_user_id, photo=photo_url, caption=message_text)
                else:
                    await context.application.bot.send_message(chat_id=target_user_id, text=message_text)
                sent_count += 1
            except Exception as e:
                failed_count += 1
                logging.error(f"Ошибка отправки сообщения пользователю {user_id_key}: {e}")

        await update.message.reply_text(f"Сообщение отправлено {sent_count} пользователям. Ошибок: {failed_count}.")
        await notify_admin(context.application, f"✅ Массовая рассылка выполнена админом {user_id}. Отправлено: {sent_count}, ошибок: {failed_count}.")
        
    except Exception as e:
        await update.message.reply_text(f"Ошибка при массовой рассылке: {e}")

# --- Функции для обработки заказов ---
def save_rental_to_firebase(rental_text):
    """Сохраняет результат аренды в Firebase через HTTP API (как в HTML)"""
    try:
        # URL вашей Firebase Realtime Database из HTML
        firebase_url = "https://arenarental-8eb7f-default-rtdb.firebaseio.com"
        
        # Данные для сохранения (точно как в HTML)
        data = {
            'text': rental_text,
            'timestamp': int(time.time() * 1000)  # timestamp в миллисекундах
        }
        
        # Отправляем POST запрос в Firebase
        response = requests.post(
            f"{firebase_url}/rentals.json",
            json=data,
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code == 200:
            logging.info("Результат аренды сохранен в Firebase (arenarental-8eb7f)")
            logging.info(f"Firebase ответ: {response.json()}")
        else:
            logging.error(f"Ошибка Firebase API: {response.status_code} - {response.text}")
            
    except Exception as e:
        logging.error(f"Ошибка сохранения в Firebase: {e}")
        # Fallback: попробуем использовать существующую конфигурацию
        try:
            if firebase_json:
                ref = db.reference('rentals')
                ref.push({
                    'text': rental_text,
                    'timestamp': int(time.time() * 1000)
                })
                logging.info("Результат сохранен через fallback конфигурацию")
        except Exception as e2:
            logging.error(f"Fallback также не сработал: {e2}")
def parse_order_info(text):
    """Парсит информацию о заказе из пересланного сообщения (улучшенная версия)"""
    order_info = {}
    
    # Извлекаем номер заказа
    order_match = re.search(r'Заказ №(\d+)', text)
    if order_match:
        order_info['order_number'] = order_match.group(1)
    
    # Извлекаем название игры - берем до цены с цифрами (учитывая двоеточия в названии)
    line_match = re.search(r'^\s*\d+\.\s*.*$', text, re.MULTILINE)
    if line_match:
        line = line_match.group(0)
        # Ищем паттерн ": цифры (" для определения конца названия
        price_match = re.search(r':\s*\d+\s*\(', line)
        if price_match:
            # Берем все до цены, убираем номер в начале
            title_part = line[:price_match.start()].strip()
            title_part = re.sub(r'^\d+\.\s*', '', title_part)
            order_info['game_name'] = title_part
        else:
            # Fallback: берем до первого двоеточия
            title_part = line.split(':')[0].replace(r'^\s*\d+\.', '', count=1).strip()
            order_info['game_name'] = title_part
        
        # Извлекаем платформу из той же строки
        platform_match = re.search(r'Платформа\s*:\s*([^,\n]+)', line, re.IGNORECASE)
        if platform_match:
            order_info['platform'] = platform_match.group(1).strip()
        
        # Извлекаем позицию и дни
        pos_match = re.search(r'(П[23])\s*(\d+)\s*дн', line, re.IGNORECASE)
        if pos_match:
            order_info['rental_type'] = pos_match.group(1).upper()
            order_info['days'] = int(pos_match.group(2))
    
    # Извлекаем имя покупателя
    name_match = re.search(r'\bName\s*:\s*([^\n\r]+)', text, re.IGNORECASE)
    if name_match:
        order_info['customer_name'] = name_match.group(1).strip()
    
    # Извлекаем дату
    date_match = re.search(r'\bDate\s*:\s*([^\n\r]+)', text, re.IGNORECASE)
    if date_match:
        order_info['order_date'] = date_match.group(1).strip()
    
    # Извлекаем промокод (если есть)
    promo_match = re.search(r'Промокод:\s+([^\n]+)', text)
    if promo_match:
        order_info['promo_code'] = promo_match.group(1).strip()
    
    # Извлекаем скидку (если есть)
    discount_match = re.search(r'Скидка:\s+(\d+)', text)
    if discount_match:
        order_info['discount'] = discount_match.group(1)
    
    # Извлекаем ник в Telegram
    telegram_match = re.search(r'Ник_в_Telegram_или_Вконтакте:\s+([^\n]+)', text)
    if telegram_match:
        order_info['telegram_nick'] = telegram_match.group(1).strip()
    
    return order_info

def parse_account_info(text):
    """Парсит информацию об аккаунте (улучшенная версия)"""
    account_info = {}
    
    if not text:
        return account_info
    
    # Извлекаем номер аккаунта (поддерживаем "Аккаунт:" и "Лот")
    acc_match = re.search(r'Аккаунт\s*:\s*(\S+)', text, re.IGNORECASE)
    if acc_match:
        account_info['account_number'] = acc_match.group(1)
    else:
        # Ищем формат "Лот X" или "ЛОТ X"
        lot_match = re.search(r'Л[Оо]т\s*(\d+)', text, re.IGNORECASE)
        if lot_match:
            account_info['account_number'] = f"ЛОТ {lot_match.group(1)}"
    
    # Определяем платформу
    if re.search(r'PS5', text, re.IGNORECASE):
        account_info['platform'] = 'PS5'
    elif re.search(r'PS4', text, re.IGNORECASE):
        account_info['platform'] = 'PS4'
    
    # Извлекаем email
    email_match = re.search(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-zА-Яа-я]{2,}', text)
    if email_match:
        account_info['email'] = email_match.group(0)
        
        # Ищем пароль после email
        after_email = text[email_match.end():]
        lines_after = [s.strip() for s in after_email.split('\n') if s.strip()]
        
        # Ищем строку без пробелов, без @, длиной >= 4
        for line in lines_after:
            if not re.search(r'\s', line) and '@' not in line and len(line) >= 4:
                account_info['password'] = line
                break
    else:
        # Если email не найден, ищем пароль в конце
        lines = [s.strip() for s in text.split('\n') if s.strip()]
        for line in reversed(lines):
            if not re.search(r'\s', line) and '@' not in line and len(line) >= 4:
                account_info['password'] = line
                break
    
    # Проверяем наличие активации
    if '✅ активация' in text or 'активация' in text:
        account_info['activation'] = '✅'
    
    return account_info

def parse_russian_date(date_str):
    """Парсит русскую дату в формате dd.mm.yyyy"""
    try:
        match = re.search(r'(\d{1,2})[.\/-](\d{1,2})[.\/-](\d{4})', date_str)
        if match:
            day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
            return datetime(year, month, day)
    except:
        pass
    return None

def calculate_end_date(start_date_str, days):
    """Вычисляет дату окончания аренды (всегда от текущей даты)"""
    try:
        # Всегда используем текущую дату как дату начала аренды
        start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        end_date = start_date + timedelta(days=days)
        return end_date.strftime("%d.%m.%Y")
    except:
        return "неизвестно"

def format_order_message(order_info, account_info):
    """Форматирует финальное сообщение для админа (улучшенная версия)"""
    customer_name = order_info.get('customer_name', 'Клиент')
    
    # Преобразуем имена (как в HTML версии)
    if customer_name == 'Алексей':
        customer_name = 'Леха'
    
    # Получаем данные
    game_name = order_info.get('game_name', 'Игра')
    platform = account_info.get('platform') or order_info.get('platform', 'PS5')
    rental_type = order_info.get('rental_type', 'П3')
    account_number = account_info.get('account_number', '')
    email = account_info.get('email', '')
    password = account_info.get('password', '')
    
    # Убираем номер из названия игры (например, "1. Dying Light" -> "Dying Light")
    game_name = re.sub(r'^\d+\.\s+', '', game_name)
    
    # Формируем тип аренды с активацией (✅ только для П3)
    rental_with_activation = rental_type
    if rental_type == 'П3':
        # Для П3 всегда показываем активацию, если не указано иное
        if not account_info.get('activation') or account_info.get('activation') == '✅':
            rental_with_activation += ' ✅'
    
    # Вычисляем дату окончания
    order_date = order_info.get('order_date', '')
    days = order_info.get('days', 7)  # По умолчанию 7 дней
    end_date = calculate_end_date(order_date, days)
    
    # Время выдачи (текущее время + 3 часа)
    current_time = (datetime.now() + timedelta(hours=3)).strftime('%H:%M')
    
    message = f"""Любимый клиент: {customer_name}
Арендовал(а) до {end_date} в {current_time}
"{game_name}", {platform}, {rental_with_activation}
Занят аккаунт №: {account_number}
почта: {email}
пароль: {password}"""
    
    return message

# --- Обработчик пересланных сообщений ---
async def handle_forwarded_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает пересланные сообщения с заказами"""
    user_id = update.effective_user.id
    
    # Проверяем, что это админ
    if user_id not in ADMIN_IDS:
        return ConversationHandler.END
    
    # Проверяем, что сообщение пересланное
    if not update.message.forward_from and not update.message.forward_from_chat:
        return ConversationHandler.END
    
    message_text = update.message.text
    if not message_text:
        await update.message.reply_text("Пересланное сообщение не содержит текста.")
        return ConversationHandler.END
    
    # Проверяем, что это сообщение с заказом
    if 'Заказ №' in message_text and 'Информация о покупателе:' in message_text:
        # Парсим информацию о заказе
        order_info = parse_order_info(message_text)
        
        # Сохраняем информацию о заказе в контексте пользователя
        context.user_data['pending_order'] = order_info
        
        # Просим данные аккаунта
        await update.message.reply_text("А теперь пришли мне данные аккаунта")
        
        # Устанавливаем состояние ожидания данных аккаунта
        return WAITING_FOR_ACCOUNT_DATA
    else:
        await update.message.reply_text("Это не похоже на сообщение с заказом. Перешлите сообщение с заказом, содержащее 'Заказ №' и 'Информация о покупателе:'.")
        return ConversationHandler.END

# --- Обработчик данных аккаунта ---
async def handle_account_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает данные аккаунта"""
    user_id = update.effective_user.id
    
    # Проверяем, что это админ
    if user_id not in ADMIN_IDS:
        return ConversationHandler.END
    
    # Проверяем, что есть ожидающий заказ
    if 'pending_order' not in context.user_data:
        await update.message.reply_text("Нет ожидающего заказа. Перешлите сначала сообщение с заказом.")
        return ConversationHandler.END
    
    message_text = update.message.text
    if not message_text:
        await update.message.reply_text("Пришлите текстовые данные аккаунта.")
        return WAITING_FOR_ACCOUNT_DATA
    
    # Парсим данные аккаунта
    account_info = parse_account_info(message_text)
    
    # Проверяем, что удалось извлечь основные данные
    if not account_info.get('account_number') or not account_info.get('email') or not account_info.get('password'):
        await update.message.reply_text(
            "Не удалось извлечь все необходимые данные аккаунта (номер, email, пароль). Попробуйте еще раз.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Отмена", callback_data="cancel_order_parsing")]
            ])
        )
        return WAITING_FOR_ACCOUNT_DATA
    
    # Получаем информацию о заказе
    order_info = context.user_data['pending_order']
    
    # Форматируем финальное сообщение
    final_message = format_order_message(order_info, account_info)
    
    # Отправляем админу
    await update.message.reply_text(final_message)

    # Сохраняем результат в Firebase
    save_rental_to_firebase(final_message)

    # Очищаем данные
    del context.user_data['pending_order']

    return ConversationHandler.END

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

# Обработчик callback'ов для кнопок
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    data = query.data
    
    if data == "search_game":
        await query.edit_message_text(
            "🔍 **Поиск игры**\n\nНапиши название игры или её часть, и я пришлю ссылку на аренду или покупку.\n\nПримеры:\n• God of War\n• FIFA\n• Spider-Man",
            reply_markup=get_main_keyboard()
        )
    elif data == "advice":
        context.user_data['last_recommended_game'] = None  # Сбрасываем для новой рекомендации
        await send_advice(update, context)
    elif data == "new_releases":
        await new_releases_command(update, context)
    elif data == "library":
        await query.edit_message_text(
            "📚 **Моя библиотека игр**\n\nВыбери категорию:",
            reply_markup=get_library_keyboard()
        )
    elif data == "rental":
        await query.edit_message_text(
            "🏠 **Аренда игр**\n\nВыбери действие:",
            reply_markup=get_rental_keyboard()
        )
    elif data == "functions":
        await query.edit_message_text(
            "Привет! 👋\n"
            "Я помогу найти игры для PlayStation: просто напиши название игры или её часть, и я пришлю ссылку на аренду или покупку.\n"
            "Кроме того, я могу:\n"
            "🎮 Посоветовать интересные игры, если не знаешь, во что поиграть\n"
            "📚 Хранить твою библиотеку пройденных и сыгранных игр, чтобы не советовать их повторно\n"
            "🆕 Показывать последние новинки — их всегда можно арендовать у нас!\n\n"
            "Выбери действие или напиши любое название игры:",
            reply_markup=get_main_keyboard()
        )
    elif data == "help":
        await query.edit_message_text(
            "❓ Помощь:\n\n"
            "🎮 **Дать совет** - получить персональную рекомендацию игры\n"
            "🆕 **Новинки** - показать последние 25 игр\n"
            "📚 **Моя библиотека** - посмотреть пройденные, сыгранные и неинтересные игры\n\n"
            "💡 **Советы по использованию:**\n"
            "• Напиши название игры для поиска\n"
            "• Используй кнопки для быстрой навигации\n"
            "• Отмечай игры, чтобы получать более точные рекомендации\n\n"
            "🔗 **Полезные ссылки:**\n"
            "• Сайт: https://arenapsgm.ru/P2P3\n"
            "• Группа: @StorePSGM",
            reply_markup=get_main_keyboard()
        )
    elif data == "completed":
        # Всегда показывать список пройденных игр (как /passed)
        await passed_command(update, context)
    elif data == "played":
        # Всегда показывать список сыгранных игр (как /played)
        await played_command(update, context)
    elif data == "not_interested":
        # Всегда показывать список неинтересных игр (как /notinterested)
        await not_interested_command(update, context)
    elif data == "back_to_main":
        await query.edit_message_text(
            "Главное меню:\n\nВыбери действие или напиши любое название игры:"
        )
    
    # Обработчики для кнопок в режиме рекомендаций
    elif data == "advice_played" and context.user_data.get('last_recommended_game'):
        # Отметить игру как сыгранную и дать новую рекомендацию
        last_game = context.user_data.get('last_recommended_game')
        add_game_mark(user_id, last_game, 'played_games')
        await query.edit_message_text("Отлично, отметил как сыгранную. Вот новая рекомендация:", reply_markup=get_new_advice_keyboard())
        context.user_data['last_recommended_game'] = None
        await send_advice(update, context)
    elif data == "advice_completed" and context.user_data.get('last_recommended_game'):
        # Отметить игру как пройденную и дать новую рекомендацию
        last_game = context.user_data.get('last_recommended_game')
        add_game_mark(user_id, last_game, 'completed_games')
        await query.edit_message_text("Отлично, отметил как пройденную. Вот новая рекомендация:", reply_markup=get_new_advice_keyboard())
        context.user_data['last_recommended_game'] = None
        await send_advice(update, context)
    elif data == "advice_not_interested" and context.user_data.get('last_recommended_game'):
        # Отметить игру как неинтересную и дать новую рекомендацию
        last_game = context.user_data.get('last_recommended_game')
        add_game_mark(user_id, last_game, 'not_interested_games')
        await query.edit_message_text("Понял, отмечаю как неинтересную. Вот новая рекомендация:", reply_markup=get_new_advice_keyboard())
        context.user_data['last_recommended_game'] = None
        await send_advice(update, context)
    
    # Обработчики для аренды
    elif data == "rent_game":
        await query.edit_message_text(
            "🎮 **Арендовать игру**\n\n"
            "Напиши название игры или её часть и я пришлю ссылку на аренду. "
            "Перейдя по ссылке выбери срок и позицию, после оплаты я передам чек админу и он всё пришлёт.\n\n"
            "Также все игры представлены на https://arenapsgm.ru/",
            reply_markup=get_rental_keyboard()
        )
    elif data == "rent_ps_plus":
        await query.edit_message_text(
            "🎯 **Арендовать PS Plus**\n\n"
            "Переходи по ссылке для аренды PS Plus:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🎮 Арендовать PS Plus", url="https://arenapsgm.ru/playstationplus/tproduct/199915107972-arenda-ps-plus-ps4ps5")],
                [InlineKeyboardButton("🔙 Назад", callback_data="rental")]
            ])
        )
    elif data == "extend_rental_profile":
        await query.edit_message_text(
            "✅ **Продлить аренду профиля**\n\n"
            "Для продления аренды используй промокод: `ARENALOVE`\n\n"
            "Напиши название или часть из названия игры, которую нужно продлить:",
            reply_markup=get_rental_keyboard()
        )
    elif data == "end_rental":
        await query.edit_message_text(
            "✅ **Завершить аренду**\n\nВыбери вариант:",
            reply_markup=get_end_rental_keyboard()
        )
    elif data == "get_2fa":
        await query.edit_message_text(
            "🔐 **Получить код 2FA**\n\n"
            "Сейчас я это делать не умею, но скоро научусь! Спроси код у @ArenaPSGMadmin",
            reply_markup=get_rental_keyboard()
        )
    elif data == "rental_expired":
        await query.edit_message_text(
            "📌 **Важно!** Если при деактивации или выключении общего доступа вы видите QR-код и запрос на вход в сеть – это значит, что консоль или аккаунт были офлайн.\n\n"
            "✅ В таком случае сначала подключитесь к интернету и войдите в аккаунт, а потом повторите процедуру деактивации.\n"
            "❌ Просто удалить аккаунт с консоли недостаточно – это может привести к проблемам с системой и консолью!\n\n"
            "**Как правильно сдать игру?**",
            reply_markup=get_console_keyboard()
        )
    elif data == "early_return":
        await query.edit_message_text(
            "📤 **Сдать игру досрочно**\n\n"
            "За досрочную сдачу аккаунта как правило полагается скидка на следующую игру около 30%\n"
            "• Скидка активна в течение недели\n"
            "• Скидка не действует на позиции дешевле 290₽\n"
            "• Скидка идёт от 14 дней\n"
            "• Скидка не действует на скидку и на новинки первые 2-3 месяца\n\n"
            "Промокод на скидку за досрочную сдачу проси у @ArenaPSGMadmin",
            reply_markup=get_early_return_confirm_keyboard()
        )
    elif data == "early_return_confirm":
        await query.edit_message_text(
            "**Как правильно сдать игру?**",
            reply_markup=get_console_keyboard()
        )
    elif data == "extend_rental":
        await query.edit_message_text(
            "💳 **Продлить игру со скидкой**\n\n"
            "Для продления игры со скидкой используй промокод: `ARENALOVE`\n\n"
            "Напиши название или часть из названия игры, которую нужно продлить:",
            reply_markup=get_rental_keyboard()
        )
    elif data == "ps4_guide":
        await query.edit_message_text(
            "🎮 **Инструкция для PS4:**\n\n"
            "• Заходите в Настройки → Управление учетной записью → Активация как основная система PS4\n"
            "• Нажимаете \"Деактивировать\"\n\n"
            "После чего пришлите фото деактивации админу @ArenaPSGMadmin\n"
            "За это уважение и респект 🫡",
            reply_markup=get_console_keyboard()
        )
    elif data == "ps5_guide":
        await query.edit_message_text(
            "🎮 **Инструкция для PS5:**\n\n"
            "• Заходите в Настройки → Пользователи и учётные записи → Другое → Общий доступ к консоли и офлайн-игра\n"
            "• Нажимаете \"Отключить\"\n\n"
            "После чего пришлите фото выключения общего доступа админу @ArenaPSGMadmin\n"
            "За это уважение и респект 🫡",
            reply_markup=get_console_keyboard()
        )
    
    # Обработчики для покупки
    elif data == "purchase":
        await query.edit_message_text(
            "🛒 **Покупка игр**\n\nВыбери категорию:",
            reply_markup=get_purchase_keyboard()
        )
    elif data == "buy_games":
        await query.edit_message_text(
            "🎮 **Купить игры**\n\nВыбери вариант:",
            reply_markup=get_buy_games_keyboard()
        )
    elif data == "buy_cheaper":
        await query.edit_message_text(
            "💰 **Купить дешевле**\n\n"
            "Переходи по ссылке для покупки игр по выгодным ценам:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💰 Купить дешевле", url="https://arenapsgm.ru/P2P3")],
                [InlineKeyboardButton("🔙 Назад", callback_data="buy_games")]
            ])
        )
    elif data == "buy_full":
        await query.edit_message_text(
            "💎 **Полная покупка**\n\nВыбери варианты:",
            reply_markup=get_buy_full_keyboard()
        )
    elif data == "buy_sale":
        await query.edit_message_text(
            "🔥 **Распродажа**\n\n"
            "Переходи по ссылке для покупки игр со скидками:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔥 Распродажа", url="https://arenapsgm.ru/whattobuysale")],
                [InlineKeyboardButton("🔙 Назад", callback_data="buy_full")]
            ])
        )
    elif data == "buy_outside_sale":
        await query.edit_message_text(
            "🎯 **Игра вне распродажи**\n\n"
            "Для уточнения стоимости игры, которой нет в распродаже, свяжитесь с администратором:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💬 Связаться с админом", url="https://t.me/ArenaPSGMadmin")],
                [InlineKeyboardButton("🔙 Назад", callback_data="buy_full")]
            ])
        )
    elif data == "buy_subscription":
        await query.edit_message_text(
            "📱 **Купить Подписку**\n\nВыбери подписку:",
            reply_markup=get_buy_subscription_keyboard()
        )
    elif data == "buy_ps_plus":
        await query.edit_message_text(
            "🎮 **Купить PS Plus**\n\n"
            "Переходи по ссылке для покупки PS Plus:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🎮 Купить PS Plus", url="https://arenapsgm.ru/playstationplus")],
                [InlineKeyboardButton("🔙 Назад", callback_data="buy_subscription")]
            ])
        )
    elif data == "buy_ea_play":
        await query.edit_message_text(
            "🎯 **Купить EA Play**\n\n"
            "Переходи по ссылке для покупки EA Play:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🎯 Купить EA Play", url="https://arenapsgm.ru/eaplay")],
                [InlineKeyboardButton("🔙 Назад", callback_data="buy_subscription")]
            ])
        )
    elif data == "cancel_order_parsing":
        # Отменяем парсинг заказа и возвращаемся к началу
        if 'pending_order' in context.user_data:
            del context.user_data['pending_order']
        
        await query.edit_message_text(
            "❌ **Парсинг заказа отменен**\n\n"
            "Бот перезагружен. Используйте команду /start для начала работы.",
            reply_markup=get_main_keyboard()
        )
        return ConversationHandler.END

async def on_startup(app):
    app.create_task(scheduled_messages_worker(app))

if __name__ == '__main__':
    TOKEN = os.getenv('BOT_TOKEN')

    conv_handler = ConversationHandler(
        entry_points=[
            MessageHandler(filters.FORWARDED, handle_forwarded_message),
            MessageHandler(filters.TEXT & (~filters.COMMAND) & (~filters.FORWARDED), handle_button_press)
        ],
        states={
            ASKING_IF_WANT_NEW: [MessageHandler(filters.TEXT & (~filters.COMMAND), handle_button_press)],
            WAITING_FOR_ACCOUNT_DATA: [MessageHandler(filters.TEXT & (~filters.COMMAND), handle_account_data)],
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
    app.add_handler(CommandHandler('sendtoall', sendtoall_command))
    app.add_handler(CommandHandler('schedule', schedule_command))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(conv_handler)

    app.post_init = on_startup

    logging.info("Бот запущен...")
    app.run_polling()
