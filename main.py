from functools import partial

from telegram import Update
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext

from src.utils import read_yaml
from src.database.database import DataBase


def subscribe(update: Update, context: CallbackContext, database: DataBase) -> None:
    chat_id = update.message.chat_id
    if database.add_subscriber(chat_id):
        update.message.reply_text('Вы подписались на обновления')
    else:
        update.message.reply_text('Вы уже подписаны на обновления')


def unsubscribe(update: Update, context: CallbackContext, database: DataBase) -> None:
    chat_id = update.message.chat_id
    if database.del_subscriber(chat_id):
        update.message.reply_text('Вы отписались от обновлений')
    else:
        update.message.reply_text('Вы уже отписаны от обновлений')


def echo(update: Update, context: CallbackContext) -> None:
    update.message.reply_text(update.message.text)


def get_board(update: Update, context: CallbackContext, database: DataBase) -> None:
    board = database.get_topics()
    description = database.topics_to_description(board)
    update.message.reply_text(text=description, parse_mode='markdown', disable_web_page_preview=True)


def update(context: CallbackContext):
    new_topics = database.update()
    if new_topics:
        description = database.topics_to_description(new_topics)
        for subscriber in database.get_subscribers():
            context.bot.send_message(
                chat_id=subscriber['id'], text=description, parse_mode='markdown', disable_web_page_preview=True
            )


def run_bot(token, database) -> None:
    """Start the bot."""
    # Create the Updater and pass it your bot's token.
    updater = Updater(token)
    updater.job_queue.run_repeating(update, interval=60)

    # Get the dispatcher to register handlers
    dispatcher = updater.dispatcher

    # on different commands - answer in Telegram
    dispatcher.add_handler(
        CommandHandler(
            "subscribe",
            partial(subscribe, database=database)
        )
    )
    dispatcher.add_handler(
        CommandHandler(
            "unsubscribe",
            partial(unsubscribe, database=database)
        )
    )
    dispatcher.add_handler(
        CommandHandler(
            "get_board",
            partial(get_board, database=database)
        )
    )

    # on non command i.e message - echo the message on Telegram
    dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, echo))

    # Start the Bot
    updater.start_polling()

    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()


if __name__ == "__main__":
    config = read_yaml('config.yaml')

    TOKEN = config['TG']['TOKEN']

    VK_CONFIG = config['VK']
    PG_CONFIG = config['POSTGRES']
    database = DataBase(PG_CONFIG, VK_CONFIG, False)

    run_bot(TOKEN, database)
