import os
import requests
from peewee import *
from src.database.connection import create_connection_factory


class DataBase:
    def __init__(self, config, url):
        self.connection_factory = create_connection_factory(config)
        self.url = url

        if os.path.isfile(config['file_name']):
            self.create_new_database(config['file_name'])
            self.update()

    def create_new_database(self, file_name):
        with self.connection_factory.conn() as db:
            cur = db.cursor()
            file = open(file_name)
            cur.execute(file.read())
            file.close()
            db.commit()

    def add_subscriber(self, subscriber_id: int) -> str:
        with self.connection_factory.conn() as db:
            subscribers = Table('subscribers').bind(db)
            q = subscribers.select(subscribers.c.id)\
                .where(subscribers.c.id == subscriber_id)

            if q.exists() == 0:
                subscribers.insert(id=subscriber_id).execute()
                message = 'Вы подписались на обновления'
            else:
                message = 'Вы уже подписаны на обновления'
            return message

    def del_subscriber(self, subscriber_id: int) -> str:
        with self.connection_factory.conn() as db:
            subscribers = Table('subscribers').bind(db)
            q = subscribers.select(subscribers.c.id)\
                .where(subscribers.c.id == subscriber_id)

            if q.exists() > 0:
                subscribers.delete().where(id=subscriber_id).execute()
                message = 'Вы отписались от обновлений'
            else:
                message = 'Вы уже отписаны от обновлений'
            return message

    def get_subscribers(self):
        with self.connection_factory.conn() as db:
            subscribers = Table('subscribers').bind(db)
            q = subscribers.select(subscribers.c.id).execute()
            return [subscriber['id'] for subscriber in list(q)]

    def update(self):
        with self.connection_factory.conn() as db:
            topics = Table('topics').bind(db)
            board = requests.get(self.url).json()
            board = [{'id': item['id'], 'description': item['title']} for item in board['response']['items']]

            new_topics = []
            for item in board:
                q = topics.select(topics.c.id) \
                    .where(topics.c.id == item['id'])
                if q.exists() == 0:
                    new_topics.append(item['description'])
            new_topics = '\n'.join(new_topics)

            if new_topics:
                topics.delete().execute()
                for item in board:
                    topics.insert(id=item['id'], description=item['description']).execute()
            return new_topics

    def get_descriptions(self):
        with self.connection_factory.conn() as db:
            topics = Table('topics').bind(db)
            q = topics.select(topics.c.description).execute()
            return '\n'.join([topic['description'] for topic in list(q)])
