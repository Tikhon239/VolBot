from datetime import datetime
import requests
from peewee import *
from src.database.connection import create_connection_factory


class DataBase:
    def __init__(self, pg_config, vk_config, recreate_database: bool = False):
        self.pg_config = pg_config
        self.vk_config = vk_config

        self.connection_factory = create_connection_factory(pg_config)
        self.topic_ids = set()

        if recreate_database:
            self.recreate_database(pg_config['file_name'])
        else:
            self.topic_ids = set(topic['id'] for topic in self.get_topics())

    @property
    def url(self):
        return f"https://api.vk.com/method/{self.vk_config['METHOD']}?group_id={self.vk_config['GROUP_ID']}&access_token={self.vk_config['TOKEN']}&v={self.vk_config['VERSION']}"

    def recreate_database(self, file_name):
        with self.connection_factory.conn() as db:
            with open(file_name, 'r') as file:
                db.execute_sql(file.read())

            topics = Table('topics').bind(db)
            vk_request = requests.get(self.url).json()
            for item in vk_request['response']['items']:
                topic = {
                    'id': item['id'],
                    'description': item['title'],
                    'created': datetime.fromtimestamp(item['created']).strftime('%Y-%m-%d %H:%M:%S')
                }

                topics.insert(**topic).execute()
                self.topic_ids.add(topic['id'])

    def add_subscriber(self, id: int) -> bool:
        with self.connection_factory.conn() as db:
            subscribers = Table('subscribers').bind(db)
            q = subscribers.select(subscribers.c.id) \
                .where(subscribers.c.id == id)

            if q.exists() == 0:
                subscribers.insert(id=id).execute()
                return True
            return False

    def del_subscriber(self, id: int) -> bool:
        with self.connection_factory.conn() as db:
            subscribers = Table('subscribers').bind(db)
            q = subscribers.select(subscribers.c.id) \
                .where(subscribers.c.id == id)

            if q.exists() > 0:
                subscribers.delete() \
                    .where(subscribers.c.id == id) \
                    .execute()
                return True
            return False

    def get_subscribers(self):
        with self.connection_factory.conn() as db:
            subscribers = Table('subscribers').bind(db)
            q = subscribers.select(subscribers.c.id)
            return list(q)

    def add_topic(self, id: int, description: str, created) -> bool:
        with self.connection_factory.conn() as db:
            topics = Table('topics').bind(db)
            q = topics.select(topics.c.id) \
                .where(topics.c.id == id)
            if q.exists() == 0:
                topics.insert(id=id, description=description, created=created).execute()
                return True
            return False

    def del_topic(self, id: int) -> bool:
        with self.connection_factory.conn() as db:
            topics = Table('topics').bind(db)
            q = topics.select(topics.c.id) \
                .where(topics.c.id == id)
            if q.exists() > 0:
                topics.delete() \
                    .where(topics.c.id == id) \
                    .execute()
                return True
            return False

    def get_topics(self):
        with self.connection_factory.conn() as db:
            topics = Table('topics').bind(db)
            q = topics.select(topics.c.id, topics.c.description, topics.c.created) \
                .order_by(topics.c.created.desc())
            return list(q)

    def update(self):

        vk_request = requests.get(self.url).json()
        new_board = {}
        for item in vk_request['response']['items']:
            new_board[item['id']] = {
                'id': item['id'],
                'description': item['title'],
                'created': datetime.fromtimestamp(item['created']).strftime('%Y-%m-%d %H:%M:%S')
            }
        new_topics_id = set(new_board.keys())

        add_topics_id = new_topics_id - self.topic_ids
        del_topics_id = self.topic_ids - new_topics_id

        new_topics = []
        for topic_id in add_topics_id:
            new_topic = new_board[topic_id]
            self.add_topic(**new_topic)
            new_topics.append(new_topic)

        for topic_id in del_topics_id:
            self.del_topic(topic_id)

        self.topic_ids = new_topics_id

        return new_topics

    def topics_to_description(self, topics):
        return '\n'.join([
            f"[{topic['description']}](https://vk.com/topic-{self.vk_config['GROUP_ID']}_{topic['id']})" for topic in topics
        ])
