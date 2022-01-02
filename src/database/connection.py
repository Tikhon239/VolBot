from contextlib import contextmanager
from playhouse.db_url import connect


class ConnectionFactory:
    def __init__(self, open_fxn, close_fxn):
        self.open_fxn = open_fxn
        self.close_fxn = close_fxn

    def getconn(self):
        return self.open_fxn()

    def putconn(self, conn):
        return self.close_fxn(conn)

    @contextmanager
    def conn(self):
        try:
            result = self.open_fxn()
            yield result
        finally:
            self.close_fxn(result)


def create_connection_factory(config):
    def open_pg():
        return connect(
            f"postgres+pool://{config['USER']}:{config['PASSWORD']}@{config['HOST']}:{config['PORT']}/{config['DATABASE']}"
        )

    def close_pg(conn):
        conn.close()

    return ConnectionFactory(open_fxn=open_pg, close_fxn=close_pg)
