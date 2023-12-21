import psycopg2


class DataBase:
    def __init__(self, host='localhost', user='postgres',
                 password='1234', port=5432):
        self.connection_args = {
            'host': host,
            'user': user,
            'password': password,
            'port': port
        }
        self.conn = None
        self.cur = None

    def create_tables(self):
        self.connect()
        self.create_table_researchers()
        self.create_table_articles()
        self.create_table_links()
        self.commit_and_close()

    def create_table_researchers(self):
        self.cur.execute("""DROP TABLE IF EXISTS researchers CASCADE""")
        self.cur.execute("""CREATE TABLE IF NOT EXISTS researchers(
                            id INT PRIMARY KEY,
                            name TEXT,
                            surname TEXT,
                            profile_link TEXT UNIQUE
                            )""")

    def create_table_articles(self):
        self.cur.execute("""DROP TABLE IF EXISTS articles CASCADE""")
        self.cur.execute("""CREATE TABLE IF NOT EXISTS articles(
                            id BIGINT PRIMARY KEY
                            )""")

    def create_table_links(self):
        self.cur.execute("""DROP TABLE IF EXISTS links CASCADE""")
        self.cur.execute("""CREATE TABLE IF NOT EXISTS links(
                            researcher_id INT REFERENCES researchers(id),
                            article_id BIGINT REFERENCES articles(id)
                            )""")

    def connect(self):
        if self.conn is not None and self.cur is not None:
            raise Exception('Already connected')

        self.conn = psycopg2.connect(**self.connection_args)
        self.cur = self.conn.cursor()

    def commit_and_close(self):
        if self.conn is None and self.cur is None:
            raise Exception('Not connected')

        self.conn.commit()
        self.cur.close()
        self.conn.close()

        self.conn, self.cur = None, None

    def add_user(self, id, name, surname, link):
        try:
            self.cur.execute("""INSERT INTO researchers VALUES(%s, %s, %s, %s)
                            ON CONFLICT DO NOTHING""", (id, name, surname, link))
        except psycopg2.errors.UniqueViolation:
            logging.error(f'User {id}: {name} {surname} already in database')

    def add_article(self, id):
        try:
            self.cur.execute("""INSERT INTO articles VALUES(%s)
                            ON CONFLICT DO NOTHING""", (id,))
        except psycopg2.errors.UniqueViolation:
            logging.error(f'Article {id} already in database')

    def add_link(self, r_id, a_id):
        try:
            self.cur.execute("""INSERT INTO links VALUES(%s, %s)
                            ON CONFLICT DO NOTHING""", (r_id, a_id))
        except psycopg2.errors.UniqueViolation:
            logging.error(f'Link {r_id} {a_id} already in database')

    def get_links(self):
        self.connect()
        self.cur.execute('SELECT * FROM links')
        links = self.cur.fetchall()
        self.commit_and_close()
        return links

    def remove_users_links(self, r_id):
        try:
            self.cur.execute("""DELETE FROM links WHERE researcher_id = %s""", (r_id,))
        except Exception as e:
            logging.error(f'Failed to remove links for user {r_id}')
            logging.error(e)