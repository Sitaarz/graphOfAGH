import math
from bs4 import BeautifulSoup
from selenium import webdriver
import requests
import psycopg2
import time


class Downloader():
    def __init__(self, web_driver_type = 'safari'):
        self.web_driver_type = web_driver_type
        self.web_driver = Downloader.new_web_dirver(web_driver_type)
        self.connection_args = {
            'host': 'localhost',
            'user': 'postgres',
            'password': '1234',
            'port': 5432
        }
        self.conn = None
        self.cur = None
        self.article_loss = 0
        self.article_gain = 0
        
        self.users: list[tuple[str, str, str, int]] = []
        """list[ ( id, link, name, article_number ) ]"""
        self.users_and_articles: dict[str: list[str]] = {}
        """dict{ id, list[ article_ids ]}"""
        
    def reload_web_driver(self):
        self.web_driver.quit()
        self.web_driver = Downloader.new_web_dirver(self.web_driver_type)
    
    @staticmethod
    def new_web_dirver(web_driver_type):
        match web_driver_type:
            case 'safari': return webdriver.Safari()
            case 'chrome': return webdriver.Chrome()
            case 'firefox': return webdriver.Firefox()
            case unknown: raise Exception(f'Unsupported web driver type: {unknown}')
    
    def scroll_down(self, sleep_time, passive_scrolls_limit):
        passive_scrolls_counter = 0
        
        while passive_scrolls_counter < passive_scrolls_limit:
            previousHeight = self.web_driver.execute_script('return document.body.scrollHeight;')
            self.web_driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
            time.sleep(sleep_time)
            newHeight = self.web_driver.execute_script('return document.body.scrollHeight;')

            if previousHeight == newHeight:
                passive_scrolls_counter += 1
            else:
                passive_scrolls_counter = 0
    
    def open_page(self, URI):
        count, limit = 0, 10
        while count < limit:
            try:
                self.web_driver.get(URI)
                return True
            except Exception as e:
                print(e)
                count += 1
                time.sleep(1.5)
        return False


    @staticmethod
    def _parse_article_number(string) -> int:
        try:
            return int(string.split(' ')[0])
        except ValueError:
            return 0
        
    @staticmethod
    def log(message, info=None, end='\n'):
        if info:
            print(f'{message}{" "*(100-len(message))}{info}', end=end)
        else:
            print(message, end=end)
    
    
    def get_users(self):
        URI = 'https://badap.agh.edu.pl/autorzy'
        
        if not self.open_page(URI):
            Downloader.log('Failed to open page', URI)
            return
        
        self.scroll_down(0.25, 40)

        htmlText = self.web_driver.page_source
        soup = BeautifulSoup(htmlText, 'lxml')
        anchors = soup.find_all('a', class_='flex flex-row hover:bg-gray-100')
        
        ids = []
        user_names = []
        user_links = []
        article_number = []
        
        for u_id, anchor in enumerate(anchors):
            divs = anchor.find_all('div')
            
            user_name = divs[0].text
            user_link = 'https://badap.agh.edu.pl' + anchor['href']
            num1 = divs[2].text
            num2 = divs[3].text
            
            art_num = Downloader._parse_article_number(num1) + Downloader._parse_article_number(num2)
            
            ids.append(u_id + 1)
            user_names.append(user_name)
            user_links.append(user_link)
            article_number.append(art_num)
        
        self.users = list(zip(ids, user_links, user_names, article_number))

    def write_users_to_db(self):
        self.connect()
        for u_id, profile_link, user_name, _ in self.users:
            parts = user_name.split(' ')
            surname = parts[0]
            name = ' '.join(parts[1:])
            
            try:
                self.cur.execute('INSERT INTO researchers (id, name, surname, profile_link) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING', (u_id, name, surname, profile_link))
                
            except psycopg2.errors.UniqueViolation:
                print(f'User {user_name} already in database')
        self.commit_and_close()

    def retrive_users_from_db(self):
        self.connect()
        self.cur.execute('SELECT * FROM researchers')
        users = self.cur.fetchall()
        self.users = [(u_id, u_link, f'{surname} {name}', -1) for u_id, name, surname, u_link in users]
            
        self.commit_and_close()


    def get_and_write_articles(self, start_from = 0, write_treshold = 1000):
        us_and_art = {}
        total_articles = 0
        
        for u_id, u_link, u_name, u_art_number in self.users[start_from:]:
            if u_id in self.users_and_articles:
                art_number_db = len(self.users_and_articles[u_id])
                
                Downloader.log(f'{u_id}: {u_name}', 'already in database')
                Downloader.log(f'\tarticle number: {art_number_db}/{u_art_number}\n')
                
                if art_number_db >= u_art_number:
                    continue
                else:
                    print('HOW THAT HAPPEND ?!')
            
            Downloader.log(f'{u_id}: {u_name}', f'getting {u_art_number} articles')
            
            ur_art = self.get_articles_of_user((u_id, u_link, u_name, u_art_number))
            total_articles += len(ur_art[u_id])
            us_and_art.update(ur_art)

            Downloader.log(f'got {len(ur_art[u_id])}', f'{total_articles}/{write_treshold} - to next db dump\n')
            
            if total_articles > write_treshold:
                self.reload_web_driver()
                self.write_articles_to_db(us_and_art)
                us_and_art = {}
                total_articles = 0
                
        self.write_articles_to_db(us_and_art)

    def write_articles_to_db(self, us_and_art = None):
        if us_and_art is None:
            us_and_art = self.users_and_articles
        else:
            self.users_and_articles.update(us_and_art)

        print(f'\n\t\t\t\t{"="*59}')
        print(f'\t\t\t\tINSERTING {len(us_and_art)} USERS WITH TOTAL NUMBER {sum(len(al) for al in us_and_art.values())} OF THEIR ARTICLES')
        print(f'\t\t\t\tCurrent article loss: {self.article_loss} - and gain: {self.article_gain}')
        print(f'\t\t\t\tTime: {time.strftime("%H:%M:%S", time.localtime())}')
        print(f'\t\t\t\t{"="*59}\n')

        self.connect()
        for user_id, articles in us_and_art.items():
            for article_id in articles:
                try:
                    self.cur.execute(
                        'INSERT INTO articles (id) VALUES (%s) ON CONFLICT DO NOTHING',
                        (article_id,))
                    
                    self.cur.execute(
                        'INSERT INTO links (researcher_id, article_id) VALUES (%s,%s) ON CONFLICT DO NOTHING',
                        (user_id, article_id))
                except psycopg2.errors.UniqueViolation:
                    print(f'Article {article_id} already in database')
        self.commit_and_close()
    
    def get_articles_of_user(self, user) -> dict[int: list[str]]:
        user_id, profile_link, user_name, article_number = user
        
        # estimate page loading time limit
        if article_number < 256:
            limit = 4
        else:
            limit = int(math.sqrt(article_number) / 4)
        
        if not self.open_page(profile_link):
            self.article_loss += article_number
            return {user_id: []}
        
        self.scroll_down(0.25, limit)
        
        content = self.web_driver.page_source
        soup = BeautifulSoup(content, 'lxml')
        anchors = soup.find_all('a', class_='font-bold p-2 hover:underline details')
        
        article_ids = []
        
        for anchor in anchors:
            article_link = 'https://badap.agh.edu.pl' + anchor['href']
            content = requests.get(article_link).content
            soup = BeautifulSoup(content, 'lxml')
            article_id = soup.find('table', class_='w-full').tbody.td.text
    
            article_ids.append(article_id)
            print('.', end='', flush=True)
        print()
            
        if len(article_ids) != article_number:
            Downloader.log(f'\t{len(article_ids)}/{article_number} - article number mismatch')
            if len(article_ids) < article_number:
                self.article_loss += article_number - len(article_ids)

        return {user_id: article_ids}
    
    def retrive_articles_from_db(self):
        self.connect()
        self.cur.execute('SELECT * FROM links')
        links = self.cur.fetchall()
        
        self.users_and_articles = {}
        for user_id, article_id in links:
            if user_id in self.users_and_articles:
                self.users_and_articles[user_id].append(article_id)
            else:
                self.users_and_articles[user_id] = [article_id,]
                
        self.commit_and_close()
    
    
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

    def create_tables(self):
        self.connect()
        
        # clear tables
        self.cur.execute("""DROP TABLE IF EXISTS researcher CASCADE""")
        self.cur.execute("""DROP TABLE IF EXISTS articles CASCADE""")
        self.cur.execute("""DROP TABLE IF EXISTS links CASCADE""")

        # create table person
        self.cur.execute("""CREATE TABLE IF NOT EXISTS researchers(
        id INT PRIMARY KEY,
        name TEXT,
        surname TEXT,
        profile_link TEXT UNIQUE
        )""")

        # create table research
        self.cur.execute("""CREATE TABLE IF NOT EXISTS articles(
        id BIGINT PRIMARY KEY
        )""")

        # create table link
        self.cur.execute("""CREATE TABLE IF NOT EXISTS links(
        researcher_id INT REFERENCES researchers(id),
        article_id BIGINT REFERENCES articles(id)
        )""")
        
        self.commit_and_close()