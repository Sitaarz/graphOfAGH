from enum import Enum
import math
from bs4 import BeautifulSoup
from selenium import webdriver
import requests
import psycopg2
import time
import logging
import json


#* ----- LOGGING CONFIGURATION -----
logging.basicConfig(
    level=logging.INFO,
    filename='./graphOfAGH/logs/info.log', filemode='w',
    format='%(asctime)s - %(levelname)s\t%(message)s'
)
#* ---------------------------------


class WebDriver:
    class Web_driver_type(Enum):
        safari = 1
        chrome = 2
        edge = 3
        firefox = 4
        
    def __init__(self, web_driver_type: Web_driver_type):
        self.web_driver_type = web_driver_type
        self.driver = self.new_web_dirver()

    def new_web_dirver(self) -> webdriver:
        match self.web_driver_type:
            case self.Web_driver_type.safari:
                return webdriver.Safari()
            case self.Web_driver_type.chrome:
                return webdriver.Chrome()
            case self.Web_driver_type.edge:
                return webdriver.Edge()
            case self.Web_driver_type.firefox:
                return webdriver.Firefox()


    def refresh(self) -> None:
        self.driver.quit()
        self.driver = self.new_web_dirver()

    def scroll_down(self, intervals=0.25, max_passive_time=10):
        passive_scrolls_limit = max_passive_time / intervals
        passive_scrolls_counter = 0
        logging.debug(f'Scrolling down with intervals {intervals} and passive limit {passive_scrolls_limit}')
        
        def get_height():
            return self.driver.execute_script("return document.body.scrollHeight;")
        
        while passive_scrolls_counter < passive_scrolls_limit:
            prev_height = get_height()
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(intervals)
            
            if get_height() == prev_height:
                passive_scrolls_counter += 1
            else:
                passive_scrolls_counter = 0
    
    def open_page(self, URL, limit=10):
        count = 0
        while count < limit:
            try:
                self.driver.get(URL)
                logging.debug(f'Page {URL} opened')
                return
            except:
                self.refresh()
                count += 1
        logging.critical(f'Page {URL} failed to open!')
        raise Exception(f'Page {URL} failed to open!')



class DataBase:
    def __init__(self, host='localhost', user='postgres',
                 password='1234', port=5432):
        self.connection_args = {
            'host':     host,
            'user':     user,
            'password': password,
            'port':     port
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



class Downloader:
    def __init__(self, web_driver: WebDriver, data_base: DataBase,
                 home_page='https://badap.agh.edu.pl'):
        self.WD = web_driver
        self.DB = data_base
        
        self.home_page = home_page
        
        self.users: list[dict[str: str|int]] = []
        """list[ dict{ id, name, surname, link, art_num }]"""
        self.u_id_2_articles: dict[str: list[str]] = {}
        """dict{ id: list[ article_ids ]}"""
        
        self.stats = {
            'art_loss': 0,
            'art_gain': 0,
        }
        

    def get_users(self):
        self.WD.open_page(self.home_page + '/autorzy')
        self.WD.scroll_down()
        
        soup = BeautifulSoup(self.WD.driver.page_source, 'lxml')
        anchors = soup.find_all('a', class_='flex flex-row hover:bg-gray-100')
        
        for u_id, anchor in enumerate(anchors):
            divs = anchor.find_all('div')
            
            full_name = divs[0].text.strip()
            name_parts = full_name.split(' ')
            num1 = divs[2].text.strip()
            num2 = divs[3].text.strip()
            
            def parse_art_num(num: str) -> int:
                try:
                    return int(num.split(' ')[0])
                except ValueError:
                    return 0
            
            #* ----- ADD USER -----
            self.users.append({
                'id': u_id + 1,
                'name': ' '.join(name_parts[1:]),
                'surname': name_parts[0],
                'link': self.home_page + anchor['href'],
                'art_num': parse_art_num(num1) + parse_art_num(num2)
            })
    
    def save_users(self):
        self.DB.connect()
        for user in self.users:
            self.DB.add_user(user['id'], user['name'], user['surname'], user['link'])
        self.DB.commit_and_close()
    
    
    def get_and_save_articles(self, start=0, batch_size=1000):
        id_2_art = {}
        current_art_num = 0
        
        for user in self.users:
            if user['id'] < start: continue
            log_message = f'User {user["id"]}: {user["surname"]} {user["name"]}'
            
            if user['id'] in self.u_id_2_articles:
                log_message += f' already in database'
                
                if len(self.u_id_2_articles[user['id']]) == user['art_num'] or user['art_num'] < 0:
                    log_message += f' with correct article number {user["art_num"]}'
                    logging.info(log_message)
                    print(log_message)
                    continue
                else:
                    self.DB.remove_users_links(user['id'])
                    
                    log_message += f' but article number mismatch {len(self.u_id_2_articles[user['id']])} vs exp. {user["art_num"]}'
                    log_message += f'\n\tclear and reatempt getting {user["art_num"]} articles'
                    print(log_message)
                    logging.warning(log_message)
            else:
                log_message += f' not in database, getting {user["art_num"]} articles'
                print(log_message)
                logging.info(log_message)
            
            u_arts = self.get_articles_from_user(user)
            current_art_num += len(u_arts)
            id_2_art[user['id']] = u_arts
            
            if current_art_num >= batch_size:
                print('db dump')
                self.WD.refresh()
                self.save_articles(id_2_art)
                id_2_art = {}
                current_art_num = 0
            else:
                print(f'next db dump: {current_art_num}/{batch_size}')
        
        self.save_articles(id_2_art)
            
    def save_articles(self, id_2_art: dict[str: list[str]]=None):
        if id_2_art is None:
            id_2_art = self.u_id_2_articles
        else:
            self.u_id_2_articles.update(id_2_art)
            
        logging.info(f'Saving {len(id_2_art)} users with {sum([len(arts) for arts in id_2_art.values()])} articles\n\tCurrent article loss: {self.stats["art_loss"]} - and gain: {self.stats["art_gain"]}')
        
        self.DB.connect()
        for u_id, articles in id_2_art.items():
            for article in articles:
                self.DB.add_article(article)
                self.DB.add_link(u_id, article)
        self.DB.commit_and_close()
          
    def get_articles_from_user(self, user: int):
        def estimate_scroll_time(art_num: int) -> int:
            if art_num < 256:
                return 1
            else:
                return math.sqrt(art_num / 64)
        
        self.WD.open_page(user['link'])
        limit = estimate_scroll_time(user['art_num'])
        self.WD.scroll_down(0.25, limit)
        
        soup = BeautifulSoup(self.WD.driver.page_source, 'lxml')
        anchors = soup.find_all('a', class_='font-bold p-2 hover:underline details')
        
        articles = []
        for i, anchor in enumerate(anchors):
            a_link = self.home_page + anchor['href']
            soup = BeautifulSoup(requests.get(a_link).content, 'lxml')
            a_id = soup.find('table', class_='w-full').tbody.td.text
            
            articles.append(a_id)
            print(f'\t{i+1}/{user["art_num"]}', end='\r', flush=True)
        print()
        
        if len(articles) != user['art_num']:
            logging.warning(f'\tArticle number mismatch {len(articles)} vs exp. {user["art_num"]}')
            if len(articles) > user['art_num']:
                self.stats['art_loss'] += user['art_num'] - len(articles)
            else:
                self.stats['art_gain'] += len(articles) - user['art_num']
        
        return articles
    
    def retrive_articles(self):
        links = self.DB.get_links()
        for u_id, a_id in links:
            if u_id not in self.u_id_2_articles:
                self.u_id_2_articles[u_id] = []
            self.u_id_2_articles[u_id].append(a_id)
    
    
    def users_from_json(self, path: str='./graphOfAGH/data/users.json'):
        with open(path, 'r') as file:
            self.users = json.load(file)

    def users_to_json(self, path: str='./graphOfAGH/data/users.json'):
        with open(path, 'w') as file:
            json.dump(self.users, file)
            


def main():
    WD = WebDriver(WebDriver.Web_driver_type.safari)
    DB = DataBase()
    # DB.create_tables()
    
    downloader = Downloader(WD, DB)
    # downloader.get_users()
    # downloader.save_users()
    # downloader.users_to_json()
    downloader.users_from_json()
    downloader.retrive_articles()
    downloader.get_and_save_articles()
    
if __name__ == "__main__":
    main()