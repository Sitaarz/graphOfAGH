from bs4 import BeautifulSoup
from selenium import webdriver
import requests
import psycopg2
import time


class Downloader():
    def __init__(self, web_driver = webdriver.Safari()):
        self.web_driver = web_driver
        self.conn = psycopg2.connect(dbname="postgres", user="postgres", password="1234", host="localhost", port=5432)
        self.cur = self.conn.cursor()
        self.loss = 0
        
        self.users: list[tuple[str, str, str, int]] = [] # list[ tuples[ id, link, name, article_number ]]
        self.users_and_articles: list[tuple[str, list[str]]] = [] # list[ tuples [id, list[ article ids ]]]
        
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
        self.web_driver.get(URI)
        time.sleep(1)


    def _parse_article_number(string) -> int:
        try:
            return int(string.split(' ')[0])
        except ValueError:
            return 0
    
    def get_users(self, URI):
        self.open_page(URI)
        # self.scroll_down(0.5, 15)

        htmlText = self.web_driver.page_source
        soup = BeautifulSoup(htmlText, "lxml")
        anchors = soup.find_all('a', class_='flex flex-row hover:bg-gray-100')
        
        ids = []
        user_names = []
        user_links = []
        article_number = []
        
        for id, anchor in enumerate(anchors):
            divs = anchor.find_all('div')
            
            user_name = divs[0].text
            user_link = 'https://badap.agh.edu.pl' + anchor['href']
            num1 = divs[2].text
            num2 = divs[3].text
            
            art_num = Downloader._parse_article_number(num1) + Downloader._parse_article_number(num2)
            
            ids.append(id + 1)
            user_names.append(user_name)
            user_links.append(user_link)
            article_number.append(art_num)
        
        self.users = list(zip(ids, user_links, user_names, article_number))

    def write_users_to_db(self):
        for id, profile_link, user_name, _ in self.users:
            parts = user_name.split(' ')
            surname = parts[0]
            name = ' '.join(parts[1:])
            
            self.cur.execute('INSERT INTO person_ (person_id, name, surname, profile_link) VALUES (%s,%s,%s,%s)', (id, name, surname, profile_link))


    def get_articles(self):
        for user in self.users:
            self.users_and_articles.append(self.get_articles_of_user(user))

    def write_articles_to_db(self):
        for user_id, articles in self.users_and_articles:
            for article_id in articles:
                self.cur.execute('INSERT INTO research (research_id) VALUES (%s) ON CONFLICT DO NOTHING', (article_id,))
                self.cur.execute('INSERT INTO link (person_id, research_id) VALUES (%s,%s)', (user_id, article_id))
    
    def get_articles_of_user(self, user) -> list[tuple[int, str]]:
        user_id, profile_link, user_name, article_number = user
        
        self.open_page(profile_link)
        self.scroll_down(0.33, 3)
        
        content = self.web_driver.page_source
        soup = BeautifulSoup(content, 'lxml')
        anchors = soup.find_all('a', class_='font-bold p-2 hover:underline details')
        
        article_ids = []
        
        for anchor in anchors:
            article_link = 'https://badap.agh.edu.pl' + anchor['href']
            content = requests.get(article_link).content
            soup = BeautifulSoup(content, 'lxml')
            article_id = soup.find('table', class_ ='w-full').tbody.td.text
    
            article_ids.append(article_id)

            print(user_id, user_name, article_link, article_id)
            print('--------------------------------------')
            
        if len(article_ids) != article_number:
            print(f'Article number mismatch for user {user_name}')
            self.loss += article_number - len(article_ids)

        return (user_id, article_ids)
    
    
    def commit_and_close(self):
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    @staticmethod
    def createTables():
        conn = psycopg2.connect(dbname="postgres", user="postgres", password="1234", host="localhost", port=5432)
        cur = conn.cursor()

        cur.execute("""CREATE TABLE IF NOT EXISTS  person_(
        person_id INT PRIMARY KEY,
        name TEXT,
        surname TEXT,
        profile_link TEXT UNIQUE
        )""")

        cur.execute("""CREATE TABLE IF NOT EXISTS research(
        research_id BIGINT PRIMARY KEY
        )""")

        cur.execute("""CREATE TABLE IF NOT EXISTS link(
        person_id INT REFERENCES person_(person_id),
        research_id BIGINT REFERENCES research(research_id)
        )""")
        conn.commit()

        cur.close()
        conn.close()