import math
from bs4 import BeautifulSoup
import requests
import logging
import json

from web_driver import WebDriver
from data_base import DataBase


#* ----- LOGGING CONFIGURATION -----
logging.basicConfig(
    level=logging.INFO,
    filename='./graphOfAGH/logs/info.log', filemode='w',
    format='%(asctime)s - %(levelname)s\t%(message)s'
)
#* ---------------------------------


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
                    
                    log_message += f' but article number mismatch {len(self.u_id_2_articles[user["id"]])} vs exp. {user["art_num"]}'
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
          
    def get_articles_from_user(self, user):
        def estimate_scroll_time(art_num: int) -> int:
            if art_num < 256:
                return 1
            else:
                return int(math.sqrt(art_num / 64))
        
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