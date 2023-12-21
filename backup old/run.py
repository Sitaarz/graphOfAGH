from dataDownloader import Downloader
import json


URI = "https://badap.agh.edu.pl/autorzy"


def create_tables(downloader):
    downloader.create_tables()


def initialize(downloader):
    downloader.retrive_articles_from_db()
    with open('./graphOfAGH/users_url.json', 'r') as file:
        downloader.users = json.load(file)

def download(downloader, start=0):
    downloader.get_and_write_articles(start)


def find_user(users, **kwargs):
    """kwargs:
        id
        full_name
    """
    res = []
    if 'id' in kwargs:
        for user in users:
            if user[0] == kwargs['id']:
                res.append(user)
    elif 'name' in kwargs:
        for user in users:
            if kwargs['name'] in user[2]:
                res.append(user)
    return res



def compare_users():
    def compare_log(log_url, log_db, title=''):
        print(title.center(24, '-'))
        print('   url             db   ')
        print(log_url, log_db)

    with open('./graphOfAGH/users_url.json', 'r') as file:
        users_url = json.load(file)
    with open('./graphOfAGH/users_db.json', 'r') as file:
        users_db = json.load(file)

    compare_log(len(users_url), len(users_db), 'LENGTH')

    art_num_url = sum([art_num for _, _, _, art_num in users_url])
    art_num_db = sum([art_num for _, _, _, art_num in users_db])
    compare_log(art_num_url, art_num_db, 'ARTICLE NUMBER')
    
    names_url = [name for _, _, name, _ in users_url]
    names_db = [name for _, _, name, _ in users_db]
    
    missing_names_url = [name for name in names_url if name not in names_db]
    missing_names_db = [name for name in names_db if name not in names_url]
    compare_log(len(missing_names_url), len(missing_names_db), 'MISSING NAMES')
    print(missing_names_url)


def get_users_to_json():
    downloader = Downloader()
    downloader.get_users()
    with open('./graphOfAGH/users_url.json', 'w') as file:
        json.dump(downloader.users, file) 


def get_users_from_db_to_json():
    downloader = Downloader()
    downloader.retrive_users_from_db()
    with open('./graphOfAGH/users_db.json', 'w') as file:
        json.dump(downloader.users, file)


if __name__ == "__main__":
    downloader = Downloader()
    initialize(downloader)
    downloader.get_and_write_articles()
    