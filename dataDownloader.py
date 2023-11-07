from bs4 import BeautifulSoup
from selenium import webdriver
import requests
import psycopg2
import time


class Downloader():
    def __init__(self, URI):
        self.URI = URI

    def getUsers(self):
        conn = psycopg2.connect(dbname="postgres", user="postgres", password="1234", host="localhost", port=5432)
        cur = conn.cursor()



        driver = webdriver.Firefox()

        driver.get(self.URI)


        time.sleep(1)

        while True:
            previousHeight = driver.execute_script('return document.body.scrollHeight;')
            driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
            time.sleep(1)
            newHeight= driver.execute_script('return document.body.scrollHeight;')

            if previousHeight == newHeight:
                break

        htmlText = driver.page_source
        soup = BeautifulSoup(htmlText, "lxml")
        anchors = soup.find_all('a', class_='flex flex-row hover:bg-gray-100')
        fulNames = [anchor.div.text for anchor in anchors]
        links = ['https://badap.agh.edu.pl'+anchor['href'] for anchor in anchors]

        for i, link in enumerate(links):
            fullName = fulNames[i]
            parts = fullName.split(' ')
            surname = parts[0]
            name = parts[1]
            if len(parts) == 3:
                name = parts[1]+' '+parts[2]

            cur.execute('INSERT INTO person_ (person_id,name, surname, profile_link) VALUES (%s,%s,%s,%s)',
                        (i+1, name, surname, link))

            driver.get(link)
            time.sleep(1)

            while True:
                previousHeight = driver.execute_script('return document.body.scrollHeight;')
                driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
                time.sleep(1)
                newHeight = driver.execute_script('return document.body.scrollHeight;')

                if previousHeight == newHeight:
                    break

            content = driver.page_source
            soup = BeautifulSoup(content, 'lxml')

            researches_links = ['https://badap.agh.edu.pl'+content['href'] for content in soup.find_all('a', class_='font-bold p-2 hover:underline details')]


            for research_link in researches_links:
                content = requests.get(research_link).content
                time.sleep(1)
                print(content)
                soup = BeautifulSoup(content, 'lxml')
                research_id = soup.find('table', class_ ='w-full').tbody.td.text

                cur.execute('INSERT INTO research (research_id) VALUES (%s) ON CONFLICT DO NOTHING', (research_id,))
                cur.execute('INSERT INTO link (person_id, research_id) VALUES (%s,%s)', (i+1, research_id))

                print(i+1, name, surname, link, research_id)
                print('--------------------------------------')

        conn.commit()
        cur.close()
        conn.close()

    def createTables(self):
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