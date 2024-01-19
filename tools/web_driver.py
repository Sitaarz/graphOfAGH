from enum import Enum
import logging
from selenium import webdriver
import time


class WebDriver:
    class WebDriverType(Enum):
        safari = 1
        chrome = 2
        edge = 3
        firefox = 4

    def __init__(self, web_driver_type: WebDriverType):
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
