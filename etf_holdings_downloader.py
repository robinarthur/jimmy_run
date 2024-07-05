#!/usr/bin/env python3
import math
import time
import pandas as pd
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait

# Suppress Selenium debug logs
logging.getLogger('selenium').setLevel(logging.ERROR)

'''
    File name: holdings_downloader.py
    Author: Piper Batey
    Date created: 7/13/2021
    Date last modified: 9/3/2021
    Python Version: 3.8
    Description: A simple Python script that downloads 
    the holdings of one or more ETFs into .csv files.
'''

# Configurable Constants
SCHWAB_URL_TEMPLATE = "https://www.schwab.wallst.com/schwab/Prospect/research/etfs/schwabETF/index.asp?type=holdings&symbol={}"

class HoldingsDownloader:
    def __init__(self, symbol, raw_mode=False, log_mode=False, sort_mode=False, window_mode=True, quiet_mode=False, wait_time=15):
        # Initialize variables
        self.firefox_options = Options()  # default: headless
        self.etf_symbols = [symbol]
        self.valid_etfs = []
        self.num_files = 0
        self.wait_time = wait_time
        self.raw_mode = raw_mode
        self.firefox_options.headless = not window_mode
        
        if self.sort_mode:
            self.etf_symbols.sort()

    def _convert_units_to_float(self, x):  # gets raw data for dataframe .apply()
        if isinstance(x, float):
            return x
        start = 1 if x[0] == '$' else 0
        if x[0] == '-':  # set negative portfolio weights to 0
            return 0
        if x[-1] == '%':
            return float(x[:-1]) / 100
        if x[-1] == 'K':
            return float(x[start:-1]) * 1e3
        if x[-1] == 'M':
            return float(x[start:-1]) * 1e6
        if x[-1] == 'B':
            return float(x[start:-1]) * 1e9
        return float(x[start:])

    def _get_etf_from_schwab(self, etf_symbol):
        driver = webdriver.Firefox(options=self.firefox_options)
        driver.implicitly_wait(self.wait_time)
        wait = WebDriverWait(driver, 30, poll_frequency=1)
        url = SCHWAB_URL_TEMPLATE.format(etf_symbol)
        try:
            driver.get(url)
            show_sixty_items = driver.find_element(By.XPATH, "//a[@perpage='60']")
            show_sixty_items.click()
        except (ec.NoSuchElementException, ec.WebDriverException):
            driver.quit()
            return False

        try:
            page_elt = wait.until(ec.visibility_of_element_located((By.CLASS_NAME, "paginationContainer")))
            num_pages = math.ceil(float(page_elt.text.split(" ")[4]) / 60)
        except ec.StaleElementReferenceException:
            page_elt = wait.until(ec.visibility_of_element_located((By.CLASS_NAME, "paginationContainer")))
            num_pages = math.ceil(float(page_elt.text.split(" ")[4]) / 60)

        dataframe_list = [pd.read_html(driver.page_source)[1]]
        for current_page in range(2, num_pages + 1):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            next_button = driver.find_element(By.XPATH, f"//li[@pagenumber='{current_page}']")
            driver.execute_script("arguments[0].click();", next_button)
            while True:  # wait until the new data has loaded (read_html() is from pandas so can't use selenium wait)
                time.sleep(.25)
                df = pd.read_html(driver.page_source, match="Symbol")[0]
                if not df.equals(dataframe_list[-1]):
                    break
            dataframe_list.append(df)
        # end while
        concat_result = pd.concat(dataframe_list)  # merge into a single dataframe
        result_df = concat_result.drop_duplicates()
        result_df.columns = ['Symbol', 'Description', 'Portfolio Weight', 'Shares Held', 'Market Value']
        if self.raw_mode:  # strip symbols and units
            result_df['Portfolio Weight'] = result_df['Portfolio Weight'].apply(self._convert_units_to_float)
            result_df['Shares Held'] = result_df['Shares Held'].apply(self._convert_units_to_float)
            result_df['Market Value'] = result_df['Market Value'].apply(self._convert_units_to_float)
        result_df.to_csv(f"{etf_symbol}-holdings.csv", index=False)  # create the csv
        driver.quit()
        return True
        # _get_etf_from_schwab()

    def run_schwab_download(self):
        for symbol in self.etf_symbols:
            if symbol in self.valid_etfs:  # skip duplicates
                continue
            if self._get_etf_from_schwab(symbol):
                self.num_files += 1
                self.valid_etfs.append(symbol)

    def print_end_summary(self):
        print(f"\n{self.num_files} file(s) have been generated for {len(self.valid_etfs)} ETF(s):")
        for symbol in self.valid_etfs:
            print(f"{symbol}-holdings.csv")
