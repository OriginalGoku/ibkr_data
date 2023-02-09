import time

import requests
from bs4 import BeautifulSoup
import logging
import pandas as pd
import os
import re
from tqdm import tqdm

# Currencies and Metal are not included because they have a different format
ALL_PRODUCTS_NAMES = {'Stocks': 'stk',
                      'Options': 'opt',
                      'Futures': 'fut',
                      'FOPs': 'fop',
                      'ETFs': 'etf',
                      'Warrants': 'war',
                      'Structured Products': 'iopt',
                      'SSFs': 'ssf',
                      # 'Currencies': 'fx',
                      # 'Metals': 'cmdty',
                      'Indices': 'ind',
                      'Fixed Income': 'bond',
                      'Mutual Funds': 'mf'}
ALL_PRODUCTS_REGIONS = {'Stocks': [],
                        'Options': [],
                        'Futures': [],
                        'FOPs': [],
                        'ETFs': [],
                        'Warrants': [],
                        'Structured Products': [],
                        'SSFs': [],
                        'Currencies': [],
                        'Metals': [],
                        'Indices': [],
                        'Fixed Income': [],
                        'Mutual Funds': []}

IBKR_BASE_URL = 'https://www.interactivebrokers.com/en/'
IBKR_URL = 'https://www.interactivebrokers.com'

hdr = {
    "authority": "interactivebrokers.com",
    "method": "GET",
    "scheme": "https",
    "accept": "text/html",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "dnt": "1",
    "pragma": "no-cache",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"
}


class AllIBKRSymbols:
    def __init__(self, verbose=False, save_data_root_folder="Product Lists"):
        self.all_products = {'product_name': [], 'product_region': [], 'link': []}
        self.all_symbol_info = {'ibkr_symbol': [], 'product_description': [], 'product_link': [], 'conid': [],
                                'symbol': [],
                                'currency': []}
        self.save_data_root_folder = save_data_root_folder
        self._create_root_folder()
        # The system should automatically generate the required folders
        self.get_product_region()
        self.save_product_region()
        self.verbose = verbose

    def _create_root_folder(self):
        if not os.path.exists(self.save_data_root_folder):
            os.makedirs(self.save_data_root_folder)

    def generate_folder(self, product_name, region):
        if not os.path.exists(self.save_data_root_folder + '/' + product_name + '/' + region):
            os.makedirs(self.save_data_root_folder + '/' + product_name + '/' + region)

    def find_regions(self, product_name, soup):
        # find the first ul element with the given class
        ul_element = soup.find("ul", class_="subtabsmenu")

        try:
            all_li = ul_element.find_all('li')
            for li in all_li:
                print('Region', li.text.strip())
                print('Region Link', (IBKR_URL + li.a['href']).strip())
                self.all_products['product_name'].append(product_name)
                self.all_products['product_region'].append(li.text.strip())
                self.all_products['link'].append((IBKR_URL + li.a['href']).strip())
                self.generate_folder(product_name, li.text.strip())
        except:
            print("No Region Found")

    def get_product_region(self):

        product_url = 'https://www.interactivebrokers.com/en/index.php?f=1563&p='
        for product, product_link in ALL_PRODUCTS_NAMES.items():
            link_to_load = product_url + product_link
            page_soup = self.get_text_from_url(link_to_load)
            print("---------", product, "---------")
            print('Loading ', link_to_load)

            self.find_regions(product, page_soup)

    def save_product_region(self):
        pd.DataFrame(self.all_products).to_csv(self.save_data_root_folder + '/all_ibkr_products_region.csv',
                                               index=False)

    @staticmethod
    def get_text_from_url(the_url):
        # print("Loading URL: ", the_url)
        confirmed = False
        global hdr
        hdr["path"] = the_url

        while not confirmed:
            try:
                r = requests.get(the_url, headers=hdr)
                r.raise_for_status()

                confirmed = True
            except requests.exceptions.HTTPError as errh:
                print("Http Error:", errh)
                logging.warning("Http Error:" + str(errh))
            except requests.exceptions.ConnectionError as errc:
                print("Error Connecting:", errc)
                logging.warning("Error Connecting" + str(errc.status_code))
            except requests.exceptions.Timeout as errt:
                print("Timeout Error:", str(errt.status_code))
                logging.warning("Timeout Error:" + errt)
            except requests.exceptions.RequestException as err:
                print("Something Other Request Error", err)
                logging.warning("Something Other Request Error" + str(err.status_code))

            if not confirmed:
                print("Waiting 1 sec to see if problem resolved then retry")
                time.sleep(1)

        # return r.text
        return BeautifulSoup(r.text, 'html.parser')

    def get_product_exchange_list(self, soup):
        exchange_info = {'country': [], 'market_center_details': [], 'market_center_details_link': [],
                         'products': [], 'hours': []}

        rows = soup.select('tbody tr')
        current_country = None
        # Extract Country information and the first row data
        for row in rows:
            cols = row.select('td')
            starting_col = 0
            if len(cols) == 4:
                # Extract Rowspan and Country Name
                country = cols[0].text.strip().split('\n')[-1]
                match = re.search('rowspan="(\d+)"', str(cols[0]))
                if match:
                    # Rowspan might be useful in future but at the moment it is not
                    rowspan = match.group(1)
                current_country = country
                starting_col = 1

            # Extract the rest of the rows
            elif len(cols) == 3:
                starting_col = 0

            market_center = cols[starting_col].text.strip()
            market_center_link = (IBKR_BASE_URL + cols[starting_col].a['href']).strip()
            products = cols[starting_col + 1].text.strip()
            hours = cols[starting_col + 2].text.strip()

            # data.append([current_country, market_center, market_center_link, products, hours])
            exchange_info['country'].append(current_country)
            exchange_info['market_center_details'].append(market_center)
            exchange_info['market_center_details_link'].append(market_center_link)
            exchange_info['products'].append(products)
            exchange_info['hours'].append(hours)

            # market_center = cols[0].text.strip()
            # market_center_link = cols[0].a['href']
            # products = cols[1].text.strip()
            if self.verbose:
                print("=====================" + current_country + "=====================")
                print("market_center: ", market_center)
                print("market_center_link: ", IBKR_BASE_URL + market_center_link)
                print("products: ", products)
                print("hours: ", hours)
                print("==================================")

        # store all_exchange_info in a csv file
        # print(self.all_exchange_info)
        # df = pd.DataFrame(self.all_exchange_info)
        # df.to_csv('all_ibkr_exchange.csv', index=False)
        # break
        return pd.DataFrame(exchange_info)

    def get_all_exchanges(self):
        print("========= ALL EXCHANGES =========")
        print(self.all_products)
        # for product, product_link in ALL_PRODUCTS_NAMES.items():
        for product_counter in tqdm(range(len(self.all_products['link']))):
            more_page_to_load = True
            link_to_load = self.all_products['link'][product_counter]
            page_soup = self.get_text_from_url(link_to_load)
            print("---------", self.all_products['product_name'][product_counter] + "/" +
                      self.all_products['product_region'][product_counter], "---------")
            print('Loading ', link_to_load)

            list_of_exchanges = self.get_product_exchange_list(page_soup)
            if len(list_of_exchanges) > 0:
                list_of_exchanges.to_csv(self.save_data_root_folder + "/" +
                                             self.all_products['product_name'][product_counter] +
                                             "/" + self.all_products['product_region'][product_counter] +
                                            ".csv", index=False)


    def exchange_symbols(self, soup):
        tbodies = soup.find_all('tbody')
        counter = 0
        td = tbodies[2].find_all('td')
        td_counter = 0

        for t in td:
            if td_counter % 4 == 0:
                print("IBKR Symbol: ", t.text)
                self.all_symbol_info['ibkr_symbol'].append(t.text)
            if td_counter % 4 == 1:
                print("Product description: ", t.a.text)
                self.all_symbol_info['product_description'].append(t.a.text)
                print("product_link: ", str(t.a['href']).split("'")[1].replace(' ', ''))
                self.all_symbol_info['product_link'].append(str(t.a['href']).split("'")[1].replace(' ', ''))
                print("conid: ", str(t.a['href']).split("conid=")[1].replace(' ', '').split("'")[0])
                self.all_symbol_info['conid'].append(str(t.a['href']).split("conid=")[1].replace(' ', '').split("'")[0])
            elif td_counter % 4 == 2:
                print("Symbol: ", t.text)
                self.all_symbol_info['symbol'].append(t.text)
            elif td_counter % 4 == 3:
                print("Currency: ", t.text)
                self.all_symbol_info['currency'].append(t.text)
            td_counter += 1


ibkr_data = AllIBKRSymbols()
ibkr_data.get_all_exchanges()

# ibkr_data.get_product_region()
# ibkr_data.save_product_region()

# ibkr_data.get_all_products()

# text = ibkr_data.get_text_from_url(url)
# soup = ibkr_data.get_text_from_url(url)
# ibkr_data.get_all_symbols(soup)


# save all symbol in a csv file
# df = pd.DataFrame(all_symbol_info)
# df.to_csv('all_ibkr_symbols.csv', index=False)

# # print(soup.prettify())
# all_body = soup.findall('tbody')
# # print(all_body)
# # for body in all_body:
# #     print(body)
# #     print('-----------------------')
# print(len(all_body))
# print(all_body.get())
