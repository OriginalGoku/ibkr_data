import time

import requests
from bs4 import BeautifulSoup
import logging
import pandas as pd
import os
import re

ALL_USD_PRODUCTS_NAME = {'Stocks': 'stk',
                         'Options': 'opt',
                         'Futures': 'fut',
                         'FOPs': 'fop',
                         'ETFs': 'etf',
                         'Warrants': 'war',
                         'Structured Products': 'iopt',
                         'SSFs': 'ssf',
                         'Currencies': 'fx',
                         'Metals': 'cmdty',
                         'Indices': 'ind',
                         'Fixed Income': 'bond',
                         'Mutual Funds': 'mf'}
REGIONS = {"North America": "",
           "Europe": "europe_",
           "Asia-Pacific": "asia_", }
IBKR_BASE_URL = 'https://www.interactivebrokers.com/en/'

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
    def __init__(self, verbose=False):
        self.all_symbol_info = {'ibkr_symbol': [], 'product_description': [], 'product_link': [], 'conid': [],
                                'symbol': [],
                                'currency': []}
        self.generate_required_folders()
        self.all_exchange_info = {'country': [], 'market_center_details': [], 'market_center_details_link': [],
                                  'products': [], 'hours': []}
        self.verbose = verbose

    @staticmethod
    def generate_required_folders():
        for folder in ALL_USD_PRODUCTS_NAME.keys():
            product_folder = 'Product List/' + folder
            if not os.path.exists(product_folder):
                os.makedirs(product_folder)
                for region in REGIONS.keys():
                    if not os.path.exists(product_folder + '/' + region):
                        os.makedirs(product_folder + '/' + region)

    @staticmethod
    def get_text_from_url(the_url):
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

    def get_symbols_from_soup(self, soup):
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

    def find_regions(self, soup):
        #view-source:https://www.interactivebrokers.com/en/index.php?f=1563&p=stk
        # find the first ul element with the given class
        ul_element = soup.find("ul", class_="your-class-name")

        # find all ul elements with the given class
        ul_elements = soup.find_all("ul", class_="your-class-name")
    def get_all_products(self):
        for region_name, region_link in REGIONS.items():
        for product_name in ALL_USD_PRODUCTS_NAME:
            print("Getting all symbols for product: ", product_name)
            url = "https://www.interactivebrokers.com/en/index.php?f=1563&p=" + ALL_USD_PRODUCTS_NAME[
                product_name]
            soup = self.get_text_from_url(url)

            # data = []
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
                market_center_link = cols[starting_col].a['href']
                products = cols[starting_col + 1].text.strip()
                hours = cols[starting_col + 2].text.strip()

                # data.append([current_country, market_center, market_center_link, products, hours])
                self.all_exchange_info['country'].append(current_country)
                self.all_exchange_info['market_center_details'].append(market_center)
                self.all_exchange_info['market_center_details_link'].append(market_center_link)
                self.all_exchange_info['products'].append(products)
                self.all_exchange_info['hours'].append(hours)

                # market_center = cols[0].text.strip()
                # market_center_link = cols[0].a['href']
                # products = cols[1].text.strip()
                # hours = cols[2].text.strip()
                if self.verbose:
                    print("=====================" + current_country + "=====================")
                    print("market_center: ", market_center)
                    print("market_center_link: ", IBKR_BASE_URL + market_center_link)
                    print("products: ", products)
                    print("hours: ", hours)
                    print("==================================")
                #
                # data.append([current_country, market_center, market_center_link, products, hours])

            # df = pd.DataFrame(data, columns=['Country/Region', 'Market Center Details', 'Link', 'Products', 'Hours'])
            # df =
            # df.to_csv('result.csv', index=False)

            # counter = 0
            # all_td = tbodies[0].find_all('td')
            # # print(all_td)
            # td_counter = 0
            # for td in all_td:
            #     print(td.text)
            #     try:
            #         print(td.a['href'])
            #     except:
            #         pass
            #     print('------------------')
            # for td in all_td:
            #     if td_counter == 0:
            #         print("Country: ", td.text)
            #         self.all_exchange_info['country'].append(td.text)
            #     elif td_counter % 4 == 1:
            #         print("Market Center Details: ", td.text)
            #         self.all_exchange_info['market_center_details'].append(td.text)
            #         print("Market Center Details Link: ", IBKR_BASE_URL+str(td.a['href']).split("'")[0].replace(' ', ''))
            #         self.all_exchange_info['market_center_details_link'].append(IBKR_BASE_URL+str(td.a['href']).split("'")[0].replace(' ', ''))
            #     elif td_counter % 4 == 2:
            #         print("Products: ", td.text)
            #         self.all_exchange_info['products'].append(td.text)
            #     elif td_counter % 4 == 3:
            #         print("Hours: ", td.text)
            #         self.all_exchange_info['hours'].append(td.text)
            #     print("------------------")
            #     td_counter += 1
            # store all_exchange_info in a csv file
            print(self.all_exchange_info)
            df = pd.DataFrame(self.all_exchange_info)
            df.to_csv('all_ibkr_exchange.csv', index=False)
            break


ibkr_data = AllIBKRSymbols()
ibkr_data.get_all_products()

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
