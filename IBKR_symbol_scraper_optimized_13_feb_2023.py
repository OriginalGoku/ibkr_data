import time

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from tqdm import tqdm
from file_utility import FileUtility
import logging
from IBKR_constants import IBKRConstants


class AllIBKRSymbols:
    # Class Level Variables

    def __init__(self, verbose=False):

        self.IBKR_constants = IBKRConstants()
        self.save_data_root_folder = self.IBKR_constants.PRODUCT_DATA_ROOT_FOLDER
        self.file_utility = FileUtility(self.save_data_root_folder, self.save_data_root_folder)
        self.verbose = verbose
        # Set up the logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(message)s',
            handlers=[logging.FileHandler(self.IBKR_constants.LOG_FILE_NAME)]
        )



    # Original not optimized
    # def get_text_from_url(the_url):
    #     # print("Loading URL: ", the_url)
    #     confirmed = False
    #     global hdr
    #     hdr["path"] = the_url
    #
    #     while not confirmed:
    #         try:
    #             r = requests.get(the_url, headers=hdr)
    #             r.raise_for_status()
    #
    #             confirmed = True
    #         except requests.exceptions.HTTPError as errh:
    #             print("Http Error:", errh)
    #             logging.warning("Http Error:" + str(errh))
    #         except requests.exceptions.ConnectionError as errc:
    #             print("Error Connecting:", errc)
    #             logging.warning("Error Connecting" + str(errc.status_code))
    #         except requests.exceptions.Timeout as errt:
    #             print("Timeout Error:", str(errt.status_code))
    #             logging.warning("Timeout Error:" + errt)
    #         except requests.exceptions.RequestException as err:
    #             print("Something Other Request Error", err)
    #             logging.warning("Something Other Request Error" + str(err.status_code))
    #
    #         if not confirmed:
    #             print("Waiting 1 sec to see if problem resolved then retry")
    #             time.sleep(1)
    #
    #     # return r.text
    #     return BeautifulSoup(r.text, 'html.parser')

    def get_text_from_url(self, the_url):
        """
        This function loads the URL and returns the text as a BeautifulSoup object
        :param the_url: full url to load
        :return: BeautifulSoup object
        """
        self.IBKR_constants.HDR["path"] = the_url
        # hdr["path"] = the_url

        for _ in range(3):
            try:
                r = requests.get(the_url, headers=self.IBKR_constants.HDR["path"])
                r.raise_for_status()
                return BeautifulSoup(r.text, 'html.parser')
            except requests.exceptions.HTTPError as errh:
                print("Http Error:", errh)
                logging.warning(f"Http Error: {errh}")
            except (requests.exceptions.Timeout, requests.exceptions.RequestException) as err:
                print(f"Request Error: {err}")
                logging.warning(f"Request Error: {err.status_code}")

            print("Waiting 1 sec to see if problem resolved then retry")
            time.sleep(1)

        raise Exception("Failed to load the URL after 3 attempts.")

    # 1. Get all Product Listings
    def generate_IBKR_product_listings(self):
        """
        This function generates a list of all Interactive Brokers products by visiting the product URL,
        'https://www.interactivebrokers.com/en/index.php?f=1563&p=', and
        parsing the HTML content and extracting relevant information such as product name, region, and link.

        Returns:
        None. The extracted data is saved as a dataframe in a csv file at the specified location.

        """
        all_products = []
        product_url = self.IBKR_constants.IBKR_PRODUCT_URL
        for product, product_link in self.IBKR_constants.ALL_PRODUCTS_NAMES.items():
            link_to_load = product_url + product_link
            page_soup = self.get_text_from_url(link_to_load)
            if self.verbose:
                print("---------", product, "---------")
                print(f'Loading {link_to_load}')

            results = self._find_regions(product, page_soup)

            # Convert the dictionary to a list of dictionaries
            observations = []
            for i in range(len(results['product_name'])):
                observation = {
                    'product_name': results['product_name'][i],
                    'region': results['region'][i],
                    'link': results['link'][i]
                }
                observations.append(observation)

            all_products.extend(observations)
        if self.verbose:
            print(f"Saving IBKR Product Listings to {self.save_data_root_folder}")
        df = pd.DataFrame(all_products)
        self.file_utility.save_data(df, "", self.save_data_root_folder, index_flag=False)

    # 1.1 Find all regions for each product in Product Listings
    def _find_regions(self, product_name, soup):
        """
        Given a product name and a BeautifulSoup object, find the regions for the product.
        :param product_name: name of the product
        :param soup: BeautifulSoup object for the page containing the product information
        :return: A dictionary containing the product name, region names, and region links
        """
        ul_element = soup.find("ul", class_="subtabsmenu")
        regions = {'product_name': [], 'region': [], 'link': []}
        try:
            all_li = ul_element.find_all('li')
            for li in all_li:
                regions["product_name"].append(product_name)
                regions["region"].append(li.text.strip())
                regions["link"].append((self.IBKR_constants.IBKR_URL + li.a['href']).strip())
                if self.verbose:
                    print('Region', li.text.strip())
                    print('Region Link', (self.IBKR_constants.IBKR_URL + li.a['href']).strip())
                    print("Creating Directory: ",
                          self.save_data_root_folder + "/" + product_name + "/" + li.text.strip())
                self.file_utility.create_directory(
                    self.save_data_root_folder + "/" + product_name + "/" + li.text.strip())
        except:
            print("No Region Found")

        return regions

    # 2. Get all exchanges for each region
    def generate_regional_exchange_information(self):
        """
        Generates a dataframe of exchanges information for each product/region combination in the all_products dataframe.

        Returns:
            None (saves dataframes to disk)
        """
        print("========= ALL EXCHANGES =========")
        product_listings = self.file_utility.load_file("", self.save_data_root_folder, None)

        for product_counter in tqdm(range(len(product_listings['link']))):
            link_to_load = product_listings['link'][product_counter]
            page_soup = self.get_text_from_url(link_to_load)

            product_name = product_listings['product_name'][product_counter]
            product_region = product_listings['region'][product_counter]
            print(f"--------- {product_name}/{product_region} ---------")
            print(f"Loading {link_to_load}")

            list_of_exchanges = self._get_region_exchange_list(page_soup)
            if len(list_of_exchanges) > 0:
                exchange_save_path = f"{product_name}/{product_region}"
                exchange_file_name = self._generate_metadata_file_name(product_name, product_region)

                self.file_utility.save_data(list_of_exchanges, exchange_save_path, exchange_file_name, index_flag=False)

    # 2.1 Get all exchanges for each region
    def _get_region_exchange_list(self, soup):
        """
        Extract exchange information from the given soup object and return a data frame

        Parameters:
            soup (bs4.BeautifulSoup): BeautifulSoup object of the page containing exchange information

        Returns:
            pd.DataFrame: DataFrame with exchange information including country, exchange, exchange_link,
                          products, hours, exchange_website.
        """
        exchange_info = {'country': [], 'exchange': [], 'exchange_link': [],
                         'products': [], 'hours': [], 'exchange_website': []}

        # Extract rows from the page
        rows = soup.select('tbody tr')
        current_country = None

        # Iterate through each row to extract information
        for row in rows:
            # Extract columns from the row
            cols = row.select('td')
            starting_col = 0

            # Extract information for a row with 4 columns
            if len(cols) == 4:
                # Extract Rowspan and Country Name
                country = cols[0].text.strip().split('\n')[-1]
                match = re.search('rowspan="(\d+)"', str(cols[0]))

                # Extract rowspan value if available
                if match:
                    rowspan = match.group(1)

                # Set current country information
                current_country = country
                starting_col = 1

            # Extract information for a row with 3 columns
            elif len(cols) == 3:
                starting_col = 0

            # Extract exchange, exchange link, products, and hours information
            market_center = cols[starting_col].text.strip()
            market_center_link = (self.IBKR_constants.IBKR_BASE_URL + cols[starting_col].a['href']).strip()
            products = cols[starting_col + 1].text.strip()
            hours = cols[starting_col + 2].text.strip()

            # Extract exchange website information
            exchange_soup = self.get_text_from_url(market_center_link)
            exchange_div = exchange_soup.find('div', {'class': 'table-responsive'})
            if (exchange_div is not None) & (exchange_div.a is not None):
                exchange_website_address = exchange_div.a['href']
            else:
                exchange_website_address = 'Missing Website information'

            # Add extracted information to exchange_info dictionary
            exchange_info['country'].append(current_country)
            exchange_info['exchange'].append(market_center)
            exchange_info['exchange_link'].append(market_center_link)
            exchange_info['products'].append(products)
            exchange_info['hours'].append(hours)
            exchange_info['exchange_website'].append(exchange_website_address)

            # Print information if verbose is True
            if self.verbose:
                print("=====================" + current_country + "=====================")
                print("exchange: ", market_center)
                print("exchange_link: ", self.IBKR_constants.IBKR_BASE_URL + market_center_link)
                print("products: ", products)
                print("hours: ", hours)
                print("==================================")

        return pd.DataFrame(exchange_info)

    def generate_all_meta_data(self):
        product_listings = self.file_utility.load_file("", self.save_data_root_folder, None)
        for product_counter in tqdm(range(len(product_listings['product_name']))):
            product_name = product_listings['product_name'][product_counter]
            product_region = product_listings['region'][product_counter]
            self._generate_meta_data_for_region(product_name, product_region)

    def _generate_meta_data_for_region(self, product, region):
        print("========= LOAD EXCHANGE INFO =========")
        print("Loading ", product + "/" + region)
        region_folder = "/" + product + "/" + region + "/"
        main_folder = self.save_data_root_folder + region_folder
        file_name = self._generate_metadata_file_name(product, region)
        # exchange_data = pd.read_csv(main_folder + product + " " + region + " Exchanges.csv")
        exchange_data = pd.read_csv(main_folder + file_name + ".csv")

        # print(exchange_data)
        for exchange_counter in tqdm(range(len(exchange_data['exchange_link']))):
            exchange_country = exchange_data.iloc[exchange_counter]['country'] if len(
                exchange_data.iloc[exchange_counter]['exchange_country']) > 0 else "Global"

            exchange_folder = exchange_country + "/" + exchange_data.iloc[exchange_counter][
                'exchange'].replace("/", '-')

            # make folder for the exchange
            # self.file_utility.create_directory(main_folder + exchange_folder)
            exchange_file_name = self._generate_metadata_file_name(product, region, exchange_country,
                                                                   exchange_data.iloc[exchange_counter][
                                                                       'exchange'])
            exchange_link = exchange_data.iloc[exchange_counter]['exchange_link']
            product_info = self._get_exchange_product_info(self.get_text_from_url(exchange_link), exchange_link)

            # self.save_file(exchange_file_name, product_info, exchange_folder)
            print("Calling save_data from generate_meta_data_for_region")
            self.file_utility.save_data(product_info, region_folder + exchange_folder, exchange_file_name,
                                        index_flag=False)

    def _get_exchange_product_info(self, page_soup, link):
        print("========= PRODUCT INFO =========")
        print("Loading ", link)
        # print(page_soup.prettify())
        product_dictionary = {'product_name': [], 'product_link': [], 'first_page_link': [], 'product_no_of_pages': []}
        product_dictionary['product_name'], product_dictionary['product_link'] = \
            self._get_exchange_product_name_link(page_soup, link)
        # print(product_dictionary)
        # if there is information available in the exchange page then go through the information
        if len(product_dictionary['product_name']) > 0:
            for prod_link in product_dictionary['product_link']:
                info_page = self.get_text_from_url(prod_link)
                f1, n1 = self._get_exchange_product_first_link_and_no_of_pages(info_page, prod_link)
                product_dictionary['first_page_link'].extend(f1)
                product_dictionary['product_no_of_pages'].extend(n1)
        else:
            return pd.DataFrame({'product_name': [self.IBKR_constants.MISSING_DATA_TEXT],
                                 'product_link': [self.IBKR_constants.MISSING_DATA_TEXT],
                                 'first_page_link': [self.IBKR_constants.MISSING_DATA_TEXT],
                                 'product_no_of_pages': [self.IBKR_constants.MISSING_DATA_TEXT]}, index=[0])

        # product_dictionary['first_page_link'], product_dictionary['product_no_of_pages'] = self.get_product_first_link_and_no_of_pages(page_soup)
        return pd.DataFrame(product_dictionary)

    def _get_exchange_product_name_link(self, page_soup, link):
        print("========= GET PRODUCT LIST =========")

        div = page_soup.find('div', {'class': 'btn-selectors'})
        product_name = []
        product_link = []
        if div is None:
            return [], []
        # if not div.contents:
        for content in div.contents:
            if len(content.text.strip()) > 0:
                print("Product Type: ", content.text.strip())
                product_name.append(content.text.strip())
                link_to_append = None
                if content.a['href'] != '#':
                    link_to_append = self.IBKR_constants.IBKR_URL + content.a['href']
                elif content.a['href'] == '#':
                    link_to_append = link

                product_link.append(link_to_append)
                print("Product Link: ", link_to_append)
                # exchange_link.append(exchange_link_address)

        # else:
        #     return [], []

        return product_name, product_link

    def _get_exchange_product_first_link_and_no_of_pages(self, page_soup, link):
        print("========= GET PRODUCT FIRST LINK AND NO OF PAGES =========")
        first_link = []
        no_of_pages = []
        ul = page_soup.find('ul', {'class': 'pagination'})

        try:
            lis = ul.find_all('li')
            f = self.IBKR_constants.IBKR_URL + lis[1].a['href']
            n = lis[-2].text
        except:
            f = link
            n = 1

        no_of_pages.append(n)
        first_link.append(f)

        return first_link, no_of_pages

    def generate_all_symbols(self):
        product_listings = self.file_utility.load_file("", self.save_data_root_folder, None)
        # Go through all products and regions
        for product_counter in tqdm(range(len(product_listings['product_name']))):
            product = product_listings.iloc[product_counter]['product_name']
            region = product_listings.iloc[product_counter]['region']
            file_name = self._generate_metadata_file_name(product, region, None, None)
            # retrieve region information
            region_data = self.file_utility.load_file(product + "/" + region, file_name, None)
            # go through all region information
            for country_counter in tqdm(range(len(region_data['country']))):
                country = region_data.iloc[country_counter]['country']
                exchange = region_data.iloc[country_counter]['exchange']
                # exchange_file_name = self._generate_metadata_file_name(product, region, country, exchange)
                # exchange_data = self.file_utility.load_file(product + "/" + region + "/" + country + "/" + exchange, exchange_file_name, None)

                self._generate_symbols_for_exchange(product, region, country, exchange)

    def _generate_symbols_for_exchange(self, product, region, country, exchange):
        print("========= AVAILABLE SYMBOLS FOR " + exchange + "=========")
        file_path = product + "/" + region + "/" + country + "/" + \
                    exchange.replace("/", '-')
        exchange_metadata_file_name = self._generate_metadata_file_name(product, region, country, exchange)
        exchange_meta_data = self.file_utility.load_file(file_path, exchange_metadata_file_name, None)

        for exchange_counter in tqdm(range(len(exchange_meta_data))):
            exchange_product_symbols = {'ibkr_symbol': [], 'product_description': [], 'product_link': [], 'conid': [],
                                        'symbol': [],
                                        'currency': []}

            product_name = exchange_meta_data.iloc[exchange_counter]['product_name']
            # print(f"Checking Exchange {exchange} - product_name: {product_name}")
            product_path = file_path + "/" + product_name
            file_name = exchange_metadata_file_name.replace("Metadata", "List of " + product_name + " Symbols")
            # print(f"exchange_meta_data[exchange_counter]['product_no_of_pages']: {exchange_meta_data.iloc[exchange_counter]['product_no_of_pages']}")

            if (not self._is_exchange_product_in_log(exchange, product_name)) and \
                    (str(
                        exchange_meta_data.iloc[exchange_counter][
                            'product_no_of_pages']).strip() != self.IBKR_constants.MISSING_DATA_TEXT):
                print("starting to retrieve information for {} in {}".format(product_name, exchange))
                for page_number in range(int(exchange_meta_data.iloc[exchange_counter]['product_no_of_pages'])):
                    exchange_link = exchange_meta_data.iloc[exchange_counter]['first_page_link'][:-1] + str(
                        page_number + 1)
                    page_soup = self.get_text_from_url(exchange_link)
                    ibkr_symbol, product_description, product_link, conid, symbol, currency = self._exchange_symbols(
                        page_soup)
                    exchange_product_symbols['ibkr_symbol'].extend(ibkr_symbol)
                    exchange_product_symbols['product_description'].extend(product_description)
                    exchange_product_symbols['product_link'].extend(product_link)
                    exchange_product_symbols['conid'].extend(conid)
                    exchange_product_symbols['symbol'].extend(symbol)
                    exchange_product_symbols['currency'].extend(currency)

                # self.file_utility.create_directory(product_path)
                print("Saving data to: ", product_path + "/" + file_name)
                exchange_product_symbols = pd.DataFrame(exchange_product_symbols)
                self.file_utility.save_data(exchange_product_symbols, product_path, file_name, False)
                logging.info(f"Country: {country} - Exchange: {exchange} - Product: {product_name}")
            else:
                print(f"Exchange {exchange} : {product_name} already saved")

    def _exchange_symbols(self, soup):
        tbodies = soup.find_all('tbody')
        index_ = 2
        # Some of the exchanges do not provie "Order Types" information
        if not soup.find(string='Order Types - Click to Expand'):
            index_ = 1

        td = tbodies[index_].find_all('td')
        td_counter = 0
        ibkr_symbol = []
        product_description = []
        product_link = []
        conid = []
        symbol = []
        currency = []

        for t in td:
            if td_counter % 4 == 0:
                print("IBKR Symbol: ", t.text)
                # self.all_symbol_info['ibkr_symbol'].append(t.text)
                ibkr_symbol.append(t.text)
            if td_counter % 4 == 1:
                # some products in some of the exchanges do not have a link
                try:
                    print("Product description: ", t.a.text)
                    # self.all_symbol_info['product_description'].append(t.a.text)
                    product_description.append(t.a.text)
                    print("product_link: ", str(t.a['href']).split("'")[1].replace(' ', ''))
                    # self.all_symbol_info['product_link'].append(str(t.a['href']).split("'")[1].replace(' ', ''))
                    product_link.append(str(t.a['href']).split("'")[1].replace(' ', ''))
                    print("conid: ", str(t.a['href']).split("conid=")[1].replace(' ', '').split("'")[0])
                    # self.all_symbol_info['conid'].append(str(t.a['href']).split("conid=")[1].replace(' ', '').split("'")[0])
                    conid.append(str(t.a['href']).split("conid=")[1].replace(' ', '').split("'")[0])
                except:
                    print("Product description: ", t.text)
                    # self.all_symbol_info['product_description'].append(t.a.text)
                    product_description.append(t.text)
                    # print("product_link: ", str(t.a['href']).split("'")[1].replace(' ', ''))
                    # self.all_symbol_info['product_link'].append(str(t.a['href']).split("'")[1].replace(' ', ''))
                    # product_link.append(str(t.a['href']).split("'")[1].replace(' ', ''))
                    product_link.append(self.IBKR_constants.MISSING_DATA_TEXT)

                    # print("conid: ", str(t.a['href']).split("conid=")[1].replace(' ', '').split("'")[0])
                    # self.all_symbol_info['conid'].append(str(t.a['href']).split("conid=")[1].replace(' ', '').split("'")[0])
                    # conid.append(str(t.a['href']).split("conid=")[1].replace(' ', '').split("'")[0])
                    conid.append(self.IBKR_constants.MISSING_CONID)

            elif td_counter % 4 == 2:
                print("Symbol: ", t.text)
                symbol.append(t.text)
                # self.all_symbol_info['symbol'].append(t.text)
            elif td_counter % 4 == 3:
                print("Currency: ", t.text)
                # self.all_symbol_info['currency'].append(t.text)
                currency.append(t.text)
            td_counter += 1

        return ibkr_symbol, product_description, product_link, conid, symbol, currency

    @staticmethod
    def _is_exchange_product_in_log(exchange_name: str, product_name: str) -> bool:
        """
        Check if the given exchange and product are present in the log file.

        :param exchange_name: The name of the exchange to search for.
        :param product_name: The name of the product to search for.
        :return: A boolean indicating whether the exchange and product were found in the log file.
        """
        # Open the log file
        with open("log.txt", "r") as f:
            # Read the contents of the file into a string variable
            log_file = f.read()

        # Split the log_file string into separate lines
        lines = log_file.split("\n")

        # Loop through each line in the log file
        for line in lines:
            # Check if the exchange_name and product_name are present in the current line
            if f"Exchange: {exchange_name} - Product: {product_name}" in line:
                # If they are present, return True
                return True
        # If the loop completes without finding a match, return False
        return False


# list_of_products = list(self.IBKR_constants.ALL_PRODUCTS_NAMES.keys())
# product = list_of_products[0]
# region = REGIONS[0]
# country = 'United States'
# exchange = 'Bats Global Markets (BATS)'

ibkr_data = AllIBKRSymbols(verbose=False)
# ibkr_data.generate_IBKR_product_listings()
# ibkr_data.generate_regional_exchange_information()
# ibkr_data.generate_all_meta_data()
# ibkr_data.generate_all_symbols()
# ibkr_data._generate_meta_data_for_region()
