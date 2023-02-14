from collections import OrderedDict
from dataclasses import dataclass


@dataclass
class IBKRConstants:
    PRODUCT_DATA_ROOT_FOLDER = 'IBKR Product Listings'
    SYMBOL_DATA_ROOT_FOLDER = 'IBKR Symbols Data'
    LOG_FILE_NAME: str = "log.txt"
    MISSING_DATA_TEXT: str = 'Missing Data From the Website'
    MISSING_CONID: int = 0
    ALL_PRODUCTS_NAMES = OrderedDict([('Stocks', 'stk'),
                                      ('Options', 'opt'),
                                      ('Futures', 'fut'),
                                      ('FOPs', 'fop'),
                                      ('ETFs', 'etf'),
                                      ('Warrants', 'war'),
                                      ('Structured Products', 'iopt'),
                                      ('SSFs', 'ssf'),
                                      ('Indices', 'ind'),
                                      ('Fixed Income', 'bond'),
                                      ('Mutual Funds', 'mf')])

    REGIONS = ["North America", "Europe", "Asia-Pacific"]
    IBKR_URL = 'https://www.interactivebrokers.com'
    IBKR_BASE_URL = 'https://www.interactivebrokers.com/en/'
    IBKR_PRODUCT_LISTINGS_URL = 'https://www.interactivebrokers.com/en/index.php?f=1563&p='

    HDR = {
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

    @staticmethod
    def generate_file_name(product, region, country=None, exchange=None, security_type=None):
        if not exchange:
            return product + "-" + region + " Metadata.csv"
        elif exchange and not security_type:
            return product + "-" + region + " (" + country + ") [" + exchange.replace("/", '-') + "] Metadata.csv"
        elif exchange and security_type:
            return product + "-" + region + " (" + country + ") [" + exchange.replace("/", '-') + "] list of " + security_type + " Symbols.csv"

