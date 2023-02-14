from IBKR_constants import IBKRConstants
from file_utility import FileUtility


class IBKRDataRetriever:
    def __init__(self):
        self.ibkr_constants = IBKRConstants()
        self.file_utility = FileUtility()
        self.main_data = self.get_main_equity_types()

    def get_main_equity_types(self):
        """
        Get a list of all equity types.
        :return: List[str], a list of all equity types
        """
        return self.file_utility.load_file(self.ibkr_constants.PRODUCT_DATA_ROOT_FOLDER,
                                           self.ibkr_constants.PRODUCT_DATA_ROOT_FOLDER + '.csv')

    def get_exchanges_for_product_region(self, product, region):
        """
        Get a list of all exchanges for a given product region.
        :param product: str, the product to get the exchanges for
        :param region: str, the region to get the exchanges for
        :return: List[str], a list of all exchanges for the given product region
        """
        file_name = self.ibkr_constants.generate_file_name(product, region)
        file_path = f"{self.ibkr_constants.PRODUCT_DATA_ROOT_FOLDER}/{product}/{region}"

        return self.file_utility.load_file(file_path, file_name)

    def get_exchange_symbols(self, product, region, country, exchange):
        """
        Get a list of all symbols for a given product region exchange.
        :param product: str, the product to get the symbols for
        :param region: str, the region to get the symbols for
        :param exchange: str, the exchange to get the symbols for
        :return: List[str], a list of all symbols for the given product region exchange
        """
        file_name = self.ibkr_constants.generate_file_name(product, region, country, exchange, product)
        file_path = f"{self.ibkr_constants.PRODUCT_DATA_ROOT_FOLDER}/{product}/{region}/{country}/{exchange}/{product}"
        return self.file_utility.load_file(file_path, file_name)


