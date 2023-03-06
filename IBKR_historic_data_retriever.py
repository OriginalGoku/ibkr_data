import csv
import os

import pytz
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
# from ibapi.ticktype import TickTypeEnum
import time
import random
import pandas as pd
from datetime import datetime, timezone, timedelta
import logging

from tqdm import tqdm

from file_utility import FileUtility
from IBKR_Product_Listings_data_retriever import IBKRDataRetriever
from line_printer import LinePrinter


class IBClient(EWrapper, EClient):
    def __init__(self, duration: str, root_path: str, exchange_path: str, symbols_saved_logger:str = 'symbol_saved.csv'):
        EClient.__init__(self, self)
        self.data = {}
        self.duration = duration
        self.root_path = root_path
        self.file_utility = FileUtility(verbose=True)
        self.file_utility.create_directory(self.root_path)
        self.exchange_path = exchange_path
        self.file_utility.create_directory(self.root_path + "/" + self.exchange_path)
        self.symbols_saved_logger = symbols_saved_logger
        self.total_number_of_symbols_to_retrieve = 0
        self.saved_symbol_counter = 0
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(message)s',
            handlers=[logging.FileHandler("Symbols_loaded.log")]
        )

    def headTimestamp(self, reqId: int, headTimestamp: str):
        print("HeadTimestamp. ReqId:", reqId, "HeadTimeStamp:", headTimestamp)

    def historicalData(self, reqId, bar):
        symbol = self.data[reqId]["symbol"]
        # print(f"Generating HistoricalData for {symbol}...")
        if symbol not in self.data:
            self.data[symbol] = []
        self.data[symbol].append([bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.wap,
                                  bar.barCount])
        # print(bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.wap, bar.barCount)

    # Original Working Code
    # def historicalDataEnd(self, reqId, start, end):
    #     symbol = self.data[reqId]["symbol"]
    #     filename = f"{symbol}.csv"
    #     with open(filename, "w", newline="") as f:
    #         writer = csv.writer(f)
    #         writer.writerow(["date", "open", "high", "low", "close", "volume", "wap", "trade_count"])
    #         for item in self.data[symbol]:
    #             writer.writerow(item)
    #     print(f"Historical data for {symbol} has been saved to {filename}.")
    def historicalDataEnd(self, reqId, start, end):
        symbol = self.data[reqId]["symbol"]

        df = pd.DataFrame(self.data[symbol], columns=["date", "open", "high", "low", "close", "volume", "wap",
                                                      "trade_count"])
        # Convert the "Date" column into pandas datetime objects
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")

        # Set the "Date" column as the index of the dataframe
        df.set_index("date", inplace=True)

        # Save the dataframe to a csv file without the default index, but with the "date" column as the index
        print(f"Saving reqId {reqId} - self.saved_symbol_counter {self.saved_symbol_counter } : "
              f"{symbol} to {symbol}_{self.duration}_daily_data.csv")

        self.file_utility.save_data(df, f"{symbol}_{self.duration}_daily_data.csv", self.root_path+"/"+self.exchange_path,
                                    index_label="date")

        # df.to_csv(f"{symbol}_{self.duration}_daily_data.csv", index=True, index_label="date")
        # print(f"Historical data for {symbol} has been saved to {symbol}_{self.duration}_daily_data.csv.")
        # logging.info(f"Symbol: {symbol} - Currency: {self.currency}")
        # save_symbol(symbol, self.currency)


        # Open the file in append mode and write the new row to the CSV file
        with open(self.symbols_saved_logger, mode='a', newline='') as file:
            writer = csv.writer(file)
            symbol_row = [datetime.today(), self.data[reqId]['country'], self.data[reqId]['exchange'],
                          self.data[reqId]['security_type'].upper(), symbol, self.data[reqId]['currency'], len(df)]
            writer.writerow(symbol_row)

        self.saved_symbol_counter += 1
        # Can not use this because the returned reqId come at random so we might get the result for the last reqId
        # before the result for the previous reqId's
        if self.total_number_of_symbols_to_retrieve < (self.saved_symbol_counter+1):
            print("All symbols have been retrieved. Exiting program.")
            self.disconnect()
            print("Disconnected from IBKR API.")


def get_new_symbols(all_symbols: pd.DataFrame, saved_symbols: pd.DataFrame) -> pd.DataFrame:
    """
    Return a dataframe containing symbols in all_symbols that are not present in saved_symbols.

    :param all_symbols: A pandas dataframe containing all symbols to check.
    :param saved_symbols: A pandas dataframe containing symbols that have already been saved.
    :return: A pandas dataframe containing symbols in all_symbols that are not present in saved_symbols.
    """

    if len(saved_symbols)<1:
        return all_symbols
    # Merge the two dataframes on "symbol" and "currency"
    # and only select the "symbol" and "currency" columns from the saved_symbols
    merged_symbols = pd.merge(all_symbols, saved_symbols[['ibkr_symbol', 'currency']], on=["ibkr_symbol", "currency"],
                              how="outer", indicator=True)

    # Filter the rows that are only in the all_symbols dataframe
    new_symbols = merged_symbols[merged_symbols['_merge'] == 'left_only']

    # Print the length of the input dataframes and the resulting dataframe for debugging
    # print(f"Input: all_symbols - {len(all_symbols)}, saved_symbols - {len(saved_symbols)} | Output: new_symbols - {len(new_symbols)} ")

    # Print the columns and first few rows of the resulting dataframe for debugging
    # print(new_symbols.columns)
    # print(new_symbols.head())
    # print("--------------------")
    # print(saved_symbols.head())

    # Return the resulting dataframe with all columns except the _merge column
    return new_symbols[new_symbols.columns[:-1]]

def retrieve_historical_data(symbols_df: pd.DataFrame, security_type: str, exchange: str, root_path: str, mode:str,
                             exchange_path: str, country: str) -> None:
    print("Start retrieving historical data...")
    duration = "30 Y"
    client = IBClient(duration, root_path, exchange_path)

    saved_symbols_header_row = ["updating_date", 'country', 'exchange', 'security_type', 'symbol', 'currency', 'no_of_rows']
    # Check if the file exists, and create it with the header row if it does not
    if not os.path.isfile(client.symbols_saved_logger):
        with open(client.symbols_saved_logger, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(saved_symbols_header_row)
            print("Created file: ", client.symbols_saved_logger)


    saved_symbols = client.file_utility.load_file(client.symbols_saved_logger, None)

    # print(saved_symbols.head())
    # print("----============-------")
    saved_symbols.columns = saved_symbols_header_row
    symbols_df = get_new_symbols(symbols_df, saved_symbols)

    # move file pointer to beginning of new line
    if len(symbols_df) > 0:
        with open(client.symbols_saved_logger, 'a') as f:
            f.write('\n')

    client.total_number_of_symbols_to_retrieve = len(symbols_df)
    LinePrinter().print_text("Total number of symbols to retrieve: " + str(client.total_number_of_symbols_to_retrieve))
    # print("Total number of symbols to retrieve: ", client.total_number_of_symbols_to_retrieve)

    if mode == "disconnect":
        print("Disconnecting client")
        client.disconnect()
        time.sleep(5)
        print("client is connected: ", client.isConnected())

    if mode != "test":
        print("Connecting client...")
        client.connect("127.0.0.1", 7497, clientId=0)
        print("client is connected: ", client.isConnected())


    symbols = symbols_df[["ibkr_symbol", "currency"]]
    end_str = (datetime.today()).strftime("%Y%m%d-%H:%M:%S")

    # loop through symbols to create Contract() objects and make API call
    reqId = 0
    # for index, row in symbols.iterrows():
    for index, row in tqdm(symbols.iterrows(), total=symbols.shape[0]):

        ibkr_symbol = row["ibkr_symbol"]
        currency = row["currency"]

        contract = Contract()
        contract.symbol = ibkr_symbol
        contract.secType = security_type.upper()
        contract.currency = currency
        contract.exchange = exchange #"SMART"

        print("Contract no: ", reqId, " : ", contract.symbol, contract.secType, contract.currency, contract.exchange)
        # This code was omitted from the original code
        client.data[reqId] = {"updating_date": datetime.today(), "symbol": ibkr_symbol, "currency": currency, "exchange": exchange,
                              "security_type": security_type, 'country': country}

        if mode != "test":
            # make API call with specified parameters
            client.reqHistoricalData(reqId, contract, end_str, duration, "1 day", "TRADES", 0, 1, False, [])
            # sleep for 1-3 second to avoid exceeding the API rate limit‘s 1 req/sec
            sleep_time = random.randint(1, 3)
            print(f"Sleeping for {sleep_time} seconds...")
            time.sleep(sleep_time)
        reqId += 1


    print("Historical data retrieval is complete.")
    if mode != "test":
        client.run()
    print("Client is connected: ", client.isConnected())

    # time.sleep(20)
    # client.disconnect()

# def retrieve_historical_data_original(symbols):
#     print("Start retrieving historical data...")
#     duration = "50 Y"
#     client = IBClient(duration)
#     client.connect("127.0.0.1", 7497, clientId=0)
#     print(client.isConnected())
#
#     end = datetime.now()
#     # start = end - timedelta(days=365 * 30)
#
#     reqId = 2
#     for symbol in symbols:
#         contract = Contract()
#         contract.symbol = symbol
#         contract.secType = "STK"
#         contract.exchange = "SMART"
#         contract.currency = "USD"
#         client.data[reqId] = {"symbol": symbol}
#         print(contract.symbol)
#         client.reqHistoricalData(reqId, contract, end.strftime("%Y%m%d %H:%M:%S"), duration, "1 day", "TRADES", 0, 1,
#                                  False, [])
#         print("Requeting historical data for " + symbol + " Finished")
#
#         reqId += 1
#         # sleep for 1-3 second to avoid exceeding the API rate limit‘s 1 req/sec
#         sleep_time = random.randint(1, 3)
#         time.sleep(sleep_time)
#
#     print("Historical data retrieval is complete.")
#     client.run()
#     print("Client is running")
#     # client.disconnect()




if __name__ == "__main__":
    print('Start Main Program')
    ibkr = IBKRDataRetriever()
    main_equity_types = ibkr.get_main_equity_types()

    product = main_equity_types.iloc[12]['product_name']
    region = main_equity_types.iloc[12]['region']
    print(f"{product} - {region}")
    # print(stop)
    all_exchanges = ibkr.get_exchanges_for_product_region(product, region)
    country = all_exchanges.iloc[-1]['country']

    # exchange = 'ArcaEdge'
    # exchange = "Chicago Stock Exchange (CHX)"
    exchange = 'Mexican Stock Exchange'
    root_path = ibkr.ibkr_constants.SYMBOL_DATA_ROOT_FOLDER
    # exchange_symbols = ibkr.get_exchange_symbols(product, region, country , exchange)
    exchange_symbols = pd.read_csv("ALL_ETF/Mexico/Stocks-North America (Mexico) [Mexican Stock Exchange (MEXI)] List of ETFs Symbols.csv")

    exchange_path = product + "/" + region + "/" + country + "/" + exchange

    mode = ["test", "train", "disconnect"]

    # run 1: [550, 1000], run 2: [1000, 1500], run 3: [1500, 2000], run 4: [2000, 2500], run 5: [2500, 3000]
    start = 0
    end = 10

    print(len(exchange_symbols))
    retrieve_historical_data(exchange_symbols.iloc[start:end], ibkr.ibkr_constants.ALL_PRODUCTS_NAMES[product], exchange, root_path,
                             mode[2], exchange_path, country)

    # data = {'ibkr_symbol': ['SSCC', 'SYYNY', 'TGRP', 'SODI'],
    #         'product_description': ['Nothing','Nothing','Nothing','Nothing'],
    #         'product_link': ['Nothing','Nothing','Nothing','Nothing'],
    #         'conid': ['192','374','374','374'],
    #         'symbol': ['SSCC', 'SYYNY','TGRP','SODI'],
    #         'currency': ['USD', 'USD','USD','USD']}
    #
    # sample_df = pd.DataFrame(data=data,
    #                   columns=['ibkr_symbol', 'product_description', 'product_link', 'conid', 'symbol', 'currency'])
    #
    #
    # retrieve_historical_data(sample_df, ibkr.ibkr_constants.ALL_PRODUCTS_NAMES[product], exchange, root_path,
    #                          mode[2], exchange_path, country)

# Not Working Code:
# class IbkrHistoricData(EWrapper, EClient):
#     def __init__(self, symbols, duration_str, bar_size_setting, what_to_show, use_rth, format_date, end_time,
#                  frequency):
#         EClient.__init__(self, self)
#         self.symbols = None
#         self.duration_str = duration_str
#         self.bar_size_setting = bar_size_setting
#         self.what_to_show = what_to_show
#         self.use_rth = use_rth
#         self.format_date = format_date
#         self.end_time = end_time
#         self.frequency = frequency
#         self.symbols = symbols
#         self.data = {}
#         self.start_time = {}
#
#     def historicalData(self, reqId, bar):
#         symbol = self.symbols[reqId]
#         if symbol not in self.data:
#             self.data[symbol] = []
#         self.data[symbol].append([bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume])
#
#     def get_historical_data(self):
#         # self.connect("127.0.0.1", 7497, 0)
#         reqId = 0
#         for symbol in self.symbols:
#             self.start_time[symbol] = time.time()
#             # self.reqHistoricalData(symbol, duration_str=self.duration_str, barSizeSetting=self.bar_size_setting,
#             #                        whatToShow=self.what_to_show, useRTH=self.use_rth, formatDate=self.format_date)
#             contract = Contract()
#             contract.symbol = symbol
#             contract.secType = "STK"
#             contract.exchange = "SMART"
#             contract.currency = "USD"
#             self.data[reqId] = {"symbol": symbol}
#             print("Requesting historical data for ", symbol)
#             self.reqHistoricalData(reqId, contract, self.end_time, self.duration_str, self.frequency,
#                                      self.what_to_show, self.use_rth, self.format_date, False, [])
#             time.sleep(2)
#             reqId += 1
#
#         self.run()
#
#     def error(self, reqId, errorCode, errorString):
#         print("Error: ", reqId, " ", errorCode, " ", errorString)
#
#
# if __name__ == "__main__":
#     # symbols = ["XLI", "XLK", "XLV", "XLY", "XLP", "XLE", "XLF", "XLB", "XLU"]
#     symbols = ["XLP", "XLE", "XLF"]
#     param = {
#         'duration_str': "30 Y",
#         'bar_size_setting': "1 day",
#         'what_to_show': 'TRADES',
#         'use_rth': True,
#         'format_date': 1,
#         'end_time': datetime.now().strftime("%Y%m%d %H:%M:%S"),
#         'frequency': '1 day'}
#
#     client = IbkrHistoricData(symbols, **param)
#     client.connect("127.0.0.1", 7497, 0)
#
#
#     print(client.isConnected())
#     if client.isConnected():
#         client.get_historical_data()
#
#         for symbol in symbols:
#             df = pd.DataFrame(client.data[symbol], columns=["date", "open", "high", "low", "close", "volume", "wap",
#                                                             "trade_count"])
#             # Convert the "Date" column into pandas datetime objects
#             df["date"] = pd.to_datetime(df["Date"], format="%Y%m%d")
#
#             # Set the "Date" column as the index of the dataframe
#             df.set_index("date", inplace=True)
#
#             # Save the dataframe to a csv file without the default index, but with the "date" column as the index
#             df.to_csv(f"{symbol}_30_years_daily_data.csv", index=True, index_label="date")
#
#         # API Activity Log
#         with open("API_activity_log.txt", "w") as f:
#             for reqId, data in client.data.items():
#                 f.write(f"{reqId}: {len(data)} bars retrieved\n")
#
#         # API Latency Metrics
#         with open("API_latency_metrics.txt", "w") as f:
#             for symbol, data in client.data.items():
#                 elapsed_time = time.time() - client.start_time[symbol]
#                 f.write(f"{symbol}: Elapsed time = {elapsed_time:.2f} seconds\n")
#
#     else:
#         print("Not connected to TWS")


# =================================================
# Not good code from stack overflow
# from ibapi.client import EClient
# from ibapi.wrapper import EWrapper
# from ibapi.contract import Contract
# import pandas as pd
# import threading
# import time
#
#
# class IBapi(EWrapper, EClient):
#     def __init__(self):
#         EClient.__init__(self, self)
#         self.data = self.reset_data()
#
#     @staticmethod
#     def reset_data():
#         return {'symbol': [],
#          'date': [],
#          'open': [],
#          'high': [],
#          'low': [],
#          'close': [],
#          'volume': [],
#          'wap': [],
#          'barCount': []
#          }
#
#     def historicalData(self, reqId, bar):
#         bar_data_dic = vars(bar)
#         bar_data_dic['symbol'] =
#         # self.data.append([bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume])
#
#
#     def error(self, reqId, errorCode, errorString):
#         print("Error. Id: ", reqId, " Code: ", errorCode, " Msg: ", errorString)
#
#     def historicalDataEnd(self, reqId: int, start: str, end: str):
#         print("HistoricalDataEnd. ReqId:", reqId, "from", start, "to", end)
#         #self.df = pd.DataFrame(self.data)
#
#
# def run_loop():
#     app.run()
#
#
# app = IBapi()
#
# # Create contract object
# ES_contract = Contract()
# ES_contract.symbol = 'ES'
# ES_contract.secType = 'FUT'
# ES_contract.exchange = 'GLOBEX'
# ES_contract.lastTradeDateOrContractMonth = '202209'
#
# # Create contract object
# VIX_contract = Contract()
# VIX_contract.symbol = 'VIX'
# VIX_contract.secType = 'IND'
# VIX_contract.exchange = 'CBOE'
# VIX_contract.currency = 'USD'
#
# # Create contract object
# DAX_contract = Contract()
# DAX_contract.symbol = 'DAX'
# DAX_contract.secType = 'FUT'
# DAX_contract.exchange = 'EUREX'
# DAX_contract.currency = 'EUR'
# DAX_contract.lastTradeDateOrContractMonth = '202209'
# DAX_contract.multiplier = '25'
#
# products = {'ES': ES_contract, 'VIX': VIX_contract, 'DAX': DAX_contract}
#
# nid = 1
#
# app.connect('127.0.0.1', 7497, 123)
# # Start the socket in a thread
# api_thread = threading.Thread(target=run_loop, daemon=True)
# api_thread.start()
# time.sleep(1)  # Sleep interval to allow time for connection to server
#
#
# def fetchdata_function(name, nid):
#     # df = pd.DataFrame()
#     # Request historical candles
#     app.reqHistoricalData(nid, products[name], '', '1 W', '5 mins', 'TRADES', 0, 2, False, [])
#     time.sleep(10)  # sleep to allow enough time for data to be returned
#     df = pd.DataFrame(app.data) #, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
#     df['date'] = pd.to_datetime(df['date'])#, unit='s')
#     df = df.set_index('Date')
#     df.to_csv(str(name) + '5min.csv')
#     print(df)
#
#     app.data = app.reset_data()
#
#
# symbols = ['ES', 'DAX', 'VIX']
#
# for symbol in symbols:
#     fetchdata_function(symbol, nid)
#     nid = nid + 1
#
# app.disconnect()
#
#
