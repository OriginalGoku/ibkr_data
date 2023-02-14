

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
# from ibapi.ticktype import TickTypeEnum
import time
import random
import pandas as pd
from datetime import datetime

from file_utility import FileUtility
from IBKR_Product_Listings_data_retriever import IBKRDataRetriever



class IBClient(EWrapper, EClient):
    def __init__(self, duration: str, symbol_info_storage_path: str):
        EClient.__init__(self, self)
        self.data = {}
        self.duration = duration
        self.exchange_symbol_save_path = symbol_info_storage_path
        self.file_utility = FileUtility()

    def historicalData(self, reqId, bar):
        print("HistoricalData. ReqId:", reqId, "BarData.", bar)
        symbol = self.data[reqId]["symbol"]
        if symbol not in self.data:
            self.data[symbol] = []
        self.data[symbol].append([bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.wap,
                                  bar.barCount])
        print(bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.wap, bar.barCount)

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
        print(f"Saving {symbol} to {symbol}_{self.duration}_daily_data.csv")

        self.file_utility.save_data(df, self.exchange_symbol_save_path, f"{symbol}_{self.duration}_daily_data.csv",
                                    index_flag=False)
        df.to_csv(f"{symbol}_{self.duration}_daily_data.csv", index=True, index_label="date")
        print(f"Historical data for {symbol} has been saved to {symbol}_{self.duration}_daily_data.csv.")


def retrieve_historical_data(symbols_df: pd.DataFrame, security_type: str, exchange: str) -> None:
    print("Start retrieving historical data...")
    duration = "50 Y"
    client = IBClient(duration)
    client.connect("127.0.0.1", 7497, clientId=0)
    print(client.isConnected())

    # start = end - timedelta(days=365 * 30)

    # extract "ibkr_symbol" and "currency" information
    symbols = symbols_df[["ibkr_symbol", "currency"]]
    end = datetime.now()

    # loop through symbols to create Contract() objects and make API call
    reqId = 0
    for index, row in symbols.iterrows():
        ibkr_symbol = row["ibkr_symbol"]
        currency = row["currency"]

        contract = Contract()
        contract.symbol = ibkr_symbol
        contract.secType = security_type.upper()
        contract.currency = currency
        contract.exchange = exchange #"SMART"

        print("Contract information: ", contract.symbol, contract.secType, contract.currency, contract.exchange)
        # This code was omitted from the original code
        client.data[reqId] = {"symbol": ibkr_symbol, "currency": currency, "exchange": exchange, "security_type": security_type}

        # make API call with specified parameters
        client.reqHistoricalData(reqId, contract, end.strftime("%Y%m%d %H:%M:%S"), duration, "1 day", "TRADES", 0, 1,
                                 False, [])
        reqId += 1
        # sleep for 1-3 second to avoid exceeding the API rate limit‘s 1 req/sec
        # sleep_time = random.randint(1, 3)
        # time.sleep(sleep_time)

    print("Historical data retrieval is complete.")
    # client.run()
    print("Client is running")
    # client.disconnect()

def retrieve_historical_data_original(symbols):
    print("Start retrieving historical data...")
    duration = "50 Y"
    client = IBClient(duration)
    client.connect("127.0.0.1", 7497, clientId=0)
    print(client.isConnected())

    end = datetime.now()
    # start = end - timedelta(days=365 * 30)

    reqId = 2
    for symbol in symbols:
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        client.data[reqId] = {"symbol": symbol}
        print(contract.symbol)
        client.reqHistoricalData(reqId, contract, end.strftime("%Y%m%d %H:%M:%S"), duration, "1 day", "TRADES", 0, 1,
                                 False, [])
        print("Requeting historical data for " + symbol + " Finished")

        reqId += 1
        # sleep for 1-3 second to avoid exceeding the API rate limit‘s 1 req/sec
        sleep_time = random.randint(1, 3)
        time.sleep(sleep_time)

    print("Historical data retrieval is complete.")
    client.run()
    print("Client is running")
    # client.disconnect()


if __name__ == "__main__":
    print('Start Main Program')
    # symbols = ["AAPL", "IBM", "GOOG"]
    # symbols = ["XLK", "XLV", "XLY"] #, "XLP", "XLE", "XLF", "XLB", "XLU"]
    # symbols = ["XLE", "XLB", "XLF", 'AGG', 'DIA', 'DOG', 'EEM', 'EFA', 'EWA', 'EWJ', 'FXI', 'GLD', 'IJH', 'IWM', 'PSQ',
    #            'QQQ', 'RWM', 'SH', 'SLV', 'SPY', 'TLT', 'UNG', 'USO', 'VTI', 'XBI', 'XHB', 'XLR']

    # symbols = ['XLRE','XLC']

    ibkr = IBKRDataRetriever()
    main_equity_types = ibkr.get_main_equity_types()
    product = main_equity_types.iloc[0]['product_name']
    region = main_equity_types.iloc[0]['region']
    all_exchanges = ibkr.get_exchanges_for_product_region(product, region)
    # print(main_equity_types)
    # print(all_exchanges)
    country = all_exchanges.iloc[0]['country']
    exchange = all_exchanges.iloc[0]['exchange']
    exchange_symbols = ibkr.get_exchange_symbols(product, region, country , exchange)
    # print(one_exchange)

    retrieve_historical_data(exchange_symbols.iloc[:30], ibkr.ibkr_constants.ALL_PRODUCTS_NAMES[product],exchange)

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
