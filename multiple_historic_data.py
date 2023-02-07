import csv
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
# from ibapi.ticktype import TickTypeEnum
from datetime import datetime, timedelta


class IBClient(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.data = {}

    def historicalData(self, reqId, bar):
        print("HistoricalData. ReqId:", reqId, "BarData.", bar)
        symbol = self.data[reqId]["symbol"]
        if symbol not in self.data:
            self.data[symbol] = []
        self.data[symbol].append([bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.wap,
                                  bar.barCount])
        print(bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.wap, bar.barCount)

    def historicalDataEnd(self, reqId, start, end):
        symbol = self.data[reqId]["symbol"]
        filename = f"{symbol}.csv"
        with open(filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Date", "Open", "High", "Low", "Close", "Volume"])
            for item in self.data[symbol]:
                writer.writerow(item)
        print(f"Historical data for {symbol} has been saved to {filename}.")


def retrieve_historical_data(symbols):
    print("Start retrieving historical data...")
    client = IBClient()
    client.connect("127.0.0.1", 7497, clientId=0)
    print(client.isConnected())

    end = datetime.now()
    start = end - timedelta(days=365 * 30)

    reqId = 0
    for symbol in symbols:
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        client.data[reqId] = {"symbol": symbol}
        print(contract.symbol)
        client.reqHistoricalData(reqId, contract, end.strftime("%Y%m%d %H:%M:%S"), "30 Y", "1 day", "TRADES", 0, 1,
                                 False, [])

        reqId += 1

    client.run()


if __name__ == "__main__":
    print('Start Main Program')
    symbols = ["AAPL", "IBM", "GOOG"]
    retrieve_historical_data(symbols)


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
