# NOT WORKING. USE IBKR_historic_data_retriever.py INSTEAD.


import time

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract, ContractDetails
from ibapi.common import BarData
import pandas as pd
from datetime import datetime, timedelta
import pytz

from IBKR_Product_Listings_data_retriever import IBKRDataRetriever


class MyWrapper(EWrapper):
    def __init__(self, symbols):
        self.symbols = symbols
        self.data = {}
        for symbol in symbols:
            self.data[symbol] = []
        print("Initializing MyWrapper...")

    # def contractDetails(self, reqId: int, contractDetails: ContractDetails):
    #     super().contractDetails(reqId, contractDetails)
    #     printinstance(contractDetails)

    ...
    def historicalData(self, reqId: int, bar: BarData):
        # print(f"self.symbols: {self.symbols} -- reqId: {reqId} -- bar: {bar}")
        print(f"bar: {bar}")
        symbol = self.symbols[reqId - 1]
        self.data[symbol].append({
            'date': bar.date,
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': bar.volume,
            'wap': bar.wap,
            'trade_count': bar.barCount
        })

class MyClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)


def retrieve_data(symbols):
    wrapper = MyWrapper(symbols)
    client = MyClient(wrapper)
    client.disconnect()
    time.sleep(4)
    client.connect("127.0.0.1", 7497, 0)
    print("client.isConnected(): ", client.isConnected())

    # end_time = datetime.now(pytz.timezone('US/Eastern'))
    # end_time = datetime.now()
    end_time = datetime.today()
    # start_time = end_time - timedelta(days=30 * 365)
    end_str = end_time.strftime("%Y%m%d %H:%M:%S")
    # start_str = start_time.strftime("%Y%m%d %H:%M:%S")


    for i, symbol in enumerate(symbols):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART" #"ArcaEdge" #
        contract.currency = "USD"
        # contract.primaryExchange = "ArcaEdge"


        # print(f"contract: {contract}")
        client.reqHistoricalData(i + 1, contract, end_str, "30 Y", "1 day", "TRADES", 0, 2, False, [])

    client.run()

    dfs = {}
    # for symbol in symbols:
    #
    #     print(f"wrapper.data[symbol]: {wrapper.data[symbol]}" )
    #     print("-------")
    #     df = pd.DataFrame(wrapper.data[symbol])
    #     df.set_index('date', inplace=True)
    #     df.sort_index(inplace=True)
    #     dfs[symbol] = df
    #
    # client.disconnect()
    #
    # return dfs


ibkr = IBKRDataRetriever()
main_equity_types = ibkr.get_main_equity_types()
product = main_equity_types.iloc[0]['product_name']
region = main_equity_types.iloc[0]['region']
all_exchanges = ibkr.get_exchanges_for_product_region(product, region)
# print(main_equity_types)
# print(all_exchanges)
country = all_exchanges.iloc[0]['country']
# exchange = all_exchanges.iloc[0]['exchange']
exchange = 'ArcaEdge'
root_path = ibkr.ibkr_constants.SYMBOL_DATA_ROOT_FOLDER
exchange_symbols = ibkr.get_exchange_symbols(product, region, country, exchange)

symbols = exchange_symbols.iloc[10:20]['ibkr_symbol']#['AAPL', 'GOOG', 'MSFT', 'AMZN']
dfs = retrieve_data(symbols)

for symbol, df in dfs.items():
    filename = f"NEW_{symbol}.csv"
    df.to_csv(filename)
