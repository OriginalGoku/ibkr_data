# NOT WORKING

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import BarData
from ibapi.common import ContractDetails
import pandas as pd
from datetime import datetime, timedelta
import pytz


class MyWrapper(EWrapper):
    def __init__(self, symbols):
        self.symbols = symbols
        self.data = {}
        self.time_zones = {}
        for symbol in symbols:
            self.data[symbol] = []
            self.time_zones[symbol] = None

    def historicalData(self, reqId: int, bar: BarData):
        symbol = self.symbols[reqId - 1]
        self.data[symbol].append({
            'date': bar.date,
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': bar.volume,
            'average': bar.average,
            'barCount': bar.barCount
        })

    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        symbol = self.symbols[reqId - 1]
        self.time_zones[symbol] = contractDetails.timeZoneId


class MyClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)


def retrieve_data(symbols):
    wrapper = MyWrapper(symbols)
    client = MyClient(wrapper)
    client.connect("127.0.0.1", 7497, 0)

    end_time = datetime.now(pytz.timezone('US/Eastern'))
    start_time = end_time - timedelta(days=30 * 365)
    end_str = end_time.strftime("%Y%m%d %H:%M:%S")
    start_str = start_time.strftime("%Y%m%d %H:%M:%S")

    for i, symbol in enumerate(symbols):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        contract.primaryExchange = "NASDAQ"

        client.reqContractDetails(i + 1, contract)

    client.run()

    for i, symbol in enumerate(symbols):
        tz = pytz.timezone(wrapper.time_zones[symbol])
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        contract.primaryExchange = "NASDAQ"
        contract.timeZoneId = wrapper.time_zones[symbol]

        end_time_tz = end_time.astimezone(tz)
        start_time_tz = start_time.astimezone(tz)

        end_str = end_time_tz.strftime("%Y%m%d %H:%M:%S")
        start_str = start_time_tz.strftime("%Y%m%d %H:%M:%S")

        client.reqHistoricalData(i + 1, contract, end_str, "30 Y", "1 day", "TRADES", 0, 1, False, [])

    client.run()

    dfs = {}
    for symbol in symbols:
        df = pd.DataFrame(wrapper.data[symbol])
        df.set_index('date', inplace=True)
        df.sort_index(inplace=True)
        dfs[symbol] = df

    client.disconnect()

    return dfs


symbols = ['AAPL', 'GOOG', 'MSFT', 'AMZN']
dfs = retrieve_data(symbols)

for symbol, df in dfs.items():
    filename = f"{
