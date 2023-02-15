import time

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import BarData
import pandas as pd
from datetime import datetime, timedelta


class MyWrapper(EWrapper):
    def __init__(self):
        self.data = []

    def historicalData(self, reqId: int, bar: BarData):
        print(bar)
        self.data.append({
            'date': bar.date,
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': bar.volume,
            'average': bar.wap,
            'barCount': bar.barCount
        })

class MyClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)

def retrieve_data():
    wrapper = MyWrapper()
    client = MyClient(wrapper)
    client.disconnect()
    print("client.isConnected(): ", client.isConnected())
    time.sleep(5)
    client.connect("127.0.0.1", 7497, 0)
    print("client.isConnected(): ", client.isConnected())

    end_time = datetime.now()
    start_time = end_time - timedelta(days=365)
    end_str = end_time.strftime("%Y%m%d %H:%M:%S")
    start_str = start_time.strftime("%Y%m%d %H:%M:%S")

    contract = Contract()
    contract.symbol = "SPY"
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"
    contract.primaryExchange = "NYSE"

    client.reqHistoricalData(1, contract, end_str, "365 D", "1 day", "TRADES", 0, 1, False, [])

    client.run()

    df = pd.DataFrame(wrapper.data)
    print(df)
    df.set_index('date', inplace=True)
    df.sort_index(inplace=True)

    client.disconnect()

    return df

data = retrieve_data()
data.to_csv('SPY_1_day_bars.csv')
