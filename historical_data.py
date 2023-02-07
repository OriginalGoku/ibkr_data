from ibapi.client import *
from ibapi.wrapper import *
import pandas as pd


class GetHistoricalData(EClient, EWrapper):
    def __init__(self):
        EClient.__init__(self, self)
        self.bars = []
        self.symbol = "SPY"
        self.end_date = '20000101'
        self.symbol_text = "AB"
        self.search_results = {'conId': [],
                               'symbol': [],
                               'secType': [],
                               'primaryExchange': [],
                               'currency': [],
                               'derivSecTypes': [],
                               'description': [],
                               'issuerId': []}

    def nextValidId(self, orderId: int):
        mycontract = Contract()
        # https://interactivebrokers.github.io/tws-api/basic_contracts.html
        symbols = ['SPY','AAPL']

        for symbol in symbols:
            mycontract.symbol = symbol#self.symbol
            mycontract.secType = "STK"
            mycontract.exchange = "SMART"
            mycontract.currency = "USD"
            param = {
                'reqId': orderId,
                'contract': mycontract,
                # 'endDateTime': self.end_date + '-00:00:00',
                'endDateTime': '',
                # 'durationStr': '30 Y',
                'durationStr': '2 D',
                'barSizeSetting': '1 day',
                'whatToShow': 'TRADES',
                'useRTH': 0,
                'formatDate': 1,
                'keepUpToDate': 0,
                'chartOptions': []
            }
            self.reqHistoricalData(**param)

    def historicalData(self, reqID, bar):
        print(f"Historical Data: {bar}")
        line = vars(bar)
        print(line.keys())
        self.bars.append(line)

    def historicalDataEnd(self, reqId, start, end):
        print(f"End of Historical Data")
        print("------- Saving Data -------")
        bar_df = pd.DataFrame(self.bars)
        print(bar_df.columns)
        bar_df.to_csv(self.symbol + "_" + self.end_date + '.csv')
        print("0000000000000")

        # self.reqMatchingSymbols(reqId, self.symbol_text)


    # Useless: it only returns 16 results
    def symbolSamples(self, reqId: int, contractDescriptions: ListOfContractDescription):
        super().symbolSamples(reqId, contractDescriptions)
        print("Symbol Samples. Request Id: ", reqId)
        for contractDescription in contractDescriptions:
            derivSecTypes = ""
            for derivSecType in contractDescription.derivativeSecTypes:
                derivSecTypes += " "
                derivSecTypes += derivSecType
            print("Contract: conId:%s, symbol:%s, secType:%s primExchange:%s, "
                  "currency:%s, derivativeSecTypes:%s, description:%s, issuerId:%s" % (
                      contractDescription.contract.conId,
                      contractDescription.contract.symbol,
                      contractDescription.contract.secType,
                      contractDescription.contract.primaryExchange,
                      contractDescription.contract.currency, derivSecTypes,
                      contractDescription.contract.description,
                      contractDescription.contract.issuerId,
                  ))
            self.search_results['conId'].append(contractDescription.contract.conId)
            self.search_results['symbol'].append(contractDescription.contract.symbol)
            self.search_results['secType'].append(contractDescription.contract.secType)
            self.search_results['primaryExchange'].append(contractDescription.contract.primaryExchange)
            self.search_results['currency'].append(contractDescription.contract.currency)
            self.search_results['derivSecTypes'].append(derivSecTypes)
            self.search_results['description'].append(contractDescription.contract.description)
            self.search_results['issuerId'].append(contractDescription.contract.issuerId)

            # .append(contractDescription.contract)

        pd.DataFrame(self.search_results).to_csv("searching_for_" + self.symbol_text + '.csv')


ibkr = GetHistoricalData()
ibkr.connect("127.0.0.1", 7497, 1000)
ibkr.run()
# ibkr.get_list_of_symbols("IB")
