import time
from abc import ABC, abstractmethod
import websocket
import json
import communication as com
from sortedcontainers import SortedDict
from collections import deque
import numpy as np
import pandas as pd
import os

'''
implementation of orderbook data streaming for different exchanges using websocket API. All exchange's orderbook class
is a subclass of OrderBook.

List of exchanges implemented:
    Coinbase
    Gemini
'''

def nameConvention(product_list, exchange):
    '''
    convert product to proper name for corresponding exchange
    :param product: name of product, i.e. BTC-USD, please capitalize all letters and put "-" between coin and base
    :param exchange: name of exchange, i.e. Coinbase
    :return: a proper name tag of the product for that exchange
    '''
    res = []
    for product in product_list:
        if not (product.isupper()) and ('-' in product):
            raise NameError('the product name format is wrong')

        if exchange  == "Coinbase":
            return product_list

        elif exchange == "Gemini":
            res.append(product.split('-')[0]+product.split('-')[1])

        elif exchange == "EnigmaX":
            return product_list

        else:
            raise Exception('such exchange has not been definied')

    return res




class OrderBook(ABC):
    """Base class for an orderbook."""
    def __init__(self, product_list):
        self.product_list = product_list
        self.bids_dict = dict()
        self.asks_dict = dict()
        for product in product_list:
            self.bids_dict[product] = SortedDict()
            #self.bids_dict[product][-np.inf] = np.nan    # default value, prevent outOfIndexError
            self.asks_dict[product] = SortedDict()
            #self.asks_dict[product][d] = np.nan    # default value, prevent outOfIndexError
        self.dataStream = None
        self.name = None

    @abstractmethod
    def getSnapshot(self, product, snapshotMessage):
        pass

    @abstractmethod
    def update(self, product, updateMessage):
        pass

    @abstractmethod
    def run(self):
        pass

    def getBestAsk(self, product):
        try:
            return self.asks_dict[product].peekitem(0)[0]
        except IndexError:
            return np.nan
    def getBestBid(self, product):
        try:
            return self.bids_dict[product].peekitem(-1)[0]
        except IndexError:
            return np.nan
    def getBestBidSize(self, product):
        try:
            return self.bids_dict[product].peekitem(-1)[1]
        except IndexError:
            return np.nan

    def getBestAskSize(self, product):
        try:
            return self.asks_dict[product].peekitem(0)[1]
        except IndexError:
            return np.nan

    def close(self):
        self.dataStream.close()

    def printAllTopBook(self):
        for product in self.product_list:
            print(f"{product}: bid-{self.getBestBid(product)} {self.getBestBidSize(product)} ask-{self.getBestAsk(product)} {self.getBestAskSize(product)}")

        print()


    # def __str__(self):
    #     askLevels = sorted(list(self.asks.keys()), key=lambda x: float(x))[0:10]
    #     bidLevels = sorted(list(self.bids.keys()), key=lambda x: float(x), reverse=True)[0:10]
    #     header = ["Size \t\t\t Bid       Ask \t\t\t Size \n"]
    #     for i in range(0, 10):
    #         header.append("{0:10} \t @ {1:12} || {3:4} \t @ {2:15} \n".format(
    #             str(self.bids[bidLevels[i]]), str(bidLevels[i]),
    #             str(self.asks[askLevels[i]]), str(askLevels[i]))
    #         )
    #     return "".join(header)


class CoinBaseOrderBook(OrderBook):
    """Class for the Coinbase oderbook."""

    def __init__(self, product_list):
        OrderBook.__init__(self, product_list)
        self.dataStream = com.webSocketConnection("Coinbase", nameConvention(product_list, 'Coinbase'), 'orderbook')
        self.name = 'Coinbase'
        self.df_dict = dict()

    def getSnapshot(self, product, snapshotMessage):
        """"Retrieve snapshot of orderbook from the exchange."""
        for single_item in snapshotMessage['bids']:
            self.bids_dict[product][float(single_item[0])] = float(single_item[1])
        for single_item in snapshotMessage['asks']:
            self.asks_dict[product][float(single_item[0])] = float(single_item[1])
        # self.bids = sorted([[float(bids[i][0]),float(bids[i][1])] for i in range(0,len(bids))],key = lambda x: x[0])
        #self.asks = {float(k): float(v) for k, v in dict(snapshotMessage['asks'][0:None]).items()}

    # self.asks = sorted([[float(asks[i][0]),float(asks[i][1])] for i in range(0,len(asks))],key = lambda x: x[0])

    def update(self, product, updateMessage):
        """"Retrieve the latest exchange message and apply the changes contained therein to the orderbook."""
        if updateMessage[0] == 'buy':
            if updateMessage[2] == '0.00000000':
                del self.bids_dict[product][float(updateMessage[1])]
            else:
                self.bids_dict[product][float(updateMessage[1])] = float(updateMessage[2])
        else:
            if updateMessage[2] == '0.00000000':
                del self.asks_dict[product][float(updateMessage[1])]
            else:
                self.asks_dict[product][float(updateMessage[1])] = float(updateMessage[2])

    def update_buy(self, product, updateMessage):

        if updateMessage[2] == '0.00000000':
            del self.bids_dict[product][float(updateMessage[1])]
        else:
            self.bids_dict[product][float(updateMessage[1])] = float(updateMessage[2])

    def update_sell(self, product, updateMessage):
        if updateMessage[2] == '0.00000000':
            del self.asks_dict[product][float(updateMessage[1])]
        else:
            self.asks_dict[product][float(updateMessage[1])] = float(updateMessage[2])




    def run(self):
        self.dataStream.start()
        while True:
            try:
                message = self.dataStream.getLatestMessage()
                if message['type'] == 'l2update':
                    self.update(message['product_id'], message['changes'][0])

                elif message['type'] == 'snapshot':
                    self.getSnapshot(message['product_id'], message)
                elif message['type'] == 'subscriptions':
                    continue

                else:
                    raise KeyError(f"Unexpected message received: {message}")

                #print(self.getBestBidSize('ETH-USD'))


            except websocket._exceptions.WebSocketConnectionClosedException:
                print('socket connection closed')


    def run_write(self, level_list, duration):
        import time
        for product in self.product_list:
            self.df_dict[product] = dict()
            for level in level_list:
                self.df_dict[product][level]= {'timestamp': deque(), "bidprice": deque(), "bidsize": deque(), "askprice": deque(),
                                     "asksize": deque()}

        start = time.time()
        self.dataStream.start()
        while (time.time()-start)<duration*60:
            try:
                updated = False
                message = self.dataStream.getLatestMessage()

                if message['type'] == 'l2update':

                    if message['changes'][0][0] == 'buy':
                        for level in level_list:
                            if float(message['changes'][0][1]) == self.bids_dict[message['product_id']].peekitem(int(-1 - level + 1))[0]:
                                self.update_buy(message['product_id'], message['changes'][0])
                                product = message['product_id']

                                self.df_dict[product][level]['timestamp'].append(message['time'])
                                self.df_dict[product][level]['askprice'].append(
                                    self.asks_dict[product].peekitem(int(level - 1))[0])
                                self.df_dict[product][level]['asksize'].append(
                                    self.asks_dict[product].peekitem(int(level - 1))[1])
                                self.df_dict[product][level]['bidprice'].append(
                                    self.bids_dict[product].peekitem(int(-1 - level + 1))[0])
                                self.df_dict[product][level]['bidsize'].append(
                                    self.bids_dict[product].peekitem(int(-1 - level + 1))[1])
                                updated = True
                                break

                    else:
                        for level in level_list:
                            if float(message['changes'][0][1]) == self.asks_dict[message['product_id']].peekitem(int(level - 1))[0]:
                                self.update_sell(message['product_id'], message['changes'][0])
                                product = message['product_id']

                                self.df_dict[product][level]['timestamp'].append(message['time'])
                                self.df_dict[product][level]['askprice'].append(
                                    self.asks_dict[product].peekitem(int(level - 1))[0])
                                self.df_dict[product][level]['asksize'].append(
                                    self.asks_dict[product].peekitem(int(level - 1))[1])
                                self.df_dict[product][level]['bidprice'].append(
                                    self.bids_dict[product].peekitem(int(-1 - level + 1))[0])
                                self.df_dict[product][level]['bidsize'].append(
                                    self.bids_dict[product].peekitem(int(-1 - level + 1))[1])
                                updated = True
                                break

                    if not updated:
                        self.update(message['product_id'], message['changes'][0])


                elif message['type'] == 'snapshot':
                    self.getSnapshot(message['product_id'], message)
                elif message['type'] == 'subscriptions':
                    continue

                else:
                    raise KeyError(f"Unexpected message received: {message}")


            except websocket._exceptions.WebSocketConnectionClosedException:
                print('socket connection closed')


    def save_orderbook(self):
        for product in self.product_list:
            for level in self.df_dict[product].keys():
                df = pd.DataFrame(self.df_dict[product][level])
                df.to_csv(os.getcwd()+f'/data/{product}_quote_level_{level}.csv')






class GeminiOrderBook_V1(OrderBook):
    def __init__(self, product_list):
        OrderBook.__init__(self, product_list)
        self.product = product_list
        self.mod_product_map = dict()      #mapping exchange own ticker symbol to our conventional symbol
        temp_list = nameConvention(product_list, 'Gemini')
        for i, temp in enumerate(temp_list):
            self.mod_product_map[temp] = product_list[i]
        self.dataStream = com.webSocketConnection("Gemini_V1", temp_list, 'orderbook')
        self.name = 'Gemini_V1'

    def getSnapshot(self, product, snapshotMessage):
        product = self.mod_product_map[product]
        for item in snapshotMessage:
            if item['side'] == 'bid':
                self.bids_dict[product][float(item['price'])] = float(item['remaining'])
            else:
                self.asks_dict[product][float(item['price'])] = float(item['remaining'])

    def update(self, product, updateMessage):
        product = self.mod_product_map[product]
        if updateMessage['side'] == 'bid':
            if updateMessage['remaining'] == '0':
                del self.bids_dict[product][float(updateMessage['price'])]
            else:
                self.bids_dict[product][float(updateMessage['price'])] = float(updateMessage['remaining'])
        if updateMessage['side'] == 'ask':
            if updateMessage['remaining'] == '0':
                del self.asks_dict[product][float(updateMessage['price'])]
            else:
                self.asks_dict[product][float(updateMessage['price'])] = float(updateMessage['remaining'])

    def run(self):
        self.dataStream.start()
        for i in range(len(self.product_list)):
            try:
                message = self.dataStream.getLatestMessage()
                if message['type'] !='update':
                    raise KeyError(f"Unexpected message received: {message}")
                self.getSnapshot(message['events'][0]['symbol'], message['events'])
            except websocket._exceptions.WebSocketConnectionClosedException:
                print('socket connection closed')


        while True:
            try:
                message = self.dataStream.getLatestMessage()
                if message['type'] != 'update':
                    raise KeyError(f"Unexpected message received: {message}")
                self.update(message['events'][0]['symbol'], message['events'][0])
            except websocket._exceptions.WebSocketConnectionClosedException:
                print('socket connection closed')







##todo modify GeminiOrderBook for multiple products
# class GeminiOrderBook(OrderBook):
#     def __init__(self, product_list):
#         OrderBook.__init__(self, product_list)
#         self.product = product_list
#         self.dataStream = com.webSocketConnection("Gemini", nameConvention(product_list, 'Gemini'), 'orderbook')
#         self.name = 'Gemini'
#
#     def getUpdateMessage(self):
#         updateMessage = self.dataStream.getLatestMessage()
#         while updateMessage['type'] != 'l2_updates':
#             if updateMessage['type'] == "trade" or "auction_result" or "auction_indicative":
#                 #print("WARNING: Non-update message received. Getting next message...")
#                 updateMessage = self.dataStream.getLatestMessage()
#             else:
#                 raise NameError("Unexpected message received.")
#
#         return updateMessage['changes']
#
#
#
#     def getSnapshot(self):
#         self.dataStream.start()
#         updateMessage = self.getUpdateMessage()
#         try:
#             for single_item in updateMessage:
#                 if single_item[2] == '0':
#                     continue
#                 elif single_item[0] == 'buy':
#                     self.bids[float(single_item[1])] = float(single_item[2])
#                 else:
#                     self.asks[float(single_item[1])] = float(single_item[2])
#         except websocket._exceptions.WebSocketConnectionClosedException:
#             print('websocket error')
#
#     def update(self):
#         updateMessage = self.getUpdateMessage()
#
#         for single_item in updateMessage:
#             if single_item[0] == 'buy':
#                 if single_item[2] == '0':
#                     try:
#                         del self.bids[float(single_item[1])]
#                     except KeyError:
#                         continue
#
#                 else:
#                     self.bids[float(single_item[1])] = float(single_item[2])
#             else:
#                 if single_item[2] == '0':
#                     try:
#                         del self.asks[float(single_item[1])]
#                     except KeyError:
#                         continue
#                 else:
#                     self.asks[float(single_item[1])] = float(single_item[2])

# class IBOrderbook(OrderBook):
#     def __init__(self, ticker):
#         OrderBook.__init__(self, ticker)
#         self.ib = TestApp()
#         self.ib.connect("127.0.0.1", 7496, 0)
#         contract = Contract()
#         self.ib.reqMarketDataType(4)
#         contract.symbol = ticker
#         contract.secType = "STK"
#         contract.exchange = "SMART"
#         contract.currency = "USD"
#         self.contract = contract
#
#     def getSnapshot(self):
#         self.ib.reqMktDepth(reqId=1, contract=self.contract, numRows = 5, isSmartDepth=True,mktDepthOptions=[])
#         self.ib.run()
#
#     def update(self):
#         pass



class EnigmaXOrderbook(OrderBook):
    '''
    EnigmaX has sorted vwap orderbook bulit-in, the size will be fixed as 0.001, 1, 5, 10, 50
    '''

    def __init__(self, product_list):
        OrderBook.__init__(self, product_list)
        self.product_list = product_list
        self.dataStream = com.webSocketConnection("EnigmaX", nameConvention(product_list, 'EnigmaX'), 'orderbook')
        self.name = "EnigmaX"
        #self.bids = {}
        #self.asks = {}

    def getUpdatedMessage(self):
        msg = self.dataStream.getLatestMessage()
        if (msg['type'] == 'info') and (msg['error'] == True):
            raise NameError("Unexpected message received.")

        if msg['type'] != 'subscriptions':
            return



    def getSnapshot(self, product, msg):
        for item in msg['asks']:
            self.asks_dict[product][float(item['quantity'])] = float(item['price'])

        for item in msg['bids']:
            self.bids_dict[product][float(item['quantity'])] = float(item['price'])

    def update(self, product, msg):
        self.getSnapshot(product, msg)


    def run(self):
        self.dataStream.start()
        while True:
            try:
                msg = self.dataStream.getLatestMessage()
                if (msg['type'] == 'info') and (msg['error'] == True):
                    raise NameError("Unexpected message received: {msg}.")

                elif msg['type'] == 'subscriptions':
                    self.update(msg['product_name'], msg)
                else:
                    continue
            except websocket._exceptions.WebSocketConnectionClosedException:
                print('socket connection closed')


    def getBestAsk(self, product):
        return self.asks_dict[product].peekitem(0)[1]
    def getBestAskSize(self, product):
        return self.asks_dict[product].peekitem(0)[0]
    def getBestBid(self,product):
        return self.bids_dict[product].peekitem(0)[1]
    def getBestBidSize(self, product):
        return self.bids_dict[product].peekitem(0)[0]










if __name__ == "__main__":
    # test for enigma orderbook
    # eng = EnigmaXOrderbook('ETH-USD')
    # eng.getSnapshot()
    # while True:
    #     eng.update()
    #     try:
    #         print(eng.getBestBid(), eng.getBestBidSize(), eng.getBestAsk(), eng.getBestAskSize())
    #     except:
    #         pass


    cb = CoinBaseOrderBook(['BTC-USD'])
    cb.run_write([1,2], 1)
    cb.save_orderbook()


        #time.sleep(0.5)



