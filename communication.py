#!/usr/bin/env python

"""
Classes and functions for establishing and managing connections/streams with exchange API.
"""

import datetime
import json as js
import websocket as ws

# channel name for different exchanges
ORDERBOOK_CHANNEL = {'Coinbase': 'level2',
					 'Gemini': 'l2',
					 'Gemini_V1': '&offers=true&bids=true',
					 'EnigmaX': ''}


class Connection(object):
	"""Base class for a connection object."""
	def __init__(self,exchange):
		self.dateTimeStarted = datetime.datetime.now()
		self.exchange = exchange
		
class webSocketConnection(Connection):
	"""Class for websocket connection."""

	def __init__(self,exchange, product, datatype):
		Connection.__init__(self,exchange)
		self.serverURL = getServerURL(exchange)
		self.subscriptionMessage = getSubscriptionMessage(exchange, product, datatype)
		if exchange == 'Gemini_V1':
			self.wsConnection = ws.create_connection(self.subscriptionMessage)
		else:
			self.wsConnection = ws.create_connection(self.serverURL)
		
	def start(self):
		"""Establishes the websocket connection."""
		if self.exchange == 'Gemini_V1':
			return
		self.wsConnection.send(self.subscriptionMessage)

		
	def getLatestMessage(self):
		"""Obtains the latest message from the websocket stream."""
		return  js.loads(self.wsConnection.recv())
		
	def close(self):
		self.wsConnection.close()
	
def getServerURL(exchange):
	"""Retrieve the server URL for a given exchange."""
	
	if exchange == 'Coinbase':
		return 'wss://ws-feed.pro.coinbase.com'

	if exchange == 'Gemini':
		return 'wss://api.gemini.com/v2/marketdata'

	if exchange == 'Gemini_V1':      # faster channel
		return 'wss://api.gemini.com/v1/multimarketdata'
	if exchange == 'EnigmaX':
		return 'wss://ws-api.enigma-securities.io'
		
	raise NameError("Exchange symbol not recognized.")
		
def getSubscriptionMessage(exchange,product_list, datatype):
	"""Retrieve the websocket subscription message for a given exchange."""
	if datatype == 'orderbook':
		channel = ORDERBOOK_CHANNEL
	else:
		raise NameError("data type has not been defined")

	if exchange == 'Coinbase':
		subscriptionMessage = js.dumps({
			"type": "subscribe",
			"product_ids": product_list,
			"channels": [channel['Coinbase']]
		})
		return subscriptionMessage

	if exchange == 'EnigmaX':
		sub_list = []
		for product in product_list:
			sub_list.append({"product": product})
		subscriptionMessage = js.dumps({
			"username": "enigma_quant",
			"password": "Wzqw$2ai",
			"subscriptions": sub_list
		})
		return subscriptionMessage

	if exchange == 'Gemini':
		SubscriptionMessage = js.dumps({
			"type": "subscribe",
			"subscriptions": [{
				"name": channel['Gemini'],
				"symbols": product_list
			}]
		})
		return SubscriptionMessage

	if exchange == 'Gemini_V1':
		productstring = '?symbols='
		for product in product_list:
			productstring += product + ','
		return getServerURL('Gemini_V1') + productstring + channel['Gemini_V1']
	raise NameError("Exchange symbol not recognized.")



if __name__ == "__main__":
	# cb = webSocketConnection('Coinbase', ['ETH-USD', 'BTC-USD'], 'orderbook')
	# cb.start()
	# while True:
	# 	print(cb.getLatestMessage())
	#
	# gen = webSocketConnection('Gemini_V1', ['ETHUSD', 'BTCUSD'], 'orderbook')
	# gen.start()
	# while True:
	# 	print(gen.getLatestMessage())

	eng = webSocketConnection('EnigmaX', 'ETH-USD', 'orderbook')
	eng.start()
	while True:
		print(eng.getLatestMessage())