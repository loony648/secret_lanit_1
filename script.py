from email import header
import requests
import json
from enum import Enum
from datetime import datetime
from urllib.error import HTTPError
import time

bitfinexURL = 'https://api-pub.bitfinex.com/v2'
influxURL = 'http://localhost:8086'
dbName = 'bitfinexdb'

headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Accept-Encoding': 'gzip'
}

tradingPairsList =[
    'SYMBOL',
    'BID', 
    'BID_SIZE', 
    'ASK', 
    'ASK_SIZE', 
    'DAILY_CHANGE', 
    'DAILY_CHANGE_RELATIVE', 
    'LAST_PRICE', 
    'VOLUME', 
    'HIGH', 
    'LOW'
]

fundingPairsList = [
    'SYMBOL',
    'FRR', 
    'BID', 
    'BID_PERIOD',
    'BID_SIZE', 
    'ASK', 
    'ASK_PERIOD',
    'ASK_SIZE',
    'DAILY_CHANGE',
    'DAILY_CHANGE_RELATIVE', 
    'LAST_PRICE',
    'VOLUME',
    'HIGH', 
    'LOW',
    '_PLACEHOLDER_1',
    '_PLACEHOLDER_2',
    'FRR_AMOUNT_AVAILABLE'
]

def bitfinexListToLine(bitfinexAnswer, dbName):
    """
        bitfinexListToLine
        
        Params:
        @bitfinexAnswer - bitfinex HTTP GET return
        @dbName - InfluxDB base name

        Returns:
        Parsed Bitfinex answer into line query for InfluxDB
    """
    lineQuery = ''
    timestamp = int(datetime.timestamp(datetime.now()))
    for bitfinexLists in bitfinexAnswer:
        if(len(bitfinexLists) == len(tradingPairsList)):
            symbol, bid, bidSize, ask, askSize, dailyChange, dailyChangeRelative, lastPrice, volume, high, low = bitfinexLists
            lineQuery += f'{dbName},pair={symbol} bid={bid},bid_size={bidSize},ask={ask},ask_size={askSize},daily_change={dailyChange},daily_change_rel={dailyChangeRelative},last_price={lastPrice},volume={volume},high={high},low={low},timestamp={timestamp}\n'
        else:
            symbol, frr, bid, bidPeriod, bidSize, ask, askPeriod, askSize, dailyChange, dailyChangeRelative, lastPrice, volume, high, low,\
                placeholder_1, placeholder_2, frrAmmountAvailable = bitfinexLists
            lineQuery += f'{dbName},pair={symbol} frr={frr},bid={bid},bid_period={bidPeriod},bid_size={bidSize},ask={ask},ask_periok={askPeriod},ask_size={askSize},daily_change={dailyChange},daily_change_relative={dailyChangeRelative},last_price={lastPrice},volume={volume},high={high},low={low},frr_ammount_available={frrAmmountAvailable},timestamp={timestamp}\n'
    return lineQuery

def bitfinexGetAllTickers():
    """
        bitfinexGetAllTickers
        
        Params:
        None

        Returns:
        Returns all bitfinex tickers by HTTP GET
    """
    bitfinexGetURL = bitfinexURL + '/tickers?symbols=ALL'
    req = requests.get(bitfinexGetURL)
    print('Sending write request to ' + f'{bitfinexGetURL}')
    return req.json()

def influxHTTPQueryv1(dbName, payload):
    """
        influxHTTPQueryv1
        
        Params:
        @dbName - InfluxDB base name
        @payload - data to send

        Returns:
        Answer from server
    """
    influxWriteURL = influxURL + '/query?' + 'db=' + dbName
    req = requests.post(influxWriteURL, headers=headers, data='q=' + payload)
    print('Sending write request to ' + f'{influxWriteURL}')
    return req

def influxHTTPWritev1(dbName, payload):
    """
        influxHTTPWritev1
        
        Params:
        @dbName - InfluxDB base name
        @payload - data to send

        Returns:
        Answer from server
    """
    influxWriteURL = influxURL + '/write?' + 'db=' + dbName
    req = requests.post(influxWriteURL, headers=headers, data=payload)
    print('Sending write request to' + f'{influxWriteURL}')
    return req

if __name__ == '__main__':
    while(True):
        try:
            bitfinexAnswer = bitfinexListToLine(bitfinexGetAllTickers(), dbName)
            influxHTTPWritev1(dbName, bitfinexAnswer)
        except Exception as unknownError:
            print(unknownError)
            raise
        except HTTPError as httpErr:
            print(httpErr)
            raise
        print('Everything okay...')
        print('Sleep for 10 seconds...')
        time.sleep(10)