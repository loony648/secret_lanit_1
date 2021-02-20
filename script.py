import requests
import time
from datetime import datetime, timezone

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

class InfluxDBClient:
    def __init__(self, dbName, url):
        self.dbName = dbName
        self.url = url

    def ping(self):
        """
            InfluxDBClient.ping
            
            Params:
            @payload - data to send

            Returns:
            Check server status
        """
        influxPingURL = self.url + '/ping'
        req = requests.post(influxPingURL, headers=headers)
        return req

    def httpQuery(self, payload):
        """
            InfluxDBClient.httpQuery
            
            Params:
            @payload - data to send

            Returns:
            Answer from server
        """
        influxWriteURL = influxURL + '/query?' + 'db=' + self.dbName
        req = requests.post(influxWriteURL, headers=headers, data='q=' + payload)
        print('Sending query request to ' + f'{influxWriteURL}')
        return req.json()

    def httpWrite(self, payload):
        """
            InfluxDBClient.httpWrite
            
            Params:
            @payload - data to send

            Returns:
            Answer from server
        """
        influxWriteURL = influxURL + '/write?' + 'db=' + self.dbName + '&precision=s'
        req = requests.post(influxWriteURL, headers=headers, data=payload)
        print('Sending write request to ' + f'{influxWriteURL}')
        return req

    def ParseToInlineQuery(self, bitfinexAnswer):
        """
            InfluxDBClient.ParseToInlineQuery
            
            Params:
            @bitfinexAnswer - bitfinex HTTP GET return

            Returns:
            Parsed Bitfinex answer into line query for InfluxDB
        """
        InlineQuery = ''
        #timestamp = (datetime.now(timezone.utc).astimezone()).isoformat()
        for bitfinexLists in bitfinexAnswer:
            if(len(bitfinexLists) == len(tradingPairsList)):
                symbol, bid, bidSize, ask, askSize, dailyChange, dailyChangeRelative, lastPrice, volume, high, low = bitfinexLists
                InlineQuery += f'{self.dbName},pair={symbol} bid={bid},bid_size={bidSize},ask={ask},ask_size={askSize},daily_change={dailyChange},daily_change_rel={dailyChangeRelative},last_price={lastPrice},volume={volume},high={high},low={low}\n'
        return InlineQuery

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
    print('Sending get request to ' + f'{bitfinexGetURL}')
    return req


if __name__ == '__main__':
    influxClient = InfluxDBClient(dbName, influxURL)
    while(True):
        pingReq = influxClient.ping()
        if(pingReq.status_code != 204):
            SystemExit('Some problems with DB. Check health of your influxDB server. Exit..')

        bitfinexTickersList = bitfinexGetAllTickers()
        bitfinexAnswer = influxClient.ParseToInlineQuery(bitfinexTickersList.json())
        influxReturn = influxClient.httpWrite(bitfinexAnswer)

        if(bitfinexTickersList.status_code != 200):
            SystemExit('Check connection with bitfinex api. Exit..')

        if(influxReturn.status_code == 404):
            influxCreateReturn = influxClient.httpQuery('CREATE DATABASE ' + dbName)
            print('InfluxDB didnt found DB. Trying to create db.')
        else:
            print('Everything okay.')
            print('Sleep for 10 seconds...')
            time.sleep(10)