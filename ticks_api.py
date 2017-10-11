import datetime
import json
import os
import time
import urllib.request

from pandas import read_csv

import plotly.offline as py
import plotly.graph_objs as go
import plotly.figure_factory as ff

py.init_notebook_mode(connected=True)


def contains_error(json):
    if not json["error"]:
        return False

    print(json["error"])
    return True


def get_pair_name(json):
    return list(filter(lambda x: x != "last", json["result"].keys()))[0]


def get_last_id(json):
    return json["result"]["last"]


def parse_ticks(json, pair):
    return json["result"][pair]


def mkdir(path):
    os.makedirs(path, exist_ok=True)


def get_ohlc_data(file, pair, since):
    # urlData = "https://api.kraken.com/0/public/OHLC?pair=" + pair + "&since=" + str(since);
    urlData = "https://api.kraken.com/0/public/Trades?pair=" + pair + "&since=" + str(since);
    webURL = urllib.request.urlopen(urlData)
    data = webURL.read()
    encoding = webURL.info().get_content_charset('utf-8')
    answer = json.loads(data.decode(encoding))

    if contains_error(answer):
        return False, 0, False

    pair_name = get_pair_name(answer)
    list_of_trades = parse_ticks(answer, pair_name);

    if not list_of_trades:
        print("Error: empty answer")
        return False, 0, False

    last_id = get_last_id(answer)
    is_last_request = False
    # <price>, <volume>, <time>, <buy/sell>, <market/limit>, <miscellaneous>

    for trade in list_of_trades:
        date = datetime.datetime.fromtimestamp(trade[2])
        file.write(date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + f"\t{trade[0]}\t{trade[1]}\t{trade[3]}\t{trade[4]}\n")

        if ((datetime.datetime.now() - date).total_seconds() < 3600):
            is_last_request = True
            print("Last request date: " + date.strftime('%Y-%m-%d %H:%M:%S.%f'))

    return True, last_id, is_last_request


def save_all_trades(pair_name):
    since = 0
    error_counter = 0
    sleep_counter = 2
    print("pair_name={}".format(pair_name))
    directory = "./result/" + pair_name + "/"
    mkdir(directory)
    file = open(directory + pair_name + datetime.datetime.now().strftime('_%Y_%m_%d_%H_%M_%S') + ".txt", 'w')

    while (True):

        result, last_id, is_last_request = get_ohlc_data(file, pair_name, since)

        if result:
            sleep_counter = 2
            error_counter = 0
            since = last_id
            time.sleep(sleep_counter)
            print("last_id={}".format(last_id))
        else:
            sleep_counter *= 2
            error_counter += 1
            print("Unable to get answer, sleep_counter={}".format(sleep_counter))
            time.sleep(sleep_counter)

        if (is_last_request) or error_counter > 8:
            file.close()
            return


# save_all_trades('LTCUSD')

class Candle:
    __slots__ = ['open', 'close', 'high', 'low', 'volume',]

    def __init__(self, open=0.0, close= 0.0, high=0.0, low=0.0, volume=0.0):
        self.open = open
        self.close = close
        self.high = high
        self.low = low
        self.volume = volume

def build_ohlc(filename):
    file = open(filename, 'r')

    fisrt_line_words = file.readline().split('\t')
    fisrt_trade_datetime = datetime.datetime.strptime(fisrt_line_words[0], '%Y-%m-%d %H:%M:%S.%f')
    trade_price = float(fisrt_line_words[1])
    trade_volume = float(fisrt_line_words[2])
    trade_direction = fisrt_line_words[3]
    # fisrt_trade_market_limit = fisrt_line_words[4]

    date = []
    open = []
    close = []
    high = []
    low = []
    volume = []

    current_minute = fisrt_trade_datetime.replace(second=0, microsecond=0)
    next_minute = current_minute + datetime.timedelta(minutes=1);
    ohlc = {'s': [[],[]], 'b': [[],[]]}

    ohlc[trade_direction][current_minute] = Candle(trade_price, trade_price, trade_price,
                                                   trade_price, trade_volume)
    date.append(current_minute)
    open.append(trade_price)
    close.append(trade_price)
    high.append(trade_price)
    low.append(trade_price)
    volume.append(trade_volume)

    count = 0

    for line in file:
        words = line.split('\t')
        # <price>, <volume>, <time>, <buy/sell>, <market/limit>, <miscellaneous>
        if len(words) < 5:
            continue

        trade_datetime = datetime.datetime.strptime(words[0], '%Y-%m-%d %H:%M:%S.%f')
        trade_price = float(words[1])
        trade_volume = float(words[2])
        trade_direction = words[3]
        # trade_market_limit = words[4]

        while trade_datetime >= next_minute:
            current_minute += datetime.timedelta(minutes=1);
            next_minute += datetime.timedelta(minutes=1);

        if current_minute not in ohlc[trade_direction]:
            ohlc[trade_direction][current_minute] = Candle(trade_price, trade_price,
                                                                 trade_price, trade_price,
                                                                 trade_volume)

            date.append(current_minute)
            open.append(trade_price)
            close.append(trade_price)
            high.append(trade_price)
            low.append(trade_price)
            volume.append(trade_volume)

        else:
            ohlc[trade_direction][current_minute].volume += trade_volume;
            ohlc[trade_direction][current_minute].close = trade_price

            if ohlc[trade_direction][current_minute].low > trade_price:
                ohlc[trade_direction][current_minute].low = trade_price

            if ohlc[trade_direction][current_minute].high < trade_price:
                ohlc[trade_direction][current_minute].high = trade_price

        print("Counter={}".format(count))
        count += 1

    return ohlc;

#def print_ohlc(ohlc):
#    btc_trace = go.Scatter(x=ohlc['s'].keys(), y=ohlc['s'].keys())
#    py.iplot([btc_trace])
#    py.offline.plot([btc_trace], filename='file.html')

def build_ohlc_2(filename, pairname):
    ohlc = read_csv(filename, "\t")
    btc_trace = go.Scatter(x=ohlc['Date'], y=ohlc['price'])
    py.iplot([btc_trace])
    py.offline.plot([btc_trace], filename=pairname+'.html')
    
    return ohlc

ohlc_ltc = build_ohlc_2('result/LTCUSD/LTCUSD.txt', 'LTCUSD')

print("OHLC size={}".format(ohlc_ltc['s']))
