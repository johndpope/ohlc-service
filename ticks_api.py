import datetime
import json
import os
import time
import urllib.request

from pandas import read_csv
from pandas import DataFrame

import plotly.offline as py
import plotly.graph_objs as go


def contains_error(answer):
    if not answer["error"]:
        return False

    print(answer["error"])
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
    # url_data = "https://api.kraken.com/0/public/OHLC?pair=" + pair + "&since=" + str(since);
    url = "https://api.kraken.com/0/public/Trades?pair=" + pair + "&since=" + str(since);
    answer = urllib.request.urlopen(url)
    encoded_data = answer.read()
    encoding = answer.info().get_content_charset('utf-8')
    json_data = json.loads(encoded_data.decode(encoding))

    if contains_error(json_data):
        return False, 0, False

    pair_name = get_pair_name(json_data)
    list_of_trades = parse_ticks(json_data, pair_name);

    if not list_of_trades:
        print("Error: empty answer")
        return False, 0, False

    last_id = get_last_id(json_data)
    is_last_request = False
    # <price>, <volume>, <time>, <buy/sell>, <market/limit>, <miscellaneous>

    for trade in list_of_trades:
        date = datetime.datetime.fromtimestamp(trade[2])
        file.write(date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + f'\t{trade[0]}\t{trade[1]}\t{trade[3]}\t{trade[4]}\n')

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
    file__path = directory + pair_name + datetime.datetime.now().strftime('_%Y_%m_%d_%H_%M_%S') + ".txt"
    file = open(file__path, 'w')

    while True:

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

        if is_last_request:
            file.close()
            return file__path

        if error_counter > 8:
            file.close()
            raise BaseException('Too many errors')


class Candle:
    __slots__ = ['open', 'close', 'high', 'low', 'volume', ]

    def __init__(self, open=0.0, close=0.0, high=0.0, low=0.0, volume=0.0):
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

    date = {'s': [], 'b': []}
    open_price = {'s': [], 'b': []}
    close_price = {'s': [], 'b': []}
    high_price = {'s': [], 'b': []}
    low_price = {'s': [], 'b': []}
    volume = {'s': [], 'b': []}

    current_minute = fisrt_trade_datetime.replace(second=0, microsecond=0)
    next_minute = current_minute + datetime.timedelta(minutes=1);

    ohlc = {'s': {}, 'b': {}}

    ohlc[trade_direction][current_minute] = Candle(trade_price, trade_price, trade_price,
                                                   trade_price, trade_volume)
    date[trade_direction].append(current_minute)
    open_price[trade_direction].append(trade_price)
    close_price[trade_direction].append(trade_price)
    high_price[trade_direction].append(trade_price)
    low_price[trade_direction].append(trade_price)
    volume[trade_direction].append(trade_volume)

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

            date[trade_direction].append(current_minute)
            open_price[trade_direction].append(trade_price)
            close_price[trade_direction].append(trade_price)
            high_price[trade_direction].append(trade_price)
            low_price[trade_direction].append(trade_price)
            volume[trade_direction].append(trade_volume)

        else:
            ohlc[trade_direction][current_minute].volume += trade_volume
            ohlc[trade_direction][current_minute].close = trade_price

            if ohlc[trade_direction][current_minute].low > trade_price:
                ohlc[trade_direction][current_minute].low = trade_price

            if ohlc[trade_direction][current_minute].high < trade_price:
                ohlc[trade_direction][current_minute].high = trade_price

            volume[trade_direction][-1] += trade_volume
            close_price[trade_direction][-1] = trade_price

            if low_price[trade_direction][-1] > trade_price:
                low_price[trade_direction][-1] = trade_price

            if high_price[trade_direction][-1] < trade_price:
                high_price[trade_direction][-1] = trade_price

        if count % 1000 == 0:
            print("Counter={}".format(count))
        count += 1

    ohlc_df = DataFrame({
        'open': open_price['s'],
        'close': close_price['s'],
        'high': high_price['s'],
        'low': low_price['s'],
        'volume': volume['s']
    }, index=date['s'])
    return ohlc_df;


def build_ohlc_3(filename):
    file = open(filename, 'r')

    # <price>, <volume>, <time>, <buy/sell>, <market/limit>, <miscellaneous>
    first_line_words = file.readline().split('\t')

    if len(first_line_words) < 5:
        raise Exception('Invalid input file: expected 5 columns row')

    first_trade_datetime = datetime.datetime.strptime(first_line_words[0], '%Y-%m-%d %H:%M:%S.%f')
    trade_price = float(first_line_words[1])
    trade_volume = float(first_line_words[2])
    trade_dir = first_line_words[3]
    # fisrt_trade_market_limit = fisrt_line_words[4]

    date = {'s': [], 'b': []}
    open_price = {'s': [], 'b': []}
    close_price = {'s': [], 'b': []}
    high_price = {'s': [], 'b': []}
    low_price = {'s': [], 'b': []}
    volume = {'s': [], 'b': []}
    weighted_average = {'s': [], 'b': []}

    current_minute = first_trade_datetime.replace(second=0, microsecond=0)
    next_minute = current_minute + datetime.timedelta(minutes=1);

    date[trade_dir].append(current_minute)
    open_price[trade_dir].append(trade_price)
    close_price[trade_dir].append(trade_price)
    high_price[trade_dir].append(trade_price)
    low_price[trade_dir].append(trade_price)
    volume[trade_dir].append(trade_volume)
    weighted_average[trade_dir].append(trade_volume * trade_price)

    count = 0

    for line in file:

        words = line.split('\t')
        if len(words) < 5:
            continue

        try:
            trade_datetime = datetime.datetime.strptime(words[0], '%Y-%m-%d %H:%M:%S.%f')
            trade_price = float(words[1])
            trade_volume = float(words[2])
            trade_dir = words[3]
            # trade_market_limit = words[4]
        except Exception as e:
            print(e)
            continue

        if trade_volume == 0:
            continue

        while trade_datetime >= next_minute:
            current_minute += datetime.timedelta(minutes=1);
            next_minute += datetime.timedelta(minutes=1);

        if not date[trade_dir] or date[trade_dir][-1] != current_minute:

            if date[trade_dir]:
                weighted_average[trade_dir][-1] = weighted_average[trade_dir][-1] / volume[trade_dir][-1]

            date[trade_dir].append(current_minute)
            open_price[trade_dir].append(trade_price)
            close_price[trade_dir].append(trade_price)
            high_price[trade_dir].append(trade_price)
            low_price[trade_dir].append(trade_price)
            volume[trade_dir].append(trade_volume)
            weighted_average[trade_dir].append(trade_volume * trade_price)

        else:

            volume[trade_dir][-1] += trade_volume
            close_price[trade_dir][-1] = trade_price

            if low_price[trade_dir][-1] > trade_price:
                low_price[trade_dir][-1] = trade_price

            if high_price[trade_dir][-1] < trade_price:
                high_price[trade_dir][-1] = trade_price

            weighted_average[trade_dir][-1] += trade_volume * trade_price

        if count % 1000 == 0:
            print("Counter={}".format(count))
        count += 1

    if weighted_average['s']:
        weighted_average['s'][-1] = weighted_average['s'][-1] / volume['s'][-1]

    if weighted_average['b']:
        weighted_average['b'][-1] = weighted_average['b'][-1] / volume['b'][-1]

    ohlc_bid = DataFrame({
        'open': open_price['s'],
        'close': close_price['s'],
        'high': high_price['s'],
        'low': low_price['s'],
        'volume': volume['s'],
        'weighted_average': weighted_average['s']
    }, index=date['s'])

    ohlc_ask = DataFrame({
        'open': open_price['b'],
        'close': close_price['b'],
        'high': high_price['b'],
        'low': low_price['b'],
        'volume': volume['b'],
        'weighted_average': weighted_average['b']
    }, index=date['b'])

    return ohlc_bid, ohlc_ask


# def print_ohlc(ohlc):
#    btc_trace = go.Scatter(x=ohlc['s'].keys(), y=ohlc['s'].keys())
#    py.iplot([btc_trace])
#    py.offline.plot([btc_trace], filename='file.html')

def build_ohlc_2(filename, pairname):
    ohlc = read_csv(filename, "\t")
    btc_trace = go.Scatter(x=ohlc['Date'], y=ohlc['price'])
    py.iplot([btc_trace])
    py.offline.plot([btc_trace], filename=pairname + '.html')

    return ohlc


def print_ohlc(dir, filename, pairname, mode='lines'):
    ohlc_bid, ohlc_ask = build_ohlc_3(dir + filename)

    ohlc_bid.to_csv(dir + os.path.splitext(filename)[0] + "_bid.csv", sep=';', encoding='utf-8')
    ohlc_ask.to_csv(dir + os.path.splitext(filename)[0] + "_ask.csv", sep=';', encoding='utf-8')

    bid_graph = go.Scatter(x=ohlc_bid.index, y=ohlc_bid['open'], name='Bid ' + pairname, mode=mode)
    ask_graph = go.Scatter(x=ohlc_ask.index, y=ohlc_ask['open'], name='Ask ' + pairname, mode=mode)
    py.iplot([bid_graph, ask_graph])
    py.offline.plot([bid_graph, ask_graph], filename=dir + pairname + '.html')

    open_bid_graph = go.Scatter(x=ohlc_bid.index, y=ohlc_bid['open'], name='Bid ' + pairname, mode=mode)
    weighted_average_bid_graph = go.Scatter(x=ohlc_bid.index, y=ohlc_bid['weighted_average'],
                                            name='Weight Bid ' + pairname, mode=mode)
    py.iplot([open_bid_graph, weighted_average_bid_graph])
    py.offline.plot([open_bid_graph, weighted_average_bid_graph],
                    filename=dir + pairname + '_' + mode + '_weighted.html')


def print_ohlc_from_csv(dir, filename, pairname, mode='lines'):
    ohlc_bid = read_csv(dir + os.path.splitext(filename)[0] + "_bid.csv", sep=';', encoding='utf-8', index_col=0,
                        nrows=10000)
    # ohlc_ask = read_csv(dir + os.path.splitext(filename)[0]+"_ask.csv", sep=';', encoding='utf-8', index_col=0)

    # bid_graph = go.Scatter(x=ohlc_bid.index, y=ohlc_bid['open'], name='Bid ' + pairname, mode=mode)
    # ask_graph = go.Scatter(x=ohlc_ask.index, y=ohlc_ask['open'], name='Ask ' + pairname, mode=mode)
    # py.iplot([bid_graph, ask_graph])
    # py.offline.plot([bid_graph, ask_graph], filename=dir + pairname + '_' + mode +'.html')

    ohlc_bid['avg'] = ohlc_bid[['high', 'low']].mean(axis=1)

    open_bid_graph = go.Scatter(x=ohlc_bid.index, y=ohlc_bid['open'], name='Open Bid ' + pairname, mode=mode)
    avg_bid_graph_w = go.Scatter(x=ohlc_bid.index, y=ohlc_bid['avg'], name='Avg Bid ' + pairname, mode=mode)
    weighted_average_bid_graph = go.Scatter(x=ohlc_bid.index, y=ohlc_bid['weighted_average'],
                                            name='Weight Avf Bid ' + pairname, mode=mode)
    curves = [open_bid_graph, avg_bid_graph_w, weighted_average_bid_graph]

    py.iplot(curves)
    py.offline.plot(curves, filename=dir + pairname + '_' + mode + '_weighted.html')



def find_longest_continious_sequence(dir, filename, pairname):
    ohlc_bid = read_csv(dir + filename, sep=';', encoding='utf-8', index_col=0)

    if len(ohlc_bid.index) == 0:
        return 0, 0, 0

    max_count = 1
    current_count = 0

    prev_time = datetime.datetime.strptime(ohlc_bid.index[0], '%Y-%m-%d %H:%M:%S')
    cur_start_interval = prev_time
    max_start_interval = prev_time
    max_end_interval = prev_time

    for date in ohlc_bid.index:

        current_time = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')

        d1_ts = time.mktime(current_time.timetuple())
        d2_ts = time.mktime(prev_time.timetuple())
        minutes_delta = int(d1_ts-d2_ts) // 60

        if minutes_delta <= 5:
            current_count += 1
        else:
            if current_count > max_count:
                max_start_interval = cur_start_interval
                max_end_interval = prev_time
                max_count = current_count

            current_count = 0
            cur_start_interval = current_time

        prev_time = current_time

    return max_count, max_start_interval, max_end_interval


def find_pivot_sequences(dir, filename):
    ohlc = read_csv(dir + filename, sep=';', encoding='utf-8', index_col=0)

    if len(ohlc.index) == 0:
        return []

    pivots_list = [];

    prev_price = ohlc['weighted_average'][0]
    pivot_price = prev_price
    pivot_index = 0
    index = 0

    minimal_increase = 4;
    minimal_recovery = 4;

    continuous_increase_counter = 0
    continuous_recovery_counter = 0
    have_potential_pivot_point = False

    for date, row in ohlc.iterrows():

        current_time = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        current_price = row['weighted_average']

        if not have_potential_pivot_point:
            if current_price >= prev_price:
                continuous_increase_counter += 1
            else:
                if continuous_increase_counter > minimal_increase:
                    pivot_index = index
                    pivot_price = prev_price
                    have_potential_pivot_point = True

                continuous_increase_counter = 0
        else:
            if current_price >= pivot_price:
                if continuous_recovery_counter > minimal_recovery:
                    pivots_list.append((pivot_index, current_time, pivot_price, continuous_recovery_counter))
                    print(pivots_list[-1])
                    have_potential_pivot_point = False
                    continuous_recovery_counter = 0
                else:
                    have_potential_pivot_point = False
                    continuous_recovery_counter = 0
            else:
                continuous_recovery_counter += 1

        index += 1
        prev_price = current_price

    return pivots_list


py.init_notebook_mode(connected=True)
# file_path = save_all_trades('LTCUSD')
# print_ohlc_from_csv('result/LTCUSD/', 'LTCUSD_2017_10_11_12_01_03.txt', 'LTCUSD', mode='markers')

result = find_pivot_sequences('result/LTCUSD/', 'LTCUSD_2017_bid.csv')
data = DataFrame(result, columns=['pivot_index', 'current_time', 'pivot_price', 'length'])
data.to_csv('result/LTCUSD/LTCUSD_2017_pivots.csv', sep=';', encoding='utf-8')

#result = find_longest_continious_sequence('result/LTCUSD/', 'LTCUSD_2017_10_11_12_01_03_bid.csv', 'LTCUSD')
print("Finish")
