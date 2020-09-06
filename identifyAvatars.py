#!/usr/bin/env python3
import json
from elasticsearch import Elasticsearch
from image_match.elasticsearch_driver import SignatureES
import psycopg2
import time
import datetime
import threading
import urllib.error
from argparse import ArgumentParser


###################################################################################
# i can use endpoint on web server and integrate all of this into pizza but fuck it
###################################################################################
from redis import Redis

parser = ArgumentParser()

parser.add_argument('-s', '--skipSet', action='store_true', default=False)

args = parser.parse_args()

default_avatar = 'fe/fef49e7fa7e1997310d705b2a6158ff8dc1cdfeb'


def search(market_avatar):
    try:
        return SES.search_image("", path=f"https://steamcommunity-a.akamaihd.net/market/image/{market_avatar}/avatar.jpg")
    except urllib.error.HTTPError as e:
        print(404, market_avatar)
        if e.code == 404:
            return []


def filter_avatars(query_result):
    if len(query_result) == 0:
        return {}
    if default_avatar in [i['id'] for i in query_result]:
        return {}

    filtered = []
    prev_dist = -1
    for i in query_result:
        if i['dist'] < 0.1:
            # filter avatars that have match higher than 0.1
            if prev_dist - i['dist'] > 0.2:
                # if dist is high jump from prev_dist it's probably not avatar we're looking for
                break
            prev_dist = i['dist']
            filtered.append(i)
    return {i['id']: i['dist'] for i in filtered[:10]}


def identify(market_avatar):
    # print(marketAvatar)
    print(f"https://steamcommunity-a.akamaihd.net/market/image/{market_avatar}/avatar.jpg")
    t = datetime.datetime.now()
    result = search(market_avatar)
    print(f"\n{datetime.datetime.now() - t} {market_avatar}")

    filtered_result = filter_avatars(result)
    for key, value in filtered_result.items():
        print(f"{key} - {value}")

    cur = Connection.cursor()

    cache = json.loads(redis.get('avatarCache'))
    cache[market_avatar] = {key: 1 - value for key, value in filtered_result.items()}
    redis.set('avatarCache', json.dumps(cache))

    if len(filtered_result) == 0:  # couldn't identify avatar
        cur.execute(f"UPDATE listings SET scanned=true where marketAvatar='{market_avatar}'")
        cur.execute(f"UPDATE litelistings SET scanned=true where marketAvatar='{market_avatar}'")
    else:
        joined_avatars = ";".join([i for i in filtered_result.keys()])
        cur.execute(f"UPDATE listings SET scanned=true, avatar=avatar || '{joined_avatars}' || ';' WHERE marketAvatar='{market_avatar}'")
        cur.execute(f"UPDATE litelistings SET scanned=true, avatar=avatar || '{joined_avatars}' || ';' WHERE marketAvatar='{market_avatar}'")


def start_thread(market_avatar):
    t = threading.Thread(
        target=identify,
        args=(
            market_avatar,
        )
    )
    t.start()
    return t


def set_scanned():
    Cursor.execute(f"UPDATE listings SET scanned=true where scanned=false")
    Cursor.execute(f"UPDATE litelistings SET scanned=true where scanned=false")


SES = SignatureES(Elasticsearch(timeout=60), timeout='59s')
redis = Redis(host='localhost', port=6379, db=0)

Connection = psycopg2.connect(database='pizza')
Connection.autocommit = True
Cursor = Connection.cursor()

if not args.skipSet:
    print("setting scanned to true")
    set_scanned()
    print("scanned set to true")

while True:
    # get count
    Cursor.execute("SELECT COUNT(*) FROM (SELECT marketAvatar FROM listings WHERE scanned = false UNION ALL SELECT marketAvatar FROM litelistings WHERE scanned = false) A")

    # apparently steam listings are "merged" at 4AM. items listed in other currencies are converted into $ which causes
    # huge wave of new listings
    if Cursor.fetchone()[0] > 15:
        set_scanned()
        continue

    Cursor.execute("SELECT marketAvatar FROM listings WHERE scanned = false UNION ALL SELECT marketAvatar FROM litelistings WHERE scanned = false")
    threads = []
    for market_avatar in Cursor.fetchall():
        threads.append(
            start_thread(market_avatar[0])
        )
    for t in threads:
        t.join()
    time.sleep(0.1)

