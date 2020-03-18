import json
import os
from collections import Counter

def load_data(name="tinyTwitter.json"):
    print("Parsing Json file")
    with open(name, 'r') as f:
        # data = f.read().splitlines()
        # data = "".join(data)
        # json_data = json.loads(data[:-1]+"]}")
        data = f.read().strip()

        # The json bracket has not been closed
        if data[-1] == ',':
            print("Closing the file")
            data = data[:-1] + "]}"

        # the json file has the following structure
        # {totoal_rows, offset, rows:{[ {id, key, value, doc} ]}}

        # NOTE : doc has the following keys
        # ['_id', '_rev', 'created_at', 'id', 'id_str', 'text', 'truncated', 
        # 'entities', 'metadata', 'source', 'in_reply_to_status_id', 
        # 'in_reply_to_status_id_str', 'in_reply_to_user_id', 'in_reply_to_user_id_str', 
        # 'in_reply_to_screen_name', 'user', 'geo', 'coordinates', 'place', 'contributors', 
        # 'is_quote_status', 'retweet_count', 'favorite_count', 'favorited', 'retweeted', 'lang', 'location']
        return json.loads(data)


def language_generator(tweets):
    return (i['doc']['lang'] for i in tweets['rows'])


def simple_cumulator(tweets):
    all_lang = language_generator(tweets)
    return Counter(all_lang)

if __name__ == "__main__":
    # print(load_data()['rows'][0]['doc']['lang'])
    tweets = load_data()
    print(simple_cumulator(tweets))
