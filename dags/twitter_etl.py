import requests
import pandas as pd 
import json
from datetime import datetime
# import s3fs 

def run_twitter_etl():

    # Twitter API on Rapid API Marketplace
    url = "https://twitter154.p.rapidapi.com/user/tweets"

    # the user name and id
    username = ""
    user_id = ""
    # max:100, min:1, default:40 per request
    tweets_num_limit = 40

    # the developer's rapid api key and host
    api_key = ""
    api_host = ""

    querystring = {"username":username,
                   "limit": tweets_num_limit,
                   "user_id":user_id,
                   "include_replies":"false",
                   "include_pinned":"false"}

    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": api_host
    }

    response = requests.get(url, headers=headers, params=querystring)

    res_json = response.json()

    # save tweets to a list
    tweet_list = []
    for result in res_json['results']:
        tweet = {"user": result['user']['username'],
                'text' : result['text'],
                'favorite_count' : result['favorite_count'],
                'retweet_count' : result['retweet_count'],
                'created_at' : result['creation_date']}
        
        tweet_list.append(tweet)

    df = pd.DataFrame(tweet_list)
    # save tweets to CSV
    df.to_csv('sample_tweets.csv', index=False)
