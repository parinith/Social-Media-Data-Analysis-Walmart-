import pandas as pd
import numpy as np
import tweepy
import time
from time import sleep
import requests
import re
import os
from os import path
import json
from datetime import date
from datetime import timedelta
import nltk
from nltk.tokenize import RegexpTokenizer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from apscheduler.schedulers.background import BackgroundScheduler

class walmart:
    consumer_key = '0PKb1w8mvYTvilvCkqlJu4wwk'
    consumer_secret = 'LXfS4wLXVecTG2VYu8aUoE7l9ho14F36Re7mGkz1DThQ5H0YUH'
    access_token = '1254760256267096064-ECkWaoNTQWLfVFbMXDfxn34TfWFSrS'
    access_token_secret = 'k8KjgYezMrZDNrTCi9nRNd3ENXDetHVGt1WWeLijM6r5t'
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth,wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    user = api.me()
    keywords = ['Samsung','iPhone','OnePlus','samsung','iphone']
    place_dictionary = {'NYC':'New York','New York':'New York','NEW YORK':'New York',
                        'SF':'San Francisco','San Francisco':'San Francisco',
                        'Bengaluru':'Bangalore','Bangalore':'Bangalore',
                        'Chicago':'Chicago','Mumbai':'Mumbai','London':'London'}
    locations = ['New York','NYC','San Francisco','SF','Chicago','Bangalore','Bengaluru','Mumbai','London','NEW YORK']
    REST_API_URL = 'https://api.powerbi.com/beta/e81af6ba-a66f-4cab-90f9-9225862c5cf8/datasets/51a56115-ac32-437a-8f2c-3ed1fa1dc37a/rows?key=24THP%2FqLUg2EWnDtFiTUr8GTjjPOU%2FxjT%2BnkTt9%2FHMlkMG%2B5BhWe0pYVfsJcE8gVNitZ3C2Fp1akv3LR7hLVNQ%3D%3D'
    tokenizer = RegexpTokenizer(r'\w+')
    sia = SentimentIntensityAnalyzer()
    pri = [1,2,3, 1,2,3, 1,2,3, 1,2,3, 1,2,3, 1,2,3]
    d = {'Bangalore':'India','Mumbai':'India','New York':'US','Chicago':'US','San Francisco':'US','London':'UK'}
    run_data_collect_for_minutes = 8
    """
    def data_collect(self):
        start_time = int(time.strftime("%M", time.localtime()))
        if start_time >= 30:
            start_time = (start_time+30)%60
        end_time = start_time + self.run_data_collect_for_minutes
        name = str(date.today()) + '.csv'
        path = os.path.join('app_walmart/datasets/',name)
        df = pd.read_csv(path)
        if df.empty:
            i = 0
        else:
            i =  len(df)+1
        print('Data collection started')
        for value in self.keywords:
            for tweet in self.limit_handle(tweepy.Cursor(self.api.search, value,result_type="recent",include_entities=True, lang='en').items(5000)):
                try :
                    for j in self.locations:
                        if re.search(j,tweet.user.location):
                            df.loc[i] = [tweet.text, tweet.favorite_count, tweet.retweet_count, self.place_dictionary.get(j), value]
                            i = i + 1                    
                    sleep(5)
                except tweepy.TweepError as e:
                    print(e.reason)
                except StopIteration:
                    break
            current_time = int(time.strftime("%M", time.localtime()))
            if current_time >= end_time:
                break
        df.drop_duplicates(subset=['Tweets', 'location', 'phone'])
        df.to_csv(path, index = False)
        print('Dataset',name,'updated to',i,'tweets')
        print('Data collection ended')
    """
    
    def data_collect(self):
        start_m = int(time.strftime("%M", time.localtime()))
        start_h = end_h = int(time.strftime("%H", time.localtime()))
        end_m = start_m + self.run_data_collect_for_minutes
        if end_m > 60:
            end_m = end_m % 60
            end_h = (end_h + 1)%24
        name = str(date.today()) + '.csv'
        path = os.path.join('app_walmart/datasets/',name)
        df = pd.read_csv(path)
        if df.empty:
            i = 0
        else:
            i =  len(df)+1
        print('Data collection started')
        for value in self.keywords:
            try:
                for tweet in self.limit_handle(tweepy.Cursor(self.api.search, value,result_type="recent",include_entities=True, lang='en').items(50)):
                    time.sleep(50)
                    for j in self.locations:
                        if re.search(j,tweet.user.location):
                            df.loc[i] = [tweet.text, tweet.favorite_count, tweet.retweet_count, self.place_dictionary.get(j), value]
                            i = i + 1
            except tweepy.TweepError as e:
                print(e.reason)
"""
                        cur_m = int(time.strftime("%M", time.localtime()))
                        cur_h = int(time.strftime("%H", time.localtime()))
                        if cur_m>=end_m and cur_h>=end_h:
                            break
                    else:
                        continue
                    break
            
            else:
                continue
            break
"""
        df.drop_duplicates(subset=['Tweets', 'location', 'phone'])
        df.to_csv(path, index = False)
        print('Dataset',name,'updated to',i,'tweets')
        print('Data collection ended')
    
    def limit_handle(self,cursor):
        while True:
            try:
                yield cursor.next()
            except tweepy.RateLimitError:
                time.sleep(1000)

    def __init__(self):
        name = str(date.today()) + '.csv'
        file = os.path.join('app_walmart/datasets/',name)
        if not path.exists(file):
            self.df_make()
        scheduler = BackgroundScheduler()
        scheduler.add_job(self.data_collect, 'cron', minute='00,10,20,30,40,50')
        scheduler.add_job(self.sentiment_analysis, 'cron', hour='7', minute='15')
        scheduler.add_job(self.df_make, 'cron', hour='0')
        scheduler.start()

    def sentiment_analysis(self):
        name = str(date.today() - timedelta(days = 1))  + '.csv'
        path = os.path.join('app_walmart/datasets/',name)
        data = pd.read_csv(path)
        tweet_list = []
        for t in data.Tweets:
            tweet_list.append(' '.join(self.tokenizer.tokenize(t)))
        data.Tweets = tweet_list
        sentiment_list = []
        for t in data.Tweets:
            if (self.sia.polarity_scores(t)['compound']) >= 0:
                sentiment_list.append("Positive")
            else:
                sentiment_list.append("Negative")
        data['PosNeg'] = sentiment_list
        data = data.loc[data['PosNeg'] == 'Positive']
        sol = data[['location','company','Tweets','rt_count']].groupby(['location','company',]).agg(['sum','count'])
        sol = sol[[('Tweets','count'),('rt_count','sum')]].sort_values(by=['location',('Tweets','count'),('rt_count','sum')],ascending=False)
        sol = sol.reset_index(level=['location','company'])
        df = np.column_stack((sol['location'], sol['company'], sol[('Tweets','count')], sol[('rt_count','sum')], self.pri))
        df = pd.DataFrame(df, columns=['Location', 'Phone', 'Tweets', 'Retweets', 'Priority'])
        country = []
        for i in df['Location']:
            country.append(self.d.get(i))
        df['Country'] = country
        data_json = bytes(df.to_json(orient='records'), encoding='utf-8')
        self.to_bi_api(data_json)

    def to_bi_api(self,data_json):
        t = time.strftime(" %H:%M:%S,%m/%d/%Y", time.localtime())
        #requests.post(self.REST_API_URL, data_json)
        with open("details.json", "w") as outfile:
            json.dump({"last_updated":t,"details":data_json}, outfile)
        print('Sent data to BI API at',t)

    def df_make(self):
        name = str(date.today()) + '.csv'
        path = os.path.join('app_walmart/datasets/',name)
        df = pd.DataFrame(columns = ['Tweets', 'fav_count', 'rt_count', 'location', 'phone'])
        df.to_csv(path, index=False)
