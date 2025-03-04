import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs
from dotenv import load_dotenv
import os

load_dotenv()

ACCESS_KEY = os.getenv('ACCESS_KEY')
ACCESS_SECRET = os.getenv('ACCESS_SECRET')
CONSUMER_KEY = os.getenv('CONSUMER_KEY')
CONSUMER_SECRET = os.getenv('CONSUMER_SECRET')


# Twitter Authentication
auth = tweepy.OAuthHandler(ACCESS_KEY,ACCESS_SECRET)
auth.set_access_token(CONSUMER_KEY,CONSUMER_SECRET)

# Creating an API Object
api = tweepy.API(auth)

tweets = api.user_timeline(screen_name='@elonmusk',
                           count = 5,
                           include_rts = False
                           )

print(tweets)