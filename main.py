import requests
import urllib
import os
import base64
import json
from typing import List
from datetime import datetime, timedelta

from dotenv import load_dotenv

load_dotenv()

class Tweet:
    def __init__(self, data):
        self.id: str = data['id']
        self.text: str = data['text']
        self.date: datetime = from_iso8601(data['created_at'])
    
    def __str__(self):
        return f'created at {self.date}'


def get_dog_tweets() -> List[Tweet]:
    api_key = os.getenv('TWITTER_API_KEY')
    api_key_secret = os.getenv('TWITTER_API_KEY_SECRET')
    bearer_token = os.getenv('TWITTER_BEARER_TOKEN')
    user_id = '846137120209190912'
    url = f'https://api.twitter.com/2/users/{user_id}/tweets'
    params = {
        'tweet.fields': 'created_at'
    }
    headers = {
        'Authorization': f'Bearer {bearer_token}',
        'Accept': 'application/json'
    }

    print(f'sending request to {url}')
    r = requests.get(
            url,
            params=params,
            headers=headers
    )
    print(f'got response {r}')

    return [Tweet(item) for item in r.json()['data']]
        

def send_text(phone_number: str, message: str):
    auth_token = os.getenv('TWILIO_AUTH_TOKEN')
    account_sid = 'ACa0ce39a5b490614bc46ada6956b0a64c'

    headers = {
        'Authorization': 'Basic ' + base64.b64encode(f'{account_sid}:{auth_token}'.encode('utf-8')).decode('utf8')
    }

    data = {
        'To': phone_number,
        'MessagingServiceSid': 'MG8438fe356d5320990608ed4940daa10c',
        'Body': message
    }

    print('sending twilio post request')
    r = requests.post(
        'https://api.twilio.com/2010-04-01/Accounts/ACa0ce39a5b490614bc46ada6956b0a64c/Messages.json',
        data = data,
        headers = headers
    )
    print(f'got response {r}')

def from_iso8601(s):
    if s[-1].lower() == 'z':
        return datetime.fromisoformat(s[:-1])
    return datetime.fromisoformat(s)

if __name__ == '__main__':
    tweets = get_dog_tweets()
    for tweet in tweets:
        print(tweet)
    
    sent_count = 0
    for tweet in tweets:
        if tweet.date > datetime.now() - timedelta(days = 10) and not tweet.text.startswith("RT"):
            sent_count += 1
            send_text(os.getenv('TO_PHONE'), f'"{tweet.text}"\n\n- Dog')
        if sent_count >= 7:
            break
        
    

    # send_text(os.getenv('TO_PHONE'), 'hello there')


