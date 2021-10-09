import requests
from dotenv import load_dotenv
import boto3
from boto3.dynamodb.conditions import Key
import botocore

from pprint import pprint
import urllib
import os
import base64
import json

from typing import List
from datetime import datetime, timedelta


load_dotenv()

class Tweet:
    def __init__(self, data):
        self.id: str = data['id']
        self.text: str = data['text']
        self.date: datetime = from_iso8601(data['created_at'])
        self.sent = data.get('text_sent', False)
    
    def __str__(self):
        return f'created at {self.date}'

class DogResource():
    def __init__(self, dynamodb=None):
        self._dynamodb = get_dynamodb(dynamodb)
        self._table = get_table('Dog')

    def create_table(self):
        self._table = self._dynamodb.create_table(
            TableName='Dog',
            KeySchema=[
                {
                    'AttributeName': 'tweet_id',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'created_at',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'tweet_id',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'created_at',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )

    def clean(self):
        try:
            self._table.delete()
        except Exception:
            pass
        self.create_table()

    def put(self, tweet: Tweet):
        return self._table.put_item(Item={
            'tweet_id': tweet.id,
            'created_at': tweet.date.isoformat(),
            'text': tweet.text,
            'text_sent': tweet.sent
        })
    
    def update(self, tweet: Tweet):
        return self._table.update_item(
            Key={
                'tweet_id': tweet.id,
                'created_at': tweet.date.isoformat()
            },
            UpdateExpression="set text_sent=:s",
            ExpressionAttributeValues={':s': tweet.sent},
            ReturnValues='UPDATED_NEW'
        )

    def get(self, tweet: Tweet):
        response = self._table.get_item(Key={
            'tweet_id': tweet.id,
            'created_at': tweet.date.isoformat()
        })
        return response.get('Item', None)

    def get_since(self, dt: datetime):
        response = self._table.scan(
            FilterExpression=Key('created_at').gt(dt.isoformat()),
            ProjectionExpression='tweet_id, created_at, #t, text_sent',
            ExpressionAttributeNames={'#t': 'text'}
        )
        data = [{
            'id': item['tweet_id'],
            'created_at': item['created_at'],
            'text': item['text'],
            'text_sent': item['text_sent']
        } for item in response['Items']]

        return [Tweet(item) for item in data]
        

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

def create_dog_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url=os.getenv('DYNAMODB_URL'))

    table = dynamodb.create_table(
        TableName='Dog',
        KeySchema=[
            {
                'AttributeName': 'tweet_id',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'created_at',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'tweet_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'created_at',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )

    return table

def load_test_data():
    with open('example_twitter_response.json') as f:
        return [Tweet(item) for item in json.load(f)['data']]

def get_dynamodb(dynamodb=None):
    if dynamodb:
        return dynamodb
    return boto3.resource('dynamodb', endpoint_url=os.getenv('DYNAMODB_URL'))

def get_table(table_name, dynamodb=None):
    dynamodb = get_dynamodb(dynamodb)
    return dynamodb.Table(table_name)

def get_dog_tweet(tweet_id, created_at, dynamodb=None):
    dynamodb = get_dynamodb(dynamodb)
    table = dynamodb.Table('Dog')
    
    return table.get_item(Key={'tweet_id': tweet_id, 'created_at': created_at})

def clean_db(tweets, dynamodb=None):
    table = get_table('Dog')

    for tweet in tweets:
        table.delete_item(Key={'tweet_id'})
        
def good_tweet(tweet):
    return tweet.date > datetime.now() - timedelta(days = 7) and not tweet.text.startswith("RT")

if __name__ == '__main__':
    dog_resource = DogResource()

    # fetch tweets and add to database
    tweets = load_test_data()
    for tweet in tweets:
        if good_tweet(tweet) and dog_resource.get(tweet) is None:
            dog_resource.put(tweet)

    # send texts for tweets which have not been sent
    sent_count = 0
    for tweet in dog_resource.get_since(datetime.now() - timedelta(7)):
        if not tweet.sent:
            print(f'sending text for tweet {tweet.date}')
            tweet.sent = True
            dog_resource.update(tweet)

