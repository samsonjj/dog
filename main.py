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
        'tweet.fields': 'created_at',
        'max_results': 20,
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
    dynamodb = get_dynamodb(dynamodb)

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

    endpoint_url = os.getenv('DYNAMODB_URL')
    if endpoint_url:
        return boto3.resource('dynamodb', endpoint_url=os.getenv('DYNAMODB_URL'))
    return boto3.resource('dynamodb', region_name='us-east-1')

def get_table(table_name, dynamodb=None):
    dynamodb = get_dynamodb(dynamodb)
    return dynamodb.Table(table_name)

def good_tweet(tweet):
    # currently removing the expired requiement
    # expired = tweet.date > datetime.now() - timedelta(days = 7)
    expired = tweet.date < datetime.strptime('2021/11/01', '%Y/%m/%d')
    retweet = tweet.text.startswith("RT")
    contains_link = ".com" in tweet.text or ".org" in tweet.text or "http" in tweet.text
    contains_at = "@" in tweet.text
    print(f'expired={expired}, retweet={retweet}, contains_link={contains_link}, contians_at={contains_at}')
    return not expired and not retweet and not contains_link and not contains_at

def main(event=None, context=None):
    dog_resource = DogResource()

    # fetch tweets and add to database
    tweets = get_dog_tweets()
    for tweet in tweets:
        print(f'tweet text: {tweet.text}')
        good = good_tweet(tweet)
        present = (not dog_resource.get(tweet) is None)
        print(f'good={good}')
        print(f'present={present}')
        if good and not present:
            if os.getenv('TEST'):
                print('skipping database put due to TEST environment variable')
                continue
            dog_resource.put(tweet)

    # send texts for tweets which have not been sent
    sent_count = 0
    for tweet in dog_resource.get_since(datetime.now() - timedelta(7)):
        if not tweet.sent:
            print(f'sending text for tweet {tweet.date}')
            if os.getenv('TEST') == "TRUE":
                print('skipping sms and database update, due to TEST environment variable')
                continue
            send_text(os.getenv('TO_PHONE'), f'{tweet.text}\n\n- Dog')
            tweet.sent = True
            dog_resource.update(tweet)

            # we only want to send one tweet per run
            break


if __name__ == '__main__':
    main()
