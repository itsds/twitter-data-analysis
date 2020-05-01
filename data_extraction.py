import tweepy
import os
import csv
import time

def extract_data():
    consumer_key = ''
    consumer_secret = ''

    access_token = ''
    access_token_secret = ''

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)

    try:
        os.remove('result.csv')
    except:
        pass

    csvFile = open('result.csv', 'w')

    # Use csv writer
    csvWriter = csv.writer(csvFile)
    data = tweepy.Cursor(api.search, q='#irrfan',lang="en",until="2020-05-01").items(500)
    while True:
        try:
            tweet = data.next()
            if tweet.user.followers_count > 0:  # collecting tweets made by users with min 100k followers
                csvWriter.writerow(
                    [tweet.user.name.encode('utf-8', errors='ignore'), tweet.user.followers_count, tweet.created_at,
                     tweet.text.encode('utf-8', errors='ignore'), tweet.id])
        except tweepy.TweepError:
            time.sleep(600)
            continue
        except StopIteration:
            break

    csvFile.close()
