import tweepy
import os
import csv
import time

def extract_data():
    consumer_key = 'JoRModfVqudVp2dxNl8n1zBpk'
    consumer_secret = 'xdh9n0QaM3bBj9WDNvmRHlg4wpGu4idDdHyXuWqmg7MJYEgQuV'

    access_token = '835535304-vx5q9XymuFcjvSEVtEfbPrySIkkCfdo7o9YvtNYm'
    access_token_secret = '7wizgIRvtHhfvozGMp3owhrwEzZSLYK0iQaxS3uWkbTeM'

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)

    try:
        os.remove('result.csv')
        # print "File deleted"
    except:
        # print "File was not present already"
        pass

    csvFile = open('result.csv', 'w')

    # Use csv writer
    csvWriter = csv.writer(csvFile)
    # Query(q) ---> AND (surge OR crash OR plunge OR high OR low OR future OR amazing OR good OR bad OR record)
    #data = tweepy.Cursor(api.search, q="bitcoin", since="2017-11-06", until="2017-11-07", lang="en").items()
    # most recent data is fetched first
    data = tweepy.Cursor(api.search, q='#covid',lang="en").items(100)
    while True:
        try:
            tweet = data.next()
            if tweet.user.followers_count > 0:  # collecting tweets made by users with min 100k followers
                # i+=1
                # Write a row to the CSV file. I use encode UTF-8
                csvWriter.writerow(
                    [tweet.user.name.encode('utf-8', errors='ignore'), tweet.user.followers_count, tweet.created_at,
                     tweet.text.encode('utf-8', errors='ignore'), tweet.id])
                # print("------wrote a tweet-----")
        except tweepy.TweepError:
            # print("---------------------In sleep In sleep In sleep In sleep----------------------------")
            time.sleep(600)
            continue
        except StopIteration:
            # print("---------------------Something is wrong----------------------------")
            break

    csvFile.close()
