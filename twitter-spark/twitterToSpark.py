# Twitter app that stream tweets and sned them to our Spark process
import socket
import sys
import requests
import requests_oauthlib
import json

import os

TWITTER_ACCESS_TOKEN = os.environ.get("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_SECRET = os.environ.get("TWITTER_ACCESS_SECRET")
TWITTER_CONSUMER_KEY = os.environ.get("TWITTER_CONSUMER_KEY")
TWITTER_CONSUMER_SECRET = os.environ.get("TWITTER_CONSUMER_SECRET")
twitter_auth = requests_oauthlib.OAuth1(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET)

# Stream tweets from Twitter API
def streamTweets():
	url = "https://stream.twitter.com/1.1/statuses/filter.json"
	# query_data = [('language', 'en'), ('locations', '-122,-37,-122,38'),('track','#')]
	stream_param = [('language', 'en'), ('locations', '-90,-20,100,50'), ('track', '#')]
	stream_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in stream_param])
	resp = requests.get(stream_url, auth=twitter_auth, stream=True)
	print (stream_url, resp)
	
	return resp

# Send tweets to Spark
def twitter_to_spark(resp, tcp_conn):
	for line in resp.iter_lines():
		try:
			whole_tweet = json.loads(line)
			tweet_pure_text = whole_tweet['text'] + '\n' #pyspark can't accept stream, needs '/n'
			print("Tweet pure text is: " + tweet_pure_text)
			print ("------------------------------------------")
			tcp_conn.send(tweet_pure_text.encode('utf-8', errors='ignore'))
		except:
			e = sys.exc_info()[0]
			print("Error is: %s" % e)

# main logic uses TCP/IP protocal. It binds to "localhost:9090". 
conn = None
TCP_IP = "localhost"
TCP_PORT = 9090
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind((TCP_IP, TCP_PORT))
sock.listen(1)
print("Twitter end server is waiting for Spark connection")
conn, addr = sock.accept()
print("Spark process is connected -- streaming tweets to Spark process")
resp = streamTweets()
twitter_to_spark(resp, conn)

