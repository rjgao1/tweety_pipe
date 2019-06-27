from kafka import SimpleProducer, KafkaClient
from tweepy.streaming import StreamListener
from tweepy import Stream
from tweepy import OAuthHandler

import os

consumer_key = os.environ.get("TWITTER_CONSUMER_KEY")
consumer_secret = os.environ.get("TWITTER_CONSUMER_SECRET")
access_token = os.environ.get("TWITTER_ACCESS_TOKEN")
access_token_secret = os.environ.get("TWITTER_ACCESS_SECRET")

class KafkaListener(StreamListener):
	def on_data(self, streamData):
		producer.send_messages("twitterStream", streamData.encode('utf-8'))
		print(streamData)
		return True

	def on_error(self, tatus):
		print(status)
# main logic

# set up kafkaClient and producer to post to topic
kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
# set up auth
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

listener = KafkaListener()
stream = Stream(auth, listener)
stream.filter(track="twitterStream")
