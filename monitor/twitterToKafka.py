
from tweepy import Stream
from tweepy import OAuthHandler

consumer_key = os.envirion.get("TWITTER_CONSUMER_KEY")
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

kafka = kafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
listener = KafkaListener()
stream = Stream(auth, listener)
stream.filter(track="twitterStream")
