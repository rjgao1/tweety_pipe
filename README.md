# tweety_pipe
A real-time data processor built with Spark and Kafka. Data visualized with a simple Flask app and Chart.js

This project has two components. The first component is 

## twitter-spark
This component gives a dynamic bargraph of the trending Twitter hastags, aiming to mimic the Twitter "trending" function.  
This component streams live tweets, processes and visualizes them in real-time

It is composed of a Twitter app, a real-time Spark processor and a Flask app.

The Twitter app streams public tweets to the Spark processor, the Spark processor counts hashtags among received tweets and
maintains an aggregated dataframe with the top 8 hashtags and their counts. Spark then streams the dataframe to the Flask app for visualization.   

Chart.js (https://www.chartjs.org/) is used for the bargraph.  
You can download the latest release here https://github.com/chartjs/Chart.js/releases

### how to use twitter-spark
Make sure you have Python2.7. Unfortunately, this program does not work with Python 3:(

Since twitter-spark streams live tweets from Titter, you need to create a Twitter App and obatin keys and tokens in order to use it.  

Once you have your Twitter App and your keys and tokens, set up environment variables: add the following lines to your .bashrc file:  
```
export TWITTER_ACCESS_TOKEN=[YOUR ACCESS TOKEN]
export TWITTER_ACCESS_SECRET=[YOUR ACCESS TOKEN SECRET]
export TWITTER_CONSUMER_KEY=[YOUR CONSUMER API KEY]
export TWITTER_CONSUMER_SECRET=[YOUR CONSUMER API SECRET KEY]
```

If you don't have Spark installed, you can download the version I used here  

https://archive.apache.org/dist/spark/spark-1.6.3/  

Install pyspark, requests and requests-oauthlib (make sure you install them for python2.7).   
```pip install pyspark requests requests-oauthlib```

Git clone this repository and cd into twitter-spark . 
```
git clone https://github.com/rjgao1/tweety_pipe.git
cd tweety_pipe/twitter-spark
```

Launch the Twitter app that streams tweets to Spark.  
```python twitterToSpark.py```

Launch the Spark processor.  
```python sparkProcess.py```

Navigate to the directory named visualize and launch the Flask app.  
```
cd visualize
python app.py
```

Now open a browser and go to `0.0.0.0:5050`,  and you should be able to see a bargraph starts to move as time goes.  
You can mostly tell what's been trending on Twitter after a few minutes!


The second component of this project is

## monitor 
This component produces a line graph that represents the change of positive and negative words posted on Twitter over time.  
It streams tweets and processes and calculates the number of positive and negative words in real-time.

It is composed of a Kafka producer and a Spark processor.

The Kafka producer uses tweepy to stream live tweets and produce tweets to Kafka topic "twitterStream". The Spark processor then consumes from that topic in real-time, calculates the numbers of positive and negative words. After termination, it produces a linegraph of the change of numbers of positive and negative words over time.  

Some features of this component are not finished yet, so I don't want to put out a tutorial for how to use it just yet. 
