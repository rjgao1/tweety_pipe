# tweety_pipe
A real-time data processor built with Spark and Kafka. Data visualized with a simple Flask app and Chart.js

This project has two components. The first component is 

## twitter-spark
This component streams live tweets and processes them in real-time. It visualizes the trending hashtags in the form of a bargraph. It aims to mimic the Twitter "trending" function.  

It is composed of a Twitter app, a real-time Spark processor and a Flask app.

The Twitter app streams public tweets to the Spark processor, the Spark processor counts hashtags among received tweets and
maintains an aggregated dataframe with the top 8 hashtags and their counts. Spark then streams the dataframe to the Flask app for visualization.   

Chart.js (https://www.chartjs.org/) is used for the bargraph.  
You can download the latest release here https://github.com/chartjs/Chart.js/releases

### how to use twitter-spark

git clone this repository and cd into twitter-spark

```
git clone https://github.com/rjgao1/tweety_pipe.git
cd tweety_pipe/twitter-spark
```
if you don't have Spark installed, you can download the version I used here  

https://archive.apache.org/dist/spark/spark-1.6.3/  

install pyspark if you don't have it already  

```pip install pyspark```
