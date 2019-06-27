# spark stream processor that processes data sent from kafka
from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext
from pyspark.streaming.kafka import KafkaUtils
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# load postivie and negative word lists
def load(wordlist):
	words = {}
	f = open(wordlist, 'rU')
	txt = f.read()
	txt = 
