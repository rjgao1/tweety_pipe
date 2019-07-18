# spark stream processor that processes data sent from kafka
from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext
from pyspark.streaming.kafka import KafkaUtils
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# load postivie and negative word lists
# returns dictionary of words
def load(wordlist):
	words = {}
	f = open(wordlist, 'rU')
	txt = f.read()
	txt = txt.split('/n')
        for line in txt:
            words[line] = 1

        f.close()

        return words

# plot positive word count against negative word count
def constructPlot(counts):
    pWordsCounts = []
    nWordsCounts = []

    for feelings_field in counts:
        pWordsCounts.append(feelings_field[0][1])
        nWordsCounts.append(feelings_field[1][1])

    time = []
    for i in range(len(counts)):
        time.append(i)

    posLine = plot(time, pWordsCounts, 'ro-', label='positive words')
    negLine = plot(time, nWordsCounts, 'ko-', label='negative words')
    plt.axis([0, len(counts), 0, max(max(pWordsCounts), max(nWordsCounts))+40])
    plt.xlable('time')
    plt.ylable('count')
    plt.legend(loc = 'upper right')
    plt.savefig('fellingAnalysis.png')

# update function that updates positive and negative counts

def sumCount(newCount, currentCount):
    return sum(newCount) + (currentCount or 0)

# build spark app skeleton: initialize spark context; load pos & neg words; plot pos & neg words
def main():
    # initialize spark context
    conf = SparkConf().setMaster("local[2]").setAppName("twitterStream")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 15) # batch interval 15 seconds
    ssc.checkpoint("checkpoint")

    # load words
    nFeelingWords = load("./Dataset/nFeeling.txt")
    pFeelingWords = load("./Dataset/pFeeling.txt")

    # Implement the core spark streaming process
    # accept Kafka data
    kstream = KafkaUtils.createDirectStream(
            ssc, topics = ["twitterStream"], 
            kafkaParams = {"metadata.broker.list": 'localhost:9092'}
    )
    
    tweets = kstream.map(lambda x: x[1].encode("ascii", "ignore"))
    words.tweets.flatMap(lambda line: line.split(" "))
    nFeelings = words.map(lambda word: ('nFeelings', 1) if word in nFeelingWords else ('nFeelings', 0))
    pFeelings = words.map(lambda word: ('pFeelings', 1) if word in pFeelingWords else ('pFeelings', 0))
    bothFeelings = pFeelings.union(nFeelings)
    feelingCounts = bothFeelings.reduceByKey(lambda x, y: x + y)
    
    currentFeelingCount = feelingCounts.updateStateByKey(sumCount)
    currentFeelingCount.pprint()

    counts = []

    feelingCounts.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))

    # start spark streaming context and plot pos & neg words
    scc.start()
    ssc.awaitTerminationOrTimeout(45)
    ssc.stop(stopGracefully = True)
    constructPlot(plots)
    
if __name__ == "__main__":
    main()









































