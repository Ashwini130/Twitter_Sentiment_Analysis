# Twitter Sentiment Analysis

This repo publishes tweets to kafka topic and consumes it using Structured Spark Streaming. It processes each tweet annd assigns a sentiment score to it using Afinn Word repository and outputs the overall sentiment(positive/negative/neutral) on a particular topic of choice.

# Pre-requisites

This code is executed on cloudera 5.11.1 using following :

1) Kafka <br>
2) Spark <br>
3) Jupyter Notebook <br>

# Structure

1) src/main/java - Codes for streaming tweets from twitter API and publishing them to Kafka Topic "tweets". <br>
2) Visualize_sentiment.ipynb - Jupyter Notebook ran in pyspark kernel to process each tweet to extract it's sentiment and visualize the same using Python matplotlib and seaborn libraries.

# Overview

- In this Project I am analysing the sentiment of a particular topic based on real time tweets.
- I used Kafka to store the tweets after streaming them from Twitter API using twitter4j library.
- I used Spark Structured Streaming to subscribe and read streamed data from Kafka with a sliding window operation.
- Using Afinn Word repository for sentiment scores, every tweet was given a sentiment score using a User defined Function in spark.
- Later data is aggregated based on positive/negative/neutral tweets.


For testing , I used the currently heated topic of Pegasus Spyware software for checking public sentiment for the same:




