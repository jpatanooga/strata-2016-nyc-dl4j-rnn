#!/bin/bash

export BEER_REVIEW_PATH=/home/ubuntu/data/beer
export MODEL_SAVE_PATH=/home/ubuntu/models
java -Xms8g -Xms10g -cp target/Strata_2016_NYC_RNN_Talk-1.0-SNAPSHOT.jar org.deeplearning4j.examples.rnn.beer.LSTMBeerReviewModelingExample 256 128 200 100 1000
