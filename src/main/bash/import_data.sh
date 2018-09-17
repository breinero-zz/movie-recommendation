#! /bin/bash

wget http://files.grouplens.org/datasets/movielens/ml-latest-small.zip
zip ml-latest-small.zip
mongoimport --type csv --headerline --uri "mongodb+srv://demo:demo@dataanalyticsworkshop-sshrq.mongodb.net/recommendation" \
-c ratings --ssl --file ml-latest-small/ratings.csv
