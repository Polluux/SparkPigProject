from operator import add
import re

from pyspark.sql import SparkSession

# A part of this code come from here : https://github.com/apache/spark/blob/master/examples/src/main/python/pagerank.py

DF = 0.85

def parseNeighbors(urls):
    res = re.split(r'\s+', urls)
    return res[0], res[1]

def map(id,rank,links):
	for link_id in links:
		yield(link_id, rank/len(links))



spark = SparkSession\
        .builder\
        .appName("SparkProject")\
        .getOrCreate()

spark.sparkContext.setLogLevel('ERROR') #Remove if you want to see ALL infos in the console


urls = "./urls.txt"

iterations = 20


lines = spark.read.text(urls).rdd.map(lambda r: r[0])

# Loads all URLs from input file and initialize their neighbors.
links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

# Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

# Calculates and updates URL ranks continuously using PageRank algorithm.
for iteration in range(iterations):
    # Calculates URL contributions to the rank of other URLs.
    contribs = links.join(ranks).flatMap(
    	lambda rdd1: map(rdd1[0],rdd1[1][1],rdd1[1][0]) )

    # Re-calculates URL ranks based on neighbor contributions.
    ranks = contribs.reduceByKey(add).mapValues(lambda rank: (1-DF)+(DF*rank))

# Collects all URL ranks and dump them to console.
print("=============RESULTS============")
for (link, rank) in ranks.collect():
    print("%s has rank: %s." % (link, rank))
print("================================")

spark.stop()