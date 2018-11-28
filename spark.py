from operator import add
import re

from pyspark.sql import SparkSession


DF = 0.85

def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]

def map(id,rank,links):
	for link_id in links:
		yield(link_id, rank/len(links))

def reduce(id,ranks): #TO USE
	pagerank = 0
	for rank in ranks:
		pagerank += rank
	pagerank = (1 - DF) + (DF * pagerank)
	yield(id,pagerank)



spark = SparkSession\
        .builder\
        .appName("SparkProject")\
        .getOrCreate()


urls = "/home/polluux/Documents/Boulot/M2/Molli/urls.txt"

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
        #lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

    # Re-calculates URL ranks based on neighbor contributions.
    ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

# Collects all URL ranks and dump them to console.
for (link, rank) in ranks.collect():
    print("%s has rank: %s." % (link, rank))

spark.stop()