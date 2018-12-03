#!/usr/bin/python
from org.apache.pig.scripting import *

# thanks to
# https://fr.hortonworks.com/blog/pagerank-implementation-in-pig/
# http://www.xavierdupre.fr/app/ensae_teaching_cs/helpsphinx2/notebooks/2015_page_rank.html


P =  Pig.compile("""
-- PR(A) = (1-d) + d (PR(T1)/C(T1) + ... + PR(Tn)/C(Tn));
initial_pagerank = LOAD '$docs_in' USING PigStorage(';') AS ( url: chararray, pagerank: float, links:{ link: ( url: chararray ) } );

DEFINE my_macro(previous_pagerank) RETURNS new_pagerank {

	outbound_pagerank =
	FOREACH $previous_pagerank
	GENERATE
	pagerank / COUNT ( links ) AS pagerank,
	FLATTEN ( links ) AS to_url;

	$new_pagerank =
	FOREACH
	( COGROUP outbound_pagerank BY to_url, $previous_pagerank BY url INNER )
	GENERATE
	group AS url,
	( 1 - $df ) + $df * SUM ( outbound_pagerank.pagerank ) AS pagerank,
	FLATTEN ( $previous_pagerank.links ) AS links;
}


Ite1 = my_macro(initial_pagerank);
Ite2 = my_macro(Ite1);
Ite3 = my_macro(Ite2); 
Ite4 = my_macro(Ite3); 
Ite5 = my_macro(Ite4); 
Ite6 = my_macro(Ite5); 
Ite7 = my_macro(Ite6); 
Ite8 = my_macro(Ite7); 
Ite9 = my_macro(Ite8); 
Ite10 = my_macro(Ite9); 
Ite11 = my_macro(Ite10); 
Ite12 = my_macro(Ite11);
Ite13 = my_macro(Ite12); 
Ite14 = my_macro(Ite13); 
Ite15 = my_macro(Ite14); 
Ite16 = my_macro(Ite15); 
Ite17 = my_macro(Ite16); 
Ite18 = my_macro(Ite17); 
Ite19 = my_macro(Ite18); 
Ite20 = my_macro(Ite19); 

STORE Ite20
INTO '$docs_out'
USING PigStorage(';');

DUMP Ite20;
""")

params = { 'df': '0.85', 'docs_in': 'pigs_urls.txt', 'docs_out' : 'out' }

stats = P.bind(params).runSingle()
if not stats.isSuccessful():
	raise 'failed'
