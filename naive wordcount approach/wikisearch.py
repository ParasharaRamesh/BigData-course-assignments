import json
from timeit import *
def keyword_search(w):
	with open('result.json') as json_data:
		token = json.load(json_data)
		#print "processed"
	try:
		#print "the urls are"
		print token[w]
	except:
		print "not found!!"

w=raw_input("enter a word")
#f=int(raw_input("enter file no"))
keyword_search(w) 

