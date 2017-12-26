#!/usr/bin/env python

import sys
token=""
key="room"
urlset=[]
tokset=False
# input comes from STDIN (standard input)
for line in sys.stdin:	
	#print "I EXIST"
	# remove leading and trailing whitespace
	words=line.split()
	# split the line into words
	if(line.startswith("URL")):
		url=words[1]
		urlset.append(url)
	elif(line.startswith("TOKEN")):
		if(words[1]==key):
			token=words[1]
			tokset=True
	if(tokset and len(urlset)>0):
		print '%s %s' % (token, urlset[-1])
		tokset=False
	


print '%s %s %s' % ("ch",key,"sh")
