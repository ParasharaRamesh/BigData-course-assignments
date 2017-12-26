#!/usr/bin/env python

import sys
token=""
key="hot"
# input comes from STDIN (standard input)
for line in sys.stdin:
	# remove leading and trailing whitespace
	words=line.split()
	# split the line into words
	if(line.startswith("TOKEN")):
		token=words[1]
        if(token==key):
            print '%s %s' % (token, 1)



print '%s %s %s' % ("ch",key,"s")
