#!/usr/bin/env python
import sys
# maps words to their counts
word2count = dict()

# input comes from STDIN
for line in sys.stdin:
    text = line.split(" ")
    word = text[0]
    key= text[1]
    if(word == "chumma"):
        break
    
    try:
        word2count[word].append(key)
    except:
        word2count[word] =[]
        word2count[word].append(key)
 
# write the tuples to stdout
# Note: they are unsorted

#for word in word2count.keys():
#    print '%s\t%s'% ( word, word2count[word] )


print word2count[key]
