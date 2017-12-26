#!/usr/bin/env python
import sys
# maps words to their counts
word2count = dict()
w="" 
# input comes from STDIN
for line in sys.stdin:
    global w
    text = line.split(" ")
    word = text[0]
    count= text[1]
    if(word=="ch"):
        w=count
    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        continue

    try:
        word2count[word] = word2count[word]+count
    except:
        word2count[word] = count
 
# write the tuples to stdout
# Note: they are unsorted

for word in word2count.keys():
    print '%s\t%s'% ( word, word2count[word] )



