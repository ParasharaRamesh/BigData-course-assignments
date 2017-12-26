import json
f=open("data4.txt","r")
j=open("result.json","w")
token=dict()
lines=f.readlines()[0:3000]
for line in lines:
    print "."
    if(line=="\n"):
        continue
    first=line.split()[0]
    if(first=="URL"):
        url=line.split()[1]
    elif(first=="MENTION"):
        continue
    elif(first=="TOKEN"):
        tok = line.split()[1]
        try:
            token[tok].append(url)
            print "*"
        except:
            token[tok] = []
            token[tok].append(url)
            print "#"

print "dictionary created!!now dumping..."
print token
json.dump(token,j)
       
print "done!!"
f.close()
j.close()





'''
data file format:
 URL\t<url>\n
 MENTION\t<mention>\t<byte_offset>\t<target_url>\n
 MENTION\t<mention>\t<byte_offset>\t<target_url>\n
 MENTION\t<mention>\t<byte_offset>\t<target_url>\n
 ...
 TOKEN\t<token>\t<byte_offset>\n
 TOKEN\t<token>\t<byte_offset>\n
 TOKEN\t<token>\t<byte_offset>\n
 ...
 \n\n
 URL\t<url>\n

'''
