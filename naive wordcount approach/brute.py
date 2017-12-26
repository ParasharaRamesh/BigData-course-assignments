key = raw_input("Enter a word:")
url_list = []
with open("data5.txt","r") as f:
	for line in f:
		if line.startswith('MENTION'):
			pass
		elif line.startswith('URL'):
			words = line.split()
			url = words[1]
		elif line.startswith('TOKEN'):
			token_arr = line.split()
			token = token_arr[1]
			if token.lower() == key.lower():
				url_list.append(url)
		else :
			pass

print(url_list)
