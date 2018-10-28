from bs4 import BeautifulSoup
import requests
import unittest
import pandas as pd
import json
from datetime import datetime

start_time = datetime.now()
url = 'https://gist.github.com/paulmillr/2657075'
url_api = "https://api.github.com/"
id_perso = ("mathieubruniquel","mykey")

# Scrapping du TOP 256 gitters
def top_giters(url):
	site = requests.get(url).text
	soup = BeautifulSoup(site, 'html.parser')
	rem = soup.find_all("th",{"scope" : "row"})

	topgiters = []
	      
	for i in range(len(rem)):
		obj = rem[i]
		id = obj.parent.find_all('td')[0].text.split(' ')
		topgiters.append(str(id[0]))

	return topgiters

# Calcul popularité des TOPgitters
def pop_giters(giters_list,id_perso, url):
	popgiters = {}

	for userid in giters_list:
		page = 1
		end = False
		starcount = 0
		moyenne = 0
		n_repo = 0

		while end == False:
			response = requests.request("GET", url + "users/" + userid + "/repos" + "?page=" + str(page) + "&per_page=100", auth = id_perso)
			response_object = response.json()
			n_repo += len(response_object)
			
			for repo in response_object:
				starcount += repo['stargazers_count']
			if len(response_object) < 100:
				end = True
			else:
				page += 1

		moyenne = starcount/n_repo
		popgiters[userid] = "{0:.2f}".format(moyenne)
		print("moyenne = " + str(moyenne))
		print("n_repo = " + str(n_repo))

	sorted_popgiters = sorted(popgiters.items(), key=lambda kv: kv[1], reverse = True)
	return sorted_popgiters
	
	print (datetime.now() - start_time)


top256giters = top_giters(url)
pop_giters(top256giters,id_perso,url_api)
