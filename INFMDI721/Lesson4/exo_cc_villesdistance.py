from bs4 import BeautifulSoup
import requests
import unittest
import pandas as pd
import json
from datetime import datetime


top_villes = pd.read_csv("/Users/mathieubruniquel/Desktop/MS BGD/3. Cours/INFMDI 721 - Kit Big Data/Cours_4_/population_villes.csv", sep = ';')
top_villes = top_villes['Population']

API_key = 'AIzaSyDAF1cj51ZzZwMH8DdgRJGyPCVpk91al8A'
url_template = 'https://maps.googleapis.com/maps/api/distancematrix/json?origins={0}&destinations={1}&key={2}'


	for i in range(len(rem)):
		obj = rem[i]
		id = obj.parent.find_all('td')[0].text.split(' ')
		topgiters.append(str(id[0]))

	return topgiters