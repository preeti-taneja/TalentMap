# This script uses BeautifulSoup to parse the urls to the stackexchange file and store them in a text file           
from bs4 import BeautifulSoup
import requests
import re


if __name__ == '__main__':

	url = 'https://archive.org/download/stackexchange'

	response = requests.get(url)
	soup = BeautifulSoup(response.text)
	weblist = []
	#html_page = urllib2.urlopen("https://archive.org/download/stackexchange")
	#soup = BeautifulSoup(html_page)
	for link in soup.findAll('a', attrs={'href': re.compile("^stackoverflow.*7z$")}):
    		weblist.append('https://archive.org/download/stackexchange/'  + link.get('href'))
    #print (link.get('href'))
    
	with open('weblist.txt', 'w+') as filehandle:
    	for listitem in weblist:
        	filehandle.write('%s\n' % listitem)


	filehandle.close()
