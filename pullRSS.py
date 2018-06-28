import yaml
import json
import urllib
import urllib.request as urllib2
from bs4 import BeautifulSoup


#create the object, assign it to a variable
file = open("content.rss","w") 
with open("listRSS.yaml") as stream:
    listRSS=yaml.load(stream)
    print ("load")
    for data in listRSS:
        print (data['url'])
        contents = urllib2.urlopen(data['url']).read().decode('utf-8')
        print (contents)
        soup = BeautifulSoup(contents)
        file.write(json.dumps(contents)+"\n")
file.close() 
