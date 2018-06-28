import yaml
import urllib
import urllib.request as urllib2
#create the object, assign it to a variable
with open("listRSS.yaml") as stream:
    listRSS=yaml.load(stream)
    print ("load")
    for data in listRSS:
        print (data['url'])
        contents = urllib2.urlopen(data['url']).read()
        print (contents)
