import yaml
import urllib
import urllib.request as urllib2
#create the object, assign it to a variable

#set http_proxy=pcyvipncp2n.edf.fr:3134
#set https_proxy=pcyvipncp2n.edf.fr:3134

with open("listRSS.yaml") as stream:
    listRSS=yaml.load(stream)
    print ("load")
    for data in listRSS:
        print (data['url'])
        contents = urllib2.urlopen(data['url']).read()
        print (contents)