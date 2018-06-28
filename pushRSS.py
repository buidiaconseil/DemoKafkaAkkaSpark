import yaml
import json
import urllib
import urllib.request as urllib2
from lxml import etree
from bs4 import BeautifulSoup

#create the object, assign it to a variable

with open("content.rss") as stream:
    for line in stream:
        xmlString=json.loads(line)
        #print ("line="+json.loads(line))

        if  xmlString :
            try: 
                tree = etree.fromstring(bytes(bytearray(xmlString, encoding='iso-8859-1')))
                for channel in tree.xpath("/rss/channel"):
                    print("CHANNEL")
                    print(channel.xpath("title")[0].text)
                    print(channel.xpath("description")[0].text)
                    for item in channel.xpath("item"):
                        print(item.xpath("title")[0].text)
                        if hasattr(item, 'description'):
                            print(item.xpath("description")[0].text)
                        if hasattr(item, 'dc:creator'):
                            print(item.xpath("dc:creator")[0].text)
                        if hasattr(item, 'author'):
                            print(item.xpath("author")[0].text)
            except :
                print ("Error")
                
        
        

