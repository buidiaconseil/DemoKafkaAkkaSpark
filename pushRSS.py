import yaml
import json
import urllib
import urllib.request as urllib2
from lxml import etree
from bs4 import BeautifulSoup

#create the object, assign it to a variable
count = 0
with open("content.rss") as stream:
    for line in stream:
        xmlString=json.loads(line)
        #print ("line="+json.loads(line))

        if  xmlString :
            try: 
                tree = etree.fromstring(bytes(bytearray(xmlString, encoding='iso-8859-1')))
                for channel in tree.xpath("/rss/channel"):

                    titleChannel=channel.xpath("title")[0].text
                    descriptionChannel=channel.xpath("title")[0].text
                    
                    for item in channel.xpath("item"):
                        try:
                            data = {}

                            data['title']=item.xpath("title")[0].text
                            if item.xpath('pubDate'):
                                data['date']=item.xpath("pubDate")[0].text
                            if item.xpath("description"):
                                data['description']=item.xpath("description")[0].text
                                #print(item.xpath("description")[0].text)
                            if item.xpath("creator"):
                                data['author']=item.xpath("creator")[0].text
                                #print(item.xpath("creator")[0].text)
                            if item.xpath("author"):
                                data['author']=item.xpath("author")[0].text
                                #print(item.xpath("author")[0].text)
                            if item.xpath("category"):
                                categories=[]
                                
                                for cat in item.xpath("category"):
                                    categories.append(cat.text)
                                data['categories']=categories
                            print (json.dumps(data))
                            count=count+1
                        except Exception as e :
                            print ("Error: ",e)

            except Exception as e :
                            print ("Error: ",e)

print ("nb:"+str(count))
                
        
        

