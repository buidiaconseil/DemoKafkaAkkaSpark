import yaml
import json
import urllib
import urllib.request as urllib2
from lxml import etree
from bs4 import BeautifulSoup
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
from time import sleep
#docker run -d --name kafka -p 9999:9999 -p 9092:9092 -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 --network kafka-net XXXXXXXXXXXXXXXXXXXXXXXXXXX
#create the object, assign it to a variable
count = 0
nbitems = 10000
nbseconds = 60
lastcount = 0
topicName = 'rss-flow'
millis = time.time() * 1000.0
producer = KafkaProducer(bootstrap_servers=['kafka:9092'])
with open("content.rss") as stream:
    for line in stream:
        xmlString=json.loads(line)
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
                            if item.xpath("creator"):
                                data['author']=item.xpath("creator")[0].text
                            if item.xpath("author"):
                                data['author']=item.xpath("author")[0].text
                            if item.xpath("category"):
                                categories=[]
                                for cat in item.xpath("category"):
                                    categories.append(cat.text)
                                data['categories']=categories
                            if count-lastcount >= nbitems/nbseconds :
                                sleeper=time.time() * 1000.0-millis
                                print("send "+ str(count-lastcount) +" messages. "+str(count)+"/"+str(nbitems), flush=True)
                                if sleeper>0 :
                                    sleep((1000-sleeper)/1000.0)
                                millis = time.time() * 1000
                                lastcount=count
                                producer.flush()
                            count=count+1
                            producer.send(topicName, str.encode(json.dumps(data)))
                        except Exception as e :
                            print ("Error: ",e)
            except Exception as e :
                            print ("Error: ",e)
print ("nb:"+str(count))
producer.close()                
        
        

