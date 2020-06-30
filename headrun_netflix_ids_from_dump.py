"""Saayan"""

from multiprocessing import Process
import sys
import os
import csv
import pymongo
import socket
import datetime
import json
homedir=os.path.expanduser("~")
sys.path.insert(0,'%s/common_lib'%homedir)
from lib import lib_common_modules
sys.setrecursionlimit(1500)

class headrun_netflix_ids:
    #TODO: INITIALIZATION
    def __init__(self):
        self.source='headrun_netflix'
        self.total=0

    #TODO: Mongoconnection set up
    def mongo_connection(self):
        self.connection=pymongo.MongoClient("mongodb://192.168.86.10:27017/")
        self.mydb=self.connection["qadb"]
        self.sourceidtable=self.mydb["headrun"]  

    #TODO: to get source_details 
    def getting_source_details(self,data): 
        #import pdb;pdb.set_trace()
        series_id=0
        _id=data.get("id").encode()
        show_type=data.get("item_type").encode()
        if show_type=="movie":
            #import pdb;pdb.set_trace()
            show_type="MO"
        elif show_type=="tvshow":
            #import pdb;pdb.set_trace()
            show_type="SM"    
        else:
            #import pdb;pdb.set_trace()
            series_id=data.get("series_id")
            show_type="SE"
        print ("\n")    
        print ({"headrun_netflix_id":_id,"series_id":series_id,"show_type":show_type,"total":self.total})
        purchase_type=data.get("purchase_info")
        if purchase_type!="":
            purchase_type='True'
        else:
            purchase_type='Null'
        return {"series_id":series_id,"_id":_id,"show_type":show_type,"purchase_type":purchase_type}                     

    def main(self,start_id,thread_name,end_id):
        ##import pdb;pdb.set_trace()
        print({"start":start_id,"end":end_id}) 
        fieldnames = ["%s_series_id"%self.source,"%s_id"%self.source,"show_type","release_year","title"
                           ,"episode_title","duration","purchase_type","language","service"]   
        result_sheet='/output_headrun_netflix/%s_id_%s_%s.csv'%(self.source,thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile:
            writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
            writer.writerow(fieldnames)
            #import pdb;pdb.set_trace()
            for _id in range(start_id,end_id,1000):
                try:
                    #import pdb;pdb.set_trace()
                    print({"skip":_id})
                    id_query=self.sourceidtable.aggregate([{"$match":{"$and":[{"item_type":{"$in":["movie","episode","tvshow"]}},{"service":"netflix"}]}},{"$project":
                        {"id":1,"_id":0,"item_type":1,"series_id":1,"title":1,"episode_title":1,"release_year":1,
                        "episode_number":1,"season_number":1,"duration":1,"image_url":1,"url":1,"description":1,"cast":1,"directors":1,"writers":1,
                        "categories":1,"genres":1,"maturity_ratings":1,"purchase_info":1,"service":1}},{"$skip":_id},{"$limit":1000}])
                    #id_query=self.sourceidtable.find({"service":"netflix"}).skip(_id).limit(10)
                    for data in id_query:
                        #import pdb;pdb.set_trace()
                        self.total+=1
                        print({"thread_name":thread_name,"total":self.total})
                        details=self.getting_source_details(data)
                        print ("\n")
                        print (data["episode_title"],data["title"])
                        writer.writerow([details["series_id"],details["_id"],details["show_type"],data["release_year"],data["title"].encode("ascii","ignore"),
                            data["episode_title"].encode("ascii","ignore"),data["duration"],details["purchase_type"],data.get("language"),data["service"].encode("utf-8")])
                except (Exception,pymongo.errors.CursorNotFound) as e:
                    print ("get exception", type(e))
                    pass        
        output_file.close()
        self.connection.close()

    # TODO : to run threads in Thread pool
    def thread_pool(self):   

        t1=Process(target=self.main,args=(0,"thread-1",10000))
        t1.start()
        t2=Process(target=self.main,args=(10000,"thread-2",20000))
        t2.start()
        t3=Process(target=self.main,args=(20000,"thread-3",30000))
        t3.start()
        t4=Process(target=self.main,args=(30000,"thread-4",40000))
        t4.start()
        t5=Process(target=self.main,args=(40000,"thread-5",51900))
        t5.start()


#starting and calling functions 
object_=headrun_netflix_ids()
object_.mongo_connection()
object_.thread_pool()    