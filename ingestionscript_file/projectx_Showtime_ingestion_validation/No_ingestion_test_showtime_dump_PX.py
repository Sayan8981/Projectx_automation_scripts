"""Saayan"""
import threading
from multiprocessing import Process
import pymongo
import sys
import os
import csv
from urllib2 import HTTPError,URLError
import socket
import datetime
import urllib2
import json
import httplib
import unidecode,pinyin
homedir=os.path.expanduser("~")
sys.path.insert(0,'%s/common_lib'%homedir)
from lib import lib_common_modules,ingestion_script_modules
sys.setrecursionlimit(1500)

class showtime_ingestion:

    retry_count=0 
    #TODO: INITIALIZATION
    def __init__(self):
        self.source="Showtime"
        self.showtime_id=0
        self.show_type=''
        self.title=''
        self.episode_title=''
        self.showtime_show_id=0
        self.series_title=''
        self.season_number=0
        self.episode_number=0
        self.year=''
        self.expiry_date=''
        self.updated_at=''
        self.px_response='Null'
        self.fieldnames = ["%s_id"%self.source,"Projectx_id","%s_show_type"%self.source,"title","series_title","episode_title","Series_id","season_number","episode_number","year","Duration","Updated_at","expiry_date","Duplicate_present","Duplicate_source_id","Ingested","Not_ingested","Px_response"]

    #TODO: one time call param
    def constant_param(self):
        self.logger=''
        self.total=0
        self.ingested_count=0
        self.not_ingested_count=0
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'

    def get_env_url(self):    
        self.source_mapping_api="http://preprod-projectx-api-545109534.us-east-1.elb.amazonaws.com/projectx/mappingfromsource?sourceIds=%s&sourceName=%s&showType=%s"
        self.source_duplicate_api="http://preprod-projectx-api-545109534.us-east-1.elb.amazonaws.com/projectx/duplicate?sourceId=%s&sourceName=%s&showType=%s"
        self.px_mapping_api='http://preprod-projectx-api-545109534.us-east-1.elb.amazonaws.com/projectx/%d/mapping/' 
        self.programs_api='https://preprod.caavo.com/programs?ids=%s&ott=true&aliases=true'   
    
    # set up connection of DB
    def mongo_connection(self):
        self.connection=pymongo.MongoClient("mongodb://127.0.0.1:27017/")
        self.sourceDB=self.connection["qadb"] 
        self.sourcetable=self.sourceDB["headrun"]

    def query_execute(self,_id):
        try:
            query_headrun_showtime=self.sourcetable.aggregate([{"$match":{"$and":[{"item_type":{"$in":["tvshow","movie","episode"]}},{"service":"showtime"}]}},{"$project":{"id":1,"_id":0,"item_type":1,"series_id":1,"title":1,"episode_title":1,"release_year":1,"episode_number":1,"season_number":1,"duration":1,"image_url":1,"url":1,"description":1,"cast":1,"directors":1,"writers":1,"categories":1,"genres":1,"expiry_date":1,"purchase_info":1,"service":1}},{"$skip":_id},{"$limit":100}])
            return query_headrun_showtime
        except (MySQLError,IntegrityError) as e:
            self.logger.debug(['Got error {!r}, errno is {}'.format(e, e.args[0])])
            self.query_execute()    

    #TODO: to check showtime_id ingestion
    def ingestion_checking(self,thread_name):
        #import pdb;pdb.set_trace()
        duplicate_px_id=[]
        try:
            showtime_mapping_api=self.source_mapping_api%(self.showtime_id,self.source,self.show_type.encode())
            data_showtime_resp=lib_common_modules().fetch_response_for_api_(showtime_mapping_api,self.token)
            if data_showtime_resp!=[]: 
                self.ingested_count+=1
                #TODO: checking program response in Programs_Id search api 
                for response in data_showtime_resp:
                    self.projectx_id = response["projectx_id"]
                    program_response=lib_common_modules().fetch_response_for_api_(self.programs_api%self.projectx_id,self.token)
                if program_response!=[]:
                    self.px_response='True'    
                self.writer.writerow([self.showtime_id,self.projectx_id,self.show_type,self.title,self.series_title,self.episode_title,self.showtime_show_id,self.season_number,self.episode_number,self.year,self.duration,self.updated_at,self.expiry_date,'','','True','',self.px_response])
            # TODO : to check duplicate source_ids 
            else:   
                if self.show_type=='SM' or self.show_type=='MO':
                    showtime_duplicate_api=self.source_duplicate_api%(self.showtime_id,self.source,self.show_type.encode())
                else:
                    #TODO: for episodes
                    showtime_duplicate_api=self.source_duplicate_api%(self.showtime_show_id,self.source,'SM')    
                data_showtime_resp_duplicate=lib_common_modules().fetch_response_for_api_(showtime_duplicate_api,self.token)
                #import pdb;pdb.set_trace()
                if data_showtime_resp_duplicate!=[]:
                    self.ingested_count+=1
                    for px_id in data_showtime_resp_duplicate:
                        duplicate_px_id.append(px_id.get("projectx_id"))
                    if self.show_type=='MO' or self.show_type=='SM':    
                        source_id_duplicate=ingestion_script_modules().getting_duplicate_source_id(duplicate_px_id,self.px_mapping_api,self.show_type,self.token,self.source)
                    else:
                        #TODO : for episode
                        source_id_duplicate=ingestion_script_modules().getting_duplicate_source_id(duplicate_px_id,self.px_mapping_api,'SM',self.token,self.source)    
                    self.writer.writerow([self.showtime_id,duplicate_px_id,self.show_type,self.title,self.series_title,self.episode_title,self.showtime_show_id,self.season_number,self.episode_number,self.year,self.duration,self.updated_at,self.expiry_date,'True',source_id_duplicate,'True'])
                else:
                    self.not_ingested_count+=1
                    self.writer.writerow([self.showtime_id,'',self.show_type,self.title,self.series_title,self.episode_title,self.showtime_show_id,self.season_number,self.episode_number,self.year,self.duration,self.updated_at,self.expiry_date,'False','','','True',self.px_response]) 
            self.logger.debug('\n')
            self.logger.debug(["%s_id:"%self.source,self.showtime_id,"show_type:",self.show_type,thread_name,"updated:","title:",self.title,"series_title: ",self.series_title,"season_no:",self.season_number,"episode_no:",self.episode_number,"ingested_count:",self.ingested_count,
                   "not_ingested_count:", self.not_ingested_count])                              
        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,URLError,RuntimeError,ValueError) as e:
            #import pdb;pdb.set_trace()
            self.retry_count+=1
            self.logger.debug(["exception caught ingestion_checking func.........................",type(e),self.showtime_id,self.show_type,thread_name])
            self.logger.debug(["Retrying.............",self.retry_count])
            if self.retry_count<=5:
                self.ingestion_checking(thread_name)
            else:
                self.retry_count=0 

    #TODO: getting source_ids from Showtimeanytime_dump
    def getting_source_details(self,data):
        self.logger.debug("\n")
        self.logger.debug(["Checking ingestion of showtime series, Movies,episodes in Projectx ........."])
        #import pdb;pdb.set_trace()
        self.showtime_id=data["id"]
        self.showtime_show_id=data["series_id"]
        self.show_type=data["item_type"]
        self.title=unidecode.unidecode(pinyin.get(data["title"]))
        self.episode_title= unidecode.unidecode(pinyin.get(data["episode_title"]))
        self.series_title=unidecode.unidecode(pinyin.get(data["title"]))
        self.season_number=data["season_number"]
        self.episode_number=data["episode_number"]
        self.year=data["release_year"]
        self.duration=data["duration"]
        self.expiry_date=data["expiry_date"]
        self.updated_at=''
        self.show_type='MO' if self.show_type=='movie' else self.show_type
        self.show_type='SE' if self.show_type=='episode' else self.show_type
        self.show_type='SM' if self.show_type=='tvshow' else self.show_type
    
    def main(self,start_id,thread_name,end_id):
        #import pdb;pdb.set_trace()
        self.mongo_connection()
        self.constant_param()
        self.get_env_url()
        self.logger=lib_common_modules().create_log(os.getcwd()+"/log/log.txt")
        result_sheet='/output/%s_Ingestion_checked_in_Px_%s_%s.csv'%(self.source,thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
            self.writer.writerow(self.fieldnames)
            for id_ in range(start_id,end_id,100):
                self.total+=1
                showtime_data = self.query_execute(id_)
                for data in showtime_data:
                    self.getting_source_details(data) 
                    if self.show_type is not None:   
                        self.ingestion_checking(thread_name)
                    else:
                        self.writer.writerow([self.showtime_id,'',self.show_type,self.title,self.series_title,self.episode_title,self.showtime_show_id,self.season_number,self.episode_number,self.year,self.duration,self.updated_at,self.expiry_date])         
                    self.logger.debug("\n")
                    self.logger.debug([{"Total":self.total,"ingested_count":self.ingested_count,
                           "not_ingested_count": self.not_ingested_count,"Thread_name":thread_name}])
                    self.logger.debug("\n")
                    self.logger.debug(["date time:", datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")])
        output_file.close() 
        self.connection.close()

    # TODO: multi process Operations to call getting_px_ids
    def thread_pool(self): 
        t1=Process(target=self.main,args=(0,"thread-1",1000))
        t1.start()
        t2=Process(target=self.main,args=(1000,"thread-2",2000))
        t2.start()
        t3=Process(target=self.main,args=(2000,"thread-3",3000))
        t3.start()

#TODO: starting and creating class object and calling functions
showtime_ingestion().thread_pool()    
