"""Saayan"""
import threading
from multiprocessing import Process
import pymysql
import sys
import os
import csv
from urllib2 import HTTPError,URLError
import socket
import datetime
import urllib2
import json
import httplib
import unidecode
homedir=os.path.expanduser("~")
sys.path.insert(0,'%s/common_lib'%homedir)
from lib import lib_common_modules,ingestion_script_modules
sys.setrecursionlimit(1500)

class starz_ingestion:
    retry_count=0
    #TODO: INITIALIZATION
    def __init__(self):
        self.source="Starz"
        self.starz_id=0
        self.projectx_id=0
        self.show_type=''
        self.title=''
        self.starz_show_id=0
        self.series_title=''
        self.season_number=0
        self.episode_number=0
        self.year=''
        self.expiry_date=''
        self.updated_at=''
        self.px_response='Null'
        self.fieldnames = ["%s_id"%self.source,"Projectx_id","%s_show_type"%self.source,"title","series_title","Series_id","season_number","episode_number","year","Duration","Updated_at","expiry_date","Duplicate_present","Duplicate_source_id","Ingested","Not_ingested","Px_response"]

    #TODO: one time call param
    def constant_param(self):
        self.logger=''
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.total=0
        self.projectx_id=0
        self.ingested_count=0
        self.not_ingested_count=0

    def get_env_url(self):    
        self.source_mapping_api="http://preprod-projectx-api-545109534.us-east-1.elb.amazonaws.com/projectx/mappingfromsource?sourceIds=%s&sourceName=%s&showType=%s"
        self.source_duplicate_api="http://preprod-projectx-api-545109534.us-east-1.elb.amazonaws.com/projectx/duplicate?sourceId=%s&sourceName=%s&showType=%s"
        self.px_mapping_api='http://preprod-projectx-api-545109534.us-east-1.elb.amazonaws.com/projectx/%d/mapping/' 
        self.programs_api='https://preprod.caavo.com/programs?ids=%s&ott=true&aliases=true'   
    
    # set up connection of DB
    def mysql_connection(self):
        self.connection=pymysql.connect(user="root",passwd="root@123",host="127.0.0.1",db="branch_service",port=3306)
        self.cur=self.connection.cursor(pymysql.cursors.DictCursor)

    def query_execute(self,_id):
        try:
            self.query="SELECT * FROM starz_programs limit %d,100"%_id
            self.cur.execute(self.query)
            self.starz_data=self.cur.fetchall()
            return self.starz_data
        except (MySQLError,IntegrityError) as e:
            self.logger.debug(['Got error {!r}, errno is {}'.format(e, e.args[0])])
            self.query_execute()    

    #TODO: to check starz_id ingestion
    def ingestion_checking(self,thread_name):
        #import pdb;pdb.set_trace()
        duplicate_px_id=[]
        try:
            starz_mapping_api=self.source_mapping_api%(self.starz_id,self.source,self.show_type.encode())
            data_starz_resp=lib_common_modules().fetch_response_for_api_(starz_mapping_api,self.token)
            if data_starz_resp!=[]: 
                self.ingested_count+=1
                #TODO: checking program response in Programs_Id search api 
                for response in data_starz_resp:
                    self.projectx_id = response["projectx_id"]
                    program_response=lib_common_modules().fetch_response_for_api_(self.programs_api%self.projectx_id,self.token)
                if program_response!=[]:
                    self.px_response='True'    
                self.writer.writerow([self.starz_id,self.projectx_id,self.show_type,self.title,self.series_title,self.starz_show_id,self.season_number,self.episode_number,self.year,self.duration,self.updated_at,self.expiry_date,'','','True','',self.px_response])
            # TODO : to check duplicate source_ids 
            else:   
                if self.show_type=='SM' or self.show_type=='MO':
                    starz_duplicate_api=self.source_duplicate_api%(self.starz_id,self.source,self.show_type.encode())
                else:
                    #TODO: for episodes
                    starz_duplicate_api=self.source_duplicate_api%(self.starz_show_id,self.source,'SM')    
                data_starz_resp_duplicate=lib_common_modules().fetch_response_for_api_(starz_duplicate_api,self.token)
                if data_starz_resp_duplicate!=[]:
                    self.ingested_count+=1
                    for px_id in data_starz_resp_duplicate:
                        duplicate_px_id.append(px_id.get("projectx_id"))
                    if self.show_type=='MO' or self.show_type=='SM':    
                        source_id_duplicate=ingestion_script_modules().getting_duplicate_source_id(duplicate_px_id,self.px_mapping_api,self.show_type,self.token,self.source)
                    else:
                        #TODO : for episode
                        source_id_duplicate=ingestion_script_modules().getting_duplicate_source_id(duplicate_px_id,self.px_mapping_api,'SM',self.token,self.source)    
                    self.writer.writerow([self.starz_id,duplicate_px_id,self.show_type,self.title,self.series_title,self.starz_show_id,self.season_number,self.episode_number,self.year,self.duration,self.updated_at,self.expiry_date,"True",source_id_duplicate,'','True',''])
                else:
                    self.not_ingested_count+=1
                    self.writer.writerow([self.starz_id,'',self.show_type,self.title,self.series_title,self.starz_show_id,self.season_number,self.episode_number,self.year,self.duration,self.updated_at,self.expiry_date,'False','','','True',self.px_response]) 
                self.logger.debug('\n')
                self.logger.debug(["%s_id:"%self.source,self.starz_id,"show_type:",self.show_type,thread_name,"updated:",self.updated_at.strftime("%m/%d/%Y %H:%M:%S"),"title:",self.title,"series_title: ",self.series_title,"season_no:",self.season_number,"episode_no:",self.episode_number,"ingested_count:",self.ingested_count,
                       "not_ingested_count:", self.not_ingested_count])                              
        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,URLError,RuntimeError,ValueError) as e:
            self.retry_count+=1
            self.logger.debug(["exception caught ingestion_checking func.........................",type(e),self.starz_id,self.show_type,thread_name])
            self.logger.debug(["Retrying.............",self.retry_count])
            if self.retry_count<=5:
                self.ingestion_checking(thread_name)
            else:
                self.retry_count=0 

    #TODO: getting source_ids from Showtimeanytime_dump
    def getting_source_details(self,data):
        self.logger.debug("\n")
        self.logger.debug(["Checking ingestion of starz series, Movies,episodes in Projectx ........."])
        self.starz_id=data["source_program_id"]
        self.starz_show_id=data["series_id"]
        self.show_type=data["item_type"]
        self.title=data["title"]
        self.series_title=data["title"]
        self.season_number=data["season_number"]
        self.episode_number=data["episode_number"]
        self.year=data["release_year"]
        self.duration=data["run_time"]
        self.expiry_date=data["expiry_time"]
        self.updated_at=data["updated_at"]
        self.show_type='MO' if self.show_type=='movie' else self.show_type
        self.show_type='SE' if self.show_type=='episode' else self.show_type
        self.show_type='SM' if self.show_type=='tvshow' else self.show_type
    
    def main(self,start_id,thread_name,end_id):
        #import pdb;pdb.set_trace()
        self.mysql_connection()
        self.constant_param()
        self.get_env_url()
        self.logger=lib_common_modules().create_log(os.getcwd()+"/log/log.txt")
        result_sheet='/output/%s_Ingestion_checked_in_Px_%s_%s.csv'%(self.source,thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
            self.writer.writerow(self.fieldnames)
            for id_ in range(start_id,end_id,100):
                starz_data = self.query_execute(id_)
                for data in starz_data:
                    self.total+=1
                    print ("\n")
                    print (data)
                    self.getting_source_details(data) 
                    if self.show_type is not None:   
                        self.ingestion_checking(thread_name)
                    else:
                        self.writer.writerow([self.starz_id,self.show_type,self.title,self.series_title,self.starz_show_id,self.season_number,self.episode_number,self.year,self.duration,self.updated_at,self.expiry_date])         
                    self.logger.debug("\n")
                    self.logger.debug([{"Total":self.total,"ingested_count":self.ingested_count,
                           "not_ingested_count": self.not_ingested_count,"Thread_name":thread_name,"source_id":self.starz_id,"Projectx_id":self.projectx_id}])
                    self.logger.debug("\n")
                    self.logger.debug(["date time:", datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")])
        output_file.close() 
        self.connection.close()

    # TODO: multi process Operations to call getting_px_ids
    def thread_pool(self): 
        t1=threading.Thread(target=self.main,args=(0,"thread-1",1000))
        t1.start()
        # t2=Process(target=self.main,args=(1000,"thread-2",2000))
        # t2.start()
        # t3=Process(target=self.main,args=(2000,"thread-3",3000))
        # t3.start()
        # t4=Process(target=self.main,args=(3000,"thread-4",4000))
        # t4.start()
        # t5=Process(target=self.main,args=(4000,"thread-5",5000))
        # t5.start()
        # t6=Process(target=self.main,args=(5000,"thread-6",6000))
        # t6.start()
        # t7=Process(target=self.main,args=(6000,"thread-7",7000))
        # t7.start()
        # t8=Process(target=self.main,args=(7000,"thread-8",9000))
        # t8.start()

#TODO: starting and creating class object and calling functions
starz_ingestion().thread_pool()    
