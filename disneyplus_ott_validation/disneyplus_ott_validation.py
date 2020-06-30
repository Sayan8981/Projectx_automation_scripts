"""Writer: Saayan"""
import threading
from multiprocessing import Process
import csv
import pymongo
import datetime
import sys
import urllib2
import json
import os
from urllib2 import URLError,HTTPError
import httplib
import socket
import pinyin
import unidecode
#import pdb;pdb.set_trace()
homedir=os.path.expanduser("~")
sys.path.insert(0,'%s/common_lib'%homedir)
from lib import lib_common_modules,ott_meta_data_validation_modules
sys.setrecursionlimit(1500) 


class disneyplus_ott_validation:

    retry_count=0
    def __init__(self):
        self.source="DisneyPlus"
        self.service="disney_plus"
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.expired_token='Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
        self.total=0
        self.count_disneyplus_id=0
        self.writer=''
        self.link_expired=''

    def mongo_mysql_connection(self):
        self.connection=pymongo.MongoClient("mongodb://192.168.86.10:27017/")
        self.sourceDB=self.connection["qadb"] 
        self.sourcetable=self.sourceDB["headrun"]

    def get_env_url(self):
        self.prod_domain="api.caavo.com"
        self.projectx_domain="preprod.caavo.com"
        self.expired_api='https://%s/expired_ott/is_available?source_program_id=%s&service_short_name=%s'
        self.source_mapping_api="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%s&sourceName=%s&showType=%s"
        self.projectx_programs_api='https://preprod.caavo.com/programs?ids=%s&ott=true&aliases=true'
        self.projectx_mapping_api='http://34.231.212.186:81/projectx/%d/mapping/'
        self.projectx_duplicate_api='http://34.231.212.186:81/projectx/duplicate?sourceId=%d&sourceName=%s&showType=%s'

    #TODO: to get source_detail OOT link id only from DB table
    def getting_source_details(self,disneyplus_id,show_type,details):
        #import pdb;pdb.set_trace()
        disneyplus_link_present='Null'
        disneyplus_id=details.get("id")
        disneyplus_link=details.get("url").encode("ascii","ignore")
        if disneyplus_link!="" or disneyplus_link is not None :
            disneyplus_link_present='True' 

        return {"source_link_present":disneyplus_link_present,"source_id":disneyplus_id,"show_type":show_type}  

    #TODO: to get projectx_id details OTT link id from programs api
    def getting_projectx_ott_details(self,projectx_id,show_type):
      
        px_video_link=[]
        px_video_link_present='False'
        px_response='Null'
        launch_id=[]
        #import pdb;pdb.set_trace()

        projectx_api=self.projectx_programs_api%projectx_id
        px_resp=urllib2.urlopen(urllib2.Request(projectx_api,None,{'Authorization':self.token}))
        data_px=px_resp.read()
        data_px_resp=json.loads(data_px)
        if data_px_resp!=[]:
            for data in data_px_resp:
                px_video_link= data.get("videos")
                if px_video_link:
                    px_video_link_present='True'
                    for linkid in px_video_link:
                        #if linkid.get("source_id")=='disneyplus':
                        launch_id.append(linkid.get("launch_id"))
            return {"px_video_link_present":px_video_link_present,"launch_id":launch_id}
        else:
            return px_response      

    #TODO: To check OTT links
    def ott_checking(self,only_mapped_ids,disneyplus_id,show_type,thread_name,data):
        #import pdb;pdb.set_trace()
        try:     
            if only_mapped_ids["source_flag"]=='True':
                self.total+=1
                #import pdb;pdb.set_trace()
                projectx_id=only_mapped_ids["px_id"]
                source_id=only_mapped_ids["source_id"]
                self.logger.debug("\n")
                self.logger.debug ({"total_only_disneyplus_mapped":self.total,"%s_id"%self.source:disneyplus_id,
                    "Px_id":projectx_id,"%s_id"%self.source:source_id,"thread_name":thread_name})
                source_details=self.getting_source_details(source_id,show_type,data)
                projectx_details=self.getting_projectx_ott_details(projectx_id,show_type)
                if projectx_details!='Null':
                    if data.get("purchase_info")!="":
                        self.link_expired=lib_common_modules().link_expiry_check_(self.expired_api,self.projectx_domain,source_id,self.service,self.expired_token)
                        ott_validation_result=ott_meta_data_validation_modules().ott_validation(projectx_details,source_id)
                        self.writer.writerow([source_id,projectx_id,show_type,projectx_details["px_video_link_present"],
                                  source_details["source_link_present"],ott_validation_result,only_mapped_ids["source_flag"],'',self.link_expired])
                    else:
                        self.writer.writerow([source_id,projectx_id,show_type,'','','',
                            only_mapped_ids["source_flag"],'purchase_info_null'])     
                else:
                    self.writer.writerow([source_id,projectx_id,show_type,projectx_details,
                             source_details["source_link_present"],'',only_mapped_ids["source_flag"],'Px_response_null'])
            elif only_mapped_ids["source_flag"]=='True(Rovi+others)':
                if show_type!='SM':
                    projectx_id=only_mapped_ids["px_id"]
                    source_id=only_mapped_ids["source_id"]
                    self.logger.debug("\n")
                    self.logger.debug ({"Px_id":projectx_id,"%s_id"%self.source:source_id,"thread_name":thread_name})
                    
                    projectx_details=self.getting_projectx_ott_details(projectx_id,show_type)
                    if projectx_details!='Null':
                        if data.get("purchase_info")!="":
                            self.link_expired=lib_common_modules().link_expiry_check_(self.expired_api,self.projectx_domain,source_id,self.service,self.expired_token)
                            ott_validation_result=ott_meta_data_validation_modules().ott_validation(projectx_details,source_id)
                            self.writer.writerow([source_id,projectx_id,show_type,'','',ott_validation_result,
                                 only_mapped_ids["source_flag"],'',self.link_expired])
                    else:
                        self.writer.writerow([source_id,projectx_id,show_type,'','','',
                            only_mapped_ids["source_flag"],'purchase_info_null'])                                                                
            else:            
                self.writer.writerow([disneyplus_id,'',show_type,'','',only_mapped_ids,'Px_id_null'])          
                
        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError) as e:
            self.retry_count+=1
            self.logger.debug("Retrying...................................",self.retry_count)
            self.logger.debug("\n")
            self.logger.debug ("exception/error caught in ott_checking func.........................",type(e),disneyplus_id,show_type,thread_name)
            if self.retry_count<=5:
                self.ott_checking(only_mapped_ids,disneyplus_id,show_type,thread_name,data)  
            else:
                self.retry_count=0

    #TODO:main func
    def main(self,start_id,thread_name,end_id):
        #import pdb;pdb.set_trace()
        self.get_env_url()
        self.mongo_mysql_connection()
        result_sheet='/result/disneyplus_ott_%s_checking%s.csv'%(thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile:
            fieldnames = ["%s_id"%self.source,"Projectx_id","show_type","px_video_link_present","%s_link_present"%self.source,"ott_link_result","mapping","","Expired"]
            self.writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
            self.writer.writerow(fieldnames)
            for _id in range(start_id,end_id,100):
                self.logger.debug({"skip":_id})
                query_disneyplus=self.sourcetable.aggregate([{"$match":{"$and":[{"item_type":{"$in":["movie","episode"]}},{"service":"disneyplus"}]}}
                        ,{"$project":{"id":1,"_id":0,"item_type":1,"series_id":1,"title":1,"episode_title":1,"release_year":1,
                        "episode_number":1,"season_number":1,"duration":1,"image_url":1,"url":1,"description":1,"cast":1,"directors":1,"writers":1,
                        "categories":1,"genres":1,"maturity_ratings":1,"purchase_info":1,"service":1}},{"$skip":_id},{"$limit":100}])
                #query_disneyplus=self.sourcetable.find({"service":"netflix","item_type":"movie","id":"70301275"})  
                for data in query_disneyplus:
                    if data.get("id")!="":
                        #import pdb;pdb.set_trace()
                        disneyplus_id=data.get("id").encode("ascii","ignore")
                        if data.get("item_type")=="movie":
                            show_type="MO"#data.get("item_type")
                        elif data.get("item_type")=="tvshow":
                            show_type="SM"
                        else:
                            show_type="SE"
                        self.count_disneyplus_id+=1
                        self.logger.debug("\n")
                        self.logger.debug datetime.datetime.now()
                        self.logger.debug("\n")
                        self.logger.debug ({"count_disneyplus_id":self.count_disneyplus_id,
                                        "thread_name":thread_name})
                        #import pdb;pdb.set_trace()
                        only_mapped_ids=ott_meta_data_validation_modules().getting_mapped_px_id_mapping_api(str(disneyplus_id),self.source_mapping_api,
                        	                                                     self.projectx_mapping_api,show_type,self.source,self.token)
                        self.ott_checking(only_mapped_ids,disneyplus_id,show_type,thread_name,data)              
            self.logger.debug("\n")                    
            self.logger.debug ({"count_disneyplus_id":self.count_disneyplus_id,"name":thread_name})  
        output_file.close()                      
        self.connection.close()


    #TODO: create threading
    def threading_pool(self):    

        t1=Process(target=self.main,args=(0,"thread-1",1000))
        t1.start()
        t2=Process(target=self.main,args=(1000,"thread-2",2000))
        t2.start()
        t3=Process(target=self.main,args=(2000,"thread-3",3000))
        t3.start()
        t4=Process(target=self.main,args=(3000,"thread-4",4000))
        t4.start()
        t5=Process(target=self.main,args=(4000,"thread-5",5000))
        t5.start()
        t6=Process(target=self.main,args=(5000,"thread-6",6000))
        t6.start()
        t7=Process(target=self.main,args=(6000,"thread-7",9140))
        t7.start()

    #Starting    
disneyplus_ott_validation().threading_pool()



#total 165204