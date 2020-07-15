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
        self.fieldnames = ["%s_id"%self.source,"%s_link_id"%self.source,"%s_Series_id"%self.source,"Expiry_date","Projectx_id","show_type","px_video_link_present","Fetched_from","Appletv_id_format","%s_link_present"%self.source,"ott_link_result","mapping","","Expired"]

    def mongo_mysql_connection(self):
        self.connection=pymongo.MongoClient("mongodb://127.0.0.1:27017/")
        self.sourceDB=self.connection["qadb"] 
        self.sourcetable=self.sourceDB["headrun"]

    def get_env_url(self):
        self.projectx_domain="test.caavo.com"
        self.expired_api='https://%s/expired_ott/is_available?source_program_id=%s&service_short_name=%s'
        self.source_mapping_api="http://beta-projectx-api-1289873303.us-east-1.elb.amazonaws.com/projectx/mappingfromsource?sourceIds=%s&sourceName=%s&showType=%s"
        self.projectx_programs_api='https://test.caavo.com/programs?ids=%s&ott=true&aliases=true&service=%s'
        self.projectx_mapping_api='http://beta-projectx-api-1289873303.us-east-1.elb.amazonaws.com/projectx/%d/mapping/'

    #TODO: to get projectx_id details OTT link id from programs api
    def getting_projectx_ott_details(self,projectx_id,show_type):
        #import pdb;pdb.set_trace()
        px_video_link=[]
        px_video_link_present='False'
        px_response='Null'
        launch_id=[]
        fetched_from = []
        appletv_id_format="False"
        projectx_api=self.projectx_programs_api%(projectx_id,self.service)
        data_px_resp=lib_common_modules().fetch_response_for_api_(projectx_api,self.token)   
        if data_px_resp!=[]:
            for data in data_px_resp:
                px_video_link= data.get("videos")
                if px_video_link:
                    px_video_link_present='True'
                    for linkid in px_video_link:
                        launch_id.append(linkid.get("launch_id"))
                        if linkid.get("fetched_from") not in fetched_from:
                            fetched_from.append(linkid.get("fetched_from").encode("utf-8"))
                    if linkid.get("appletv_id") == '{}{}'.format("%s://www.%s.com/en-gb/video/"%(self.source.lower(),self.source.lower()),linkid.get("launch_id")):
                        appletv_id_format= 'True'    
            return {"px_video_link_present":px_video_link_present,"launch_id":launch_id,"fetched_from": fetched_from,"appletv_id_format": appletv_id_format}
        else:
            return px_response      

    #TODO: To check OTT links
    def ott_checking(self,only_mapped_ids,thread_name,data):
        #import pdb;pdb.set_trace()
        try:     
            if only_mapped_ids["source_flag"]=='True' or only_mapped_ids["source_flag"]=='True(Rovi+others)':
                self.total+=1
                projectx_id=only_mapped_ids["px_id"]
                print("\n")
                print ({"total_only_disneyplus_mapped":self.total,"%s_id"%self.source:self.disneyplus_id,
                    "Px_id":projectx_id,"thread_name":thread_name})
                projectx_details=self.getting_projectx_ott_details(projectx_id,self.show_type)
                if projectx_details!='Null':
                    if data.get("purchase_info")!="":
                        self.link_expired=lib_common_modules().link_expiry_check_(self.expired_api,self.projectx_domain,self.disneyplus_link_id,self.service,self.expired_token)
                        ott_validation_result=ott_meta_data_validation_modules().ott_validation(projectx_details,self.disneyplus_link_id)
                        self.writer.writerow([self.disneyplus_id,self.disneyplus_link_id,data["series_id"],data["expiry_date"],projectx_id,self.show_type,projectx_details["px_video_link_present"],projectx_details["fetched_from"],projectx_details["appletv_id_format"],'',ott_validation_result,only_mapped_ids["source_flag"],'',self.link_expired])
                    else:
                        self.writer.writerow([self.disneyplus_id,self.disneyplus_link_id,data["series_id"],data["expiry_date"],projectx_id,self.show_type,'','','','','',only_mapped_ids["source_flag"],'purchase_info_null'])     
                else:
                    self.writer.writerow([self.disneyplus_id,self.disneyplus_link_id,data["series_id"],data["expiry_date"],projectx_id,self.show_type,projectx_details,'','','','',only_mapped_ids["source_flag"],'Px_response_null'])
            else:
                self.total+=1            
                self.writer.writerow([self.disneyplus_id,self.disneyplus_link_id,data["series_id"],data["expiry_date"],'',self.show_type,'','','','',only_mapped_ids,'Px_id_null'])              
        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError) as e:
            self.retry_count+=1
            print("Retrying...............................",self.retry_count)
            print("\n")
            print ("exception/error caught in ott_checking func.....................",type(e),self.disneyplus_id,self.disneyplus_link_id,self.show_type,thread_name)
            if self.retry_count<=5:
                self.ott_checking(only_mapped_ids,thread_name,data)  
            else:
                self.retry_count=0

    #TODO:main func
    def main(self,start_id,thread_name,end_id):
        self.get_env_url()
        self.mongo_mysql_connection()
        result_sheet='/result/disneyplus_ott_%s_checking%s.csv'%(thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
            self.writer.writerow(self.fieldnames)
            for _id in range(start_id,end_id,100):
                print({"skip":_id})
                query_disneyplus=self.sourcetable.aggregate([{"$match":{"$and":[{"item_type":{"$in":["movie","episode"]}},{"service":"%s"%self.source.lower()}]}},{"$project":{"id":1,"_id":0,"item_type":1,"series_id":1,"title":1,"episode_title":1,"release_year":1,"episode_number":1,"season_number":1,"duration":1,"image_url":1,"url":1,"description":1,"cast":1,"directors":1,"writers":1,"categories":1,"genres":1,"maturity_ratings":1,"purchase_info":1,"service":1,"expiry_date":1}},{"$skip":_id},{"$limit":100}])
                #query_disneyplus=self.sourcetable.aggregate([{"$match":{"$and":[{"item_type":{"$in":["movie","episode"]}},{"service":"%s"%self.source.lower(),"id":{"$in":array}}]}}])
                for data in query_disneyplus:
                    if data.get("id")!="":
                        self.disneyplus_id=data.get("id").encode("ascii","ignore")
                        self.disneyplus_link_id = json.loads(data.get("purchase_info").encode("ascii","ignore"))[0]["template_platforms"]["template_values"]["sk"]
                        if data.get("item_type")=="movie":
                            self.show_type="MO"
                        else:
                            self.show_type="SE"
                        self.count_disneyplus_id+=1
                        print("\n")
                        print datetime.datetime.now()
                        print("\n")
                        print ({"count_disneyplus_id":self.count_disneyplus_id,
                               "id":self.disneyplus_id,"thread_name":thread_name,"show_type":self.show_type})
                        only_mapped_ids=ott_meta_data_validation_modules().getting_mapped_px_id_mapping_api(str(self.disneyplus_id),self.source_mapping_api,self.projectx_mapping_api,self.show_type,self.source,self.token)
                        self.ott_checking(only_mapped_ids,thread_name,data)              
            print("\n")                    
            print ({"count_disneyplus_id":self.count_disneyplus_id,"name":thread_name})  
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
        t7=Process(target=self.main,args=(6000,"thread-7",7000))
        t7.start()
        t8=Process(target=self.main,args=(7000,"thread-8",8000))
        t8.start()
        t9=Process(target=self.main,args=(8000,"thread-9",9000))
        t9.start()
        t10=Process(target=self.main,args=(9000,"thread-10",11000))
        t10.start()
        t11=Process(target=self.main,args=(11000,"thread-11",12000))
        t11.start()

#Starting    
if __name__=="__main__":
    disneyplus_ott_validation().threading_pool()

