"""Writer: Saayan"""
# to check the metadata validation for those programs which are only mapped to headrun_fox
from multiprocessing import Process
import threading
import csv
import pymongo
import datetime
import sys
import re
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


class source_meta_data:
    retry_count=0

    def getting_images(self,source_id,show_type,source_show_id,details,sourcetable):
        #import pdb;pdb.set_trace()
        try:
            images_details=[]
            if details.get("image_url")!="":
                images_details.append({'url':json.loads(details.get("image_url"))[0]})
            if show_type=='SE' and (images_details==[] or images_details):
                images_query=sourcetable.find({"item_type":"tvshow","id":source_show_id},{"image_url":1,"_id":0})
                for images in images_query: 
                    images_details.append({'url':json.loads(images.get("image_url"))[0]})
            return images_details
        except (Exception,RuntimeError):
            print ("exception caught getting_source_images........",source_id,show_type,source_show_id)    
            self.getting_images(source_id,show_type,source_show_id,details,sourcetable)

    def getting_credits(self,details):
        #import pdb;pdb.set_trace()
        cast_details=[]
        if details.get("cast")!="":
            cast_details.extend(json.loads(details.get("cast")))
        if details.get("directors")!="":
            cast_details.extend(json.loads(details.get("directors")))
        if details.get("writers")!="":
            cast_details.extend(json.loads(details.get("writers")))
        return filter(None,cast_details)

    def getting_source_details(self,headrun_nbc_id,show_type,source,thread_name,details,sourcetable):
        #import pdb;pdb.set_trace()
        headrun_nbc_credit_present='Null'
        headrun_nbc_genres=[]
        headrun_nbc_duration=''
        headrun_nbc_title=''
        headrun_nbc_episode_title=''
        headrun_nbc_description=''
        headrun_nbc_alternate_titles=[]
        headrun_nbc_release_year=0
        headrun_nbc_link_present='Null'
        try:
            headrun_nbc_credits=self.getting_credits(details)
            if headrun_nbc_credits:
                headrun_nbc_credit_present='True'
            try:        
                headrun_nbc_title=unidecode.unidecode(pinyin.get(details.get("title")))
            except Exception:
                pass
            try:        
                headrun_nbc_description=unidecode.unidecode(pinyin.get(details.get("description")).lower())
            except Exception:
                pass

            headrun_nbc_show_id=details.get("series_id")

            if details.get("genres")!="":
                headrun_nbc_genres.extend(json.loads(details.get("genres").lower()))

            headrun_nbc_link=unidecode.unidecode(pinyin.get(details.get("url")))
            if headrun_nbc_link!="" or headrun_nbc_link is not None :
                headrun_nbc_link_present='True' 
            if details.get("release_year")!="":
                headrun_nbc_release_year=details.get("release_year").encode("ascii","ignore")
            #import pdb;pdb.set_trace()

            headrun_nbc_duration=details.get("duration")
            if headrun_nbc_duration is None or headrun_nbc_duration=="0":
                headrun_nbc_duration='0'

            #import pdb;pdb.set_trace()
            if show_type=="SE":
                headrun_nbc_title=unidecode.unidecode(pinyin.get(details.get("episode_title")))
            headrun_nbc_episode_number=details.get("episode_number").encode("ascii","ignore")
            if headrun_nbc_episode_number=="":
                headrun_nbc_episode_number=0
            headrun_nbc_season_number=details.get("season_number").encode("ascii","ignore")
            if headrun_nbc_season_number =="":
                headrun_nbc_season_number=0

            headrun_nbc_images_details=self.getting_images(headrun_nbc_id,show_type,headrun_nbc_show_id,details,sourcetable)

            return {"source_credits":headrun_nbc_credits,"source_credit_present":headrun_nbc_credit_present,"source_title":headrun_nbc_title,"source_description":headrun_nbc_description,
            "source_genres":filter(None,headrun_nbc_genres),"source_alternate_titles":headrun_nbc_alternate_titles,"source_release_year":headrun_nbc_release_year,"source_duration":headrun_nbc_duration,
            "source_season_number":headrun_nbc_season_number,"source_episode_number":headrun_nbc_episode_number,"source_link_present":headrun_nbc_link_present,"source_images_details":headrun_nbc_images_details}    
        except (httplib.BadStatusLine,Exception,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError):
            self.retry_count+=1
            print ("exception caught getting_source_details func..............",headrun_nbc_id,show_type,source,thread_name)
            print ("\n") 
            print ("Retrying.............",self.retry_count)
            print ("\n")    
            if self.retry_count<=5:
                self.getting_source_details(headrun_nbc_id,show_type,source,thread_name,details,sourcetable)    
            else:
                self.retry_count=0

#main class
class mata_data_validation_headrun_nbc:

    def __init__(self):
        self.source="Nbc"
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.expired_token='Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
        self.total=0
        self.count_headrun_nbc_id=0
        self.writer=''
        self.link_expired=''
        self.fieldnames = ["%s_id"%self.source,"Projectx_id","show_type","%s_title"%self.source,"Px_title"
                    ,"Px_episode_title","title_match","description_match","genres_match","aliases_match",
                    "release_year_match","duration_match","season_number_match","episode_number_match",
                    "px_video_link_present","%s_link_present"%self.source,"image_url_missing","Wrong_url",
                    "credit_match","credit_mismatch"]

    def mongo_mysql_connection(self):
        self.connection=pymongo.MongoClient("mongodb://192.168.86.10:27017/")
        self.sourceDB=self.connection["qadb"] 
        self.sourcetable=self.sourceDB["headrun"]

    def executed_query(self,skip_id):
        query=self.sourcetable.aggregate([{"$match":{"$and":[{"item_type":{"$in":[ "movie","episode","tvshow" ]}}
            ,{"service":"%s"%self.source.lower()}]}},{"$project":{"id":1,"_id":0,"item_type":1,"series_id":1,"title":
            1,"episode_title":1,"release_year":1,"episode_number":1,"season_number":1,"duration":1,"image_url":1,
            "url":1,"description":1,"cast":1,"directors":1,"writers":1,"categories":1,"genres":1,"maturity_ratings":1
            ,"purchase_info":1,"service":1}},{"$skip":skip_id},{"$limit":100}])
        return query        

    def get_env_url(self):
        self.prod_domain="api.caavo.com"
        self.expired_api='https://%s/expired_ott/is_available?source_program_id=%s&service_short_name=%s'
        self.source_mapping_api="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%s&sourceName=%s&showType=%s"
        self.projectx_programs_api='https://preprod.caavo.com/programs?ids=%s&ott=true&aliases=true'
        self.projectx_mapping_api='http://34.231.212.186:81/projectx/%d/mapping/'

    #TODO: Validation
    def meta_data_validation_(self,data,projectx_id,thread_name,only_mapped_ids):
        #import pdb;pdb.set_trace()
        source_details=source_meta_data().getting_source_details(self.headrun_nbc_id,self.show_type,self.source,thread_name,data,self.sourcetable)
        projectx_details=ott_meta_data_validation_modules().getting_projectx_details(projectx_id,self.show_type,self.source,thread_name,self.projectx_programs_api,self.token)
        meta_data_validation_result=ott_meta_data_validation_modules().meta_data_validate_headrun().meta_data_validation(self.headrun_nbc_id,source_details,projectx_details,self.show_type)
        credits_validation_result=ott_meta_data_validation_modules().credits_validation(source_details,projectx_details)
        images_validation_result=ott_meta_data_validation_modules().images_validation(source_details,projectx_details)
        try:
            if projectx_details!='Null':
                self.writer.writerow([self.headrun_nbc_id,projectx_id,self.show_type,source_details["source_title"],projectx_details["px_long_title"],projectx_details["px_episode_title"],meta_data_validation_result["title_match"],meta_data_validation_result["description_match"],meta_data_validation_result["genres_match"]
                ,meta_data_validation_result["aliases_match"],meta_data_validation_result["release_year_match"],meta_data_validation_result["duration_match"],meta_data_validation_result["season_number_match"],meta_data_validation_result["episode_number_match"],meta_data_validation_result["px_video_link_present"],meta_data_validation_result["source_link_present"]
                ,images_validation_result[0],images_validation_result[1],credits_validation_result[0],credits_validation_result[1],only_mapped_ids["source_flag"]])
            else:
                self.writer.writerow([self.headrun_nbc_id,projectx_id,self.show_type,'','','','','',''
                ,'','','','','','','','','','','','','Px_response_null'])    
        except Exception as e:
            print ("get exception in meta_data_validation func........",type(e))
            pass                

    #TODO: Getting_projectx_ids which is only mapped to headrun_nbc
    def main(self,start_id,thread_name,end_id):
        #import pdb;pdb.set_trace()
        self.get_env_url()
        self.mongo_mysql_connection()
        result_sheet='/result/headrun_nbc_meta_data_checking%s_%s.csv'%(thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
            self.writer.writerow(self.fieldnames)
            projectx_id=0   
            source_id=0
            #import pdb;pdb.set_trace()
            for _id in range(start_id,end_id,100):
                query_headrun_nbc=self.executed_query(_id)
                #query_headrun_nbc=self.sourcetable.find({"service":"nbc","item_type":"episode","id":"80192967"})
                for data in query_headrun_nbc:
                    if data.get("id")!="":
                        #import pdb;pdb.set_trace()
                        self.headrun_nbc_id=data.get("id")
                        if data.get("item_type")=="movie":
                            self.show_type="MO"
                        elif data.get("item_type")=="tvshow":
                            self.show_type="SM"
                        else:
                            self.show_type="SE"    
                        self.count_headrun_nbc_id+=1
                        print("\n")
                        print datetime.datetime.now()
                        print ("\n")
                        print("%s_id:"%self.source,"id:",self.headrun_nbc_id,"count_headrun_nbc_id:"
                            +str(self.count_headrun_nbc_id),"name:"+str(thread_name))
                        only_mapped_ids=ott_meta_data_validation_modules().getting_mapped_px_id_mapping_api(self.headrun_nbc_id,self.source_mapping_api
                                                           ,self.projectx_mapping_api,self.show_type,self.source,self.token)
                        if only_mapped_ids["source_flag"]=='True':
                            self.total+=1
                            #import pdb;pdb.set_trace()
                            projectx_id=only_mapped_ids["px_id"]
                            print("\n")
                            print ({"total":self.total,"id":self.headrun_nbc_id,"Px_id":projectx_id,
                                "%s_id"%self.source:source_id,"thread_name":thread_name,"source_map":only_mapped_ids['source_map']})
                            self.meta_data_validation_(data,projectx_id,thread_name,only_mapped_ids)                    

        output_file.close()
        self.connection.close()                    


    #TODO: to set up threading part
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
        t9=Process(target=self.main,args=(8000,"thread-9",9200))
        t9.start()

    #starting     
mata_data_validation_headrun_nbc().threading_pool()


# > db.headrun.count({"service":"nbc"})
# 9197