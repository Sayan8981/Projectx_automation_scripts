"""Writer: Saayan"""

from multiprocessing import Process
import threading
import csv
import pymysql
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

    def mod_credits(self,credits):
        credits_name=[]
        for credit in credits:
            credits_name.append(unidecode.unidecode(pinyin.get(credit["fullName"])))
        return credits_name

    def getting_credits(self,details):
        cast_details=[]
        if details.get("actors")!="" and details.get("actors") is not None and details.get("actors")!=[]:
            cast=self.mod_credits(json.loads(details["actors"]))
            for credits in cast:
                if credits!='':
                    cast_details.append(credits)
        if details.get("directors")!="" and details.get('directors') is not None:
            cast=self.mod_credits(json.loads(details["directors"]))
            for credits in cast:
                if credits!='':
                    cast_details.append(credits)
        return filter(None,cast_details)

    def getting_source_details(self,starz_id,show_type,source,thread_name,details,cursor):
        #import pdb;pdb.set_trace()
        starz_credit_present='Null'
        starz_genres=[]
        starz_duration=''
        starz_title=''
        starz_episode_title=''
        starz_description=''
        starz_alternate_titles=[]
        starz_release_year=0
        starz_episode_number=0
        starz_season_number=0
        starz_link_present='Null'
        try:
            starz_credits=self.getting_credits(details)
            if starz_credits:
                starz_credit_present='True'
            try:        
                starz_title=unidecode.unidecode(pinyin.get(details.get("title")))
            except Exception:
                pass
            try:        
                starz_description=unidecode.unidecode(pinyin.get(details.get("description")).lower())
            except Exception:
                pass

            starz_show_id=details.get("series_id")

            starz_link=unidecode.unidecode(pinyin.get(details.get("url")))
            if starz_link!="" or starz_link is not None :
                starz_link_present='True' 
            if details.get("release_year")!="" and details.get("release_year") is not None:
                starz_release_year=details.get("release_year")
            starz_duration=str(details.get("run_time"))
            if starz_duration =='None' or starz_duration=='':
                starz_duration='0'
            if show_type=="SE":
                starz_episode_number=details.get("episode_number")
                if starz_episode_number is None:
                    starz_episode_number=0
                starz_season_number=details.get("season_number")
                if starz_season_number is None:
                    starz_season_number=0
            starz_images_details=[]#self.getting_images(starz_id,show_type,starz_show_id,details,cursor)
            print("\n")
            print {"source_credits":starz_credits,"source_credit_present":starz_credit_present,"source_title":starz_title,"source_description":starz_description,"source_genres":filter(None,starz_genres),"source_alternate_titles":starz_alternate_titles,"source_release_year":starz_release_year,"source_duration":starz_duration,"source_season_number":starz_season_number,"source_episode_number":starz_episode_number,"source_link_present":starz_link_present,"source_images_details":starz_images_details,"source_program_id":starz_id}
            return {"source_credits":starz_credits,"source_credit_present":starz_credit_present,"source_title":starz_title,"source_description":starz_description,"source_genres":filter(None,starz_genres),"source_alternate_titles":starz_alternate_titles,"source_release_year":starz_release_year,"source_duration":starz_duration,"source_season_number":starz_season_number,"source_episode_number":starz_episode_number,"source_link_present":starz_link_present,"source_images_details":starz_images_details}    
        except (httplib.BadStatusLine,Exception,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError):
            self.retry_count+=1
            print ("exception caught getting_source_details func..............",starz_id,show_type,source,thread_name)
            print ("\n") 
            print ("Retrying.............",self.retry_count)
            print ("\n")    
            if self.retry_count<=5:
                self.getting_source_details(starz_id,show_type,source,thread_name,details,cursor)    
            else:
                self.retry_count=0

#main class
class mata_data_validation_starz:

    def __init__(self):
        self.source="Starz"
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.expired_token='Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
        self.total=0
        self.count_starz_id=0
        self.writer=''
        self.link_expired=''
        self.running_datetime=datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        self.fieldnames = ["%s_id"%self.source,"Projectx_id","show_type","is_group_language_primary",
            "record_language","%s_title"%self.source,"Px_title"
            ,"Px_episode_title","title_match","description_match","genres_match","aliases_match",
            "release_year_match","duration_match","season_number_match","episode_number_match",
            "px_video_link_present","%s_link_present"%self.source,"image_url_missing","Wrong_url",
            "credit_match","credit_mismatch"]

    def mongo_mysql_connection(self):
        self.connection= pymysql.connect(host="127.0.0.1", user="root", password="root@123",
                                                  db="branch_service", charset='utf8', port=3306)
        self.cur= self.connection.cursor(pymysql.cursors.DictCursor)

    def get_env_url(self):
        self.prod_domain="api.caavo.com"
        self.expired_api='https://%s/expired_ott/is_available?source_program_id=%s&service_short_name=%s'
        self.source_mapping_api="http://preprod-projectx-api-545109534.us-east-1.elb.amazonaws.com/projectx/mappingfromsource?sourceIds=%s&sourceName=%s&showType=%s"
        self.projectx_programs_api='https://preprod.caavo.com/programs?ids=%s&ott=true&aliases=true'
        self.projectx_mapping_api='http://preprod-projectx-api-545109534.us-east-1.elb.amazonaws.com/projectx/%d/mapping/'
        self.projectx_duplicate_api='http://preprod-projectx-api-545109534.us-east-1.elb.amazonaws.com/projectx/duplicate?sourceId=%d&sourceName=%s&showType=%s'

    #TODO: Validation
    def meta_data_validation_(self,data,projectx_id,thread_name,only_mapped_ids):
        #import pdb;pdb.set_trace()
        source_details=source_meta_data().getting_source_details(self.starz_id,self.show_type,self.source,thread_name,data,self.cur)
        projectx_details=ott_meta_data_validation_modules().getting_projectx_details(projectx_id,self.show_type,self.source,thread_name,self.projectx_programs_api,self.token)
        meta_data_validation_result=ott_meta_data_validation_modules().meta_data_validate_headrun().meta_data_validation(self.starz_id,source_details,projectx_details,self.show_type)
        credits_validation_result=ott_meta_data_validation_modules().credits_validation(source_details,projectx_details)
        images_validation_result=ott_meta_data_validation_modules().images_validation(source_details,projectx_details)
        try:
            if projectx_details!='Null':
                print ({"projectx_details":projectx_details})
                self.writer.writerow([self.starz_id,projectx_id,self.show_type,projectx_details["is_group_language_primary"],projectx_details["record_language"],source_details["source_title"],projectx_details["px_long_title"],projectx_details["px_episode_title"],meta_data_validation_result["title_match"],meta_data_validation_result["description_match"],meta_data_validation_result["genres_match"]
                ,meta_data_validation_result["aliases_match"],meta_data_validation_result["release_year_match"],meta_data_validation_result["duration_match"],meta_data_validation_result["season_number_match"],meta_data_validation_result["episode_number_match"],meta_data_validation_result["px_video_link_present"],meta_data_validation_result["source_link_present"]
                ,images_validation_result[0],images_validation_result[1],credits_validation_result[0],credits_validation_result[1],only_mapped_ids["source_flag"]])
            else:
                self.writer.writerow([self.starz_id,projectx_id,self.show_type,'','','','','',''
                ,'','','','','','','','','','','','','Px_response_null'])    
        except Exception as e:
            print ("get exception in meta_data_validation func........",type(e),self.starz_id,self.show_type)
            pass                

    #TODO: Getting_projectx_ids which is only mapped to starz
    def main(self,start_id,thread_name,end_id):
        self.get_env_url()
        self.mongo_mysql_connection()
        result_sheet='/result/starz_meta_data_checking%s_%s.csv'%(thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
            self.writer.writerow(self.fieldnames)
            projectx_id=0   
            #import pdb;pdb.set_trace()
            for _id in range(start_id,end_id,1000):
                query_starz=self.cur.execute("SELECT * FROM starz_programs limit %d,1000"%(_id))
                #query_starz=self.cur.execute("SELECT * FROM starz_programs where launch_id='urn:hbo:feature:GVU4OxQ8lp47DwvwIAb24' and (expired_at is null or expired_at > '%s') "%(self.running_datetime))
                query_starz_result=self.cur.fetchall()
                for data in query_starz_result:
                    if data.get("source_program_id")!="" and data.get("source_program_id") is not None:
                        #import pdb;pdb.set_trace()
                        self.starz_id=data.get("source_program_id")    
                        self.show_type=data.get("item_type")
                        if self.show_type is not None and self.show_type!='season':
                            self.show_type='MO' if self.show_type=='movie' else self.show_type
                            self.show_type='SE' if self.show_type=='episode' else self.show_type
                            self.show_type='SM' if self.show_type=='series' else self.show_type
                            self.count_starz_id+=1
                            print("\n")
                            print datetime.datetime.now()
                            print ("\n")
                            print("%s_id:"%self.source,"id:",self.starz_id,"count_starz_id:"
                                +str(self.count_starz_id),"show_type:"+self.show_type,"name:"+str(thread_name))
                            only_mapped_ids=ott_meta_data_validation_modules().getting_mapped_px_id_mapping_api(self.starz_id,self.source_mapping_api,self.projectx_mapping_api,self.show_type,self.source,self.token)
                            try:
                                if only_mapped_ids["source_flag"]=='True':
                                    self.total+=1
                                    #import pdb;pdb.set_trace()
                                    projectx_id=only_mapped_ids["px_id"]
                                    print("\n")
                                    print ({"total":self.total,"id":self.starz_id,"Px_id":projectx_id,
                                        "thread_name":thread_name,"source_map":only_mapped_ids['source_map']})
                                    self.meta_data_validation_(data,projectx_id,thread_name,only_mapped_ids)
                            except Exception as e:
                                print ("got exception in main....",self.starz_id
                                                       ,self.show_type,only_mapped_ids,type(e),thread_name)
                                pass                            

        output_file.close()
        self.connection.close()                    
        self.cur.close()

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
        t8=Process(target=self.main,args=(7000,"thread-8",9000))
        t8.start()


#starting     
if __name__=="__main__":
    mata_data_validation_starz().threading_pool()
