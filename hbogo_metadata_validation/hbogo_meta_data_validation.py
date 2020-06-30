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
        #import pdb;pdb.set_trace()
        return credits.encode('utf-8').replace('---\n- :','').replace('\n','').replace('  :',',').replace(',','').replace('- :',',').replace('- ',',').replace('-','').replace(': ',':').replace('  ',',')

    def getting_images(self,source_id,show_type,source_show_id,details,cursor):
        #import pdb;pdb.set_trace()
        try:
            images_details=[]
            if details.get("image_url")!="":
                images_details.append({'url':json.loads(details.get("image_url"))[0]})
            if show_type=='SE' and (images_details==[] or images_details):
                images_query=sourcetable.find({"item_type":"tvshow","id":source_show_id},{"image_url":1,"_id":0})
                for images in images_query: 
                    if images.get("image_url") != "":
                        images_details.append({'url':json.loads(images.get("image_url"))[0]})
            return images_details
        except (Exception,RuntimeError):
            print ("exception caught getting_source_images........",source_id,show_type,source_show_id)    
            self.getting_images(source_id,show_type,source_show_id,details,cursor)

    def getting_credits(self,details):
        #import pdb;pdb.set_trace()
        cast_details=[]
        credits_details=[]
        if details.get("cast")!="" and details.get("cast") is not None:
            cast=self.mod_credits(details["cast"])
            for credits in cast.strip().split(','):
                if credits!='':
                    cast_details.append({credits.split(':')[i].strip(): credits.split(':')[i + 1].strip() for i in range(0, len(credits.split(':')), 2)})
        if details.get("directors")!="" and details.get('directors') is not None:
            cast=self.mod_credits(details["directors"])
            for credits in cast.strip().split(','):
                if credits!='':
                    cast_details.append({credits.split(':')[i].strip(): credits.split(':')[i + 1].strip() for i in range(0, len(credits.split(':')), 2)})
        if details.get("writers")!="" and details.get('writers') is not None:
            cast=self.mod_credits(details["writers"])
            for credits in cast.strip().split(','):
                if credits!='':
                    cast_details.append({credits.split(':')[i].strip(): credits.split(':')[i + 1].strip() for i in range(0, len(credits.split(':')), 2)}) 
        for cast_detail in cast_details:
            try:
                credits_details.append(cast_detail["full_credit_name"])
            except Exception:
                pass
        return filter(None,credits_details)

    def get_mod_duration(self,duration):
        #import pdb;pdb.set_trace()
        hr_mins_convert=0
        mins=0
        if 'hr' in duration:
            hour_splited=duration.split('hr')
            for time in hour_splited:
                if 'min' not in time and time!='':
                    hr_mins_convert=eval(time.strip())*60
                elif time!='':
                    mins=eval(time.replace('min','').strip())
        else:
            mins=eval(duration.replace('min','').strip())

        return  str((hr_mins_convert+mins)*60)


    def getting_source_details(self,hbogo_id,show_type,source,thread_name,details,cursor):
        hbogo_credit_present='Null'
        hbogo_genres=[]
        hbogo_duration=''
        hbogo_title=''
        hbogo_episode_title=''
        hbogo_description=''
        hbogo_alternate_titles=[]
        hbogo_release_year=0
            hbogo_episode_number=0
            hbogo_season_number=0
        hbogo_link_present='Null'
        try:
            hbogo_credits=self.getting_credits(details)
            if hbogo_credits:
                hbogo_credit_present='True'
            try:        
                hbogo_title=unidecode.unidecode(pinyin.get(details.get("title")))
            except Exception:
                pass
            try:        
                hbogo_description=unidecode.unidecode(pinyin.get(details.get("description")).lower())
            except Exception:
                pass

            hbogo_show_id=details.get("series_launch_id")

            hbogo_link=unidecode.unidecode(pinyin.get(details.get("url")))
            if hbogo_link!="" or hbogo_link is not None :
                hbogo_link_present='True' 
            if details.get("release_year")!="" and details.get("release_year") is not None:
                hbogo_release_year=details.get("release_year")
            if hbogo_duration is not None and hbogo_duration!="0 min" and hbogo_duration!="0 hr":
                hbogo_duration=self.get_mod_duration(details.get("duration"))
            else:    
                hbogo_duration='0'

            if show_type=="SE":
                hbogo_episode_number=details.get("episode_number")
                if hbogo_episode_number is None:
                    hbogo_episode_number=0
                hbogo_season_number=details.get("season_number")
                if hbogo_season_number is None:
                    hbogo_season_number=0

            hbogo_images_details=[]#self.getting_images(hbogo_id,show_type,hbogo_show_id,details,cursor)
            print("\n")
            print {"source_credits":hbogo_credits,"source_credit_present":hbogo_credit_present,"source_title":hbogo_title,"source_description":hbogo_description,"source_genres":filter(None,hbogo_genres),"source_alternate_titles":hbogo_alternate_titles,"source_release_year":hbogo_release_year,"source_duration":hbogo_duration,"source_season_number":hbogo_season_number,"source_episode_number":hbogo_episode_number,"source_link_present":hbogo_link_present,"source_images_details":hbogo_images_details}
            return {"source_credits":hbogo_credits,"source_credit_present":hbogo_credit_present,"source_title":hbogo_title,"source_description":hbogo_description,"source_genres":filter(None,hbogo_genres),"source_alternate_titles":hbogo_alternate_titles,"source_release_year":hbogo_release_year,"source_duration":hbogo_duration,"source_season_number":hbogo_season_number,"source_episode_number":hbogo_episode_number,"source_link_present":hbogo_link_present,"source_images_details":hbogo_images_details}    
        except (httplib.BadStatusLine,Exception,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError):
            self.retry_count+=1
            print ("exception caught getting_source_details func..............",hbogo_id,show_type,source,thread_name)
            print ("\n") 
            print ("Retrying.............",self.retry_count)
            print ("\n")    
            if self.retry_count<=5:
                self.getting_source_details(hbogo_id,show_type,source,thread_name,details,cursor)    
            else:
                self.retry_count=0

#main class
class mata_data_validation_hbogo:

    def __init__(self):
        self.source="HBOGO"
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.expired_token='Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
        self.total=0
        self.count_hbogo_id=0
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
        self.connection= pymysql.connect(host="192.168.86.10", user="root", password="branch@123",
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
        source_details=source_meta_data().getting_source_details(self.hbogo_id,self.show_type,self.source,thread_name,data,self.cur)
        projectx_details=ott_meta_data_validation_modules().getting_projectx_details(projectx_id,self.show_type,self.source,thread_name,self.projectx_programs_api,self.token)
        meta_data_validation_result=ott_meta_data_validation_modules().meta_data_validate_headrun().meta_data_validation(self.hbogo_id,source_details,projectx_details,self.show_type)
        credits_validation_result=ott_meta_data_validation_modules().credits_validation(source_details,projectx_details)
        images_validation_result=ott_meta_data_validation_modules().images_validation(source_details,projectx_details)
        try:
            if projectx_details!='Null':
                print ({"projectx_details":projectx_details})
                self.writer.writerow([self.hbogo_id,projectx_id,self.show_type,projectx_details["is_group_language_primary"],projectx_details["record_language"],source_details["source_title"],projectx_details["px_long_title"],projectx_details["px_episode_title"],meta_data_validation_result["title_match"],meta_data_validation_result["description_match"],meta_data_validation_result["genres_match"]
                ,meta_data_validation_result["aliases_match"],meta_data_validation_result["release_year_match"],meta_data_validation_result["duration_match"],meta_data_validation_result["season_number_match"],meta_data_validation_result["episode_number_match"],meta_data_validation_result["px_video_link_present"],meta_data_validation_result["source_link_present"]
                ,images_validation_result[0],images_validation_result[1],credits_validation_result[0],credits_validation_result[1],only_mapped_ids["source_flag"]])
            else:
                self.writer.writerow([self.hbogo_id,projectx_id,self.show_type,'','','','','',''
                ,'','','','','','','','','','','','','Px_response_null'])    
        except Exception as e:
            print ("get exception in meta_data_validation func........",type(e),self.hbogo_id,self.show_type)
            pass                

    #TODO: Getting_projectx_ids which is only mapped to hbogo
    def main(self,start_id,thread_name,end_id):
        #import pdb;pdb.set_trace()
        self.get_env_url()
        self.mongo_mysql_connection()
        result_sheet='/result/hbogo_meta_data_checking%s_%s.csv'%(thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
            self.writer.writerow(self.fieldnames)
            projectx_id=0   
            #import pdb;pdb.set_trace()
            for _id in range(start_id,end_id,1000):
                query_hbogo=self.cur.execute("SELECT * FROM hbogo_programs where (expired_at is null or expired_at > '%s') and expired='0' limit %d,1000 "%(self.running_datetime,_id))
                #query_hbogo=self.cur.execute("SELECT * FROM hbogo_programs where launch_id='urn:hbo:feature:GVU4OxQ8lp47DwvwIAb24' and (expired_at is null or expired_at > '%s') "%(self.running_datetime))
                query_hbogo_result=self.cur.fetchall()
                for data in query_hbogo_result:
                    if data.get("launch_id")!="" and data.get("launch_id") is not None:
                        #import pdb;pdb.set_trace()
                        self.hbogo_id=data.get("launch_id")    
                        self.show_type=data.get("show_type")
                        if self.show_type is not None and self.show_type!='SN':
                            self.show_type='MO' if self.show_type=='OT' else self.show_type
                            self.count_hbogo_id+=1
                            print("\n")
                            print datetime.datetime.now()
                            print ("\n")
                            print("%s_id:"%self.source,"id:",self.hbogo_id,"count_hbogo_id:"
                                +str(self.count_hbogo_id),"show_type:"+self.show_type,"name:"+str(thread_name))
                            only_mapped_ids=ott_meta_data_validation_modules().getting_mapped_px_id_mapping_api(self.hbogo_id,self.source_mapping_api,self.projectx_mapping_api,self.show_type,self.source,self.token)
                            try:
                                if only_mapped_ids["source_flag"]=='True':
                                    self.total+=1
                                    #import pdb;pdb.set_trace()
                                    projectx_id=only_mapped_ids["px_id"]
                                    print("\n")
                                    print ({"total":self.total,"id":self.hbogo_id,"Px_id":projectx_id,
                                        "thread_name":thread_name,"source_map":only_mapped_ids['source_map']})
                                    self.meta_data_validation_(data,projectx_id,thread_name,only_mapped_ids)
                            except Exception as e:
                                print ("got exception in main....",self.hbogo_id
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
    mata_data_validation_hbogo().threading_pool()
