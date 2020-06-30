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


#main class
class meta_data_validation_gracenote(object):

    logger=lib_common_modules().create_log(os.getcwd()+'/log.txt')

    def __init__(self):
        self.source="Gracenote"
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.expired_token='Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
        self.total=0
        self.count_gracenote_id=0
        self.writer=''
        self.link_expired=''
        self.sm_list=[]   

    def get_env_url(self):
        self.prod_domain="api.caavo.com"
        self.expired_api='https://%s/expired_ott/is_available?source_program_id=%s&service_short_name=%s'
        self.source_mapping_api="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%s&sourceName=%s&showType=%s"
        self.projectx_programs_api='https://preprod.caavo.com/programs?ids=%s&ott=true&aliases=true'
        self.projectx_mapping_api='http://34.231.212.186:81/projectx/%d/mapping/'
        self.projectx_duplicate_api='http://34.231.212.186:81/projectx/duplicate?sourceId=%d&sourceName=%s&showType=%s'
        self.gracenote_api= 'http://34.231.212.186:81/program/GracenoteShow/%s/%s'

    #TODO: Validation
    def meta_data_validation_(self,projectx_id,source_id,show_type,thread_name,only_mapped_ids,data):
        #import pdb;pdb.set_trace()
        source_details=source_meta_data().getting_source_details(source_id,show_type,self.source,thread_name,data)
        projectx_details=ott_meta_data_validation_modules().getting_projectx_details(projectx_id,show_type,self.source,thread_name,self.projectx_programs_api,self.token)
        meta_data_validation_result=ott_meta_data_validation_modules().meta_data_validate_gracenote().meta_data_validation(source_id,source_details,projectx_details,show_type)
        credits_validation_result=ott_meta_data_validation_modules().credits_validation(source_details,projectx_details)
        images_validation_result=ott_meta_data_validation_modules().images_validation(source_details,projectx_details)
        try:
            if projectx_details!='Null':
                self.writer.writerow([source_id,projectx_id,show_type,source_details["source_title"],projectx_details["px_long_title"],projectx_details["px_episode_title"],meta_data_validation_result["title_match"],meta_data_validation_result["description_match"],meta_data_validation_result["genres_match"]
                ,meta_data_validation_result["aliases_match"],meta_data_validation_result["release_year_match"],meta_data_validation_result["duration_match"],meta_data_validation_result["season_number_match"],meta_data_validation_result["episode_number_match"],meta_data_validation_result["px_video_link_present"],meta_data_validation_result["source_link_present"]
                ,images_validation_result[0],images_validation_result[1],credits_validation_result[0],credits_validation_result[1],only_mapped_ids["source_flag"]])
            else:
                self.writer.writerow([source_id,projectx_id,show_type,'','','','','',''
                ,'','','','','','','','','','','','','Px_response_null'])    
        except Exception as e:
            self.logger.debug ({"get exception in meta_data_validation func........":type(e)})
            pass 

    def to_check_only_mapping_to_source(self,gracenote_id,show_type,thread_name):
        try:
            only_mapped_ids=ott_meta_data_validation_modules().getting_mapped_px_id_mapping_api(str(gracenote_id),self.source_mapping_api
                                               ,self.projectx_mapping_api,show_type,self.source,self.token)
            self.logger.debug({"%s_id"%self.source:gracenote_id,"count_gracenote_id":
                str(self.count_gracenote_id),"name":str(thread_name),"only_mapped_ids":only_mapped_ids["source_flag"]})
            if only_mapped_ids["source_flag"]=='True':
                source_details=lib_common_modules().fetch_response_for_api_(self.gracenote_api%(show_type,gracenote_id),self.token)
                self.total+=1
                #import pdb;pdb.set_trace()
                projectx_id=only_mapped_ids["px_id"]
                source_id=only_mapped_ids["source_id"]
                self.logger.debug("\n")
                self.logger.debug ({"total":self.total,"MO_id":gracenote_id,"Px_id":projectx_id,
                    "%s_id"%self.source:source_id,"thread_name":thread_name})
                #self.meta_data_validation_(projectx_id,source_id,show_type,thread_name,only_mapped_ids,source_details)
                return ({"projectx_id":projectx_id,"only_mapped_ids":only_mapped_ids,"source_details":source_details})
        except (Exception,URLError,HTTPError,httplib.BadStatusLine) as error:
            self.logger.debug({"exception caught in to_check_only_mapping_to_source func.. ": str(type(error),gracenote_id,show_type)})
            self.to_check_only_mapping_to_source(gracenote_id,show_type,thread_name)        
                   

    #TODO: Getting_projectx_ids which is only mapped to gracenote
    def main(self,start_id,thread_name,end_id):
        try:
            #import pdb;pdb.set_trace()
            projectx_id=0   
            source_id=0
            self.get_env_url()
            input_sheet='/input/gracenote_only_mapped_ids'
            data=lib_common_modules().read_csv(input_sheet)
            
            result_sheet='/output/gracenote_meta_data_checking%s_%s.csv'%(thread_name,datetime.date.today())
            output_file=lib_common_modules().create_csv(result_sheet)
            with output_file as mycsvfile:
                fieldnames = ["%s_id"%self.source,"Projectx_id","show_type","%s_title"%self.source,"Px_title"
                            ,"Px_episode_title","title_match","description_match","genres_match","aliases_match",
                            "release_year_match","duration_match","season_number_match","episode_number_match",
                            "px_video_link_present","%s_link_present"%self.source,"image_url_missing","Wrong_url",
                            "credit_match","credit_mismatch"]
                self.writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
                self.writer.writerow(fieldnames)
                #import pdb;pdb.set_trace()
                for input_data in range(start_id,end_id):
                    gracenote_episode_id=[]
                    gracenote_id= (data[input_data][0])
                    show_type= (data[input_data][1])
                    self.count_gracenote_id+=1
                    if show_type=="MO":
                        self.logger.debug("\n")
                        self.logger.debug(datetime.datetime.now())
                        self.logger.debug ("\n")
                        result_movies=self.to_check_only_mapping_to_source(gracenote_id,show_type,thread_name)
                        if result_movies:
                            self.meta_data_validation_(result_movies["projectx_id"],gracenote_id,show_type,thread_name,
                                         result_movies["only_mapped_ids"],result_movies["source_details"])
                    else:
                        self.logger.debug("\n")
                        self.logger.debug( datetime.datetime.now())
                        self.logger.debug ("\n")
                        result_series=self.to_check_only_mapping_to_source(gracenote_id,show_type,thread_name)
                        if result_series:
                            self.meta_data_validation_(result_series["projectx_id"],gracenote_id,show_type,thread_name
                                    ,result_series["only_mapped_ids"],result_series["source_details"]["showMeta"])
                            episodes_details=result_series["source_details"]["episodes"]
                            for episodes in episode_details:
                                gracenote_id=episode["program"]["id"].encode("utf-8")
                                show_type=episode["showType"]["id"].encode("utf-8")
                                self.logger.debug("\n")
                                self.logger.debug( datetime.datetime.now())
                                self.logger.debug ("\n")
                                only_mapped_ids=ott_meta_data_validation_modules().getting_mapped_px_id_mapping_api(str(gracenote_id),self.source_mapping_api
                                                                   ,self.projectx_mapping_api,show_type,self.source,self.token)
                                self.logger.debug({"%s_SE_id"%self.source:gracenote_id,"count_gracenote_id":
                                    str(self.count_gracenote_id),"name":str(thread_name),"only_mapped_ids":only_mapped_ids["source_flag"]})
                                if only_mapped_ids["source_flag"]=='True':
                                    self.total+=1
                                    #import pdb;pdb.set_trace()
                                    projectx_id=only_mapped_ids["px_id"]
                                    source_id=only_mapped_ids["source_id"]
                                    self.logger.debug("\n")
                                    self.logger.debug ({"total":self.total,"SE_id":gracenote_id,"Px_id":projectx_id,
                                        "%s_id"%self.source:source_id,"thread_name":thread_name})
                                    self.meta_data_validation_(projectx_id,source_id,show_type,thread_name,only_mapped_ids,episodes)
            output_file.close()
        except Exception as error:
            self.logger.debug ({"exception caught in main func": str(type(error)
                                                     ,gracenote_id,show_type,thread_name)})
            pass    


    #TODO: to set up threading part
    def threading_pool(self):    

        t1=Process(target=self.main,args=(1,"thread-1",1000))
        t1.start()
        t2=Process(target=self.main,args=(1000,"thread-2",2000))
        t2.start()
        t3=Process(target=self.main,args=(2000,"thread-3",3000))
        t3.start()
        t4=Process(target=self.main,args=(3000,"thread-4",4000))
        t4.start()
        t5=Process(target=self.main,args=(4000,"thread-5",5000))
        t5.start()
        t6=Process(target=self.main,args=(5000,"thread-6",5428))
        t6.start() 


class source_meta_data(meta_data_validation_gracenote):

    def __init__(self):
        self.gracenote_credit_present='Null'
        self.gracenote_genres=[]
        self.gracenote_credits=[]
        self.gracenote_duration=''
        self.gracenote_title=''
        self.gracenote_show_id=0
        self.gracenote_episode_number=''
        self.gracenote_season_number=''
        self.gracenote_description=''
        self.gracenote_alternate_titles=[]
        self.gracenote_release_year=0
        self.gracenote_link_present="Null"

    retry_count=0

    def getting_genres(self,data,show_type):
        #import pdb;pdb.set_trace()
        genres_array=[]
        genres_details=data["genres"]
        for genres in genres_details:
            genres_array.append(genres["genre"].lower())

        return genres_array                        

    def getting_images(self,source_id,show_type,details):
        #import pdb;pdb.set_trace()
        try:
            images_details=[]
            images_details_=details["images"]
            for images in images_details_:
                if images["url"] is not None:    
                    images_details.append({'url':unidecode.unidecode(images["url"])})
            return images_details
        except (Exception,RuntimeError):
            self.logger.debug ({"exception caught getting_source_images........":str(source_id,show_type)})    
            self.getting_images(source_id,show_type,details)

    def getting_credits(self,details):
        #import pdb;pdb.set_trace()
        cast_details=[]
        for data in details:
            cast_details.append(unidecode.unidecode(pinyin.get(data["full_credit_name"])))
        return filter(None,cast_details)

    def getting_source_details(self,gracenote_id,show_type,source,thread_name,data):
        #import pdb;pdb.set_trace()
        try:
            if data["castNames"] or data["directorNames"]: 
                self.gracenote_credits=self.getting_credits(data["directorNames"]+data["castNames"])
                if self.gracenote_credits:
                    self.gracenote_credit_present='True'
            #release_year part from MO
            self.gracenote_release_year=data["program"]["releaseYear"]
            if self.gracenote_release_year is None or self.gracenote_release_year=='':
                self.gracenote_release_year='0'        
            #title part    
            try:
                self.gracenote_title=unidecode.unidecode(pinyin.get(data["program"]["originalTitle"]))
            except Exception:
                pass  
            #description part    
            try:
                self.gracenote_description=pinyin.get(data["program"]["description"].encode("ascii","ignore").lower())         
            except Exception:
                pass         
            #duration part        
            self.gracenote_duration=data["program"]["runTime"]
            if self.gracenote_duration is None or self.gracenote_duration==0:
                self.gracenote_duration='0' 
            #genres part 
            #import pdb;pdb.set_trace()   
            self.gracenote_genres=self.getting_genres(data,show_type)                    
            if show_type=='SE':
                #title part    
                try:
                    self.gracenote_title=unidecode.unidecode(pinyin.get(data["program"]["originalEpisodeTitle"]))
                except Exception:
                    pass  
                self.gracenote_show_id=data["program"]["seriesId"]
                self.gracenote_episode_number=data["episodeSequence"]["episodeNumber"]
                if self.gracenote_episode_number is None:
                    self.gracenote_episode_number=0
                self.gracenote_season_number=data["episodeSequence"]["seasonNumber"]
                if self.gracenote_season_number is None:
                    self.gracenote_season_number=0
            #images_part 
            gracenote_images_details=self.getting_images(gracenote_id,show_type,data)
            gracenote_link=data["otts"]
            if gracenote_link:
                self.gracenote_link_present="True"
            return {"source_credits":self.gracenote_credits,"source_credit_present":self.gracenote_credit_present,"source_title":self.gracenote_title,"source_description":self.gracenote_description,
            "source_genres":filter(None,self.gracenote_genres),"source_alternate_titles":self.gracenote_alternate_titles,"source_release_year":self.gracenote_release_year,"source_duration":self.gracenote_duration,
            "source_season_number":self.gracenote_season_number,"source_episode_number":self.gracenote_episode_number,"source_link_present":self.gracenote_link_present,"source_images_details":gracenote_images_details}    
        except (httplib.BadStatusLine,Exception,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError):
            self.retry_count+=1
            self.logger.debug ({"exception caught getting_source_details func..............":str(gracenote_id,show_type,source,thread_name)})
            self.logger.debug ("\n") 
            self.logger.debug ({"Retrying.............":self.retry_count})
            self.logger.debug ("\n")    
            if self.retry_count<=5:
                self.getting_source_details(gracenote_id,show_type,source,thread_name,data)    
            else:
                self.retry_count=0


#starting     
meta_data_validation_gracenote().threading_pool()



