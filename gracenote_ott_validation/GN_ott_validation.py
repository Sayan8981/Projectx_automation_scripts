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


class gracenote_ott_validation:

    logger=lib_common_modules().create_log(os.getcwd()+'/log_%s.txt'%datetime.date.today())
    retry_count=0

    def __init__(self):
        self.source="Gracenote"
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.expired_token='Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
        self.fieldnames = ["%s_series_id"%self.source,"%s_id"%self.source,"Projectx_id","show_type","original_language",
                        "title","episode_title","release_year","season_number","episode_number","px_video_link_present",
                        "%s_link_present"%self.source,"Present_otts","Not_present_otts","mapping","Mapped_sources",
                        "Ott_link_result","Expired_link_list","Expired_status","Comment","Result"]
        self.total=0
        self.count_gracenote_id=0
        self.writer=''
        self.Fail=0
        self.link_expired=''

    def mongo_mysql_connection(self):
        self.connection=pymongo.MongoClient("mongodb://192.168.86.10:27017/")
        self.sourceDB=self.connection["qadb"] 
        self.sourcetable=self.sourceDB["GNS_validation_id_1"]

    def get_env_url(self):
        self.prod_domain="api.caavo.com"
        self.projectx_domain="preprod.caavo.com"
        self.expired_api='https://%s/expired_ott/is_available?source_program_id=%s&service_short_name=%s'
        self.source_mapping_api="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%s&sourceName=%s&showType=%s"
        self.projectx_programs_api='https://preprod.caavo.com/programs?ids=%s&ott=true&aliases=true'
        self.projectx_mapping_api='http://34.231.212.186:81/projectx/%d/mapping/'
        self.projectx_duplicate_api='http://34.231.212.186:81/projectx/duplicate?sourceId=%d&sourceName=%s&showType=%s'

    #TODO: default parameter defined
    def default_param(self):
        self.present_otts=[]
        self.not_present_otts=[]
        self.Comment=''
        self.result='Pass'

    def executed_query(self,skip_id,limit_id):
        self.query=self.sourcetable.aggregate([{"$match":{"$and":[{"show_type":{"$in":["SM","MO"]}}]}}
                    ,{"$project":{"_id":0,"original_language":1,"show_type":1,"title":1,"episode_title":1,"release_year":1
                    ,"Videos.iTunes Store":1,"Videos.VUDU":1,"Videos.Hulu":1,"Videos.Netflix":1,"Videos.Amazon":1,
                    "Videos.Showtime":1,"Videos.YouTube":1,"episode_number":1,"season_number":1,"sequence_id":1,
                    "series_id":1}},{"$skip":skip_id},{"$limit":limit_id}])
        return self.query

    def check_ott(self,projectx_details,link_id,service):
        #import pdb;pdb.set_trace()
        try:
            ott_validation_result=ott_meta_data_validation_modules().ott_validation(projectx_details,link_id)
            if ott_validation_result=="Present":
                self.present_otts.append({service:link_id})
            elif ott_validation_result=="Not_Present":
                self.not_present_otts.append({service:link_id})
            else:
                self.Comment="px_videos_null"
            self.logger.debug("\n")
            if self.not_present_otts:
                ott_validation_result="Not_Present"
            self.logger.debug({"ott_validation_result":ott_validation_result,"present_ott_list":self.present_otts,
                        "not_present_otts":self.not_present_otts,"comment":self.Comment})
            return {"ott_validation_result":ott_validation_result,"present_ott_list":self.present_otts,
                    "not_present_otts":self.not_present_otts,"comment":self.Comment}
        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError) as error:
            self.retry_count+=1
            self.logger.debug({"Retrying...................................":self.retry_count})
            self.logger.debug("\n")
            self.logger.debug ({"exception/error caught in check_ott func.............."
                                :'{},{},{}'.format(type(error),projectx_details,link_id)})
            if self.retry_count<=5:
                self.check_ott(projectx_details,link_id)
            else:
                self.retry_count=0

    def to_check_service_proper_name(self,service):
        #import pdb;pdb.set_trace()
        if 'amazon_prime' in service or 'amazon_buy' in service or service=="Amazon":
            service='amazon'
        elif 'Netflix' in service:
            service='netflixusa'
        elif service=="iTunes Store":
            service="itunes"
        return service

    #TODO: to get source_detail OOT link id only from DB table
    def getting_source_ott_details(self,gracenote_id,show_type,details):
        #import pdb;pdb.set_trace()
        gracenote_link=[]
        expired_link=[]
        gracenote_link_present='Null'
        gracenote_link.append(details["Videos"])
        if gracenote_link!=[]:
            gracenote_link_present='True'
        for links in gracenote_link:
            for key in links.keys():
                for link_id in links[key]:
                    service=self.to_check_service_proper_name(key)
                    self.link_expired=lib_common_modules().link_expiry_check(self.expired_api
                            ,self.projectx_domain,link_id,service.lower(),self.expired_token,self.logger)
                    if self.link_expired=='True':
                        expired_link.append({key:link_id})
        self.logger.debug("\n")
        self.logger.debug({"source_link_present":gracenote_link_present,"link_id":gracenote_link,"show_type":show_type
                              ,"expired_link":expired_link,"expired_status":self.link_expired})
        return {"source_link_present":gracenote_link_present,"link_id":gracenote_link,"show_type":show_type
               ,"expired_link":expired_link,"expired_status":self.link_expired}

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
                        launch_id.append(linkid.get("launch_id"))
                        if linkid.get("source_program_id") not in launch_id:
                            launch_id.append(linkid.get("source_program_id"))
            self.logger.debug("\n")
            self.logger.debug({"px_video_link_present":px_video_link_present,"launch_id":launch_id})
            return {"px_video_link_present":px_video_link_present,"launch_id":launch_id}
        else:
            return px_response   

    def data_checking(self,data):
        try:
            release_year=data["release_year"]
        except Exception:
            release_year=0
        try:
            episode_title=unidecode.unidecode(data["episode_title"])
        except Exception:
            episode_title='None'    
        try:
            original_language=data["original_language"]
        except Exception:
            original_language="None"
        try:
           season_number=data["season_number"]
        except Exception:
           season_number=0
        try:
            episode_number=data["episode_number"]
        except Exception:
            episode_number=0
        return {"release_year":release_year,"episode_title":episode_title,"original_language":original_language
               ,"season_number":season_number,"episode_number":episode_number}               

    #TODO: To check OTT links sub main func
    def ott_checking(self,only_mapped_ids,gracenote_id,show_type,thread_name,data):
        self.default_param()
        #import pdb;pdb.set_trace()
        data_check_result=self.data_checking(data)
        if data["show_type"]=="MO":
            data["series_id"]=0
            data["episode_title"]="None"
        else:
            data["original_language"]="None"
        try:     
            if only_mapped_ids["source_flag"]=='True':
                self.total+=1
                #import pdb;pdb.set_trace()
                projectx_id=only_mapped_ids["px_id"]
                source_id=only_mapped_ids["source_id"]
                self.logger.debug("\n")
                self.logger.debug ({"total_only_gracenote_mapped":self.total,"%s_id"%self.source:gracenote_id,
                "Px_id":projectx_id,"%s_id"%self.source:source_id,"thread_name":thread_name,"Fail_count":self.Fail})
                source_details=self.getting_source_ott_details(source_id,show_type,data)
                projectx_details=self.getting_projectx_ott_details(projectx_id,show_type)
                if projectx_details!='Null':
                    if data.get("Videos")!={}:
                        for link in source_details["link_id"]:
                            for key in link.keys():
                                for link_id in link[key]:
                                    result=self.check_ott(projectx_details,link_id,key)
                        for expired_link in source_details["expired_link"]:
                            if expired_link in result["not_present_otts"]:
                                result["not_present_otts"].remove(expired_link)
                        if result["not_present_otts"] or result["comment"]=="px_videos_null":
                            self.Fail+=1
                            self.result='Fail'
                        self.writer.writerow([data["series_id"],source_id,projectx_id,show_type,data_check_result["original_language"],unidecode.unidecode(data["title"])
                              ,data_check_result["episode_title"],data_check_result["release_year"],data_check_result["season_number"],data_check_result["episode_number"],projectx_details["px_video_link_present"],
                              source_details["source_link_present"],result["present_ott_list"],result["not_present_otts"],
                              only_mapped_ids["source_flag"],only_mapped_ids["source_map"],result["ott_validation_result"],source_details["expired_link"]
                              ,source_details["expired_status"],result["comment"],self.result])
                    else:
                        self.writer.writerow([data["series_id"],source_id,projectx_id,show_type,
                            data_check_result["original_language"],unidecode.unidecode(data["title"])
                            ,data_check_result["episode_title"],data_check_result["release_year"],
                            data_check_result["season_number"],data_check_result["episode_number"],'','','',''
                            ,only_mapped_ids["source_flag"],only_mapped_ids["source_map"],'','','source_videos_null',self.result])
                else:
                    self.Fail+=1
                    self.result='Fail'
                    self.writer.writerow([data["series_id"],source_id,projectx_id,show_type,data_check_result["original_language"],unidecode.unidecode(data["title"])
                            ,data_check_result["episode_title"],data_check_result["release_year"],data_check_result["season_number"],data_check_result["episode_number"]
                            ,projectx_details,source_details["source_link_present"],'','',only_mapped_ids["source_flag"],
                            only_mapped_ids["source_map"],'','','','Px_response_null',self.result])
            elif only_mapped_ids["source_flag"]=='True(Rovi+others)':
                projectx_id=only_mapped_ids["px_id"]
                source_id=only_mapped_ids["source_id"]
                self.logger.debug("\n")
                self.logger.debug ({"Px_id":projectx_id,"%s_id"%self.source:source_id,"thread_name":thread_name,"Fail_count":self.Fail})
                source_details=self.getting_source_ott_details(source_id,show_type,data)
                projectx_details=self.getting_projectx_ott_details(projectx_id,show_type)
                if projectx_details!='Null':
                    if data.get("Videos")!="":
                        for link in source_details["link_id"]:
                            for key in link.keys():
                                for link_id in link[key]:
                                    result=self.check_ott(projectx_details,link_id,key)
                        for expired_link in source_details["expired_link"]:
                            if expired_link in result["not_present_otts"]:
                                result["not_present_otts"].remove(expired_link)
                        if result["not_present_otts"] or result["comment"]=="px_videos_null":
                            self.Fail+=1
                            self.result='Fail'
                        self.writer.writerow([data["series_id"],source_id,projectx_id,show_type,data_check_result["original_language"],unidecode.unidecode(data["title"])
                             ,data_check_result["episode_title"],data_check_result["release_year"],data_check_result["season_number"],data_check_result["episode_number"]
                              ,projectx_details["px_video_link_present"],source_details["source_link_present"],result["present_ott_list"],result["not_present_otts"],
                              only_mapped_ids["source_flag"],only_mapped_ids["source_map"],result["ott_validation_result"],source_details["expired_link"]
                              ,source_details["expired_status"],result["comment"],self.result])
                    else:
                        self.writer.writerow([data["series_id"],source_id,projectx_id,show_type,data_check_result["original_language"],unidecode.unidecode(data["title"])
                            ,data_check_result["episode_title"],data_check_result["release_year"],data_check_result["season_number"],data_check_result["episode_number"]
                            ,'','','','',only_mapped_ids["source_flag"],only_mapped_ids["source_map"],'','','source_video_null',self.result])
                else:
                    self.Fail+=1
                    self.result='Fail'
                    self.writer.writerow([data["series_id"],source_id,projectx_id,show_type,data_check_result["original_language"],unidecode.unidecode(data["title"])
                        ,data_check_result["episode_title"],data_check_result["release_year"],data_check_result["season_number"],data_check_result["episode_number"]
                        ,projectx_details,source_details["source_link_present"],'','',only_mapped_ids["source_flag"],
                        only_mapped_ids["source_map"],'','','','Px_response_null',self.result])
            else:
                self.Fail+=1
                self.result='Fail'
                self.writer.writerow([data["series_id"],gracenote_id,'',show_type,data_check_result["original_language"],unidecode.unidecode(data["title"])
                ,data_check_result["episode_title"],data_check_result["release_year"],data_check_result["season_number"],data_check_result["episode_number"]
                ,'','','','','',only_mapped_ids,'','','','Px_id_null',self.result])
        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError) as e:
            self.retry_count+=1
            self.logger.debug({"Retrying............................":self.retry_count})
            self.logger.debug("\n")
            self.logger.debug ({"exception/error caught in ott_checking func............"
                                :'{},{},{}'.format(type(e),gracenote_id,show_type,thread_name)})
            if self.retry_count<=5:
                self.ott_checking(only_mapped_ids,gracenote_id,show_type,thread_name,data)  
            else:
                self.retry_count=0

    #TODO:main func
    def main(self,start_id,thread_name,end_id):
        #import pdb;pdb.set_trace()
        self.get_env_url()
        self.mongo_mysql_connection()
        result_sheet='/result/gracenote_ott_%s_checking%s.csv'%(thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
            self.writer.writerow(self.fieldnames)
            for _id in range(start_id,end_id,100):
                self.logger.debug({"skip":_id})
                query_gracenote=self.executed_query(_id,100)
                for data in query_gracenote:
                    if data.get("Videos")!={}:
                        #import pdb;pdb.set_trace()
                        gracenote_id=data.get("sequence_id").encode("ascii","ignore")
                        if data.get("show_type")=="SM":
                            show_type="SE"
                        else:
                            show_type="MO"
                        self.count_gracenote_id+=1
                        self.logger.debug("\n")
                        self.logger.debug (datetime.datetime.now())
                        self.logger.debug("\n")
                        self.logger.debug ({"count_gracenote_id":self.count_gracenote_id,
                                        "thread_name":thread_name,"details":data})
                        #import pdb;pdb.set_trace()
                        only_mapped_ids=ott_meta_data_validation_modules().getting_mapped_px_id_mapping_api(str(gracenote_id)
                                    ,self.source_mapping_api,self.projectx_mapping_api,show_type,self.source,self.token)
                        #TODO: Sub_main : here OTT validation result printed
                        self.ott_checking(only_mapped_ids,gracenote_id,show_type,thread_name,data)
            self.logger.debug("\n")                    
            self.logger.debug ({"count_gracenote_id":self.count_gracenote_id,"name":thread_name})  
        output_file.close()                      
        self.connection.close()


    #TODO: create threading process
    def threading_pool(self):    

        t1=Process(target=self.main,args=(0,"thread-1",40000))
        t1.start()
        t2=Process(target=self.main,args=(40000,"thread-2",80000))
        t2.start()
        t3=Process(target=self.main,args=(80000,"thread-3",120000))
        t3.start()
        t4=Process(target=self.main,args=(120000,"thread-4",160000))
        t4.start()
        t5=Process(target=self.main,args=(160000,"thread-5",200000))
        t5.start()
        t6=Process(target=self.main,args=(200000,"thread-6",240000))
        t6.start()
        t7=Process(target=self.main,args=(240000,"thread-7",280000))
        t7.start()
        t8=Process(target=self.main,args=(280000,"thread-8",320000))
        t8.start()
        t9=Process(target=self.main,args=(320000,"thread-9",360000))
        t9.start()
        t10=Process(target=self.main,args=(360000,"thread-10",400000))
        t10.start()
        t11=Process(target=self.main,args=(400000,"thread-11",440000))
        t11.start()
        t12=Process(target=self.main,args=(440000,"thread-12",480000))
        t12.start()
        t13=Process(target=self.main,args=(480000,"thread-13",523000))
        t13.start()

#Starting
if __name__=="__main__":
    gracenote_ott_validation().threading_pool()




#samples:
# {"sequence_id":{"$in":["EP003735550155","EP003735550154","EP003735550061","EP002258240001"
#     ,"EP002258240002","EP002258240003","EP002258240004","EP002258240005","EP008319770012"
#     ,"EP008319770015","EP017157100021","EP018259010008","EP018259010009","EP018259010003"]}}