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
        self.fieldnames = ["%s_series_id"%self.source,"%s_id"%self.source,"Projectx_id","show_type",
                             "Present_otts","Not_present_otts","Ott_link_result","Expired_link_list"
                             ,"Expired_status","Comment","Result"]
        self.total=0
        self.count_gracenote_id=0
        self.writer=''
        self.Fail=0
        self.link_expired=''

    def mongo_connection(self):
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

    def executed_query(self,gracenote_id):
        #import pdb;pdb.set_trace()
        self.query=self.sourcetable.aggregate([{"$match":{"$and":[{"show_type":{"$in":["SM","MO"]}},{"sequence_id":{"$in":["%s"%gracenote_id]}}]}}
                    ,{"$project":{"_id":0,"original_language":1,"show_type":1,"title":1,"episode_title":1,"release_year":1
                    ,"Videos.iTunes Store":1,"Videos.VUDU":1,"Videos.Hulu":1,"Videos.Netflix":1,"Videos.Amazon":1,
                    "Videos.Showtime":1,"Videos.YouTube":1,"episode_number":1,"season_number":1,"sequence_id":1,
                    "series_id":1}}])
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
    def getting_source_ott_details(self,details):
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
        self.logger.debug({"source_link_present":gracenote_link_present,"link_id":gracenote_link,"show_type":self.show_type
                              ,"expired_link":expired_link,"expired_status":self.link_expired})
        return {"source_link_present":gracenote_link_present,"link_id":gracenote_link,"show_type":self.show_type
               ,"expired_link":expired_link,"expired_status":self.link_expired}

    #TODO: to get projectx_id details OTT link id from programs api
    def getting_projectx_ott_details(self):
        px_video_link=[]
        px_video_link_present='False'
        px_response='Null'
        launch_id=[]
        #import pdb;pdb.set_trace()
        projectx_api=self.projectx_programs_api%self.projectx_id
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

    #TODO: To check OTT links sub main func
    def ott_checking(self,thread_name):
        self.default_param()
        #import pdb;pdb.set_trace()
        try:     
            self.mongo_connection()
            self.logger.debug("\n")
            self.logger.debug ({"total":self.total,"%s_id"%self.source:self.GN_id,
            "Px_id":self.projectx_id,"thread_name":thread_name,"Fail_count":self.Fail})
            source_data=self.executed_query(self.GN_id)
            for Data in source_data:
                source_details=self.getting_source_ott_details(Data)
                self.connection.close()
            projectx_details=self.getting_projectx_ott_details()
            if projectx_details!='Null':
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
                self.writer.writerow([self.GN_SM_id,self.GN_id,self.projectx_id,self.show_type,
                      result["present_ott_list"],result["not_present_otts"],
                      result["ott_validation_result"],source_details["expired_link"]
                      ,source_details["expired_status"],result["comment"],self.result])
            else:
                self.Fail+=1
                self.result='Fail'
                self.writer.writerow([self.GN_SM_id,self.GN_id,self.projectx_id,self.show_type,
                    '','','','','','Px_response_null',self.result])
        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError) as e:
            self.retry_count+=1
            self.logger.debug({"Retrying............................":self.retry_count})
            self.logger.debug("\n")
            self.logger.debug ({"exception/error caught in ott_checking func............"
                                :'{},{},{}'.format(type(e),thread_name,self.GN_id)})
            if self.retry_count<=5:
                self.ott_checking(thread_name,data)  
            else:
                self.retry_count=0

    def getting_input_from_sheet(self,input_data,id_):
        #import pdb;pdb.set_trace()
        self.GN_SM_id=str(input_data[id_][0])
        self.GN_id=str(input_data[id_][1])
        self.projectx_id=str(input_data[id_][2])
        self.show_type=str(input_data[id_][3])            

    #TODO:main func
    def main(self,start_id,thread_name,end_id):
        #import pdb;pdb.set_trace()
        self.get_env_url()
        input_file="/input/gracenote_ott_missing07_02_20"
        input_data=lib_common_modules().read_csv(input_file)
        result_sheet='/result/gracenote_ott_%s_checking%s.csv'%(thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
            self.writer.writerow(self.fieldnames)
            for _id in range(start_id,end_id):
                self.logger.debug({"skip":_id})
                self.total+=1
                self.getting_input_from_sheet(input_data,_id)
                self.ott_checking(thread_name)
        output_file.close()                      
        


    #TODO: create threading process
    def threading_pool(self):    

        t1=Process(target=self.main,args=(1,"thread-1",4))
        t1.start()
        # t2=Process(target=self.main,args=(40000,"thread-2",80000))
        # t2.start()


#Starting
if __name__=="__main__":
    gracenote_ott_validation().threading_pool()




#samples:
# {"sequence_id":{"$in":["EP003735550155","EP003735550154","EP003735550061","EP002258240001"
#     ,"EP002258240002","EP002258240003","EP002258240004","EP002258240005","EP008319770012"
#     ,"EP008319770015","EP017157100021","EP018259010008","EP018259010009","EP018259010003"]}}