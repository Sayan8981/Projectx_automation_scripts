"""writer: Sayan"""

"""input : manuaaly added OTTLink id for the respective services from Rovi
output : result is rovi_id should match in the sheet data and the Link id should available in Prod and Preprod both and fetch from Rovi/Rovi+other sources itself"""

import threading
from multiprocessing import Process 
import sys
import os
import csv
from urllib2 import HTTPError,URLError
import socket
import re
import unidecode
import datetime
import urllib2
import json
import httplib
homedir=os.path.expanduser("~")
sys.path.insert(0,'%s/common_lib'%homedir)
from lib import lib_common_modules
sys.setrecursionlimit(1500)


class linkid_checking:

    #TODO: INITIALIZATION
    def __init__(self):
        self.link_id=''
        self.writer=''
        self.rovi_id=''
        self.px_id=[]
        self.link_expired=''
        self.prod_videos_response=[]
        self.projectx_videos_response=[]
        self.prod_launch_id=[]
        self.prod_video_link=[]
        self.prod_link_status=''
        self.projectx_launch_id=[]
        self.projectx_video_link=[]
        self.prod_api_response=''
        self.projectx_api_response=''
        self.fetch_from=[]
        self.fetched_source=''
        self.comment=''
        self.projectx_link_status=''

    #TODO: Refresh
    def cleanup(self):
        self.link_id=''
        self.rovi_id=''
        self.px_id=[] 
        self.link_expired='Null'  
        self.prod_videos_response=[] 
        self.projectx_videos_response=[]
        self.prod_launch_id=[] 
        self.prod_video_link=[]
        self.prod_link_status=''
        self.projectx_launch_id=[] 
        self.projectx_video_link=[]
        self.prod_api_response='True'
        self.projectx_api_response='None'
        self.fetch_from=[]
        self.fetched_source='None'
        self.comment='Null'
        self.projectx_link_status=''
    
    #TODO: one time call param
    def default_param(self):
        self.service=''
        self.projectx_domain="projectx.caavo.com"
        self.prod_domain="api.caavo.com"
        self.host_IP='18.214.4.22:81'
        self.expired_token='Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.total=0
        self.pass_count=0
        self.fail_count=0    
        self.column_fieldnames = []

    def get_env_url(self):    
        #self.reverse_mapping_api="http://%s/projectx/%s/%s/ottprojectx"
        self.px_mapping_api='http://%s/projectx/%d/mapping/' 
        self.source_mapping_api='http://%s/projectx/mappingfromsource?sourceIds=%s&sourceName=Rovi'
        self.programs_api='https://%s/programs?ids=%s&ott=true'   
        self.expired_api='https://%s/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=%s'

    def getting_input_from_sheet(self,input_data,id_):
        #import pdb;pdb.set_trace()
        self.service=re.sub('[^A-Za-z]+', '',str(input_data[id_][0]))
        self.link_id=re.sub('[^A-Za-z0-9.:_-]+', '',str(input_data[id_][1]))
        self.rovi_id=re.sub('[^A-Za-z0-9.:_-]+', '',str(input_data[id_][2]))

    def link_ingestion_check(self,launch_id,link_id,video_link):
        #import pdb;pdb.set_trace()
        if link_id in launch_id or link_id in [link for link in video_link][0]:
            self.link_id_present='True'
        else:
            self.link_id_present='False'
        return {"link_id_present":self.link_id_present}                      

    def main(self,start_id,thread_name,end_id):
        #import pdb;pdb.set_trace()
        inputFile="input/input_file_new"
        input_data=lib_common_modules().read_csv(inputFile)
        self.default_param()
        self.logger=lib_common_modules().create_log(os.getcwd()+'/logs/log_%s_%s.txt'%(thread_name,datetime.date.today()))
        result_sheet="/output/manual_ott_result_%s_%s.csv"%(thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        self.column_fieldnames=["Service_name","Link_id","Rovi_id","Projectx_id","Link_expired","Prod_program_api_response","Projectx_program_api_response",
                          "Prod_link_status","Projectx_link_status","Link_fetched_from","Fetched_from_sources","Comment"]
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
            self.writer.writerow(self.column_fieldnames)
            for id_ in range(start_id,end_id):
                self.cleanup()
                self.getting_input_from_sheet(input_data,id_)
                self.total+=1
                #import pdb;pdb.set_trace()
                mapping_api_response=lib_common_modules().fetch_response_for_api(self.source_mapping_api%(self.host_IP,
                                                                  self.rovi_id),self.token,self.logger)
                if mapping_api_response:
                    for data in mapping_api_response:
                        if data["data_source"]=='Rovi' and data["type"]=='Program':
                            self.px_id.append(data["projectxId"])
                    if self.px_id:
                        self.link_expired=lib_common_modules().link_expiry_check(self.expired_api,self.prod_domain,
                                                     self.link_id,self.service,self.expired_token,self.logger)
                        try:
                            prod_api_response=lib_common_modules().fetch_response_for_api(self.programs_api%(self.prod_domain,
                                                                             self.rovi_id),self.token,self.logger) 
                            if not prod_api_response:
                                self.prod_api_response='False'
                            self.prod_videos_response=[data['videos'] for data in prod_api_response]
                            if self.prod_videos_response[0]:
                                for data in self.prod_videos_response[0]:
                                    if data["source_id"]==self.service:
                                        self.prod_launch_id.append(unidecode.unidecode(data["launch_id"]))
                                        if data["link"]!=None:
                                            self.prod_video_link.append(data["link"]["uri"])
                                #import pdb;pdb.set_trace() 
                                if self.prod_launch_id or self.prod_video_link:      
                                    link_status_prod=self.link_ingestion_check(self.prod_launch_id,self.link_id,
                                                                                           self.prod_video_link)           
                                    if link_status_prod["link_id_present"]=="True":
                                        """comment"""
                                        self.prod_link_status="link_id_present_in_Prod"
                                    else:
                                        self.prod_link_status='link_id_not_present_in_Prod'                        
                                else:
                                    self.prod_link_status='link_id_not_present_in_Prod'        
                            else:
                                """comment"""
                                self.prod_link_status='videos_not_available'
                        except (Exception,urllib2.HTTPError,httplib.BadStatusLine) as e:
                            self.comment=str(type(e))+"In Prod" 
                        #import pdb;pdb.set_trace()    
                        try:                                
                            projectx_api_response=lib_common_modules().fetch_response_for_api(self.programs_api%(self.projectx_domain,
                                                                                              self.px_id[0]),self.token,self.logger)    
                            if projectx_api_response:
                                self.projectx_api_response='True'
                            self.projectx_videos_response=[data['videos'] for data in projectx_api_response]
                            if self.projectx_videos_response[0]:
                                for data in self.projectx_videos_response[0]:
                                    if data["source_id"]==self.service:
                                        if data["launch_id"] not in self.projectx_launch_id: 
                                            self.projectx_launch_id.append(unidecode.unidecode(data["launch_id"]))
                                        if data["link"]!=None:    
                                            if data["link"]["uri"] not in self.projectx_video_link:    
                                                self.projectx_video_link.append(data["link"]["uri"])
                                        if data["fetched_from"] not in self.fetch_from:    
                                            self.fetch_from.append(data["fetched_from"])
                                #import pdb;pdb.set_trace()            
                                if self.projectx_launch_id or self.projectx_video_link: 
                                    link_status_projectx= self.link_ingestion_check(self.projectx_launch_id,self.link_id,
                                                                                                self.projectx_video_link)       
                                    if link_status_projectx["link_id_present"]=='True':
                                        """comment"""
                                        self.projectx_link_status="link_id_present_in_Projectx"
                                    else:
                                        self.projectx_link_status='link_id_not_present_in_Projectx'    
                                    for source in self.fetch_from:
                                        if 'Rovi' in self.fetch_from and len(self.fetch_from)==1:
                                            self.fetched_source='Rovi'
                                        elif 'Rovi' in self.fetch_from and len(self.fetch_from)>1:
                                            self.fetched_source='Rovi+others'
                                        elif 'Rovi' not in self.fetch_from and len(self.fetch_from)>1:
                                            self.fetched_source='others'
                                        else:
                                            self.fetched_source='others'
                                else:
                                    self.projectx_link_status='link_id_not_present_in_Projectx'            
                            else:
                                """comment"""
                                self.projectx_link_status='videos_not_available'
                        except (Exception,urllib2.HTTPError,httplib.BadStatusLine) as e:
                            self.comment=str(type(e))+"In Projectx"        
                else:
                    self.link_expired=lib_common_modules().link_expiry_check(self.expired_api,self.prod_domain
                                                   ,self.link_id,self.service,self.expired_token,self.logger)
                    prod_api_response=lib_common_modules().fetch_response_for_api(self.programs_api%(self.prod_domain,
                                                                     self.rovi_id),self.token,self.logger)
                    if not prod_api_response:
                        self.prod_api_response='False'
                self.logger.debug("\n")
                #import pdb;pdb.set_trace()
                if self.projectx_link_status=='link_id_present_in_Projectx':
                    self.pass_count+=1
                    self.comment='Pass'
                else:
                    self.fail_count+=1  
                    self.comment='Fail'  
                self.logger.debug (["date time:", datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")])    
                self.logger.debug("\n")
                self.logger.debug ([{"total":self.total,"pass":self.pass_count,"fail":self.fail_count,"Process":thread_name}])
                self.logger.debug("\n")
                self.logger.debug([{"Service":self.service,"link_id":self.link_id,"Rovi_id":self.rovi_id,"Projectx_api_response":self.projectx_api_response,
                    "px_id":self.px_id,"Link_expired":self.link_expired,"prod_api_response":self.prod_api_response,"Prod_link_status":self.prod_link_status,
                    "projectx_link_status":self.projectx_link_status,"link_fetched":self.fetched_source,"comment":self.comment,
                    "Process":thread_name}])

                self.writer.writerow([self.service,self.link_id,self.rovi_id,self.px_id,
                    self.link_expired,self.prod_api_response,self.projectx_api_response,self.prod_link_status,
                    self.projectx_link_status,self.fetched_source,self.fetch_from,self.comment])
        output_file.close() 

    # TODO: multi process Operations 
    def thread_pool(self): 
        t1=threading.Thread(target=self.main,args=(1,"process-1",36))
        t1.start()
        """t2=Process(target=self.main,args=(1000,"process-2",2000))
        t2.start()
        t3=Process(target=self.main,args=(2000,"process-3",3000))
        t3.start()
        t4=Process(target=self.main,args=(3000,"process-4",4000))
        t4.start()
        t5=Process(target=self.main,args=(4000,"process-5",5000))
        t5.start()
        t6=Process(target=self.main,args=(5000,"process-6",6000))
        t6.start()
        t7=Process(target=self.main,args=(6000,"process-7",6785))
        t7.start()"""

#6786

if __name__ == "__main__": 
    #TODO: starting and creating class object and calling functions
    object_=linkid_checking()
    object_.__init__()
    object_.get_env_url()
    object_.thread_pool()   