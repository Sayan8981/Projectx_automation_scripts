"""writer: Sayan"""

"""input : Link id for the respective services from Rovi
output : result is rovi_id should match in the sheet data and the Link id should available in Prod and Preprod both and fetch from Rovi/Rovi+other sources itself"""

import threading
#from multiprocessing import Process 
import sys
import os
import csv
from urllib2 import HTTPError,URLError
import socket
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
        self.source_id=[]
        self.link_expired=''
        self.prod_videos_response=[]
        self.preprod_videos_response=[]
        self.same_rovi_id_present=''
        self.prod_launch_id=[]
        self.prod_video_link=[]
        self.prod_link_status=''
        self.preprod_launch_id=[]
        self.preprod_video_link=[]
        self.fetch_from=[]
        self.fetched_source=''
        self.comment=''
        self.preprod_link_status=''

    #TODO: Refresh
    def cleanup(self):
        self.link_id=''
        self.rovi_id=''
        self.px_id=[] 
        self.source_id=[] 
        self.link_expired='Null'  
        self.prod_videos_response=[] 
        self.preprod_videos_response=[]
        self.same_rovi_id_present='False'  
        self.prod_launch_id=[] 
        self.prod_video_link=[]
        self.prod_link_status='link_id_not_present_in_Prod'
        self.preprod_launch_id=[] 
        self.preprod_video_link=[]
        self.fetch_from=[]
        self.fetched_source='None'
        self.comment='Null'
        self.preprod_link_status='link_id_not_present_in_Preprod'
    
    #TODO: one time call param
    def default_param(self):
        self.service=''
        self.preprod_domain="preprod.caavo.com"
        self.prod_domain="api.caavo.com"
        self.host_IP='34.231.212.186:81'
        self.expired_token='Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.total=0
        self.pass_count=0
        self.fail_count=0    
        self.column_fieldnames = []

    def get_env_url(self):    
        self.reverse_mapping_api="http://%s/projectx/%s/%s/ottprojectx"
        self.px_mapping_api='http://%s/projectx/%d/mapping/' 
        self.programs_api='https://%s/programs?ids=%s&ott=true'   
        self.expired_api='https://%s/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=%s'

    def getting_input_from_sheet(self,input_data,id_):
        #import pdb;pdb.set_trace()
        self.service=str(input_data[id_][0])
        self.link_id=str(input_data[id_][1])
        self.rovi_id=str(input_data[id_][2])

    def link_ingestion_check(self,launch_id,link_id,video_link):
        #import pdb;pdb.set_trace()
        if link_id in launch_id or link_id in [link for link in video_link][0]:
            self.link_id_present='True'
        else:
            self.link_id_present='False'
        return {"link_id_present":self.link_id_present}                      

    def main(self,start_id,thread_name,end_id):
        #import pdb;pdb.set_trace()
        inputFile="input/input_file"
        input_data=lib_common_modules().read_csv(inputFile)
        self.default_param()
        self.logger=lib_common_modules().create_log(os.getcwd()+'/logs/log_%s.txt'%thread_name)
        result_sheet='/output/output_file_%s_%s.csv'%(thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        self.column_fieldnames=["Service_name","Link_id","Rovi_id","Projectx_id","Rovi_id_form_reverse_api","Rovi_id_match","Link_expired",
                          "Prod_link_status","Preprod_link_status","Link_fetched_from","Fetched_from_sources","Comment"]
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
            self.writer.writerow(self.column_fieldnames)
            for id_ in range(start_id,end_id):
                self.cleanup()
                self.getting_input_from_sheet(input_data,id_)
                self.total+=1
                reverse_api_response=lib_common_modules().fetch_response_for_api(self.reverse_mapping_api%(self.host_IP,
                                                                   self.link_id,self.service),self.token,self.logger)
                if reverse_api_response:
                    for data in reverse_api_response:
                        if data["projectxId"] not in self.px_id and data["data_source"]=='Rovi' and data["type"]=='Program':
                            self.px_id.append(data["projectxId"])
                            self.source_id.append(data["sourceId"])
                    if self.rovi_id in self.source_id:
                        """comment"""
                        self.same_rovi_id_present='True'
                    if self.px_id:
                        self.link_expired=lib_common_modules().link_expiry_check(self.expired_api,self.prod_domain,self.link_id,self.service,self.expired_token,self.logger)
                        prod_api_response=lib_common_modules().fetch_response_for_api(self.programs_api%(self.prod_domain,
                                                                             self.source_id[0]),self.token,self.logger) 
                        self.prod_videos_response=[data['videos'] for data in prod_api_response]
                        if self.prod_videos_response[0]:
                            for data in self.prod_videos_response[0]:
                                if data["source_id"]==self.service:
                                    self.prod_launch_id.append(data["launch_id"])
                                    self.prod_video_link.append(data["link"]["uri"])
                            link_status_prod=self.link_ingestion_check(self.prod_launch_id,self.link_id,
                                                                                   self.prod_video_link)           
                            if link_status_prod["link_id_present"]=="True":
                                """comment"""
                                self.prod_link_status="link_id_present_in_Prod"
                        else:
                            """comment"""
                            self.prod_link_status='videos_not_available'
                        preprod_api_response=lib_common_modules().fetch_response_for_api(self.programs_api%(self.preprod_domain,
                                                                                          self.px_id[0]),self.token,self.logger)    
                        self.preprod_videos_response=[data['videos'] for data in preprod_api_response]
                        if self.preprod_videos_response[0]:
                            for data in self.preprod_videos_response[0]:
                                if data["source_id"]==self.service:
                                    if data["launch_id"] not in self.preprod_launch_id: 
                                        self.preprod_launch_id.append(data["launch_id"])
                                    if data["link"]["uri"] not in self.preprod_video_link:    
                                        self.preprod_video_link.append(data["link"]["uri"])
                                    if data["fetched_from"] not in self.fetch_from:    
                                        self.fetch_from.append(data["fetched_from"])
                            if self.preprod_launch_id or self.preprod_video_link: 
                                link_status_preprod= self.link_ingestion_check(self.preprod_launch_id,self.link_id,
                                                                                            self.preprod_video_link)       
                                if link_status_preprod["link_id_present"]=='True':
                                    """comment"""
                                    self.preprod_link_status="link_id_present_in_Preprod"
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
                            """comment"""
                            self.preprod_link_status='videos_not_available'
                    else:
                        self.link_expired=lib_common_modules().link_expiry_check(self.expired_api,self.prod_domain,self.link_id,self.service,self.expired_token,self.logger)        
                else:
                    self.link_expired=lib_common_modules().link_expiry_check(self.expired_api,self.prod_domain,self.link_id,self.service,self.expired_token,self.logger)
                self.logger.debug("\n")
                if self.same_rovi_id_present=="True" and self.preprod_link_status=='link_id_present_in_Preprod':
                    self.pass_count+=1
                    self.comment='Pass'
                else:
                    self.fail_count+=1  
                    self.comment='Fail'  
                self.logger.debug (["date time:", datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")])    
                self.logger.debug("\n")
                self.logger.debug ([{"total":self.total,"pass":self.pass_count,"fail":self.fail_count,"Process":thread_name}])
                self.logger.debug("\n")
                self.logger.debug([{"Service":self.service,"link_id":self.link_id,"Rovi_id":self.rovi_id,
                    "px_id":self.px_id,"rovi_id_form_reverse_api":self.source_id,"rovi_id_status":self.same_rovi_id_present,"Link_expired":self.link_expired,
                    "Prod_link_status":self.prod_link_status,"preprod_link_status":self.preprod_link_status,"link_fetched":self.fetched_source,
                    "comment":self.comment,"Process":thread_name}])

                self.writer.writerow([self.service,self.link_id,self.rovi_id,self.px_id,self.source_id,self.same_rovi_id_present,
                    self.link_expired,self.prod_link_status,self.preprod_link_status,self.fetched_source,self.fetch_from,
                    self.comment])
        output_file.close() 

    # TODO: multi process Operations 
    def thread_pool(self): 
        t1=Process(target=self.main,args=(1,"process - 1",1000))
        t1.start()
        t2=Process(target=self.main,args=(1000,"process - 2",2000))
        t2.start()
        t3=Process(target=self.main,args=(2000,"process - 3",3000))
        t3.start()
        t4=Process(target=self.main,args=(3000,"process - 4",4000))
        t4.start()
        t5=Process(target=self.main,args=(4000,"process - 5",5000))
        t6.start()
        t7=Process(target=self.main,args=(5000,"process - 6",6000))
        t7.start()



if __name__ == "__main__": 
    #TODO: starting and creating class object and calling functions
    object_=linkid_checking()
    object_.__init__()
    object_.get_env_url()
    object_.thread_pool()   