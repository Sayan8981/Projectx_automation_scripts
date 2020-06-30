"""Saayan"""


from multiprocessing import Process
import threading
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


class netflix_link_checking_from_sheet_input:

    #TODO: INITIALIZATION
    def __init__(self):
        self.link_id=''
        self.writer=''
        self.projectx_id=''
        self.px_id=[]
        self.source_id=[]
        self.preprod_videos_response=[]
        self.preprod_launch_id=[]
        self.preprod_video_link=[]
        self.fetch_from=[]
        self.fetched_source=''
        self.comment=''
        self.preprod_link_status=''
        self.logger=''
        self.link_expired=''

    #TODO: Refresh
    def cleanup(self):
        self.link_id=''
        self.link_expired=''
        self.projectx_id=''
        self.px_id=[] 
        self.source_id=[] 
        self.preprod_launch_id=[] 
        self.preprod_video_link=[]
        self.fetch_from=[]
        self.fetched_source='None'
        self.comment='Fail'
        self.preprod_link_status='link_id_not_present_in_Projectx'
    
    #TODO: one time call param
    def constant_param(self):
        self.netflix_id=''
        self.preprod_domain="projectx.caavo.com"
        self.prod_domain="api.caavo.com"
        self.host_IP='54.175.96.97:81'
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
        self.netflix_id=str(input_data[id_][0])
        self.link_id=str(input_data[id_][1])
        self.projectx_id=str(input_data[id_][2])
        self.show_type=str(input_data[id_][3])

    def link_ingestion_check(self,launch_id,link_id,video_link):
        #import pdb;pdb.set_trace()
        if link_id in launch_id or link_id in [link for link in video_link][0]:
            self.link_id_present='True'
        else:
            self.link_id_present='False'

        return {"link_id_present":self.link_id_present}

    def main(self,start_id,thread_name,end_id):
        #import pdb;pdb.set_trace()
        inputFile="input/netflix_link_mising_sheet"
        input_data=lib_common_modules().read_csv(inputFile)
        self.constant_param()
        self.logger=lib_common_modules().create_log(os.getcwd()+'/logs/log_%s.txt'%thread_name)
        result_sheet='/output/output_file_%s_%s.csv'%(thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        self.column_fieldnames=["netflix_id","launch_id","Projectx_id","Preprod_link_status","Link_fetched_from","Fetched_from_sources","link_expired","Comment"]
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
            self.writer.writerow(self.column_fieldnames)
            for id_ in range(start_id,end_id):
                self.cleanup()
                self.getting_input_from_sheet(input_data,id_)
                self.total+=1
                self.link_expired=lib_common_modules().link_expiry_check_(self.expired_api,self.preprod_domain,self.netflix_id,'netflix',self.expired_token)
                preprod_api_response=lib_common_modules().fetch_response_for_api_(self.programs_api%(self.preprod_domain,
                                                                                  self.projectx_id),self.token)    
                self.preprod_videos_response=[data['videos'] for data in preprod_api_response]
                if self.preprod_videos_response[0]:
                    for data in self.preprod_videos_response[0]:
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
                            self.preprod_link_status="link_id_present_in_Projectx"
                            self.comment='Pass'
                        for source in self.fetch_from:
                            if 'Rovi' in self.fetch_from and len(self.fetch_from)==1:
                                self.fetched_source='Rovi'
                            elif ('Rovi' in self.fetch_from and 'netflix' in self.fetch_from) and len(self.fetch_from)>1:
                                self.fetched_source='Rovi+others'
                                self.comment='Pass'
                            elif ('Rovi' not in self.fetch_from or 'netflix' not in self.fetch_from) and len(self.fetch_from)>1:
                                self.fetched_source='others'
                            else:
                                self.fetched_source='others'

                else:
                    """comment"""
                    self.preprod_link_status='videos_not_available'
                    self.comment='Fail'

                self.logger.debug("\n")  
                self.logger.debug([{"netflix_id":self.netflix_id,"link_id":self.link_id,"px_id":self.projectx_id,
                    "preprod_link_status":self.preprod_link_status,"link_fetched":self.fetched_source,"fetch_from":self.fetch_from,
                    "comment":self.comment,"Process":thread_name,"total":self.total}])

                self.writer.writerow([self.netflix_id,self.link_id,self.projectx_id,self.preprod_link_status,self.fetched_source,self.fetch_from,self.link_expired,
                    self.comment])
        output_file.close() 

    # TODO: multi process Operations 
    def thread_pool(self): 
        t1=Process(target=self.main,args=(1,"process - 1",35))
        t1.start()
        # t2=Process(target=self.main,args=(350,"process - 2",763))
        # t2.start()


if __name__ == "__main__": 
    #TODO: starting and creating class object and calling functions
    object_=netflix_link_checking_from_sheet_input()
    object_.get_env_url()
    object_.thread_pool()