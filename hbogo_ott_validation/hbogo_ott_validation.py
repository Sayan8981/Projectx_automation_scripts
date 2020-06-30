"""Writer: Saayan"""
import threading
from multiprocessing import Process
import csv
import pymysql
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


class hbogo_ott_validation:

    retry_count=0
    def __init__(self):
        self.source="HBOGO"
        self.service="hbogo"
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.expired_token='Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
        self.total=0
        self.count_hbogo_id=0
        self.writer=''
        self.link_expired=''
        self.running_datetime=datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        self.fieldnames = ["%s_id"%self.service,"Projectx_id","show_type","px_video_link_present","%s_link_present"%self.service,"ott_link_result","mapping","","Expired"]

    def mongo_mysql_connection(self):
        self.connection= pymysql.connect(host="127.0.0.1", user="root", password="root@123",
                                                  db="branch_service", charset='utf8', port=3306)
        self.cur= self.connection.cursor(pymysql.cursors.DictCursor)

    def get_env_url(self):
        self.prod_domain="api.caavo.com"
        self.projectx_domain="api.caavo.com"
        self.expired_api='https://%s/expired_ott/is_available?source_program_id=%s&service_short_name=%s'
        self.source_mapping_api="http://production-projectx-api-57650076.us-east-1.elb.amazonaws.com/projectx/mappingfromsource?sourceIds=%s&sourceName=%s&showType=%s"
        self.projectx_programs_api='https://api.caavo.com/programs?ids=%s&ott=true&aliases=true&service=%s'
        self.projectx_mapping_api='http://production-projectx-api-57650076.us-east-1.elb.amazonaws.com/projectx/%d/mapping/'

    #TODO: to get projectx_id details OTT link id from programs api
    def getting_projectx_ott_details(self,projectx_id,show_type): 
        px_video_link=[]
        px_video_link_present='False'
        px_response='Null'
        launch_id=[]
        #import pdb;pdb.set_trace()
        projectx_api=self.projectx_programs_api%(projectx_id,self.service)
        px_resp=urllib2.urlopen(urllib2.Request(projectx_api,None,{'Authorization':self.token}))
        data_px=px_resp.read()
        data_px_resp=json.loads(data_px)
        if data_px_resp!=[]:
            for data in data_px_resp:
                px_video_link= data.get("videos")
                if px_video_link:
                    px_video_link_present='True'
                    for linkid in px_video_link:
                        #if linkid.get("source_id")=='hbogo':
                        launch_id.append(linkid.get("launch_id"))
            return {"px_video_link_present":px_video_link_present,"launch_id":launch_id}
        else:
            return px_response      

    #TODO: To check OTT links
    def ott_checking(self,only_mapped_ids,thread_name,data):
        #import pdb;pdb.set_trace()
        try:     
            if only_mapped_ids["source_flag"]=='True' or only_mapped_ids["source_flag"]=='True(Rovi+others)':
                self.total+=1
                #import pdb;pdb.set_trace()
                projectx_id=only_mapped_ids["px_id"]
                print("\n")
                print ({"total_only_hbogo_mapped":self.total,"%s_id"%self.source:self.hbogo_id,
                    "Px_id":projectx_id,"thread_name":thread_name})
                projectx_details=self.getting_projectx_ott_details(projectx_id,self.show_type)
                if projectx_details!='Null':
                    self.link_expired=lib_common_modules().link_expiry_check_(self.expired_api,self.projectx_domain,self.hbogo_id,self.service,self.expired_token)
                    ott_validation_result=ott_meta_data_validation_modules().ott_validation(projectx_details,self.hbogo_id)
                    self.writer.writerow([self.hbogo_id,projectx_id,self.show_type,projectx_details["px_video_link_present"],'',ott_validation_result,only_mapped_ids["source_flag"],'',self.link_expired,data.get("expired_at")])
                else:
                    self.writer.writerow([self.hbogo_id,projectx_id,self.show_type,projectx_details,
                             '','',only_mapped_ids["source_flag"],'Px_response_null','',data.get("expired_at")])
            else:
                self.total+=1            
                self.writer.writerow([self.hbogo_id,'',self.show_type,'','',only_mapped_ids,'Px_id_null','','',data.get("expired_at")])              
        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError) as e:
            self.retry_count+=1
            print("Retrying...............................",self.retry_count)
            print("\n")
            print ("exception/error caught in ott_checking func.....................",type(e),self.hbogo_id,self.show_type,thread_name)
            if self.retry_count<=5:
                self.ott_checking(only_mapped_ids,thread_name,data)  
            else:
                self.retry_count=0

    #TODO:main func
    def main(self,start_id,thread_name,end_id):
        #import pdb;pdb.set_trace()
        self.get_env_url()
        self.mongo_mysql_connection()
        result_sheet='/result/%s_ott_%s_checking%s.csv'%(self.service,thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
            self.writer.writerow(self.fieldnames)
            for _id in range(start_id,end_id,100):
                print({"skip":_id})
                #import pdb;pdb.set_trace()
                query_hbogo=self.cur.execute("SELECT * FROM hbogo_programs where (expired_at is null or expired_at > '%s') and expired='0' and show_type in ('MO','SE','OT') limit %d,100"%(self.running_datetime,_id))
                query_hbogo_result=self.cur.fetchall()
                for data in query_hbogo_result:
                    if data.get("launch_id")!="" and data.get("launch_id") is not None:
                        #import pdb;pdb.set_trace()
                        self.hbogo_id=data.get("launch_id")
                        self.show_type=data.get("show_type")
                        self.show_type='MO' if self.show_type=='OT' else self.show_type
                        self.count_hbogo_id+=1
                        print("\n")
                        print datetime.datetime.now()
                        print("\n")
                        print ({"count_hbogo_id":self.count_hbogo_id,"show_type":self.show_type,
                               "id":self.hbogo_id,"thread_name":thread_name})
                        #import pdb;pdb.set_trace()
                        only_mapped_ids=ott_meta_data_validation_modules().getting_mapped_px_id_mapping_api(str(self.hbogo_id),self.source_mapping_api,self.projectx_mapping_api,self.show_type,self.source,self.token)
                        self.ott_checking(only_mapped_ids,thread_name,data)              
            print("\n")                    
            print ({"count_hbogo_id":self.count_hbogo_id,"name":thread_name})  
        output_file.close()                      
        self.connection.close()
        self.cur.close()


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
        t8=Process(target=self.main,args=(7000,"thread-8",9000))
        t8.start()

#Starting    
if __name__=="__main__":
    hbogo_ott_validation().threading_pool()



