"""Writer: Saayan"""
import threading
from multiprocessing import Process
import csv,pymongo,datetime
import sys,os,urllib2,json
from urllib2 import URLError,HTTPError
import httplib,pinyin,unidecode
homedir=os.path.expanduser("~")
sys.path.insert(0,'%s/common_lib'%homedir)
from lib import lib_common_modules
sys.setrecursionlimit(1500) 

class deadsystem_content_validation:

    retry_count=0
    def __init__(self):
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.expired_token='Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
        self.link_expired=''
        self.fieldnames = []

    def mongo_connection(self):
        self.connection=pymongo.MongoClient("mongodb://127.0.0.1:27017/")
        self.sourceDB=self.connection["DeadSystem"] 
        self.sourcetable=self.sourceDB["Content"]

    def get_env_url(self):
        self.expired_api='https://%s/expired_ott/is_available?source_program_id=%s&service_short_name=%s'

    #TODO:main func
    def main(self,start_id,thread_name,end_id):
        self.get_env_url()
        self.mongo_connection()
        result_sheet='/result/disneyplus_ott_%s_checking%s.csv'%(thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
            self.writer.writerow(self.fieldnames)
            for _id in range(start_id,end_id,100):




    #TODO: create threading
    def threading_pool(self):    

        t1=Process(target=self.main,args=(0,"thread-1",1000))
        t1.start()


#Starting    
if __name__=="__main__":
    disneyplus_ott_validation().threading_pool()

