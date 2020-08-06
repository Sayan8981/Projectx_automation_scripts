"""Writer: Saayan"""
import threading
from multiprocessing import Process
from beautifultable import BeautifulTable
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

    #initialization
    def __init__(self):
        self.current_date = str((datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
        self.token = 'Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.expired_token = 'Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
        self.source_array = ["amazon","netflix","tbs","nbc","tnt","hbomax","cbs","hbogo","hbonow","starz","showtime"]
        self.result = ''
        self.total_count = 0
        self.pass_count = 0
        self.fail_count = 0
        self.csv_fieldnames = ["service_name","Url","source_id","Episode_title","Title","Show_type","Release_year","Season_number","Episode_number","Expiry_date","is_valid","modified_at","created_at","API_expiry_response","Result"]

    # Connection
    def mongo_connection(self):
        self.connection=pymongo.MongoClient("mongodb://127.0.0.1:27017/")
        self.sourceDB=self.connection["deadsystem"] 
        self.sourcetable=self.sourceDB["content"]

    # APIs list and domain
    def get_env_url(self):
        self.domain= "api.caavo.com"
        self.expired_api='https://%s/expired_ott/is_available?source_program_id=%s&service_short_name=%s'

    #db query
    def query(self,_id): 
        try:
            self.query_result=self.sourcetable.aggregate([{"$match":{"$and":[{"item_type":{"$in":["movie","episode"]}},{"dump_date":self.current_date}]}},{"$skip":_id},{"$limit":100}])   
            return self.query_result
        except pymongo.errors.OperationFailure as error:
            raise error
            self.retry_count+=1
            if self.retry_count <= 5:
                self.query(_id)
            else:
                self.retry_count = 0 

    def get_data_from_db(self,data):
        self.service_name = data["source_id"]            
        self.url = data["url"]
        self.item_type = data["item_type"]
        self.source_id = data["id"]
        self.episode_title = data["episode_title"]
        self.title = data["title"]
        self.release_year = data["release_year"]
        self.season_number = data["season_number"]
        self.episode_number = data["episode_number"]
        self.expiry_date = data["expiry_date"]
        self.is_valid = data["is_valid"]
        self.modified_at = data["modified_at"]
        self.created_at = data["created_at"]

    #TODO:main func
    def main(self,start_id,thread_name,end_id):
        self.get_env_url()
        self.mongo_connection()
        self.table = BeautifulTable()
        self.table.column_headers = ["Total_tested", "Pass", "Fail"]
        self.logger = lib_common_modules().create_log(os.getcwd()+"/log/log.txt")
        result_sheet = '/output/Deadcontent_%s_validation%s.csv'%(thread_name,datetime.date.today())
        output_file = lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
            self.writer.writerow(self.csv_fieldnames)
            for _id in range(start_id,end_id,100):
                db_result = self.query(_id)
                for data in db_result:
                    if data["source_id"] in self.source_array:
                        self.total_count += 1
                        self.logger.debug("\n")
                        self.logger.debug(data)
                        self.get_data_from_db(data)
                        self.expiry_check = lib_common_modules().link_expiry_check(self.expired_api,self.domain,self.source_id,self.service_name,self.expired_token,self.logger)
                        if self.expiry_check == 'True':
                            self.result = "Pass"
                            self.pass_count += 1
                        else:
                            self.result = "Fail"
                            self.fail_count += 1
                        self.logger.debug("\n")    
                        self.logger.debug([[self.service_name,self.url,self.source_id,self.episode_title,self.title,self.item_type,self.release_year,self.season_number,self.episode_number,self.expiry_date,self.is_valid,self.modified_at,self.created_at,self.expiry_check,self.result],thread_name])    
                        self.writer.writerow([self.service_name,self.url,self.source_id,self.episode_title,self.title,self.item_type,self.release_year,self.season_number,self.episode_number,self.expiry_date,self.is_valid,self.modified_at,self.created_at,self.expiry_check,self.result]) 
                    else:
                        self.logger.debug("\n")
                        self.logger.debug(["%s is not considerable for test"%data["source_id"],data["id"],data["title"],data["item_type"]])       
                    self.logger.debug("\n")
                    self.logger.debug(self.current_date) 

            self.table.append_row([self.total_count, self.pass_count, self.fail_count])
            self.logger.debug(self.table)
            self.connection.close()           

    #TODO: create threading
    def threading_pool(self):    
        t1=Process(target=self.main,args=(0,"thread-1",1000))
        t1.start()
        t2=Process(target=self.main,args=(1000,"thread-2",2000))
        t2.start()


#Starting    
if __name__=="__main__":
    deadsystem_content_validation().threading_pool()

