"""Writer: Saayan"""
import threading
from multiprocessing import Process
from beautifultable import BeautifulTable
import csv,pymongo,datetime,time
import sys,os,urllib2,json
from urllib2 import URLError,HTTPError
import httplib,pinyin,unidecode
homedir=os.path.expanduser("~")
sys.path.insert(0,'%s/common_lib'%homedir)
from lib import lib_common_modules
sys.setrecursionlimit(1500) 

class unexpiry_content_validation:

    retry_count=0  
    total_count = 0
    pass_count = 0
    fail_count = 0
    code_exception = 0

    #initialization
    def __init__(self):
        self.current_date = (datetime.datetime.now() - datetime.timedelta(days=0)).strftime("%Y-%m-%d")
        self.token = 'Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.expired_token = 'Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
        self.source_array = ["amazon","vudu","showtimeanytime","itunes","hulu","netflix","tbs","nbc","tnt","hbomax","cbs","hbogo","hbonow","starz","showtime","disneyplus","appletv"]
        self.result = ''
        self.csv_fieldnames = ["service_name","Url","source_id","series_id","Episode_title","Title","Show_type","Release_year","Season_number","Episode_number","Expiry_date","is_valid","modified_at","created_at","API_expiry_response","Result","Curent_date"]

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
    def query(self): 
        try:
            self.query_result=self.sourcetable.find({"item_type":{"$in":["movie","episode"]},"dump_date":{"$nin":[self.current_date]}})   
            return self.query_result
        except pymongo.errors.OperationFailure as error:
            raise error
            self.retry_count+=1
            if self.retry_count <= 5:
                self.query()
            else:
                self.retry_count = 0 

    def get_data_from_db(self,data):
        self.service_name = data["source_id"]
        if self.service_name == "netflix":
            self.service_name = "netflixusa"
        elif self.service_name == "disneyplus":
            self.service_name = "disney_plus" 
        elif self.service_name == "appletv":
            self.service_name = "apple_tv_plus"
        self.url = data["url"]
        self.item_type = data["item_type"]
        self.source_id = data["id"]
        self.series_id = data["series_id"]
        self.episode_title = unidecode.unidecode(pinyin.get(data["episode_title"]))
        self.title = unidecode.unidecode(pinyin.get(data["title"]))
        self.release_year = data["release_year"]
        self.season_number = data["season_number"]
        self.episode_number = data["episode_number"]
        self.expiry_date = data["expiry_date"]
        self.is_valid = data["is_valid"]
        self.modified_at = data["modified_at"]
        self.created_at = data["created_at"]

    #TODO:main func
    def main(self):
        self.get_env_url()
        self.mongo_connection()
        self.table = BeautifulTable()
        self.table.column_headers = ["Test name","Total test", "Test Pass", "Test Fail", "Code Exception/error"]
        self.logger = lib_common_modules().create_log(os.getcwd()+"/log/log.txt")
        result_sheet = '/output/Unexpiry_validation%s.csv'%(datetime.date.today())
        output_file = lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
            self.writer.writerow(self.csv_fieldnames)
            db_result = self.query()
            for data in db_result:
                try:
                    if data["source_id"] in self.source_array:
                        self.total_count += 1
                        self.logger.debug("\n")
                        self.logger.debug(data)
                        self.get_data_from_db(data)
                        self.expiry_check = lib_common_modules().link_expiry_check(self.expired_api,self.domain,self.source_id,self.service_name,self.expired_token,self.logger)
                        if self.expiry_check == 'True':
                            self.result = "Fail"
                            self.fail_count += 1
                        else:
                            self.result = "Pass"
                            self.pass_count += 1
                        self.logger.debug("\n")    
                        self.logger.debug([{"pass":self.pass_count},{"fail":self.fail_count},{"total":self.total_count}]) 
                        self.writer.writerow([self.service_name,self.url,self.source_id,self.series_id,self.episode_title,self.title,self.item_type,self.release_year,self.season_number,self.episode_number,self.expiry_date,self.is_valid,self.modified_at,self.created_at,self.expiry_check,self.result,self.current_date]) 
                    else:
                        self.logger.debug("\n")
                        self.logger.debug(["%s is not considerable for test"%data["source_id"],data["id"],data["title"],data["item_type"]])       
                    self.logger.debug("\n")
                    self.logger.debug(datetime.datetime.now())
                except Exception as error: 
                    self.code_exception += 1  
                    self.logger.debug([{"error":error},{"data":data},thread_name]) 
                    pass 
            self.logger.debug("\n")
            self.table.append_row(["Checking Unexpired Content",self.total_count, self.pass_count, self.fail_count ,self.code_exception])
            self.logger.debug(self.table)                   
        self.connection.close()           
        output_file.close()

    #TODO: create threading
    def process(self):    

        t1 = Process(target=self.main)
        t1.start()

#Starting    
if __name__=="__main__":
    unexpiry_content_validation().process()

