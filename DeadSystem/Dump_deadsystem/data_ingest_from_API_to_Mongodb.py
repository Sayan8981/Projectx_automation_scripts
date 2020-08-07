""" Writer - Sayan"""

import sys,os,pymongo
import urllib2
from urllib2 import HTTPError,URLError
import json,datetime
import httplib
homedir=os.path.expanduser("~")
sys.path.insert(0,'%s/common_lib'%homedir)
from lib import lib_common_modules
sys.setrecursionlimit(1500)


class deadsystem_content_ingestion:

    retry_count = 0

    #initialization
    def __init__(self):
        self.db_array = []
        self.update_array = []
        self.insert_array = []
        self.next_page_url = ''
        self.current_date=(datetime.datetime.now() - datetime.timedelta(days=0)).strftime("%Y-%m-%d")
   
    #connection 
    def mongo_connection(self):
        self.connection=pymongo.MongoClient("mongodb://127.0.0.1:27017/")
        self.sourceDB=self.connection["deadsystem"]
        self.sourcetable=self.sourceDB["content"]

    #APIs
    def get_API_url(self):
        self.deadsystem_API = "http://data.headrun.com/api/?sort_by=True&format=json&is_valid=0"

    #TODO: fetching response for the given API
    def fetch_response_for_api_(self,api):  
        try:
            resp = urllib2.urlopen(urllib2.Request(api,None,{}))
            data = resp.read()
            data_resp = json.loads(data)
            return data_resp
        except (Exception,URLError,HTTPError,httplib.BadStatusLine) as e:
            self.retry_count+=1
            if self.retry_count <= 10:
                self.fetch_response_for_api_(api)
            else:
                self.retry_count = 0

    def cleanup(self):
        self.insert_array = []
        self.update_array = []            

    #db_insertion_updation
    def db_insertion_updation(self,data):
        for details in data:
            self.logger.debug ([details])
            self.created_at = details.pop("created_at")
            self.modified_at = details.pop("modified_at")
            if details in self.db_array:
                details["created_at"] = self.created_at
                details["modified_at"] = self.modified_at
                self.update_array.append(details)
            else:    
                details["created_at"] = self.created_at
                details["modified_at"] = self.modified_at
                details["dump_date"] = self.current_date
                self.insert_array.append(details)
        if self.update_array:
            self.sourcetable.update_many({"$or":self.update_array}, { "$set": { "dump_date": self.current_date } })
        if self.insert_array:
            self.sourcetable.insert_many(self.insert_array)

    def api_pagination_call_db_insertion_updation(self,api):
        self.logger.debug("\n")
        self.logger.debug (["fetching API", api])
        api_data = self.fetch_response_for_api_(api)
        if api_data["results"]:
            self.cleanup()
            self.db_insertion_updation(api_data["results"])
            if api_data["next"] is not None:
                self.next_page_url = api_data["next"]
                self.api_pagination_call_db_insertion_updation(self.next_page_url)
            else:
                self.logger.debug (["next page not present ....",self.next_page_url])
                self.connection.close()          

    def main(self):
        self.mongo_connection()
        self.get_API_url()
        self.logger=lib_common_modules().create_log(os.getcwd()+"/log/log.txt")
        self.mycursor = self.sourcetable.find({})
        self.logger.debug ([self.mycursor.explain()])
        for item in self.mycursor:
            item.pop("modified_at")
            item.pop("_id")
            item.pop("created_at")           
            item.pop("dump_date")           
            self.db_array.append(item)
        self.api_pagination_call_db_insertion_updation(self.deadsystem_API)
        self.logger.debug (["data inserted and updated into MongoDB!!",datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")])

if __name__=="__main__":
    deadsystem_content_ingestion().main()       
