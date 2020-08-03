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

    def __init__(self):
        self.db_array = []
        self.current_date=str(datetime.datetime.now().strftime("%Y-%m-%d"))

    def mongo_connection(self):
        self.connection=pymongo.MongoClient("mongodb://127.0.0.1:27017/")
        self.sourceDB=self.connection["DeadSystem"]
        self.sourcetable=self.sourceDB["Content"]

    def get_API_url(self):
        self.deadsystem_API = "http://data.headrun.com/api/?sort_by=True&format=json&is_valid=0&modified_at=%s"

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

    def db_insertion(self,data):
        for details in data:
            self.logger.debug ([details])
            self.created_at = details.pop("created_at")
            self.modified_at = details.pop("modified_at")
            if details in self.db_array:
                details["created_at"] = self.created_at
                details["modified_at"] = self.modified_at
                update_date = { "$set": { "dump_date": self.current_date } }
                self.sourcetable.update_one(details, update_date)
            else:    
                details["created_at"] = self.created_at
                details["modified_at"] = self.modified_at
                details["dump_date"] = self.current_date
                self.sourcetable.insert_one(details)  

    def api_pagination_call_db_insertion(self,api):
        self.logger.debug("\n")
        self.logger.debug (["fetching API", api])
        api_data = self.fetch_response_for_api_(api)
        if api_data["results"]:
            self.db_insertion(api_data["results"])
            if api_data["next"] is not None:
                self.api_pagination_call_db_insertion(api_data["next"])
            else:
                self.logger.debug (["next page not present ....",api_data["previous"]])
                self.connection.close()          

    def main(self):
        self.mongo_connection()
        self.get_API_url()
        self.logger=lib_common_modules().create_log(os.getcwd()+"/log/log.txt")
        #delete all previous record first
        #self.sourcetable.remove()
        self.mycursor = self.sourcetable.find({})
        self.logger.debug ([self.mycursor.explain()])
        for item in self.mycursor:
            item.pop("modified_at")
            item.pop("_id")
            item.pop("created_at")           
            item.pop("dump_date")           
            self.db_array.append(item)
        self.api_pagination_call_db_insertion(self.deadsystem_API%self.current_date)
        self.logger.debug (["data inserted into MongoDB!!",self.current_date])

if __name__=="__main__":
    deadsystem_content_ingestion().main()       
