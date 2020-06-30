"""Writer: Saayan"""
import threading
from multiprocessing import Process
import csv
import datetime
import sys,os,unidecode,pinyin,httplib,urllib2
from urllib2 import URLError,HTTPError
homedir=os.path.expanduser("~")
sys.path.insert(0,'%s/common_lib'%homedir)
from lib import lib_common_modules
sys.setrecursionlimit(1500) 


class season_number_0_validation:

    total = 0
    retry_count=0
    def __init__(self):
        self.writer=''
        self.season_number_0=''
        self.season_program_id = ''
        self.season_name = ''
        self.result = ''
        self.comment = ''
        self.data_source = ''
        self.season_served = ''
        self.Serving_Sources_season_program_id= []
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.fieldnames = ["Projetcx_SM_id","season_number_0","season_program_id","season_name","Result","Comment","Serving Sources for season_program_id"]

    def cleanup(self):
        self.season_number_0 = "Not_present"
        self.season_program_id = ''
        self.season_name = ''
        self.result = 'Pass'
        self.comment = ''  
        self.data_source = ''
        self.season_served = ''  
        self.Serving_Sources_season_program_id = []

    def get_env_url(self):
        self.seasons_api="https://test.caavo.com/programs/%s/seasons"
        self.all_episode_sequence_api="http://beta-projectx-api-1289873303.us-east-1.elb.amazonaws.com/program/all/episode-sequence/%s"

    def season_0_checking(self,thread_name,projectx_sm_id,token):
        try:
            self.seasons_api_response = lib_common_modules().fetch_response_for_api_(self.seasons_api%projectx_sm_id,token)
            if self.seasons_api_response != []:
                for seasons in self.seasons_api_response:
                    print ("\n")
                    print ({"seasons":seasons})
                    if seasons["season_number"] == 0:
                        self.season_number_0 = "Present"
                        self.season_program_id = seasons["season_program_id"] 
                        self.season_name = unidecode.unidecode(pinyin.get(seasons["season_name"]))
                        self.result = "Fail"
                        self.comment = "Season number 0 present in Season API"
                        self.all_episode_sequence_response= lib_common_modules().fetch_response_for_api_(self.all_episode_sequence_api%projectx_sm_id,token)
                        if self.all_episode_sequence_response != []:
                            for sequence in self.all_episode_sequence_response:
                                if sequence["season_program_id"] == self.season_program_id:
                                    self.data_source = sequence["data_source"]
                                    self.season_served = sequence["season_number"]
                                    if {str(self.data_source):'Season '+str(self.season_served)} not in self.Serving_Sources_season_program_id:
                                        self.Serving_Sources_season_program_id.append({str(self.data_source):'Season '+str(self.season_served)})
                            if self.Serving_Sources_season_program_id == []:            
                                self.Serving_Sources_season_program_id=["Season_program id not there in all episode sequence API"]
            else:
                self.comment = "Empty_Season_response"            
                self.result = "Fail"
        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,urllib2.URLError) as e:
            self.retry_count+=1
            print("Retrying...................................",self.retry_count)
            print("\n")
            print ("exception/error caught in season0_checking func...................",type(e),projectx_sm_id,thread_name)
            if self.retry_count<=10:
                self.season_0_checking(thread_name,projectx_sm_id,token)  
            else:
                self.retry_count=0        
        
    #TODO:main func
    def main(self,start_id,thread_name,end_id):
        self.get_env_url()
        input_file="/input/projectx_sm_ids_season_0"
        input_data=lib_common_modules().read_csv(input_file)
        result_sheet='/result/season_number_0_%s_checking%s.csv'%(thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
            self.writer.writerow(self.fieldnames)
            for _id in range(start_id,end_id):
                self.cleanup()
                self.projetcx_sm_id=str(input_data[_id][0])
                self.total += 1
                try:
                    self.season_0_checking(thread_name,self.projetcx_sm_id,self.token)
                    print("\n")
                    print ({"projectx_sm_id":self.projetcx_sm_id,"season_number_0":self.season_number_0,"season_program_id":self.season_program_id,"result":self.result,"total":self.total,"thread":thread_name})    
                    self.writer.writerow([self.projetcx_sm_id,self.season_number_0,self.season_program_id,self.season_name,self.result,self.comment,self.Serving_Sources_season_program_id])    
                except Exception as Error:
                    print ("Got exception in main............",self.projetcx_sm_id,Error)
                    pass        
        output_file.close()                      

    #TODO: create threading
    def threading_pool(self):    

        # t1=Process(target=self.main,args=(1,"thread-1",1000))
        # t1.start()
        # t2=Process(target=self.main,args=(1000,"thread-2",2000))
        # t2.start()
        # t3=Process(target=self.main,args=(2000,"thread-3",3000))
        # t3.start()
        # t4=Process(target=self.main,args=(3000,"thread-4",4000))
        # t4.start()
        # t5=Process(target=self.main,args=(4000,"thread-5",5000))
        # t5.start()
        # t6=Process(target=self.main,args=(5000,"thread-6",6000))
        # t6.start()
        # t7=Process(target=self.main,args=(6000,"thread-7",7000))
        # t7.start()
        # t8=Process(target=self.main,args=(7000,"thread-8",8000))
        # t8.start()
        # t9=Process(target=self.main,args=(8000,"thread-9",9000))
        # t9.start()
        # t10=Process(target=self.main,args=(9000,"thread-10",10000))
        # t10.start()
        # t11=Process(target=self.main,args=(10000,"thread-11",11000))
        # t11.start()
        # t12=Process(target=self.main,args=(11000,"thread-12",12000))
        # t12.start()
        # t13=Process(target=self.main,args=(12000,"thread-13",13000))
        # t13.start()
        # t14=Process(target=self.main,args=(13000,"thread-14",14000))
        # t14.start()
        # t15=Process(target=self.main,args=(14000,"thread-15",15000))
        # t15.start()
        t16=Process(target=self.main,args=(16404,"thread-16",16643))
        t16.start()

#Starting    
if __name__=="__main__":
    season_number_0_validation().threading_pool()



