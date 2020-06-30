
import os
from multiprocessing import Process
import sys
import threading
import datetime
import csv
import json
homedir=os.path.expanduser("~")
sys.path.insert(0,'%s/common_lib'%homedir)
from lib import lib_common_modules,checking_any_two_px_programs
sys.setrecursionlimit(1500)
sys.path.insert(1,os.getcwd())

class movies_mapp_fail_cases:

    def __init__(self):
        self.fieldnames= ["rovi_id","hulu_id","px_id1","px_id1_show_type","px_id1_variant_parent_id","px_id1_is_group_language_primary",
                         "px_id1_record_language","px_id2","px_id2_show_type","px_id2_variant_parent_id","px_id2_is_group_language_primary",
                         "px_id2_record_language","px_id1_credits_null","px_id1_db_credit_present","px_id2_credits_null","px_id2_db_credit_present"
                         ,"long_title_match","original_title_match","runtime_match","release_year_match","alias_title_match","credit_match",
                          "match_link_id","link_match","comment"]                 
        self.px_array=[]
        self.rovi_id=''
        self.hulu_id=''
        self.px_id1=''
        self.px_id2=''
        self.total=0

    def cleanup(self):
        self.px_array=[]
        self.rovi_id=''
        self.hulu_id=''
        self.px_id1=''
        self.px_id2=''

    def default_param(self):
        self.source="Hulu"
        self.projectx_domain_name="https://preprod.caavo.com"
        self.prod_domain="api.caavo.com"
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.projectx_api='https://preprod.caavo.com/programs?ids=%s&ott=true&aliases=true'
        self.projectx_mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"
        self.credit_db_api="http://34.231.212.186:81/projectx/%d/credits/"                         

    def main(self,start_id,thread_name,end_id):
        #import pdb;pdb.set_trace()
        self.default_param()
        inputFile="input_series_mappFail/hulu_rovi_series_mapping_fail_cases_preprod"
        input_data=lib_common_modules().read_csv(inputFile)

        result_sheet='/output_automated_fail_cases_series/result_preprod_%s_%s.csv'%(thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        
        with output_file as outputcsvfile:
            self.writer= csv.DictWriter(outputcsvfile,fieldnames=self.fieldnames,dialect="excel",
                                                                                 lineterminator = '\n')
            self.writer.writeheader()

            for data in range(start_id,end_id):
                print("Checking same program of map fail series.............")
                self.cleanup()
                self.rovi_id=eval(input_data[data][0])
                self.hulu_id=eval(input_data[data][1])
                self.px_id1=eval(input_data[data][2])
                self.px_id2=eval(input_data[data][3])
                #import pdb;pdb.set_trace()
                self.px_array.insert(0,self.px_id1)
                self.px_array.insert(1,self.px_id2)
                print("\n")
                self.total +=1
                print(self.rovi_id,self.hulu_id,[self.px_id1,self.px_id2],thread_name,"total:",self.total)
                result=checking_any_two_px_programs().checking_same_program(self.px_array,
                                   self.projectx_api,self.credit_db_api,self.source,self.token)
                result.update({"rovi_id":self.rovi_id,"hulu_id":self.hulu_id})
                self.writer.writerow(result)
                print("\n")
                print(datetime.datetime.now())


    def thread_pool(self):
        t1=Process(target=self.main,args=(1,"thread-1",64))
        t1.start()
        # t2=Process(target=self.main,args=(100,"thread-2",135))
        # t2.start()



movies_mapp_fail_cases().thread_pool()

            
