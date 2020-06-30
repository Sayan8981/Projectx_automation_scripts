""" Here we are checking duplicate programs in search api """
"""Saayan"""

import threading
from multiprocessing import Process
import pymysql
import sys
import os
import csv
from urllib2 import URLError,HTTPError
import socket
import datetime
import urllib2
import json
import httplib
import unidecode
import pinyin
homedir=os.path.expanduser("~")
sys.path.insert(0,'%s/common_lib'%homedir)
from lib import lib_common_modules,duplicate_script_modules,checking_any_two_px_programs
sys.setrecursionlimit(1500)


class hbogo_program_duplicate_checking:

    #import pdb;pdb.set_trace()
    def __init__(self):
        self.id = 0
        self.show_id=0
        self.series_title = ''
        self.episode_title=''
        self.series_id_px=[]
        self.movie_title = ''
        self.release_year = 0
        self.px_id=[]
        self.link_expired=''
        self.credit_match=''
        self.credit_array=[]
        self.link_details=[]
        self.comment_variant_parent_rovi_id=[]
        self.mo_list=[]
        self.sm_list=[]
        self.running_datetime=datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')

    def cleanup(self):
        self.id = 0
        self.show_id=0
        self.series_title = None
        self.episode_title=''
        self.series_id_px=[]
        self.movie_title = ''
        self.release_year = 0
        self.px_id=[]
        self.link_expired=''
        self.credit_match=''
        self.credit_array=[]
        self.link_details=[] 

    def mongo_mysql_connection(self):
        self.connection= pymysql.connect(host="192.168.86.10", user="root", password="branch@123",
                                                  db="branch_service", charset='utf8', port=3306)
        self.cur= self.connection.cursor(pymysql.cursors.DictCursor)

    def get_env_url(self):
        self.expire_api='https://%s/expired_ott/is_available?source_program_id=%s&service_short_name=%s'
        self.reverse_api_domain='http://preprod-projectx-api-545109534.us-east-1.elb.amazonaws.com/projectx/%s/%s/ottprojectx'
        self.projectx_preprod_search_api='https://preprod.caavo.com/v3_1/voice_search?q=%s&safe_search=false&credit_summary=true&credit_types=Actor&aliases=true&ott=true'
        self.projectx_preprod_api='https://preprod.caavo.com/programs?ids=%s&ott=true&aliases=true'
        self.beta_programs_api='https://preprod.caavo.com/programs?ids=%s?ott=true'
        self.duplicate_api='http://preprod-projectx-api-545109534.us-east-1.elb.amazonaws.com/projectx/duplicate?sourceId=%s&sourceName=%s&showType=%s'
        self.projectx_mapping_api="http://preprod-projectx-api-545109534.us-east-1.elb.amazonaws.com/projectx/%d/mapping/"
        self.credit_db_api="http://preprod-projectx-api-545109534.us-east-1.elb.amazonaws.com/projectx/%d/credits/"

    def default_param(self):
        self.source="HBOGO"
        self.service='hbogo'
        self.Token='Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
        self.preprod_domain_name="preprod.caavo.com"
        self.prod_domain="api.caavo.com"
        self.projectx_domain="https://preprod.caavo.com"
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.fieldnames_tag = ["source","sm_id","id","show_type","movie_title","series_title","episode_title",
               "release_year","link_expired","Projectx_ids","Variant id present for episode",
               "Variant parent id for Episode","Projectx_series_id","comment","Result","Gb_id duplicate",
               'Duplicate ids in search',"Credit_match","Rovi_id count for duplicate","Gb_id count for duplicate",
               "%s_id count for duplicate"%self.source,"Hulu_id count","vudu_id_count","Mapped rovi_id for duplicate","Mapped Gb_id for duplicate",
               "Mapped %s_id for duplicate"%self.source,"Mapped_hulu_id","Mapped_vudu_id","variant_parent_id_present","variant_parent_id","link_fetched_from",
               "Sources of link"]
        self.fieldnames_credit_match_false = ["px_id1","px_id1_show_type","px_id1_variant_parent_id","px_id1_is_group_language_primary",
                "px_id1_record_language","px_id2","px_id2_show_type","px_id2_variant_parent_id","px_id2_is_group_language_primary",
                "px_id2_record_language","px_id1_credits_null","px_id1_db_credit_present","px_id2_credits_null","px_id2_db_credit_present",
                "long_title_match","original_title_match","runtime_match","release_year_match","alias_title_match","credit_match"
                ,"match_link_id","link_match","comment"]               

    #TODO: To get projectx_id from reverse_API
    def reverse_api_extract(self, link_details,source,show_type):
        #import pdb;pdb.set_trace()
        ## TODO: Only first index has been referred.
        reverse_api=self.reverse_api_domain%(link_details,self.service)
        reverse_api_resp=lib_common_modules().fetch_response_for_api_(reverse_api,self.token)
        for data in reverse_api_resp:
            if data.get("data_source")=='Rovi' and data.get("type")=='Program' and data.get("sub_type")=='ALL':
                if data.get("projectx_id") not in self.px_id:      
                    self.px_id.append(data.get("projectx_id"))
            if data.get("data_source")!=source and data.get("type")=='Program' and data.get("sub_type")==show_type:
                if data.get("projectx_id") not in self.px_id:      
                    self.px_id.append(data.get("projectx_id"))        
            if data.get("data_source")==source and data.get("type")=='Program' and data.get("sub_type")==show_type:
                if data.get("projectx_id") not in self.px_id:      
                    self.px_id.append(data.get("projectx_id"))    
    
    #TODO: sub_main func to check duplicate series
    def duplicate_checking_series(self, series_data, writer,writer_credit_match_false,thread_name): 
        #import pdb;pdb.set_trace()  
        duplicate=""  
        credit_array=[]   
        print("\nFunction duplicate_checking_series_called.................")
        self.id=series_data.get("launch_id")
        self.show_id=series_data.get("series_launch_id")
        self.release_year=series_data.get("release_year")
        try:
            self.series_title=unidecode.unidecode(pinyin.get(series_data.get("series_title")))
        except Exception:
            pass    
        self.episode_title=unidecode.unidecode(pinyin.get(series_data.get("title")))
        if self.series_title is not None:
            data_expired_api_resp = lib_common_modules().link_expiry_check_(self.expire_api,
                                                  self.preprod_domain_name,self.id,self.service,self.Token)
            if data_expired_api_resp:
                self.link_expired = "False" if data_expired_api_resp=='False' else "True"
            self.reverse_api_extract(self.id, self.source,'SE')
            print ("projectx_ids : {},hbogo_se_id: {}, hbogo_SM_id: {}".format(self.px_id,self.id,self.show_id),"thread_name:",thread_name)
            if len(self.px_id) > 1:
                px_link=self.projectx_preprod_api%'{}'.format(",".join([str(data) for data in self.px_id]))
                data_resp_link=lib_common_modules().fetch_response_for_api_(px_link,self.token)
                for id_ in data_resp_link:
                    if id_.get("series_id") not in self.series_id_px:
                        self.series_id_px.append(id_.get("series_id"))
                print ("projectx_ids_series : {0}".format(self.series_id_px),"thread:",thread_name) 
                if len(self.series_id_px)>1:
                    data_resp_search=duplicate_script_modules().search_api_call_response(self.series_title,self.projectx_preprod_search_api,
                                          self.projectx_domain,self.token)
                    if data_resp_search is not None:
                        result=duplicate_script_modules().search_api_response_validation(data_resp_search, self.source, self.series_id_px, duplicate,
                                'SM',self.token,self.projectx_preprod_api,
                                self.projectx_mapping_api,self.beta_programs_api
                                ,self.duplicate_api,self.credit_db_api)
                        if (self.credit_match=='False' or self.credit_match=='') and len(result["search_px_id"])==2:
                            #import pdb;pdb.set_trace()
                            px_link=self.projectx_preprod_api %'{}'.format(",".join([str(i) for i in result["search_px_id"]]))
                            data_resp_credits=lib_common_modules().fetch_response_for_api_(px_link,self.token)
                            #import pdb;pdb.set_trace()
                            for uu in data_resp_credits:
                                if uu.get("credits"):
                                    for tt in uu.get("credits"):
                                        credit_array.append(unidecode.unidecode(tt.get("full_credit_name")))        
                            if credit_array:
                                for cc in credit_array:
                                    if credit_array.count(cc)>1:
                                        self.credit_match='True'
                                        break
                                    else:
                                        self.credit_match='False'
                            result_credit_match_false=checking_any_two_px_programs().checking_same_program(result["search_px_id"],self.projectx_preprod_api
                                                ,self.credit_db_api,self.source,self.token)
                            #import pdb;pdb.set_trace()
                            writer_credit_match_false.writerow(result_credit_match_false)            
                        writer.writerow([self.source,self.show_id,self.id,'SE','',self.series_title,self.episode_title
                            ,self.release_year,self.link_expired,self.series_id_px,'','','',result["comment"],result["comment"],
                            result["duplicate"],result["search_px_id"],self.credit_match,result["count_rovi"],result["count_guidebox"],result["count_source"],
                            result["count_hulu"],result["count_vudu"],result["rovi_mapping"],result["guidebox_mapping"],result["source_mapping"],result["hulu_mapping"],result["vudu_mapping"],
                            result["comment_variant_parent_id_present"],result["comment_variant_parent_id"]])
                    else:
                        duplicate_api=self.duplicate_api%(self.show_id,self.source,'SM')
                        data_resp_duplicate=lib_common_modules().fetch_response_for_api_(duplicate_api,self.token)
                        if data_resp_duplicate:
                            duplicate='True'
                        else:
                            duplicate='False'
                        self.comment="search_api_has_no_response"
                        self.result="search_api_has_no_response"
                        writer.writerow([self.source,self.show_id,self.id,'SE', '',self.series_title, 
                                     self.episode_title, self.release_year, self.link_expired, self.series_id_px, 
                                     '', '', '' ,self.comment,self.result,duplicate])               
                else:
                    self.comment=('No multiple ids for this series',self.id,self.show_id)
                    self.result="No multiple ids for this series"
                    writer.writerow([self.source, self.show_id, self.id,'SE' ,'',self.series_title, 
                                     self.episode_title, self.release_year, self.link_expired, self.series_id_px, 
                                     '', '', '' ,self.comment,self.result])        
            else:
                self.comment=('No multiple ids for this episode',self.id,self.show_id)
                self.result="No multiple ids for this episode"
                writer.writerow([self.source, self.show_id, self.id,'SE', '',self.series_title, 
                                 self.episode_title, self.release_year, self.link_expired, self.px_id, 
                                 '', '', '' ,self.comment,self.result])    
        else:
            self.comment=('No series_title for this episode',self.id,self.show_id)
            self.result="No series_title for this episode"
            writer.writerow([self.source, self.show_id, self.id,'SE', '',self.series_title, 
                             self.episode_title, self.release_year, '', '', 
                             '', '', '' ,self.comment,self.result])    
    
    #TODO: sub_main func to check duplicate movies
    def duplicate_checking_movies(self, movie_data, writer,writer_credit_match_false,thread_name): 
        #import pdb;pdb.set_trace()
        duplicate=""  
        credit_array=[]   
        print("\nFunction duplicate_checking_movies_called.........................")
        self.id=movie_data.get("launch_id")
        self.movie_title=unidecode.unidecode(pinyin.get(movie_data.get("title")))
        self.release_year=movie_data.get("release_year")

        data_expired_api_resp = lib_common_modules().link_expiry_check_(self.expire_api,
                                              self.preprod_domain_name,self.id,self.service,self.Token)
        if data_expired_api_resp:
            self.link_expired = "False" if data_expired_api_resp=='False' else "True"
        self.reverse_api_extract(self.id, self.source,'MO')
        print ("projectx_ids : {}, hbogo_mo_id: {}".format(self.px_id,self.id),"threads:",thread_name)
        if len(self.px_id) > 1:               
            data_resp_search=duplicate_script_modules().search_api_call_response(self.movie_title,
                                          self.projectx_preprod_search_api,
                                          self.projectx_domain,self.token)     
            if data_resp_search is not None:
                result=duplicate_script_modules().search_api_response_validation(data_resp_search, self.source, self.px_id, duplicate,
                        'MO',self.token,self.projectx_preprod_api,
                        self.projectx_mapping_api,self.beta_programs_api
                        ,self.duplicate_api,self.credit_db_api)

                if (self.credit_match=='False' or self.credit_match=='') and len(result["search_px_id"])==2:
                    px_link=self.projectx_preprod_api %'{}'.format(",".join([str(i) for i in result["search_px_id"]]))
                    data_resp_credits=lib_common_modules().fetch_response_for_api_(px_link,self.token)
                    for uu in data_resp_credits:
                        if uu.get("credits"):
                            for tt in uu.get("credits"):
                                credit_array.append(unidecode.unidecode(tt.get("full_credit_name")))        
                    if credit_array:
                        for cc in credit_array:
                            if credit_array.count(cc)>1:
                                self.credit_match='True'
                                break
                            else:
                                self.credit_match='False'
                    result_credit_match_false=checking_any_two_px_programs().checking_same_program(result["search_px_id"],self.projectx_preprod_api
                                        ,self.credit_db_api,self.source,self.token)
                    writer_credit_match_false.writerow(result_credit_match_false)            
                writer.writerow([self.source,'',self.id,'MO',self.movie_title,'',''
                    ,self.release_year,self.link_expired,self.px_id,'','','',result["comment"],result["comment"],
                    result["duplicate"],result["search_px_id"],self.credit_match,result["count_rovi"],result["count_guidebox"],result["count_source"],result["count_hulu"],result["count_vudu"],result["rovi_mapping"],result["guidebox_mapping"],result["source_mapping"],result["hulu_mapping"],result["vudu_mapping"],result["comment_variant_parent_id_present"],result["comment_variant_parent_id"]])
            else:
                duplicate_api=self.duplicate_api%(self.id,self.source,'MO')
                data_resp_duplicate=lib_common_modules().fetch_response_for_api_(duplicate_api,self.token)
                if data_resp_duplicate:
                    duplicate='True'
                else:
                    duplicate='False'
                self.comment="search_api_has_no_response"
                self.result="search_api_has_no_response"
                writer.writerow([self.source,'', self.id,'MO', self.movie_title, 
                                 '', '', self.release_year, self.link_expired, self.px_id, 
                                 '', '', '' ,self.comment,self.result, duplicate])
        else:
            self.comment=('No multiple ids for this link',self.id)
            self.result="No multiple ids for this link"
            writer.writerow([self.source,'', self.id,'MO', self.movie_title, 
                             '', '', self.release_year, self.link_expired, self.px_id, 
                             '', '', '' ,self.comment,self.result])    
                 
           
    #TODO: main func                      
    def main(self,start_id,thread_name,end_id):
        self.get_env_url()
        self.default_param()
        self.mongo_mysql_connection()
        result_sheet_credit_match_false='/output_credit_match_false/duplicate_programs_credit_match_false_result_%s_%s.csv'%(thread_name,datetime.date.today())
        output_file_credit_match_false=lib_common_modules().create_csv(result_sheet_credit_match_false)
        result_sheet='/output/duplicate_checked_in_search_api_%s_%s.csv'%(thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile, output_file_credit_match_false as mycsvfile1:
            writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
            writer.writerow(self.fieldnames_tag)
            writer_credit_match_false = csv.DictWriter(mycsvfile1,fieldnames=self.fieldnames_credit_match_false,dialect="csv",lineterminator = '\n')
            writer_credit_match_false.writeheader() 
            for _id in range(start_id,end_id,100):
                try:
                    query=self.cur.execute("SELECT * FROM hbogo_programs where (expired_at is null or expired_at > '%s') and show_type in ('MO','SE','OT') limit %d,100"%(self.running_datetime,_id))
                    #query=self.cur.execute("SELECT * FROM hbogo_programs where (expired_at is null or expired_at > '%s') and launch_id in ('urn:hbo:episode:GXaoEcARMaKCkmwEAAAse') and show_type in ('MO','SE','OT') limit %d,100"%(self.running_datetime,_id))
                    print ("\n")
                    print({"start": start_id,"end":end_id})
                    query_result=self.cur.fetchall()
                    for data in query_result:
                        series_title=''
                        _id_=data.get("launch_id")
                        sm_id=data.get("series_launch_id")
                        show_type=data.get("show_type")
                        if _id_ is not None and _id_!="":
                            if show_type=='SE':
                                if sm_id not in self.sm_list:
                                    self.sm_list.append(sm_id)
                                    self.cleanup()
                                    self.duplicate_checking_series(data,writer,writer_credit_match_false,thread_name)
                            elif show_type=='MO' or show_type=='OT':
                                show_type='MO' if show_type=='OT' else show_type
                                if _id_ not in self.mo_list:
                                    self.mo_list.append(_id_)
                                    self.cleanup()
                                    self.duplicate_checking_movies(data,writer,writer_credit_match_false,thread_name)
                        print("\n")                             
                        print ({"Total SM":len(self.sm_list),"Total MO":len(self.mo_list),"Thread_name":thread_name})
                        print("\n")
                        print datetime.datetime.now()
                except (pymysql.Error,Exception) as e:
                    print ("exception caught in main func.............",type(e),_id_,sm_id,show_type,thread_name)
                    continue
        output_file.close()   
        output_file_credit_match_false.close()                   
        self.connection.close()                        
                    
    #TODO: create threads                
    def thread_pool(self):

        t1=Process(target=self.main,args=(0,"thread-1",9000))
        t1.start()


#starting
if __name__=="__main__":
    hbogo_program_duplicate_checking().thread_pool()
