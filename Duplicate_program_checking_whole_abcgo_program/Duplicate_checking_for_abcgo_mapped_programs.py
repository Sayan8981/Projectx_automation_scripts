""" Here we are checking duplicate programs in search api """
"""Saayan"""

import threading
from multiprocessing import Process
import pymongo
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
from lib import lib_common_modules,duplicate_script_modules
sys.setrecursionlimit(1500)


class abcgo_program_duplicate_checking:

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

    def cleanup(self):
        #import pdb;pdb.set_trace()
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

    def mongo_connection(self):
        self.connection=pymongo.MongoClient("mongodb://192.168.86.10:27017/")
        self.sourcedb=self.connection["qadb"]
        self.sourcetable=self.sourcedb["headrun"]    

    def default_param(self):
        self.source="AbcGo"
        self.service='abcgo'
        self.Token='Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
        self.preprod_domain_name="preprod.caavo.com"
        self.prod_domain="api.caavo.com"
        self.projectx_domain="https://preprod.caavo.com"
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'

        self.expire_api='https://%s/expired_ott/is_available?source_program_id=%s&service_short_name=%s'
        self.reverse_api_domain='http://34.231.212.186:81/projectx/%s/%s/ottprojectx'
        self.projectx_preprod_search_api='https://preprod.caavo.com/v3/voice_search?q=%s&safe_search=false&credit_summary=true&credit_types=Actor&aliases=true&ott=true'
        self.projectx_preprod_api='https://preprod.caavo.com/programs?ids=%s&ott=true&aliases=true'
        self.beta_programs_api='https://preprod.caavo.com/programs?ids=%s?ott=true'
        self.duplicate_api='http://34.231.212.186:81/projectx/duplicate?sourceId=%d&sourceName=%s&showType=%s'
        self.projectx_mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"
        self.credit_db_api="http://34.231.212.186:81/projectx/%d/credits/"


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

    def duplicate_checking_series(self, series_data, source, writer,writer_credit_match_false,thread_name): 
        #import pdb;pdb.set_trace()  
        duplicate=""  
        credit_array=[]   
        print("\nFunction duplicate_checking_series_called.................")
        self.id=series_data.get("id").encode("ascii","ignore")
        self.show_id=series_data.get("series_id").encode("ascii","ignore")
        self.release_year=series_data.get("release_year").encode("ascii","ignore")
        self.series_title=unidecode.unidecode(pinyin.get(series_data.get("title")))
        self.episode_title=unidecode.unidecode(pinyin.get(series_data.get("episode_title")))

        if self.series_title is not None:
            data_expired_api_resp = lib_common_modules().link_expiry_check_(self.expire_api,
                                                  self.preprod_domain_name,self.id,self.service,self.Token)
            if data_expired_api_resp:
                self.link_expired = "False" if data_expired_api_resp=='False' else "True"
            self.reverse_api_extract(self.id, source,'SE')
            print ("projectx_ids : {},abcgo_se_id: {}, abcgo_SM_id: {}".format(self.px_id,self.id,self.show_id),"thread_name:",thread_name)
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
                        result=duplicate_script_modules().search_api_response_validation(data_resp_search, source, self.series_id_px, duplicate,
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
                            result_credit_match_false=duplicate_script_modules().validation().meta_data_validation(result["search_px_id"],self.projectx_preprod_api
                                                ,self.credit_db_api,source,self.token)
                            #import pdb;pdb.set_trace()
                            writer_credit_match_false.writerow(result_credit_match_false)            
                        writer.writerow([source,self.show_id,self.id,'SE','',self.series_title,self.episode_title
                            ,self.release_year,self.link_expired,self.series_id_px,'','','',result["comment"],result["comment"],
                            result["duplicate"],result["search_px_id"],self.credit_match,result["count_rovi"],result["count_guidebox"],result["count_source"],
                            result["count_hulu"],result["count_vudu"],result["rovi_mapping"],result["guidebox_mapping"],result["source_mapping"],result["hulu_mapping"],result["vudu_mapping"],
                            result["comment_variant_parent_id_present"],result["comment_variant_parent_id"]])
                    else:
                        duplicate_api=self.duplicate_api%(self.show_id,source,'SM')
                        data_resp_duplicate=lib_common_modules().fetch_response_for_api_(duplicate_api,self.token)
                        if data_resp_duplicate:
                            duplicate='True'
                        else:
                            duplicate='False'
                        self.comment="search_api_has_no_response"
                        self.result="search_api_has_no_response"
                        writer.writerow([source,self.show_id,self.id,'SE', '',self.series_title, 
                                     self.episode_title, self.release_year, self.link_expired, self.series_id_px, 
                                     '', '', '' ,self.comment,self.result,duplicate])               
                else:
                    self.comment=('No multiple ids for this series',self.id,self.show_id)
                    self.result="No multiple ids for this series"
                    writer.writerow([source, self.show_id, self.id,'SE' ,'',self.series_title, 
                                     self.episode_title, self.release_year, self.link_expired, self.series_id_px, 
                                     '', '', '' ,self.comment,self.result])        
            else:
                self.comment=('No multiple ids for this episode',self.id,self.show_id)
                self.result="No multiple ids for this episode"
                writer.writerow([source, self.show_id, self.id,'SE', '',self.series_title, 
                                 self.episode_title, self.release_year, self.link_expired, self.px_id, 
                                 '', '', '' ,self.comment,self.result])    
        else:
            self.comment=('No series_title for this episode',self.id,self.show_id)
            self.result="No series_title for this episode"
            writer.writerow([source, self.show_id, self.id,'SE', '',self.series_title, 
                             self.episode_title, self.release_year, '', '', 
                             '', '', '' ,self.comment,self.result])    

    def duplicate_checking_movies(self, movie_data, source, writer,writer_credit_match_false,thread_name): 
        #import pdb;pdb.set_trace()  
        duplicate=""  
        credit_array=[]   
        print("\nFunction duplicate_checking_movies_called.........................")
        self.id=movie_data.get("id").encode("ascii","ignore")
        self.movie_title=unidecode.unidecode(pinyin.get(movie_data.get("title")))
        self.release_year=movie_data.get("release_year").encode("ascii","ignore")

        data_expired_api_resp = lib_common_modules().link_expiry_check_(self.expire_api,
                                              self.preprod_domain_name,self.id,self.service,self.Token)
        if data_expired_api_resp:
            self.link_expired = "False" if data_expired_api_resp=='False' else "True"
        self.reverse_api_extract(self.id, source,'MO')
        print ("projectx_ids : {}, abcgo_mo_id: {}".format(self.px_id,self.id),"threads:",thread_name)
        if len(self.px_id) > 1:               
            data_resp_search=duplicate_script_modules().search_api_call_response(self.movie_title,
                                          self.projectx_preprod_search_api,
                                          self.projectx_domain,self.token)     
            if data_resp_search is not None:
                result=duplicate_script_modules().search_api_response_validation(data_resp_search, source, self.px_id, duplicate,
                        'MO',self.token,self.projectx_preprod_api,
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
                    result_credit_match_false=duplicate_script_modules().validation().meta_data_validation(result["search_px_id"],self.projectx_preprod_api
                                        ,self.credit_db_api,source,self.token)
                    #import pdb;pdb.set_trace()
                    writer_credit_match_false.writerow(result_credit_match_false)            
                writer.writerow([source,'',self.id,'MO',self.movie_title,'',''
                    ,self.release_year,self.link_expired,self.px_id,'','','',result["comment"],result["comment"],
                    result["duplicate"],result["search_px_id"],self.credit_match,result["count_rovi"],result["count_guidebox"],result["count_source"]
                    ,result["count_hulu"],result["count_vudu"],result["rovi_mapping"],result["guidebox_mapping"],result["source_mapping"],result["hulu_mapping"]
                    ,result["vudu_mapping"],result["comment_variant_parent_id_present"],result["comment_variant_parent_id"]])
            else:
                duplicate_api=self.duplicate_api%(self.id,source,'MO')
                data_resp_duplicate=lib_common_modules().fetch_response_for_api_(duplicate_api,self.token)
                if data_resp_duplicate:
                    duplicate='True'
                else:
                    duplicate='False'
                self.comment="search_api_has_no_response"
                self.result="search_api_has_no_response"
                writer.writerow([source,'', self.id,'MO', self.movie_title, 
                                 '', '', self.release_year, self.link_expired, self.px_id, 
                                 '', '', '' ,self.comment,self.result, duplicate])
        else:
            self.comment=('No multiple ids for this link',self.id)
            self.result="No multiple ids for this link"
            writer.writerow([source,'', self.id,'MO', self.movie_title, 
                             '', '', self.release_year, self.link_expired, self.px_id, 
                             '', '', '' ,self.comment,self.result])    
                 
           
    #TODO: to get source id of abcgo                      
    def main(self,start_id,thread_name,end_id):
        #import pdb;pdb.set_trace()
        self.default_param()
        self.mongo_connection()
        fieldnames_tag = ["source","sm_id","id","show_type","movie_title","series_title","episode_title",
                       "release_year","link_expired","Projectx_ids","Variant id present for episode",
                       "Variant parent id for Episode","Projectx_series_id","comment","Result","Gb_id duplicate",
                       'Duplicate ids in search',"Credit_match","Rovi_id count for duplicate","Gb_id count for duplicate",
                       "%s_id count for duplicate"%self.source,"Hulu_id count","vudu_id_count","Mapped rovi_id for duplicate","Mapped Gb_id for duplicate",
                       "Mapped %s_id for duplicate"%self.source,"Mapped_hulu_id","Mapped_vudu_id","variant_parent_id_present","variant_parent_id","link_fetched_from",
                       "Sources of link"]
        fieldnames_credit_match_false = ["px_id1","px_id1_show_type","px_id1_variant_parent_id","px_id1_is_group_language_primary",
                        "px_id1_record_language","px_id2","px_id2_show_type","px_id2_variant_parent_id","px_id2_is_group_language_primary",
                        "px_id2_record_language","px_id1_credits_null","px_id1_db_credit_present","px_id2_credits_null","px_id2_db_credit_present",
                        "long_title_match","original_title_match","runtime_match","release_year_match","alias_title_match","comment"]
        result_sheet_credit_match_false='/output_credit_match_false/duplicate_programs_credit_match_false_result_%s_%s.csv'%(thread_name,datetime.date.today())
        output_file_credit_match_false=lib_common_modules().create_csv(result_sheet_credit_match_false)
        result_sheet='/output/duplicate_checked_in_search_api_%s_%s.csv'%(thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile, output_file_credit_match_false as mycsvfile1:
            writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
            writer.writerow(fieldnames_tag)
            writer_credit_match_false = csv.DictWriter(mycsvfile1,fieldnames=fieldnames_credit_match_false,dialect="excel",lineterminator = '\n')
            writer_credit_match_false.writeheader() 
            mo_list=[]
            sm_list=[]
            for _id in range(start_id,end_id,100):
                try:
                    query=self.sourcetable.aggregate([{"$match":{"$and":[{"item_type":{"$in":["movie","episode"]}},{"service":"abcgo"}]}}
                        ,{"$project":{"id":1,"_id":0,"item_type":1,"series_id":1,"title":1,"episode_title":1,"release_year":1,
                        "episode_number":1,"season_number":1,"duration":1,"image_url":1,"url":1,"description":1,"cast":1,"directors":1,"writers":1,
                        "categories":1,"genres":1,"maturity_ratings":1,"purchase_info":1,"service":1}},{"$skip":_id},{"$limit":100}]) 
                    #query=self.sourcetable.find({"service":"netflix","item_type":"movie","id":"269880"})
                    print ("\n")
                    print({"start": start_id,"end":end_id})
                    for data in query:
                        series_title=''
                        _id=data.get("id")
                        sm_id=data.get("series_id")
                        show_type=data.get("item_type").encode('utf-8')
                        if _id is not None and _id!="":
                            if show_type=='episode':
                                if sm_id not in sm_list:
                                    sm_list.append(sm_id)
                                    self.cleanup()
                                    self.duplicate_checking_series(data,self.source,writer,writer_credit_match_false,thread_name)
                            elif show_type=='movie':
                                if _id not in mo_list:
                                    mo_list.append(_id)
                                    self.cleanup()
                                    self.duplicate_checking_movies(data,self.source,writer,writer_credit_match_false,thread_name)
                        print("\n")                             
                        print ({"Total SM":len(sm_list),"Total MO":len(mo_list),"Thread_name":thread_name})
                        print("\n")
                        print datetime.datetime.now()
                except (pymongo.errors.CursorNotFound,Exception) as e:
                    print ("exception caught in main func.............",type(e),thread_name)
                    continue
        output_file.close()   
        output_file_credit_match_false.close()                   
        self.connection.close()                        
                    
    #TODO: create threads                
    def thread_pool(self):

        t1=Process(target=self.main,args=(0,"thread-1",1000))
        t1.start()
        t2=Process(target=self.main,args=(1000,"thread-2",2000))
        t2.start()
        t3=Process(target=self.main,args=(2000,"thread-3",3000))
        t3.start()
        t4=Process(target=self.main,args=(3000,"thread-4",4000))
        t4.start()
        t5=Process(target=self.main,args=(4000,"thread-5",6000))
        t5.start()


#starting
abcgo_program_duplicate_checking().thread_pool()