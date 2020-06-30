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


class gracenote_program_duplicate_checking:

    logger=lib_common_modules().create_log(os.getcwd()+'/log_%s.txt'%datetime.date.today())
    #import pdb;pdb.set_trace()
    def __init__(self):
        self.id = 0
        self.show_id=0
        self.series_title = ''
        self.episode_title=''
        self.series_id_px=[]
        self.movie_title = ''
        self.release_year = 0
        self.language=''
        self.px_id=[]
        self.mo_list=[]
        self.sm_list=[]
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
        self.sourcetable=self.sourcedb["GNS_validation_id_1"]  
        
    def executed_query(self,skip_id,limit_id):
        self.query=self.sourcetable.aggregate([{"$match":{"$and":[{"show_type":{"$in":["SM","MO"]}}]}}
                    ,{"$project":{"_id":0,"original_language":1,"show_type":1,"title":1,"episode_title":1,"release_year":1
                    ,"Videos.iTunes Store":1,"Videos.VUDU":1,"Videos.Hulu":1,"Videos.Netflix":1,"Videos.Amazon":1,
                    "Videos.Showtime":1,"Videos.YouTube":1,"episode_number":1,"season_number":1,"sequence_id":1,
                    "series_id":1}},{"$skip":skip_id},{"$limit":limit_id}])
        return self.query

    def get_env_url(self):
        self.expire_api='https://%s/expired_ott/is_available?source_program_id=%s&service_short_name=%s'
        self.reverse_api_domain='http://34.231.212.186:81/projectx/%s/%s/ottprojectx'
        self.projectx_preprod_search_api='https://preprod.caavo.com/v3/voice_search?q=%s&safe_search=false&credit_summary=true&credit_types=Actor&aliases=true&ott=true'
        self.projectx_preprod_api='https://preprod.caavo.com/programs?ids=%s&ott=true&aliases=true'
        self.beta_programs_api='https://preprod.caavo.com/programs?ids=%s?ott=true'
        self.duplicate_api='http://34.231.212.186:81/projectx/duplicate?sourceId=%s&sourceName=%s&showType=%s'
        self.projectx_mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"
        self.credit_db_api="http://34.231.212.186:81/projectx/%d/credits/"

    def default_param(self):
        self.source="Gracenote"
        self.Token='Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
        self.preprod_domain_name="preprod.caavo.com"
        self.prod_domain="api.caavo.com"
        self.projectx_domain="https://preprod.caavo.com"
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.fieldnames_tag = ["source","sm_id","id","link_id","show_type","movie_title","series_title","episode_title",
                       "release_year","link_expired","Projectx_ids","Variant id present for episode",
                       "Variant parent id for Episode","Projectx_series_id","comment","Result","Gb_id duplicate",
                       'Duplicate ids in search',"Credit_match","Rovi_id count for duplicate","Gb_id count for duplicate",
                       "%s_id count for duplicate"%self.source,"Hulu_id count","vudu_id_count","Mapped rovi_id for duplicate","Mapped Gb_id for duplicate",
                       "Mapped %s_id for duplicate"%self.source,"Mapped_hulu_id","Mapped_vudu_id","variant_parent_id_present","variant_parent_id","link_fetched_from",
                       "Sources of link"] 
        self.fieldnames_credit_match_false = ["px_id1","px_id1_show_type","px_id1_variant_parent_id","px_id1_is_group_language_primary",
                        "px_id1_record_language","px_id2","px_id2_show_type","px_id2_variant_parent_id","px_id2_is_group_language_primary",
                        "px_id2_record_language","px_id1_credits_null","px_id1_db_credit_present","px_id2_credits_null","px_id2_db_credit_present",
                        "long_title_match","original_title_match","runtime_match","release_year_match","alias_title_match","comment"]               


    def reverse_api_extract(self, link_details,service,show_type):
        #import pdb;pdb.set_trace()
        ## TODO: Only first index has been referred.
        reverse_api=self.reverse_api_domain%(link_details,service)
        reverse_api_resp=lib_common_modules().fetch_response_for_api(reverse_api,self.token,self.logger)
        for data in reverse_api_resp:
            if data.get("data_source")=='Rovi' and data.get("type")=='Program' and data.get("sub_type")=='ALL':
                if data.get("projectx_id") not in self.px_id:      
                    self.px_id.append(data.get("projectx_id"))
            if data.get("data_source")!=self.source and data.get("type")=='Program' and data.get("sub_type")==show_type:
                if data.get("projectx_id") not in self.px_id:      
                    self.px_id.append(data.get("projectx_id"))        
            if data.get("data_source")==self.source and data.get("type")=='Program' and data.get("sub_type")==show_type:
                if data.get("projectx_id") not in self.px_id:      
                    self.px_id.append(data.get("projectx_id"))  

    def to_check_service_proper_name(self,service):
        #import pdb;pdb.set_trace()
        if 'amazon_prime' in service or 'amazon_buy' in service or service=="Amazon":
            service='amazon'
        elif 'Netflix' in service:
            service='netflixusa'
        elif service=="iTunes Store":
            service="itunes"
        return service

    #TODO: to get source_detail OOT link id only from DB table
    def getting_source_ott_details(self,details):
        #import pdb;pdb.set_trace()
        gracenote_link=[]
        link_ids=[]
        expired_link=[]
        gracenote_link.append(details["Videos"])
        for links in gracenote_link:
            for key in links.keys():
                for link_id in links[key]:
                    service=self.to_check_service_proper_name(key)
                    link_ids.append(link_id)  
                    self.link_expired=lib_common_modules().link_expiry_check_(self.expire_api
                            ,self.preprod_domain_name,link_id,service.lower(),self.Token)
                    if self.link_expired=='True':
                        expired_link.append({key:link_id})
                    else:
                        break
                if link_ids:
                    self.link_expired="False"
                    break
        self.logger.debug("\n")            
        self.logger.debug({"link_id":link_ids,"service":service.lower()
               ,"expired_link":expired_link,"expired_status":self.link_expired})                                
        return {"link_id":link_ids,"service":service.lower()
               ,"expired_link":expired_link,"expired_status":self.link_expired}                  

    def duplicate_checking_series(self, series_data, writer,writer_credit_match_false,thread_name): 
        #import pdb;pdb.set_trace()  
        duplicate=""  
        credit_array=[]   
        self.logger.debug("\nFunction duplicate_checking_series_called.................")
        self.id=series_data.get("sequence_id").encode("ascii","ignore")
        self.show_id=series_data.get("series_id").encode("ascii","ignore")
        self.release_year=series_data.get("release_year")
        try:
            self.series_title=unidecode.unidecode(pinyin.get(series_data.get("title")))
        except Exception:
            self.series_title=None    
        try:
            self.episode_title=unidecode.unidecode(pinyin.get(series_data.get("episode_title")))
        except Exception:
            pass    
        if self.series_title is not None and self.series_title!="":
            extracting_link_details=self.getting_source_ott_details(series_data)
            if extracting_link_details["link_id"]:
                self.reverse_api_extract(extracting_link_details["link_id"][0], extracting_link_details["service"],'SE')
                self.logger.debug ({"projectx_ids" : self.px_id,"gracenote_se_id":self.id,"gracenote_SM_id":self.show_id
                    ,"link":str({extracting_link_details["service"]:extracting_link_details["link_id"]}),"thread_name":thread_name})
                if len(self.px_id) > 1:
                    px_link=self.projectx_preprod_api%'{}'.format(",".join([str(data) for data in self.px_id]))
                    data_resp_link=lib_common_modules().fetch_response_for_api(px_link,self.token,self.logger)
                    for id_ in data_resp_link:
                        if id_.get("series_id") not in self.series_id_px:
                            self.series_id_px.append(id_.get("series_id"))
                    self.logger.debug ({"projectx_ids_series ":self.series_id_px,"thread":thread_name}) 
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
                                data_resp_credits=lib_common_modules().fetch_response_for_api(px_link,self.token,self.logger)
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
                                                    ,self.credit_db_api,self.source,self.token)
                                #import pdb;pdb.set_trace()
                                writer_credit_match_false.writerow(result_credit_match_false)            
                            writer.writerow([self.source,self.show_id,self.id,str({extracting_link_details["service"]:extracting_link_details["link_id"]}),'SE','',self.series_title,self.episode_title
                                ,self.release_year,self.link_expired,self.series_id_px,'','','',result["comment"],result["comment"],
                                result["duplicate"],result["search_px_id"],self.credit_match,result["count_rovi"],result["count_guidebox"],result["count_source"],
                                result["count_hulu"],result["count_vudu"],result["rovi_mapping"],result["guidebox_mapping"],result["source_mapping"],result["hulu_mapping"],result["vudu_mapping"],
                                result["comment_variant_parent_id_present"],result["comment_variant_parent_id"]])
                        else:
                            duplicate_api=self.duplicate_api%(self.show_id,self.source,'SM')
                            data_resp_duplicate=lib_common_modules().fetch_response_for_api(duplicate_api,self.token,self.logger)
                            if data_resp_duplicate:
                                duplicate='True'
                            else:
                                duplicate='False'
                            self.comment="search_api_has_no_response"
                            self.result="search_api_has_no_response"
                            writer.writerow([self.source,self.show_id,self.id,str({extracting_link_details["service"]:extracting_link_details["link_id"]}),
                                'SE', '',self.series_title,self.episode_title, self.release_year, self.link_expired,
                                 self.series_id_px,'', '', '' ,self.comment,self.result,duplicate])               
                    else:
                        self.comment=('No multiple ids for this series',self.id,self.show_id)
                        self.result="No multiple ids for this series"
                        writer.writerow([self.source, self.show_id, self.id,str({extracting_link_details["service"]:extracting_link_details["link_id"]}),
                            'SE' ,'',self.series_title,self.episode_title, self.release_year, self.link_expired,
                             self.series_id_px,'', '', '' ,self.comment,self.result])        
                else:
                    self.comment=('No multiple ids for this episode',self.id,self.show_id)
                    self.result="No multiple ids for this episode"
                    writer.writerow([self.source, self.show_id, self.id,str({extracting_link_details["service"]:extracting_link_details["link_id"]})
                           ,'SE', '',self.series_title, self.episode_title, self.release_year, self.link_expired,
                            self.px_id, '', '', '' ,self.comment,self.result]) 
            else:
                self.comment=('link_expired',self.id,self.show_id)
                self.result="link expired for this episode"
                writer.writerow([self.source, self.show_id,'', self.id,'SE', '',self.series_title, 
                                 self.episode_title, self.release_year, '', '', 
                                 '', '', '' ,self.comment,self.result])
        else:
            self.comment=('No series_title for this episode',self.id,self.show_id)
            self.result="No series_title for this episode"
            writer.writerow([self.source, self.show_id,'', self.id,'SE', '',self.series_title, 
                             self.episode_title, self.release_year, '', '', 
                             '', '', '' ,self.comment,self.result])    

    def duplicate_checking_movies(self, movie_data, writer,writer_credit_match_false,thread_name): 
        #import pdb;pdb.set_trace()  
        duplicate=""  
        credit_array=[]   
        print("\nFunction duplicate_checking_movies_called.........................")
        self.id=movie_data.get("sequence_id").encode("ascii","ignore")
        self.movie_title=unidecode.unidecode(pinyin.get(movie_data.get("title"))).replace("#","")
        self.release_year=movie_data.get("release_year")
        try:
            self.language=movie_data.get("original_language")
        except Exception:
            self.language=None     
        extracting_link_details=self.getting_source_ott_details(movie_data)
        if extracting_link_details["link_id"]:
            self.reverse_api_extract(extracting_link_details["link_id"][0], extracting_link_details["service"],'MO')
            self.logger.debug ({"projectx_ids":self.px_id,"gracenote_mo_id":self.id,"threads":thread_name})
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
                        #import pdb;pdb.set_trace()
                        px_link=self.projectx_preprod_api %'{}'.format(",".join([str(i) for i in result["search_px_id"]]))
                        data_resp_credits=lib_common_modules().fetch_response_for_api(px_link,self.token,self.logger)
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
                                            ,self.credit_db_api,self.source,self.token)
                        #import pdb;pdb.set_trace()
                        writer_credit_match_false.writerow(result_credit_match_false)            
                    writer.writerow([self.source,'',self.id,str({extracting_link_details["service"]:extracting_link_details["link_id"]}),'MO',self.movie_title,'',''
                        ,self.release_year,self.link_expired,self.px_id,'','','',result["comment"],result["comment"],
                        result["duplicate"],result["search_px_id"],self.credit_match,result["count_rovi"],result["count_guidebox"],result["count_source"]
                        ,result["count_hulu"],result["count_vudu"],result["rovi_mapping"],result["guidebox_mapping"],result["source_mapping"],result["hulu_mapping"]
                        ,result["vudu_mapping"],result["comment_variant_parent_id_present"],result["comment_variant_parent_id"],self.language])
                else:
                    duplicate_api=self.duplicate_api%(self.id,self.source,'MO')
                    data_resp_duplicate=lib_common_modules().fetch_response_for_api(duplicate_api,self.token,self.logger)
                    if data_resp_duplicate:
                        duplicate='True'
                    else:
                        duplicate='False'
                    self.comment="search_api_has_no_response"
                    self.result="search_api_has_no_response"
                    writer.writerow([self.source,'', self.id,'','MO', self.movie_title, 
                                     '', '', self.release_year, self.link_expired, self.px_id, 
                                     '', '', '' ,self.comment,self.result, duplicate,'','','','','','','',''
                                     '','','','','','',self.language])
            else:
                self.comment=('No multiple ids for this link',self.id)
                self.result="No multiple ids for this link"
                writer.writerow([self.source,'', self.id,'','MO', self.movie_title, 
                                 '', '', self.release_year, self.link_expired, self.px_id, 
                                 '', '', '' ,self.comment,self.result,'','','','','','','','','','','',''
                                 '','','',self.language])                  
           
    #TODO: to get source id of gracenote                      
    def main(self,start_id,thread_name,end_id):
        #import pdb;pdb.set_trace()
        self.get_env_url()
        self.default_param()
        self.mongo_connection()
        result_sheet_credit_match_false='/output_credit_match_false/duplicate_programs_credit_match_false_result_%s_%s.csv'%(thread_name,datetime.date.today())
        output_file_credit_match_false=lib_common_modules().create_csv(result_sheet_credit_match_false)
        result_sheet='/output/duplicate_checked_in_search_api_%s_%s.csv'%(thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile, output_file_credit_match_false as mycsvfile1:
            writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
            writer.writerow(self.fieldnames_tag)
            writer_credit_match_false = csv.DictWriter(mycsvfile1,fieldnames=self.fieldnames_credit_match_false
                                           ,dialect="csv",lineterminator = '\n')
            writer_credit_match_false.writeheader() 
            for _id in range(start_id,end_id,100):
                self.logger.debug ("\n")
                self.logger.debug({"start": start_id,"end":end_id,"skip":_id})
                try:
                    #import pdb;pdb.set_trace()
                    query=self.executed_query(_id,100)
                    for data in query:
                        if data.get("Videos")!={}:
                            #import pdb;pdb.set_trace()
                            series_title=''
                            self.gracenote_id=data.get("sequence_id")
                            show_type=data.get("show_type").encode('utf-8')
                            if self.gracenote_id is not None and self.gracenote_id!="":
                                if show_type=="SM":
                                    sm_id=data.get("series_id")
                                    if sm_id not in self.sm_list:
                                        self.sm_list.append(sm_id)
                                        self.cleanup()
                                        self.duplicate_checking_series(data,writer,writer_credit_match_false,thread_name)
                                elif show_type=='MO':
                                    if self.gracenote_id not in self.mo_list:
                                        self.mo_list.append(self.gracenote_id)
                                        self.cleanup()
                                        self.duplicate_checking_movies(data,writer,writer_credit_match_false,thread_name)
                            self.logger.debug("\n")                             
                            self.logger.debug ({"Total SM":len(self.sm_list),"Total MO":len(self.mo_list),"Thread_name":thread_name})
                            self.logger.debug("\n")
                            self.logger.debug(datetime.datetime.now())
                except (pymongo.errors.CursorNotFound,Exception) as e:
                    #import pdb;pdb.set_trace()
                    self.logger.debug ({"exception caught in main func.............":str(type(e))+','+str(self.gracenote_id)+','+str(thread_name)})
                    continue
        output_file.close()   
        output_file_credit_match_false.close()                   
        self.connection.close()                        
                    
    #TODO: create threads                
    def thread_pool(self):

        t1=Process(target=self.main,args=(0,"thread-1",40000))
        t1.start()
        t2=Process(target=self.main,args=(40000,"thread-2",80000))
        t2.start()
        t3=Process(target=self.main,args=(80000,"thread-3",120000))
        t3.start()
        t4=Process(target=self.main,args=(120000,"thread-4",160000))
        t4.start()
        t5=Process(target=self.main,args=(160000,"thread-5",200000))
        t5.start()
        t6=Process(target=self.main,args=(200000,"thread-6",240000))
        t6.start()
        t7=Process(target=self.main,args=(240000,"thread-7",280000))
        t7.start()
        t8=Process(target=self.main,args=(280000,"thread-8",320000))
        t8.start()
        t9=Process(target=self.main,args=(320000,"thread-9",360000))
        t9.start()
        t10=Process(target=self.main,args=(360000,"thread-10",400000))
        t10.start()
        t11=Process(target=self.main,args=(400000,"thread-11",440000))
        t11.start()
        t12=Process(target=self.main,args=(440120,"thread-12",480000))
        t12.start()
        t13=Process(target=self.main,args=(480000,"thread-13",523000))
        t13.start()

#starting
if __name__=="__main__":    
    gracenote_program_duplicate_checking().thread_pool()


# > db.GNS_validation_id_1.distinct("series_id",{"show_type":"SM"}).length
# 19621
# > db.GNS_validation_id_1.count({"show_type":"MO"})
# 65967