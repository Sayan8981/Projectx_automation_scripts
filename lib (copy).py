"""*Saayan"""

import logging
import sys
import os
import csv
import MySQLdb
from urllib2 import HTTPError,URLError
import socket
import urllib2
import json
import pinyin
import httplib
import unidecode
import re
from fuzzywuzzy import fuzz
sys.setrecursionlimit(1500)

class lib_common_modules:
    retry_count=0

    #TODO: to read CSV
    def read_csv(self,inputFile):
        input_file = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
        reader = csv.reader(input_file)
        input_data=list(reader)
        return input_data

    #TODO: creating file for writing
    def create_csv(self,result_sheet):
        if (os.path.isfile(os.getcwd()+result_sheet)):
            os.remove(os.getcwd()+result_sheet)
        csv.register_dialect('csv',lineterminator = '\n',skipinitialspace=True,escapechar='')
        output_file=open(os.getcwd()+result_sheet,"wa")
        return output_file

    #TODO: create log file
    def create_log(self,log_file):
        #import pdb;pdb.set_trace()
        self.logger = logging.getLogger()
        logging.basicConfig(filename=log_file,format=[],filemode='wa')
        self.logger.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler(sys.stdout)
        self.logger.addHandler(stream_handler)
        return self.logger

    #TODO: fetching response for the given API
    def fetch_response_for_api_(self,api,token):
        #import pdb;pdb.set_trace()
        try:
            resp = urllib2.urlopen(urllib2.Request(api,None,{'Authorization':token,"User-Agent":'Branch Fyra v1.0'}))
            data = resp.read()
            data_resp = json.loads(data)
            return data_resp
        except (Exception,URLError,HTTPError,httplib.BadStatusLine) as e:
            print ("\n Retrying...................",self.retry_count)
            print ("\n exception caught fetch_response_for_api_ Function..........",type(e),api)
            self.retry_count+=1
            if self.retry_count <=10:
                self.fetch_response_for_api_(api,token)
            else:
                self.retry_count = 0

    #TODO: fetching response for the given API using logger
    def fetch_response_for_api(self,api,token,logger):
        #import pdb;pdb.set_trace()
        try:
            resp = urllib2.urlopen(urllib2.Request(api,None,{'Authorization':token,"User-Agent":'Branch Fyra v1.0'}))
            data = resp.read()
            data_resp = json.loads(data)
            return data_resp
        except (Exception,URLError,HTTPError,httplib.BadStatusLine) as e:
            logger.debug ("\n Retrying...................",self.retry_count)
            logger.debug (["\n exception caught fetch_response_for_api Function..........",type(e),api])
            self.retry_count+=1
            if self.retry_count <=10:
                self.fetch_response_for_api(api,token,logger)
            else:
                self.retry_count = 0

    #TODO: To check link expiry with logger
    def link_expiry_check(self,expired_api,domain,link_id,service,expired_token,logger):
        #import pdb;pdb.set_trace()
        try:
            expired_api_response=self.fetch_response_for_api(expired_api%(domain,link_id,service),expired_token,logger)
            if expired_api_response["is_available"]==False:
                self.link_expired='False'
            else:
                self.link_expired='True'
            return self.link_expired
        except (Exception,HTTPError,URLError,httplib.BadStatusLine) as e:
            logger.debug ("\n Retrying...................",self.retry_count)
            logger.debug (["\n exception caught link_expiry_check Function..........",type(e),expired_api,link_id,service])
            self.retry_count+=1
            if self.retry_count <=10:
                self.link_expiry_check(expired_api,domain,link_id,service,expired_token,logger)
            else:
                self.retry_count = 0

    #TODO: To check link expiry without logger
    def link_expiry_check_(self,expired_api,domain,link_id,service,expired_token):
        #import pdb;pdb.set_trace()
        try:
            expired_api_response=self.fetch_response_for_api_(expired_api%(domain,link_id,service),expired_token)
            if expired_api_response["is_available"]==False:
                self.link_expired='False'
            else:
                self.link_expired='True'
            return self.link_expired
        except (Exception,URLError,HTTPError,httplib.BadStatusLine) as e:
            self.retry_count+=1
            print ("\n Retrying...................",self.retry_count)
            print (["\n exception caught link_expiry_check_ Function..........",type(e),expired_api,link_id,service])
            if self.retry_count <=10:
                self.link_expiry_check_(expired_api,domain,link_id,service,expired_token)
            else:
                self.retry_count = 0


    #TODO: to get source_id from mapping Db
    def getting_mapped_source_id(self,_id,show_type,source,px_mappingdb_cur):
        #import pdb;pdb.set_trace()
        try:
            source_id=[]
            query="select sourceId from projectx_mapping where data_source=%s and projectxId =%s and sub_type=%s"
            px_mappingdb_cur.execute(query,(source,_id,show_type))
            data_resp_mapping=px_mappingdb_cur.fetchall()

            for data in data_resp_mapping:
                if data:
                    source_id.append(data[0])

            return source_id
            px_mappingdb_cur.close()

        except (Exception,MySQLdb.Error, MySQLdb.Warning,socket.error,RuntimeError) as e:
            self.retry_count+=1
            print ("exception caught getting_mapped_source_id.................",type(e),_id,source,show_type)
            print ("\n")
            print ("Retrying.............",self.retry_count)
            if self.retry_count<=5:
                self.getting_mapped_source_id(_id,show_type,source,px_mappingdb_cur)
            else:
                self.retry_count=0


class ingestion_script_modules:

    #TODO: creating file for writing
    def create_csv(self,result_sheet):
        if (os.path.isfile(os.getcwd()+result_sheet)):
            os.remove(os.getcwd()+result_sheet)
        csv.register_dialect('csv',lineterminator = '\n',skipinitialspace=True,escapechar='')
        output_file=open(os.getcwd()+result_sheet,"wa")
        return output_file

    #TODO: fetching response for the given API
    def fetch_response_for_api(self,api,token):
        #import pdb;pdb.set_trace()
        try:
            retry_count=0
            resp = urllib2.urlopen(urllib2.Request(api,None,{'Authorization':token}))
            data = resp.read()
            data_resp = json.loads(data)
            return data_resp
        except (Exception,URLError,httplib.BadStatusLine) as e:
            print ("\n Retrying...................",retry_count)
            print (["\n exception caught fetch_response_for_api Function..........",type(e),api])
            retry_count+=1
            if retry_count <=5:
                self.fetch_response_for_api(api,token)
            else:
                retry_count = 0

    #TODO: to get duplicate source ids from mapping API
    def getting_duplicate_source_id(self,px_id,px_mapping_api,show_type,token,source):
        source_list=[]
        retry_count=0
        #import pdb;pdb.set_trace()
        try:
            for _id in px_id:
                data_resp_mapping=self.fetch_response_for_api(px_mapping_api%_id,token)
                for resp in data_resp_mapping:
                    if resp.get("data_source")==source and resp.get("sub_type")==show_type and resp.get("type")=='Program':
                        source_list.append(resp.get("sourceId"))
            return source_list
        except (Exception,URLError,httplib.BadStatusLine) as e:
            print ("\n Retrying...................",retry_count)
            print (["\n exception caught getting_duplicate_source_id Function..........",type(e),px_mapping_api])
            retry_count+=1
            if retry_count <=5:
                self.getting_duplicate_source_id(px_id,px_mapping_api,show_type,token,source)
            else:
                retry_count = 0

    #TODO: getting px_ids form source_mapping API
    def getting_px_ids(self,source_mapping_api,_id,show_type,source,token):
        #import pdb;pdb.set_trace()
        retry_count=0
        projectx_id=[]
        try:
            data_response_api=self.fetch_response_for_api(source_mapping_api%(_id,show_type),token)
            if data_response_api:
                for data in data_response_api:
                    if data["data_source"]==source and data["type"]=="Program" and data["sub_type"]==show_type:
                        projectx_id.append(data["projectxId"])
                return projectx_id
            else:
                return projectx_id
        except (Exception,HTTPError,urllib2.URLError,socket.error) as e:
            retry_count+=1
            print ("Exception caught............",type(e),source_mapping_api,_id,show_type,source,token)
            print ("Retrying........",retry_count)
            if retry_count<=5:
                self.getting_px_ids(source_mapping_api,_id,show_type,source,token)
            else:
                retry_count=0

class mapping_script_modules:

    def __init__(self):
        self.multiple_mapped_count=0
        self.mapped_count=0
        self.not_mapped_count=0
        self.rovi_id_not_ingested_count=0
        self.source_id_not_ingested_count=0
        self.both_source_not_ingested_count=0
        self.id_not_present_in_DB_count=0

    #TODO: fetching response for the given API
    def fetch_response_for_api(self,api,token):
        #import pdb;pdb.set_trace()
        try:
            retry_count=0
            resp = urllib2.urlopen(urllib2.Request(api,None,{'Authorization':token}))
            data = resp.read()
            data_resp = json.loads(data)
            return data_resp
        except (Exception,URLError,httplib.BadStatusLine) as e:
            print ("\n Retrying...................",retry_count)
            print (["\n exception caught fetch_response_for_api Function..........",type(e),api])
            retry_count+=1
            if retry_count <=5:
                self.fetch_response_for_api(api,token)
            else:
                retry_count = 0

    def getting_px_ids(self,source_mapping_api,source_id,rovi_mapping_api,rovi_id,source,show_type,token):
        #import pdb;pdb.set_trace()
        retry_count=0
        source_projectx_id=[]
        rovi_projectx_id=[]
        try:
            url_rovi=rovi_mapping_api%rovi_id
            data_resp_rovi=self.fetch_response_for_api(url_rovi,token)
            url_source=source_mapping_api%(source_id,source,show_type)
            data_resp_source=self.fetch_response_for_api(url_source,token)

            for ii in data_resp_rovi:
                if ii["data_source"]=="Rovi" and ii["type"]=="Program":
                    rovi_projectx_id.append(ii["projectxId"])
            for jj in data_resp_source:
                if jj["data_source"]==source and jj["type"]=="Program" and jj["sub_type"]==show_type:
                    source_projectx_id.append(jj["projectxId"])
            print("\n")
            print ({"source_projectx_id":source_projectx_id,"rovi_projectx_id":rovi_projectx_id})
            #TODO: return px_ids
            return {"source_projectx_id":source_projectx_id,"rovi_projectx_id":rovi_projectx_id}
        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError) as e:
            retry_count+=1
            print ("Exception caught ........................",type(e),source_id,rovi_id)
            if retry_count<=5:
                self.getting_px_ids(source_mapping_api,source_id,rovi_mapping_api,rovi_id,source,show_type,token)
            else:
                retry_count=0

    def to_check_presence_different_source_id(self,data_mapped_resp,dev_rovi_px_mapped,source_id,source,show_type):
        another_source_id_present=''
        for mapped in data_mapped_resp:
            if mapped.get("type")=='Program' and mapped.get("data_source")=='Rovi':
                dev_rovi_px_mapped.append({'rovi_projectx_id_mapped':"rovi_id:"+str(mapped.get("sourceId")+','+'Map_reason:'+str(mapped.get("map_reason")))})

            elif mapped.get("type")=="Program" and mapped.get("data_source")==source and mapped.get("sub_type")==show_type:
                dev_rovi_px_mapped.append({"sourceId":str(mapped.get("sourceId")+','+'Map_reason:'+str(mapped.get("map_reason")))})
                if mapped.get("sourceId")!=str(source_id):
                    another_source_id_present='True'
                else:
                    another_source_id_present='False'
        return {"another_source_id_present":another_source_id_present,"dev_rovi_px_mapped":dev_rovi_px_mapped}

    def to_check_presence_different_rovi_id(self,data_mapped_resp_source,dev_source_px_mapped,rovi_id,source,show_type):
        another_rovi_id_present=''
        for mapped in data_mapped_resp_source:
            if mapped.get("type")=='Program' and mapped.get("data_source")=='Rovi':
                dev_source_px_mapped.append({'source_projectx_id_mapped':"rovi_id:"+str(mapped.get("sourceId")+','+'Map_reason:'+str(mapped.get("map_reason")))})
                #TODO: taking decision
                if mapped.get("sourceId")!=str(rovi_id):
                    another_rovi_id_present='True'
                else:
                    another_rovi_id_present='False'

            elif mapped.get("type")=="Program" and mapped.get("data_source")==source and mapped.get("sub_type")==show_type:
                dev_source_px_mapped.append({"sourceId":str(mapped.get("sourceId"))})

        return {"another_rovi_id_present":another_rovi_id_present,"dev_source_px_mapped":dev_source_px_mapped}

    def to_check_variant_parent_id(self,projectx_api_response,px_array,px_variant_id):
        variant_present='False'
        for kk in projectx_api_response:
            if kk.get("variant_parent_id") is not None:
                px_variant_id.append(kk.get("variant_parent_id"))
        for id_ in px_array:
            if id_ in px_variant_id:
                variant_present='True'
                break
        return variant_present

    def to_check_series_match(self,projectx_api_response,px_series_id):
        series_id_match='False'
        for kk in projectx_api_response:
            if kk.get("series_id") not in px_series_id:
                px_series_id.append(kk.get("series_id"))
            else:
                series_id_match='True'
        return {"series_id_match":series_id_match,"px_series_id":px_series_id}

    def checking_mapping_series(self,source_sm_id,rovi_sm_id,source_projectx_id,rovi_projectx_id,source,token
                                 ,projectx_mapping_api_,projectx_preprod_api,api_duplicate_checking):
        #import pdb;pdb.set_trace()
        dev_rovi_px_mapped=[]
        dev_source_px_mapped=[]
        px_series_id=[]
        px_array=[]
        px_variant_id=[]

        #TODO: condition 1 , to check multiple projectx ids for a source
        if len(rovi_projectx_id)>1 or len(source_projectx_id)>1:
            self.multiple_mapped_count=self.multiple_mapped_count+1
            status='Nil'
            comment=''
            if len(source_projectx_id)==1 and len(rovi_projectx_id)>1:
                if source_projectx_id in rovi_projectx_id:
                    status='Pass'
                    comment='Multiple ingestion for same content of rovi'
                else:
                    comment= 'Multiple ingestion for same content of rovi'
                    status='Fail'
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)==1:
                if rovi_projectx_id[0] in source_projectx_id:
                    status='Pass'
                    comment='Multiple ingestion for same content of %s'%source
                else:
                    comment= 'Fail:Multiple ingestion for same content of %s'%source
                    status='Fail'
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)>1:
                for x in source_projectx_id:
                    if x in rovi_projectx_id:
                        status='Pass'
                        comment='Multiple ingestion for same content of both sources'
                        break;
                    else:
                        comment= 'Fail:Multiple ingestion for same content of both sources'
                        status='Fail'
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)==0:
                comment='Fail:Multiple ingestion for same content of source and rovi_id not ingested'
                status='Fail'
            elif len(rovi_projectx_id)>1 and len(source_projectx_id)==0:
                comment= 'Fail:Multiple ingestion for same content of rovi and source_id not ingested'
                status='Fail'
            writer.writerow({"%s_sm_id"%source:source_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_%s"%source:source_projectx_id,
                                                                                                   "Comment":comment,"Result of mapping series":status})
        #TODO: condition 2, to check both projectx ids are same or not
        elif len(rovi_projectx_id)==1 and len(source_projectx_id)==1:
            if rovi_projectx_id == source_projectx_id:
                self.mapped_count=self.mapped_count+1
                return {"%s_sm_id"%source:source_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":str(rovi_projectx_id[0]),"projectx_id_%s"%source:str(source_projectx_id[0]),
                                                                                                      "Comment":'Pass',"Result of mapping series":'Pass'}
            else:
                #TODO: to check the mapping of rovi_projectx_id from mapping API
                projectx_mapping_api=projectx_mapping_api_%rovi_projectx_id[0]
                data_mapped_resp=self.fetch_response_for_api(projectx_mapping_api,token)
                another_source_id_present_status=self.to_check_presence_different_source_id(data_mapped_resp,dev_rovi_px_mapped,source_sm_id,source,'SM')
                #TODO: To check the mapping of source_projetx_id from mapping API
                projectx_mapping_api=projectx_mapping_api_%source_projectx_id[0]
                data_mapped_resp_source=self.fetch_response_for_api(projectx_mapping_api,token)
                another_rovi_id_present_status=self.to_check_presence_different_rovi_id(data_mapped_resp_source,dev_source_px_mapped,rovi_sm_id,source,'SM')
                #TODO: checking variant parent id for PX_ids
                px_array=[source_projectx_id[0],rovi_projectx_id[0]]
                projectx_api=projectx_preprod_api%'{}'.format(",".join([str(i) for i in px_array]))
                projectx_api_response=self.fetch_response_for_api(projectx_api,token)
                variant_present=self.to_check_variant_parent_id(projectx_api_response,px_array,px_variant_id)

                self.not_mapped_count=self.not_mapped_count+1
                return {"%s_sm_id"%source:source_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":str(rovi_projectx_id[0]),"projectx_id_%s"%source:str(source_projectx_id[0]),
                                 "Dev_%s_px_mapped"%source:another_rovi_id_present_status["dev_source_px_mapped"],"Dev_rovi_px_mapped":another_source_id_present_status["dev_rovi_px_mapped"],"another_rovi_id_present":another_rovi_id_present_status["another_rovi_id_present"],
                                 "another_%s_id_present"%source:another_source_id_present_status["another_source_id_present"],"variant_present":variant_present,"Comment":'Fail',"Result of mapping series":'Fail'}
        #TODO: condition 3, to check rovi_id ingested
        elif len(source_projectx_id)==1 and not rovi_projectx_id:
            self.rovi_id_not_ingested_count=self.rovi_id_not_ingested_count+1
            return {"%s_sm_id"%source:source_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":'Nil',"projectx_id_%s"%source:source_projectx_id,
                                                                   "Comment":'rovi_id not ingested',"Result of mapping series":'Fail'}
        #TODO: condition 4, to check source id ingested and checking for another source_ids
        elif len(rovi_projectx_id)==1 and not source_projectx_id:
            retry_count=0
            try:
                duplicate_api=api_duplicate_checking%(source_sm_id,source)
                data_resp_url=self.fetch_response_for_api(duplicate_api,token)
                if data_resp_url==[]:
                    self.source_id_not_ingested_count=self.source_id_not_ingested_count+1
                    return {"%s_sm_id"%source:source_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_%s"%source:'Nil',
                                                                 "Comment":'source_id not ingested',"Result of mapping series":'Fail'}
                else:
                    for ll in data_resp_url:
                        if ll.get("projectxId") not in source_projectx_id:
                            source_projectx_id.append(ll.get("projectxId"))
                    if rovi_projectx_id[0] in source_projectx_id:
                        self.mapped_count=self.mapped_count+1
                        return {"%s_sm_id"%source:source_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":str(rovi_projectx_id),"projectx_id_%s"%source:str(source_projectx_id),
                                                                                                         "Comment":'Pass',"Result of mapping series":'Pass'}
                    elif len(source_projectx_id)==1:
                        #TODO: to check the mapping of rovi_projectx_id from mapping API
                        projectx_mapping_api=projectx_mapping_api_%rovi_projectx_id[0]
                        data_mapped_resp=self.fetch_response_for_api(projectx_mapping_api,token)
                        another_source_id_present_status=self.to_check_presence_different_source_id(data_mapped_resp,dev_rovi_px_mapped,source_sm_id,source,'SM')
                        #TODO: To check the mapping of source_projetx_id from mapping API
                        projectx_mapping_api=projectx_mapping_api_%source_projectx_id[0]
                        data_mapped_resp1=self.fetch_response_for_api(projectx_mapping_api,token)
                        another_rovi_id_present_status=self.to_check_presence_different_rovi_id(data_mapped_resp_source,dev_source_px_mapped,rovi_sm_id,source,'SM')
                        #TODO: checking variant parent id for PX_ids
                        px_array=[source_projectx_id[0],rovi_projectx_id[0]]
                        projectx_api=projectx_preprod_api%'{}'.format(",".join([str(i) for i in px_array]))
                        projectx_api_response=self.fetch_response_for_api(projectx_api,token)
                        variant_present=self.to_check_variant_parent_id(projectx_api_response,px_array,px_variant_id)

                        self.source_id_not_ingested_count=self.source_id_not_ingested_count+1
                        return {"%s_sm_id"%source:source_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":str(rovi_projectx_id),
                                        "projectx_id_%s"%source:str(source_projectx_id),"Dev_%s_px_mapped"%source:another_rovi_id_present_status["dev_source_px_mapped"],"Dev_rovi_px_mapped":another_source_id_present_status["dev_rovi_px_mapped"],
                                        "another_rovi_id_present":another_rovi_id_present_status["another_rovi_id_present"],"another_%s_id_present"%source:another_source_id_present_status["another_source_id_present"],"variant_present":variant_present,
                                                             "Comment":'Fail',"Result of mapping series":'Fail'}
                    elif len(source_projectx_id)>1:
                        self.multiple_mapped_count=self.multiple_mapped_count+1
                        return {"%s_sm_id"%source:source_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_%s"%source:'Nil',
                                                                                          "Comment":'multiple source_px_id from duplicate API',"Result of mapping series":'Fail'}
                    else:
                        self.source_id_not_ingested_count=self.source_id_not_ingested_count+1
                        return {"%s_sm_id"%source:source_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_%s"%source:'Nil',
                                                                                          "Comment":'source_id not ingested',"Result of mapping series":'Fail'}

            except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,URLError) as e:
                retry_count+=1
                print ("exception caught ...........................................",type(e),source_sm_id,rovi_sm_id)
                if retry_count<=5:
                    self.checking_mapping_series(source_sm_id,rovi_sm_id,source_projectx_id,rovi_projectx_id,source,token
                                 ,projectx_mapping_api,projectx_preprod_api,api_duplicate_checking)
                else:
                    retry_count=0

        #TODO: condition 5, to check both source id ingested
        elif len(source_projectx_id)==0 and len(rovi_projectx_id)==0:
            self.both_source_not_ingested_count=self.both_source_not_ingested_count+1
            return {"%s_sm_id"%source:source_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":'',"projectx_id_%s"%source:'',"Comment":'both ids not ingested',
                                                                                                   "Result of mapping series":'N.A.'}
        print("\n")
        print("multiple mapped: ",self.multiple_mapped_count ,"mapped :",self.mapped_count ,"Not mapped :", self.not_mapped_count,
              "rovi id not ingested :", self.rovi_id_not_ingested_count, "source Id not ingested : ",self.source_id_not_ingested_count,
               "both not ingested: ",self.both_source_not_ingested_count)

    #this function works only for Guidebox mapping
    def checking_mapping_episodes(self,source_id,rovi_id,source_sm_id,episode_title,ozoneepisodetitle,OzoneOriginalEpisodeTitle
                              ,scheme,rovi_series_id,rovi_projectx_id,source_projectx_id,source_projectx_id_sm,source,
                              token,projectx_mapping_api_,projectx_preprod_api,api_duplicate_checking
                             ,api_check_presence_data,projectx_preprod_api_episodes):
        #import pdb;pdb.set_trace()
        dev_rovi_px_mapped=[]
        dev_source_px_mapped=[]
        px_series_id=[]
        px_array=[]
        #TODO: condition 1, to check both projectx ids are same or not
        if len(rovi_projectx_id)==1 and len(source_projectx_id)==1:
            if rovi_projectx_id == source_projectx_id:
                self.mapped_count+=1
                return {"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"episode_title":episode_title,
                                 "Scheme":scheme,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                                                         "projectx_id_%s"%source:str(source_projectx_id[0]),"Episode mapping":'Pass',"Comment":'Pass'}

            else:
                #import pdb;pdb.set_trace()
                projectx_mapping_api=projectx_mapping_api_%rovi_projectx_id[0]
                data_mapped_resp=self.fetch_response_for_api(projectx_mapping_api,token)
                another_source_id_present_status=self.to_check_presence_different_source_id(data_mapped_resp,dev_rovi_px_mapped,source_id,source,'SE')

                projectx_mapping_api=projectx_mapping_api_%source_projectx_id[0]
                data_mapped_resp_source=self.fetch_response_for_api(projectx_mapping_api,token)
                another_rovi_id_present_status=self.to_check_presence_different_rovi_id(data_mapped_resp_source,dev_source_px_mapped,rovi_id,source,'SE')

                px_array=[source_projectx_id[0],rovi_projectx_id[0]]
                projectx_api=projectx_preprod_api%'{}'.format(",".join([str(i) for i in px_array]))
                projectx_api_response=self.fetch_response_for_api(projectx_api,token)
                series_id_match_status=self.to_check_series_match(projectx_api_response,px_series_id)

                self.not_mapped_count+=1
                return {"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"episode_title":episode_title,
                                 "Scheme":scheme,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                 "projectx_id_%s"%source:str(source_projectx_id[0]),"Dev_%s_px_mapped"%source:another_rovi_id_present_status["dev_source_px_mapped"],"Dev_rovi_px_mapped":another_source_id_present_status["dev_rovi_px_mapped"],
                                 "another_rovi_id_present":another_rovi_id_present_status["another_rovi_id_present"],"another_%s_id_present"%source:another_source_id_present_status["another_source_id_present"],"series_id_match":series_id_match_status["series_id_match"],"Px_series_id":series_id_match_status["px_series_id"],
                                  "Episode mapping":'Fail',"Comment":'Fail'}
        #TODO: condition 2 , to check multiple projectx ids for a source
        elif len(rovi_projectx_id)>1 or len(source_projectx_id)>1:
            self.multiple_mapped_count+self.multiple_mapped_count+1
            status='Nil'
            comment=''
            if len(source_projectx_id)==1 and len(rovi_projectx_id)>1:
                if source_projectx_id[0] in rovi_projectx_id:
                    status='Pass'
                    comment='Multiple ingestion for same content of rovi'
                else:
                    status='Fail'
                    comment='Fail:Multiple ingestion for same content of rovi'
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)==1:
                if rovi_projectx_id[0] in source_projectx_id:
                    status='Pass'
                    comment='Multiple ingestion for same content of %s'%source
                else:
                    status='Fail'
                    comment=  'Fail:Multiple ingestion for same content of %s'%source
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)>1:
                for x in source_projectx_id:
                    if x in rovi_projectx_id:
                        status='Pass'
                        comment='Multiple ingestion for same content of both sources'
                        break;
                    else:
                        status='Fail'
                        comment= 'Fail:Multiple ingestion for same content of both sources'
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)==0:
                comment='Fail:Multiple ingestion for same content of source and rovi_id not ingested'
                status='Fail'
                self.rovi_id_not_ingested_count=self.rovi_id_not_ingested_count+1
            elif len(rovi_projectx_id)>1 and len(source_projectx_id)==0:
                comment= 'Fail:Multiple ingestion for same content of rovi and source_id not ingested'
                status='Fail'
                self.ource_id_not_ingested_count=self.source_id_not_ingested_count+1

            return {"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"episode_title":episode_title,
                            "Scheme":scheme,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":rovi_projectx_id,
                                                                         "projectx_id_%s"%source:source_projectx_id,"Comment":comment,"Episode mapping":status}
        #TODO: condition 3, to check rovi_id ingested
        elif len(source_projectx_id)==1 and not rovi_projectx_id:
            self.rovi_id_not_ingested_count=self.rovi_id_not_ingested_count+1
            return {"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"episode_title":episode_title,
                            "Scheme":scheme,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":'Nil',
                            "projectx_id_%s"%source:source_projectx_id,"Episode mapping":'N.source_projectx_id',"Comment":'No ingestion of rovi_id of episodes and multiple ingestion for GB_id'}
        #TODO: condition 4, to check both source id ingested
        elif len(source_projectx_id)==0 and len(rovi_projectx_id)==0:
            self.both_source_not_ingested_count=self.both_source_not_ingested_count+1
            return {"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"episode_title":episode_title,
                            "Scheme":scheme,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":'',
                                      "projectx_id_%s"%source:'',"Episode mapping":'N.source_projectx_id',"Comment":'No Ingestion of both sources'}
        #TODO: condition 5, to check source id ingested and checking for another source_ids
        elif len(source_projectx_id)==0 and len(rovi_projectx_id)==1:
            retry_count=0
            try:
                duplicate_api=api_duplicate_checking%(source_sm_id,source)
                data_resp_url=self.fetch_response_for_api(duplicate_api,token)
                if data_resp_url==[]:
                    id_api=api_check_presence_data%(source,source_sm_id)
                    data_resp_url_source=self.fetch_response_for_api(id_api,token)
                    if data_resp_url_source==True:
                        self.source_id_not_ingested_count=self.source_id_not_ingested_count+1
                        return {"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"episode_title":episode_title,
                                        "Scheme":scheme,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                                                               "projectx_id_%s"%source:'',"Episode mapping":'Fail',"Comment":'No Ingestion of source_id of episode'}
                    else:
                        self.id_not_present_in_DB_count=self.id_not_present_in_DB_count+1
                        return {"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"episode_title":episode_title,
                                         "Scheme":scheme,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                                                     "projectx_id_%s"%source:'',"Episode mapping":'Fail',"Comment":'Not present of source_id of series in GB DB'}
                else:
                    for ll in data_resp_url:
                        if ll.get("projectxId") not in source_projectx_id_sm:
                            source_projectx_id_sm.append(ll.get("projectxId"))

                    for sm_id in source_projectx_id_sm:
                        projectx_url=projectx_preprod_api_episodes%sm_id
                        data_resp_url=self.fetch_response_for_api(projectx_url,token)
                        if data_resp_url!=[]:
                            for hh in data_resp_url:
                                source_projectx_id.append(hh.get("id"))

                    if source_projectx_id!=[]:
                        if rovi_projectx_id[0] in source_projectx_id:
                            self.mapped_count=self.mapped_count+1
                            return {"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"episode_title":episode_title,
                                            "Scheme":scheme,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str(rovi_projectx_id),
                                                                                  "projectx_id_%s"%source:str(rovi_projectx_id),"Episode mapping":'Pass',"Comment":'Pass'}
                        else:
                            projectx_mapping_api=projectx_mapping_api_%rovi_projectx_id[0]
                            data_mapped_resp=self.fetch_response_for_api(projectx_mapping_api,token)
                            another_source_id_present_status=self.to_check_presence_different_source_id(data_mapped_resp,dev_rovi_px_mapped,source_id,source,'SE')

                            self.not_mapped_count=self.not_mapped_count+1
                            return {"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"episode_title":episode_title,
                                             "Scheme":scheme,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str(rovi_projectx_id),
                                             "projectx_id_%s"%source:['not found match id'],"Dev_rovi_px_mapped":another_source_id_present_status["dev_rovi_px_mapped"],
                                            "another_%s_id_present"%source:another_source_id_present_status["another_source_id_present"],"Episode mapping":'Fail',"Comment":'Fail'}
                    else:
                        self.source_id_not_ingested_count=self.source_id_not_ingested_count+1
                        return {"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"episode_title":episode_title,
                                         "Scheme":scheme,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                                                                   "projectx_id_%s"%source:'',"Episode mapping":'Fail',"Comment":'No Ingestion of source_id of episode'}

            except (httplib.BadStatusLine,urllib2.HTTPError,socket.error,URLError,Exception) as e:
                retry_count+=1
                print ("exception caught ...........................................",type(e),source_id,rovi_id)
                print("\n")
                print ("Retrying.............",retry_count)
                if retry_count<=5:
                    self.checking_mapping_episodes(source_id,rovi_id,source_sm_id,episode_title,ozoneepisodetitle,OzoneOriginalEpisodeTitle
                                 ,scheme,rovi_series_id,rovi_projectx_id,source_projectx_id,source_projectx_id_sm,
                                source,token,projectx_mapping_api,projectx_preprod_api,api_duplicate_checking,api_check_presence_data,projectx_preprod_api_episodes)
                else:
                   retry_count=0

        print("\n")
        print("multiple mapped: ",self.multiple_mapped_count,"mapped :",self.mapped_count ,
              "Not mapped :", self.not_mapped_count, "rovi id not ingested :", self.rovi_id_not_ingested_count,
               "source Id not ingested : ",self.source_id_not_ingested_count, "both not ingested: ",self.both_source_not_ingested_count,
                 "id not present in db:" ,self.id_not_present_in_DB_count)

    # this func works for sources mapping except Guidebox
    def checking_mapping_episodes_(self,source_id,rovi_id,source_sm_id,rovi_series_id,rovi_projectx_id,source_projectx_id,
                              source_projectx_id_sm,source,token,projectx_mapping_api_,projectx_preprod_api,
                              api_duplicate_checking,projectx_preprod_api_episodes):
        #import pdb;pdb.set_trace()
        dev_rovi_px_mapped=[]
        dev_source_px_mapped=[]
        px_series_id=[]
        px_array=[]
        #TODO: condition 1, to check both projectx ids are same or not
        if len(rovi_projectx_id)==1 and len(source_projectx_id)==1:
            if rovi_projectx_id == source_projectx_id:
                self.mapped_count=self.mapped_count+1
                return {"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                                                        "projectx_id_%s"%source:str(source_projectx_id[0]),"Episode mapping":'Pass',"Comment":'Pass'}

            else:
                #import pdb;pdb.set_trace()
                projectx_mapping_api=projectx_mapping_api_%rovi_projectx_id[0]
                data_mapped_resp=self.fetch_response_for_api(projectx_mapping_api,token)
                another_source_id_present_status=self.to_check_presence_different_source_id(data_mapped_resp,dev_rovi_px_mapped,source_id,source,'SE')

                projectx_mapping_api=projectx_mapping_api_%source_projectx_id[0]
                data_mapped_resp_source=self.fetch_response_for_api(projectx_mapping_api,token)
                another_rovi_id_present_status=self.to_check_presence_different_rovi_id(data_mapped_resp_source,dev_source_px_mapped,rovi_id,source,'SE')

                px_array=[source_projectx_id[0],rovi_projectx_id[0]]
                projectx_api=projectx_preprod_api%'{}'.format(",".join([str(i) for i in px_array]))
                projectx_api_response=self.fetch_response_for_api(projectx_api,token)
                series_id_match_status=self.to_check_series_match(projectx_api_response,px_series_id)

                self.not_mapped_count=self.not_mapped_count+1
                return {"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                "projectx_id_%s"%source:str(source_projectx_id[0]),"Dev_%s_px_mapped"%source:another_rovi_id_present_status["dev_source_px_mapped"],"Dev_rovi_px_mapped":another_source_id_present_status["dev_rovi_px_mapped"],
                                 "another_rovi_id_present":another_rovi_id_present_status["another_rovi_id_present"],"another_%s_id_present"%source:another_source_id_present_status["another_source_id_present"],"series_id_match":series_id_match_status["series_id_match"],
                                 "Px_series_id":series_id_match_status["px_series_id"],"Episode mapping":'Fail',"Comment":'Fail'}
        #TODO: condition 2 , to check multiple projectx ids for a source
        elif len(rovi_projectx_id)>1 or len(source_projectx_id)>1:
            self.multiple_mapped_count=self.multiple_mapped_count+1
            status='Nil'
            comment=''
            if len(source_projectx_id)==1 and len(rovi_projectx_id)>1:
                if source_projectx_id[0] in rovi_projectx_id:
                    status='Pass'
                    comment='Multiple ingestion for same content of rovi'
                else:
                    status='Pass'
                    comment='Fail:Multiple ingestion for same content of rovi'
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)==1:
                if rovi_projectx_id[0] in source_projectx_id:
                    status='Pass'
                    comment='Multiple ingestion for same content of %s'%source
                else:
                    status='Fail'
                    comment=   'Fail:Multiple ingestion for same content of %s'%source
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)>1:
                for x in source_projectx_id:
                    if x in rovi_projectx_id:
                        status='Pass'
                        comment='Multiple ingestion for same content of both sources'
                        break;
                    else:
                        status='Fail'
                        comment= 'Fail:Multiple ingestion for same content of both sources'
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)==0:
                comment='Fail:Multiple ingestion for same content of source and rovi_id not ingested'
                status='Fail'
                self.rovi_id_not_ingested_count=self.rovi_id_not_ingested_count+1
            elif len(rovi_projectx_id)>1 and len(source_projectx_id)==0:
                comment= 'Fail:Multiple ingestion for same content of rovi and source_id not ingested'
                status='Fail'
                self.source_id_not_ingested_count=self.source_id_not_ingested_count+1

            return {"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":rovi_projectx_id,
                                                                 "projectx_id_%s"%source:source_projectx_id,"Comment":comment,"Episode mapping":status}
        #TODO: condition 3, to check both source id ingested
        elif len(source_projectx_id)==0 and len(rovi_projectx_id)==0:
            self.both_source_not_ingested_count=self.both_source_not_ingested_count+1
            return {"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":'',"projectx_id_%s"%source:'',
                                "Episode mapping":'Fail',"Comment":'No Ingestion of both sources'}
        #TODO: condition 4, to check rovi_id ingested
        elif len(source_projectx_id)==1 and len(rovi_projectx_id)==0:
            self.rovi_id_not_ingested_count=self.rovi_id_not_ingested_count+1
            return {"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":'',
                               "projectx_id_%s"%source:str(source_projectx_id[0]),"Episode mapping":'Fail',"Comment":'No Ingestion of rovi_id of episode'}
        #TODO: condition 5, to check source id ingested and checking for another source_ids
        elif len(source_projectx_id)==0 and len(rovi_projectx_id)==1:
            retry_count=0
            try:
                duplicate_api=api_duplicate_checking%(source_sm_id,source)
                data_resp_url=self.fetch_response_for_api(duplicate_api,token)
                if data_resp_url==[]:
                    self.source_id_not_ingested_count=self.source_id_not_ingested_count+1
                    return {"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                        "projectx_id_%s"%source:'',"Episode mapping":'Fail',"Comment":'No Ingestion of source_id of episode'}

                else:
                    for ll in data_resp_url:
                        if ll.get("projectx_id") not in source_projectx_id_sm:
                            source_projectx_id_sm.append(ll.get("projectx_id"))
                    for sm_id in source_projectx_id_sm:
                        projectx_url=projectx_preprod_api_episodes%sm_id
                        data_resp_url=self.fetch_response_for_api(projectx_url,token)
                        if data_resp_url!=[]:
                            for hh in data_resp_url:
                                source_projectx_id.append(hh.get("id"))

                    if source_projectx_id!=[]:
                        if rovi_projectx_id[0] in source_projectx_id:
                            self.mapped_count=self.mapped_count+1
                            return {"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":str(rovi_projectx_id),
                                                                              "projectx_id_%s"%source:str(rovi_projectx_id),"Episode mapping":'Pass',"Comment":'Pass'}
                        else:
                            projectx_mapping_api=projectx_mapping_api_%rovi_projectx_id[0]
                            data_mapped_resp=self.fetch_response_for_api(projectx_mapping_api,token)
                            another_source_id_present_status=self.to_check_presence_different_source_id(data_mapped_resp,dev_rovi_px_mapped,source_id,source,'SE')

                            self.not_mapped_count=self.not_mapped_count+1
                            return {"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":str(rovi_projectx_id),
                                            "projectx_id_%s"%source:['not found match id'],"Dev_rovi_px_mapped":another_source_id_present_status["dev_rovi_px_mapped"],
                                            "another_%s_id_present"%source:another_source_id_present_status["another_source_id_present"],"Episode mapping":'Fail',"Comment":'Fail'}
                    else:
                        self.source_id_not_ingested_count=self.source_id_not_ingested_count+1
                        return {"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                                                   "projectx_id_%s"%source:'',"Episode mapping":'Fail',"Comment":'No Ingestion of source_id of episode'}

            except (httplib.BadStatusLine,urllib2.HTTPError,socket.error,URLError,Exception) as e:
                retry_count+=1
                print ("exception caught ................................................",type(e),source_id,rovi_id)
                print("\n")
                print ("Retrying.............",retry_count)
                if retry_count<=5:
                    self.checking_mapping_episodes(source_id,rovi_id,source_sm_id,rovi_series_id,rovi_projectx_id,source_projectx_id,source_projectx_id_sm,source,
                              token,projectx_mapping_api_,projectx_preprod_api,api_duplicate_checking,projectx_preprod_api_episodes)
                else:
                   retry_count=0

        print("\n")
        print("multiple mapped: ",self.multiple_mapped_count,"mapped :",self.mapped_count ,"Not mapped :", self.not_mapped_count,
              "rovi id not ingested :", self.rovi_id_not_ingested_count, "source Id not ingested : ",self.source_id_not_ingested_count,
                "both not ingested: ",self.both_source_not_ingested_count)

    #TODO: checking mapping movies for the given sources(ROVI, others)
    def checking_mapping_movies(self,source_id,rovi_id,movie_name,release_year,source_projectx_id,rovi_projectx_id
                               ,source,token,projectx_mapping_api_,projectx_preprod_api,api_duplicate_checking):
        #import pdb;pdb.set_trace()
        dev_rovi_px_mapped=[]
        dev_source_px_mapped=[]
        px_series_id=[]
        px_array=[]
        px_variant_id=[]
        #TODO: condition 1 , to check multiple projectx ids for a source
        if len(rovi_projectx_id)>1 or len(source_projectx_id)>1:
            self.multiple_mapped_count+=1
            status='Nil'
            comment=''
            if len(source_projectx_id)==1 and len(rovi_projectx_id)>1:
                if source_projectx_id in rovi_projectx_id:
                    status='Pass'
                    comment='Multiple ingestion for same content of rovi'
                else:
                    comment= 'Multiple ingestion for same content of rovi'
                    status='Fail'
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)==1:
                if rovi_projectx_id[0] in source_projectx_id:
                    status='Pass'
                    comment='Multiple ingestion for same content of %s'%source
                else:
                    comment= 'Fail:Multiple ingestion for same content of %s'%source
                    status='Fail'
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)>1:
                for x in source_projectx_id:
                    if x in rovi_projectx_id:
                        status='Pass'
                        comment='Multiple ingestion for same content of both sources'
                        break;
                    else:
                        comment= 'Fail:Multiple ingestion for same content of both sources'
                        status='Fail'
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)==0:
                comment='Fail:Multiple ingestion for same content of source and rovi_id not ingested'
                status='Fail'
            elif len(rovi_projectx_id)>1 and len(source_projectx_id)==0:
                comment= 'Fail:Multiple ingestion for same content of rovi and source_id not ingested'
                status='Fail'
            return {"%s_id"%source:source_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":rovi_projectx_id,
                               "projectx_id_%s"%source:source_projectx_id,"Comment":comment,"Result of mapping":status}

        #TODO: condition 2, to check both projectx ids are same or not
        elif len(rovi_projectx_id)==1 and len(source_projectx_id)==1:
            if rovi_projectx_id == source_projectx_id:
                self.mapped_count+=1
                return {"%s_id"%source:source_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                             "projectx_id_%s"%source:str(source_projectx_id[0]),"Comment":'Pass',"Result of mapping":'Pass'}

            else:
                self.not_mapped_count+=1
                #import pdb;pdb.set_trace()
                #TODO: to check the mapping of rovi_projectx_id from mapping API
                projectx_mapping_api=projectx_mapping_api_%rovi_projectx_id[0]
                data_mapped_resp=self.fetch_response_for_api(projectx_mapping_api,token)
                another_source_id_present_status=self.to_check_presence_different_source_id(data_mapped_resp,dev_rovi_px_mapped,source_id,source,'MO')
                #TODO: To check the mapping of source_projetx_id from mapping API
                projectx_mapping_api=projectx_mapping_api_%source_projectx_id[0]
                data_mapped_resp_source=self.fetch_response_for_api(projectx_mapping_api,token)
                another_rovi_id_present_status=self.to_check_presence_different_rovi_id(data_mapped_resp_source,dev_source_px_mapped,rovi_id,source,'MO')
                px_array=[source_projectx_id[0],rovi_projectx_id[0]]
                #TODO: checking variant parent id for PX_ids
                projectx_api=projectx_preprod_api%'{}'.format(",".join([str(i) for i in px_array]))
                projectx_api_response=self.fetch_response_for_api(projectx_api,token)
                variant_present=self.to_check_variant_parent_id(projectx_api_response,px_array,px_variant_id)

                return {"%s_id"%source:source_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                      "projectx_id_%s"%source:str(source_projectx_id[0]),"Dev_%s_px_mapped"%source:another_rovi_id_present_status["dev_source_px_mapped"],"Dev_rovi_px_mapped":another_source_id_present_status["dev_rovi_px_mapped"],
                                     "another_rovi_id_present":another_rovi_id_present_status["another_rovi_id_present"],"another_%s_id_present"%source:another_source_id_present_status["another_source_id_present"],"variant_present":variant_present,
                                                         "Comment":'Fail',"Result of mapping":'Fail'}

        #TODO: condition 3, to check rovi_id ingested
        elif len(source_projectx_id)==1 and not rovi_projectx_id:
            self.rovi_id_not_ingested_count+=1
            return {"%s_id"%source:source_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":'Nil',
                              "projectx_id_%s"%source:str(source_projectx_id),"Comment":'rovi_id not ingested',"Result of mapping":'Fail'}

        #TODO: condition 4, to check source id ingested and checking for another source_ids
        elif len(rovi_projectx_id)==1 and not source_projectx_id:
            try:
                retry_count=0
                #import pdb;pdb.set_trace()
                #TODO: to check duplicate id from source mapping API
                duplicate_api=api_duplicate_checking%(eval(source_id),source)
                data_resp_url=self.fetch_response_for_api(duplicate_api,token)
                if data_resp_url==[]:
                    self.source_id_not_ingested_count+=1
                    return {"%s_id"%source:source_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":str(rovi_projectx_id),
                                               "projectx_id_%s"%source:'Nil',"Comment":'source_id not ingested',"Result of mapping":'Fail'}
                else:
                    #TODO: taking source_px_id from duplicate API
                    for ll in data_resp_url:
                        if ll.get("projectxId") not in source_projectx_id:
                            source_projectx_id.append(ll.get("projectxId"))
                    if rovi_projectx_id[0] in source_projectx_id:
                        self.mapped_count+=1
                        return {"%s_id"%source:source_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":str(rovi_projectx_id),
                                             "projectx_id_%s"%source:str(source_projectx_id),"Comment":'Pass',"Result of mapping":'Pass'}
                    elif len(source_projectx_id)==1:
                        self.not_mapped_count+=1
                        #TODO: checking the mapping of rovi_projectx_id
                        projectx_mapping_api=projectx_mapping_api_%rovi_projectx_id[0]
                        data_mapped_resp=self.fetch_response_for_api(projectx_mapping_api,token)
                        another_source_id_present_status=self.to_check_presence_different_source_id(data_mapped_resp,dev_rovi_px_mapped,source_id,source,'MO')
                        #TODO: checking the mapping of source_projectx_id
                        projectx_mapping_api=projectx_mapping_api_%source_projectx_id[0]
                        data_mapped_resp_source=self.fetch_response_for_api(projectx_mapping_api,token)
                        another_rovi_id_present_status=to_check_presence_different_rovi_id(data_mapped_resp_source,dev_source_px_mapped,rovi_id,source,'MO')
                        #TODO : to check variant parent ids for Px_ids
                        px_array=[source_projectx_id[0],rovi_projectx_id[0]]
                        projectx_api=projectx_preprod_api%'{}'.format(",".join([str(i) for i in px_array]))
                        projectx_api_response=self.fetch_response_for_api(projectx_api,token)
                        variant_present=self.to_check_variant_parent_id(projectx_api_response,px_array,px_variant_id)

                        return {"%s_id"%source:source_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":str(rovi_projectx_id),
                                        "projectx_id_%s"%source:str(source_projectx_id),"Dev_%s_px_mapped"%source:another_rovi_id_present_status["dev_source_px_mapped"],"Dev_rovi_px_mapped":another_source_id_present_status["dev_rovi_px_mapped"],
                                        "another_rovi_id_present":another_rovi_id_present_status["another_rovi_id_present"],"another_%s_id_present"%source:another_source_id_present_status["another_source_id_present"],"variant_present":variant_present,
                                                                   "Comment":'Fail',"Result of mapping":'Fail',}
                    elif len(source_projectx_id)>1:
                        self.multiple_mapped_count=self.multiple_mapped_count+1
                        return {"%s_id"%source:source_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":str(rovi_projectx_id),
                                           "projectx_id_%s"%source:'Nil',"Comment":'multiple source_px_id from duplicate API',"Result of mapping":'Fail'}
                    else:
                        self.source_id_not_ingested_count=self.source_id_not_ingested_count+1
                        return {"%s_id"%source:source_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":str(rovi_projectx_id),
                                           "projectx_id_%s"%source:'Nil',"Comment":'Source_id not ingested',"Result of mapping":'Fail'}

            except (httplib.BadStatusLine,urllib2.HTTPError,socket.error,URLError) as e:
                print ("exception caught ..................................................",type(e),source_id,rovi_id)
                if retry_count<=5:
                    self.checking_mapping_movies(source_id,rovi_id,movie_name,release_year,source_projectx_id,rovi_projectx_id
                               ,source,token,projectx_mapping_api_,projectx_preprod_api,api_duplicate_checking)
                else:
                   retry_count=0

        #TODO: condition 5, to check both source id ingested
        elif len(source_projectx_id)==0 and len(rovi_projectx_id)==0:
            self.both_source_not_ingested_count+=1
            return {"%s_id"%source:source_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":'',"projectx_id_%s"%source:'',
                                                              "Comment":'both ids not ingested',"Result of mapping":'N.A'}

        print("multiple mapped: ",self.multiple_mapped_count ,"mapped :",self.mapped_count ,"Not mapped :", self.not_mapped_count,
              "rovi id not ingested :", self.rovi_id_not_ingested_count, "source Id not ingested : ",self.source_id_not_ingested_count, "both not ingested: ",self.both_source_not_ingested_count)


class duplicate_script_modules:

    def variant_parent_id_checking_px(self,data_resp_preprod,px_id):
        #import pdb;pdb.set_trace()
        self.comment_variant_parent_id_present=''
        self.comment_variant_parent_id=[]
        for oo in data_resp_preprod:
            if oo.get("variant_parent_id") is not None:
                if oo.get("variant_parent_id") not in px_id:
                    self.comment_variant_parent_id_present='Different'
                    self.comment_variant_parent_id.append({'variant_parent_id':str(oo.get("variant_parent_id"))})
                else:
                    self.comment_variant_parent_id_present='True'
                    self.comment_variant_parent_id.append({'variant_parent_id':str(oo.get("variant_parent_id"))})
            else:
                self.comment_variant_parent_id_present='Null'
        return [self.comment_variant_parent_id,self.comment_variant_parent_id_present]

    def variant_parent_id_checking_rovi(self,data_resp_preprod,rovi_id):
        self.comment_variant_parent_rovi_id=[]
        self.comment_variant_parent_id_present=''
        for oo in data_resp_preprod:
            if oo.get("variant_parent_id") is not None:
                if oo.get("variant_parent_id") not in rovi_id:
                    self.comment_variant_parent_id_present='Different'
                    self.comment_variant_parent_rovi_id.append({'variant_parent_id':str(oo.get("variant_parent_id"))})
                else:
                    self.comment_variant_parent_id_present='True'
                    self.comment_variant_parent_rovi_id.append({'variant_parent_id':str(oo.get("variant_parent_id"))})
            else:
                self.comment_variant_parent_id_present='Null'
        return [self.comment_variant_parent_rovi_id,self.comment_variant_parent_id_present]

    def duplicate_results(self,search_px_id__tmp,duplicate,source,show_type,token
                          ,projectx_preprod_api,projectx_mapping_api,beta_programs_api,credit_db_api):
        #import pdb;pdb.set_trace()
        rovi_id=[]
        rovi_mapping=[]
        source_mapping=[]
        guidebox_mapping=[]
        hulu_mapping=[]
        vudu_mapping=[]
        count_guidebox=0
        count_rovi=0
        count_hulu=0
        count_vudu=0
        count_source=0
        comment=''
        comment_variant_parent_id=[]
        comment_variant_parent_id_present=''

        for aa in search_px_id__tmp:
           projectx_mapping_api_=projectx_mapping_api%aa
           data_resp_mapped=lib_common_modules().fetch_response_for_api_(projectx_mapping_api_,token)
           for yy in data_resp_mapped:
               if yy.get("data_source")=='Rovi' and yy.get("type")=='Program':
                   count_rovi=1+count_rovi
                   rovi_id.append(eval(yy.get("source_id")))
                   rovi_mapping.append({str(aa):"Rovi_id:"+yy.get("source_id")})
               elif yy.get("data_source")==source and yy.get("type")=='Program' and yy.get('sub_type')==show_type:
                   count_source=1+count_source
                   source_mapping.append({str(aa):"id:"+yy.get("source_id")})
               elif yy.get("data_source")=='GuideBox' and yy.get("type")=='Program' and yy.get('sub_type')==show_type:
                   count_guidebox=1+count_guidebox
                   guidebox_mapping.append({str(aa):"Gb_id:"+yy.get("source_id")})
               elif yy.get("data_source")=='Hulu' and yy.get("type")=='Program' and yy.get('sub_type')==show_type:
                   count_hulu=1+count_hulu
                   hulu_mapping.append({str(aa):"hulu_id:"+yy.get("source_id")})
               elif yy.get("data_source")=='Vudu' and yy.get("type")=='Program' and yy.get('sub_type')==show_type:
                   count_vudu=1+count_vudu
                   vudu_mapping.append({str(aa):"vudu_id:"+yy.get("source_id")})
        if count_rovi>1:

            beta_api=beta_programs_api%'{}'.format(",".join([str(i) for i in rovi_id]))
            data_resp_beta=lib_common_modules().fetch_response_for_api_(beta_api,token)
            variant_result=self.variant_parent_id_checking_rovi(data_resp_beta,rovi_id)
            comment_variant_parent_id=variant_result[0]
            comment_variant_parent_id_present=variant_result[1]

            comment='Duplicate projectx ids found in search api and rovi has duplicate'
        else:
            comment='Duplicate ids found in search api'
        return {"comment":comment,"comment_variant_parent_id":comment_variant_parent_id,"comment_variant_parent_id_present":comment_variant_parent_id_present
               ,"count_rovi":count_rovi,"count_guidebox":count_guidebox,"count_vudu":count_vudu,"count_hulu":count_hulu,"count_source":count_source,"rovi_mapping":rovi_mapping,"search_px_id":search_px_id__tmp
               ,"source_mapping":source_mapping,"guidebox_mapping":guidebox_mapping,"duplicate":duplicate,"hulu_mapping":hulu_mapping,"vudu_mapping":vudu_mapping}


    def search_api_response_validation(self, data_resp_search, source, px_id, duplicate,show_type,token,
                       projectx_preprod_api,projectx_mapping_api,beta_programs_api,duplicate_api,credit_db_api):
        #import pdb;pdb.set_trace()
        search_px_id_tmp=[]
        search_px_id=[]
        search_px_id__tmp=[]
        search_px_id1=[]
        comment=''
        rovi_id=[]
        count_hulu=0
        count_vudu=0
        hulu_mapping=[]
        vudu_mapping=[]
        rovi_mapping=[]
        source_mapping=[]
        guidebox_mapping=[]
        count_guidebox=0
        count_rovi=0
        count_source=0
        comment_variant_parent_id=[]
        comment_variant_parent_id_present=''

        if duplicate=='False' or duplicate=='':
            for nn in data_resp_search.get('results'):
                #if nn.get("results"):
                for jj in nn.get("results"):
                    if show_type=='MO':
                        if jj.get("object").get("show_type")==show_type or jj.get("object").get("show_type")=='OT':
                            search_px_id.append(jj.get("object").get("id"))
                            search_px_id1=search_px_id1+search_px_id
                    else:
                        search_px_id.append(jj.get("object").get("id"))
                        search_px_id1=search_px_id1+search_px_id

                if search_px_id:
                    for mm in search_px_id:
                        if mm in px_id:
                            search_px_id_tmp.append(mm)
                search_px_id=[]
                if len(search_px_id_tmp)==1 or search_px_id_tmp==[]:
                    search_px_id_tmp=[]
                    duplicate='False'
                elif search_px_id_tmp!=search_px_id__tmp:
                    search_px_id__tmp=search_px_id__tmp+search_px_id_tmp
                    duplicate='True'
                else:
                    duplicate='True'

            if search_px_id1:
                if len(search_px_id__tmp)>1 and duplicate=='True':
                    return self.duplicate_results(search_px_id__tmp,duplicate,source,show_type,token
                                    ,projectx_preprod_api,projectx_mapping_api,beta_programs_api,credit_db_api)
                else:
                    comment='Duplicate ids not found in search api'
                    #import pdb;pdb.set_trace()
                    projectx_api=projectx_preprod_api%'{}'.format(",".join([str(i) for i in px_id]))
                    data_resp_projectx=lib_common_modules().fetch_response_for_api_(projectx_api,token)
                    variant_result=self.variant_parent_id_checking_px(data_resp_projectx,px_id)
                    comment_variant_parent_id=variant_result[0]
                    comment_variant_parent_id_present=variant_result[1]

            return {"comment":comment,"duplicate":duplicate,"comment_variant_parent_id":comment_variant_parent_id,
                     "comment_variant_parent_id_present":comment_variant_parent_id_present,"search_px_id":search_px_id__tmp
                    ,"count_rovi":count_rovi,"count_guidebox":count_guidebox,"count_vudu":count_vudu,"count_hulu":count_hulu,"count_source":count_source,"rovi_mapping":rovi_mapping
                    ,"source_mapping":source_mapping,"guidebox_mapping":guidebox_mapping,"hulu_mapping":hulu_mapping,"vudu_mapping":vudu_mapping}

    def search_api_call_response(self, title,projectx_preprod_search_api,domain_name,token):
        #import pdb;pdb.set_trace()
        next_page_url=""
        data_resp_search=dict()

        search_api=projectx_preprod_search_api%urllib2.quote(title)
        data_resp_search = lib_common_modules().fetch_response_for_api_(search_api,token)

        while data_resp_search.get("results"):
            ## TODO: We are taking only the last response. We should take all
            for nn in data_resp_search.get("results"):
                if nn.get("action_type")=="ott_search" and (nn.get("results")==[] or nn.get("results")):
                    next_page_url=nn.get("next_page_url")
                    if next_page_url is not None:
                        search_api=domain_name+next_page_url.replace(' ',"%20")
                        data_resp_search = lib_common_modules().fetch_response_for_api_(search_api,token)
                        return data_resp_search
                    else:
                        data_resp_search={"results":[]}
                else:
                    data_resp_search={"results":[]}


    class gb_link_id_extract:

        def getting_purchase_link_ids(self,data_GB_resp):
            #import pdb;pdb.set_trace()
            gb_purchase_web_link=''
            gb_purchase_web_source=''
            gb_purchase_web_id=[]

            for link in range(0,len(data_GB_resp.get("purchase_web_sources"))):#data_GB_resp.get("purchase_web_sources")
                gb_purchase_web_source=data_GB_resp.get("purchase_web_sources")[link].get("source").encode('utf-8')
                gb_purchase_web_link=data_GB_resp.get("purchase_web_sources")[link].get("link").encode('utf-8')
                if 'amazon_prime' in gb_purchase_web_source or 'amazon_buy' in gb_purchase_web_source:
                    gb_purchase_web_source='amazon'
                if 'netflix' in gb_purchase_web_source:
                    gb_purchase_web_source='netflixusa'
                if gb_purchase_web_source=='hbo':
                    gb_purchase_web_source='hbogo'
                if gb_purchase_web_source=='hbo_now':
                    gb_purchase_web_source='hbogo'
                if gb_purchase_web_source=='google_play':
                    gb_purchase_web_source='googleplay'
                if gb_purchase_web_source=='hulu_plus':
                    gb_purchase_web_source='hulu'
                if gb_purchase_web_source =='verizon_on_demand':
                    gb_purchase_web_source='verizon'
                if gb_purchase_web_source=='showtime_subscription':
                    gb_purchase_web_source='showtime'

                if 'vuduapp' in gb_purchase_web_link:
                    gb_purchase_web_id.append(re.findall("\w+.*?", gb_purchase_web_link)[-1:][0])
                try:
                    if '//itunes.apple.com/us/tv-season' in gb_purchase_web_link:
                        gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+", gb_purchase_web_link)[-2:-1][0]})
                except IndexError:
                    try:
                        gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+", gb_purchase_web_link)[1:-2][0]})
                    except IndexError:
                        gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+", gb_purchase_web_link)[0]})
                try:
                    if '//itunes.apple.com/us/movie' in gb_purchase_web_link:
                        gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+", gb_purchase_web_link)[0:-2][2:][1]})
                except IndexError:
                    try:
                        gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+", gb_purchase_web_link)[1:-2][1]})
                    except IndexError:
                        try:
                            gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+",gb_purchase_web_link)[0:-2][1:2][0]})
                        except IndexError:
                            gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+", gb_purchase_web_link)[0]})
                if '//www.amazon.com/gp' in gb_purchase_web_link:
                    gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\w+\d+\w+", gb_purchase_web_link)[0]})
                if '//click.linksynergy.com/' in gb_purchase_web_link:
                    gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+.*?", gb_purchase_web_link)[-1:][0]})
                if gb_purchase_web_id:
                    return gb_purchase_web_id
                    break


        def getting_link_subscription_ids(self,data_GB_resp):

            gb_subscription_web_source=''
            gb_subscription_web_id=[]
            #import pdb;pdb.set_trace()
            for link in range(0,len(data_GB_resp.get("subscription_web_sources"))):
                gb_subscription_web_source=data_GB_resp.get("subscription_web_sources")[link].get('source').encode('utf-8')
                gb_subscription_web_link=data_GB_resp.get("subscription_web_sources")[link].get('link').encode('utf-8')

                if 'amazon_prime' in gb_subscription_web_source or 'amazon_buy' in gb_subscription_web_source:
                    gb_subscription_web_source='amazon'
                if 'netflix' in gb_subscription_web_source:
                    gb_subscription_web_source='netflixusa'
                if gb_subscription_web_source=='hbo':
                    gb_subscription_web_source='hbogo'
                if gb_subscription_web_source=='hbo_now':
                    gb_subscription_web_source='hbogo'
                if gb_subscription_web_source=='google_play':
                    gb_subscription_web_source='googleplay'
                if gb_subscription_web_source=='hulu_plus':
                    gb_subscription_web_source='hulu'
                if gb_subscription_web_source =='verizon_on_demand':
                    gb_subscription_web_source='verizon'
                if gb_subscription_web_source=='showtime_subscription':
                    gb_subscription_web_source='showtime'

                if 'vuduapp' in gb_subscription_web_link:
                    gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?", gb_subscription_web_link)[-1:][0]})

                if 'aiv://aiv/play' in gb_subscription_web_link:
                    gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?", gb_subscription_web_link)[-1:][0]})
                if '//itunes.apple.com/us/movie' in gb_subscription_web_link:
                    gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\d+", gb_subscription_web_link)[0]})
                if '//www.amazon.com/gp' in gb_subscription_web_link:
                    gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+\d+\w+", gb_subscription_web_link)[0]})
                if "www.cbs.com/shows" in gb_subscription_web_link:
                    try:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+\d+\w+", gb_subscription_web_link)[0]})
                    except IndexError:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+", gb_subscription_web_link)[7]})
                if '//click.linksynergy.com/' in gb_subscription_web_link:
                    gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\d+", gb_subscription_web_link)[-1:][0]})
                if 'play.google' in gb_subscription_web_link:
                    try:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+-\w+.*?",gb_subscription_web_link)[0]})
                    except IndexError:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?", gb_subscription_web_link)[-1:][0]})
                if '//play.hbonow.com/' in gb_subscription_web_link:
                    try:
                        a10=re.findall("\w+.*?", gb_subscription_web_link)
                        gb_subscription_web_id.append({gb_subscription_web_source:':'.join(map(str, [a10[i] for i in range(5,9)]))  })
                    except IndexError:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", gb_subscription_web_link)[0]})
                if 'netflix.com' in gb_subscription_web_link:
                    gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?",gb_subscription_web_link)[-1:][0]})
                if 'http://www.showtime.com/#' in gb_subscription_web_link:
                    gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?",gb_subscription_web_link)[-1:][0]})
                if 'http://www.hulu.com' in gb_subscription_web_link:
                    try:
                        a14=re.findall("\w+.*?",gb_subscription_web_link)[-5:]
                        gb_subscription_web_id.append({gb_subscription_web_source:'-'.join(map(str,[a14[i] for i in range(0,len(a14))]))})
                    except IndexError:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?",gb_subscription_web_link)[-1:][0] })
                if gb_subscription_web_id:
                    return gb_subscription_web_id
                    break

        def getting_link_tveverywhere_ids(self,data_GB_resp):

            gb_tv_everywhere_web_source=''
            gb_tv_everywhere_web_link=''
            gb_tv_everywhere_web_id=[]


            for link in range(0,len(data_GB_resp.get("tv_everywhere_web_sources"))):
                gb_tv_everywhere_web_source=data_GB_resp.get("tv_everywhere_web_sources")[link].get('source').encode('utf-8')
                gb_tv_everywhere_web_link=data_GB_resp.get("tv_everywhere_web_sources")[link].get('link').encode('utf-8')
                if 'amazon_prime' in gb_tv_everywhere_web_source or 'amazon_buy' in gb_tv_everywhere_web_source:
                    gb_tv_everywhere_web_source='amazon'
                if 'netflix' in gb_tv_everywhere_web_source:
                    gb_tv_everywhere_web_source='netflixusa'
                if gb_tv_everywhere_web_source=='hbo':
                    gb_tv_everywhere_web_source='hbogo'
                if gb_tv_everywhere_web_source=='hbo_now':
                    gb_tv_everywhere_web_source='hbogo'
                if gb_tv_everywhere_web_source=='starz_tveverywhere':
                    gb_tv_everywhere_web_source='starz'
                if "starz://play" in gb_tv_everywhere_web_link or "//www.starz.com/" in gb_tv_everywhere_web_link:
                    gb_tv_everywhere_web_id.append({gb_tv_everywhere_web_source:re.findall("\w+.*?", gb_tv_everywhere_web_link)[-1:][0]})
                if "www.cbs.com/shows" in gb_tv_everywhere_web_link:
                    try:
                        gb_tv_everywhere_web_id.append({gb_tv_everywhere_web_source:re.findall("\w+\d+\w+", gb_tv_everywhere_web_link)[0]})
                    except IndexError:
                        gb_tv_everywhere_web_id.append({gb_tv_everywhere_web_source:re.findall("\w+", gb_tv_everywhere_web_link)[7]})
                if gb_tv_everywhere_web_id:
                    return gb_tv_everywhere_web_id
                    break

        def getting_link_free_web_ids(self,data_GB_resp):

            gb_free_web_source=''
            gb_free_web_link=''
            gb_free_web_id=[]

            for link in range(0,len(data_GB_resp.get("free_web_sources"))):
                gb_free_web_source=data_GB_resp.get("free_web_sources")[link].get('source').encode('utf-8')
                gb_free_web_link=data_GB_resp.get("free_web_sources")[link].get('link').encode('utf-8')
                if 'amazon_prime' in gb_free_web_source or 'amazon_buy' in gb_free_web_source:
                    gb_free_web_source='amazon'
                if 'https://www.vudu.com' in gb_free_web_source:
                    gb_free_web_source='vudu'
                if '//www.amazon.com/gp' in gb_free_web_link:
                    gb_free_web_id.append({gb_free_web_source:re.findall("\w+\d+\w+", gb_free_web_link)[0]})
                if 'https://www.vudu.com' in gb_free_web_link:
                    gb_free_web_id.append({gb_free_web_source:re.findall("\w+.*?",gb_free_web_link)[-3:][0]})
                if "//play.hbogo.com/feature" in gb_free_web_link:
                    try:
                        gb_free_web_id.append({gb_free_web_source:re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", gb_free_web_link)[0]})
                    except IndexError:
                        try:
                            a3=re.findall("\w+.*?", gb_free_web_link)
                            gb_free_web_id.append({gb_free_web_source:':'.join(map(str, [a3[i] for i in range(5,9)]))})
                        except IndexError:
                            gb_free_web_id.append({gb_free_web_source:re.findall("\w+.*?",gb_free_web_link)[-1:][0]})
                if "http://www.nbc.com" in gb_free_web_link:
                    gb_free_web_id.append({gb_free_web_source:re.findall("\d+",gb_free_web_link)[0]})
                if gb_free_web_id:
                    return gb_free_web_id
                    break
    class validation:

        def __init__(self):
            self.px_id1_alias=[]
            self.px_id1_alias_comment=''
            self.px_id2_alias=[]
            self.px_id2_alias_comment=''
            self.comment=''
            self.px_id2_credits_null='False'
            self.px_id1_credits_null='False'

            self.px_id1=0
            self.px_id1_show_type=''
            self.px_id1_variant_parent_id=0
            self.px_id1_is_group_language_primary=''
            self.px_id1_long_title=''
            self.px_id1_original_title=''
            self.px_id1_run_time=''
            self.px_id1_release_year=''
            self.px_id1_record_language=''
            self.px_id1_aliases=[]
            self.px_id1_credits=[]
            self.px_id1_db_credit_present='False'

            self.px_id2=0
            self.px_id2_show_type=''
            self.px_id2_variant_parent_id=0
            self.px_id2_is_group_language_primary=''
            self.px_id2_long_title=''
            self.px_id2_original_title=''
            self.px_id2_run_time=''
            self.px_id2_release_year=''
            self.px_id2_record_language=''
            self.px_id2_aliases=[]
            self.px_id2_credits=[]
            self.px_id2_db_credit_present='False'

            self.long_title_match=''
            self.original_title_match=''
            self.runtime_match=''
            self.release_year_match=''
            self.alias_title_match=''

        def cleanup(self):
            self.px_id1_alias=[]
            self.px_id1_alias_comment=''
            self.px_id2_alias=[]
            self.px_id2_alias_comment=''
            self.comment=''
            self.px_id2_credits_null='True'
            self.px_id1_credits_null='True'

            self.px_id1=0
            self.px_id1_show_type=''
            self.px_id1_variant_parent_id=0
            self.px_id1_is_group_language_primary=''
            self.px_id1_long_title=''
            self.px_id1_original_title=''
            self.px_id1_run_time=''
            self.px_id1_release_year=''
            self.px_id1_record_language=''
            self.px_id1_aliases=[]
            self.px_id1_credits=[]
            self.px_id1_db_credit_present='False'

            self.px_id2=0
            self.px_id2_show_type=''
            self.px_id2_variant_parent_id=0
            self.px_id2_is_group_language_primary=''
            self.px_id2_long_title=''
            self.px_id2_original_title=''
            self.px_id2_run_time=''
            self.px_id2_release_year=''
            self.px_id2_record_language=''
            self.px_id2_aliases=[]
            self.px_id2_credits=[]
            self.px_id2_db_credit_present='False'

            self.long_title_match=''
            self.original_title_match=''
            self.runtime_match=''
            self.release_year_match=''
            self.alias_title_match=''

        def checking_same_program(self,duplicate_id,projectx_preprod_api,
                        credit_db_api,source,token):
            #import pdb;pdb.set_trace()
            try:
                projectx_api_preprod=projectx_preprod_api%'{}'.format(",".join([str(i) for i in duplicate_id]))
                data_resp_ids=lib_common_modules().fetch_response_for_api_(projectx_api_preprod,token)

                self.px_id1=data_resp_ids[0].get("id")
                self.px_id1_show_type=data_resp_ids[0].get("show_type").encode('utf-8')
                self.px_id1_variant_parent_id=data_resp_ids[0].get("variant_parent_id")
                self.px_id1_is_group_language_primary=data_resp_ids[0].get("is_group_language_primary")
                self.px_id1_long_title=unidecode.unidecode(data_resp_ids[0].get("long_title"))
                self.px_id1_original_title=unidecode.unidecode(data_resp_ids[0].get("original_title"))
                self.px_id1_run_time=data_resp_ids[0].get("run_time")
                self.px_id1_release_year=data_resp_ids[0].get("release_year")
                self.px_id1_record_language=data_resp_ids[0].get("record_language").encode('utf-8')
                self.px_id1_aliases=data_resp_ids[0].get("aliases")
                self.px_id1_credits=data_resp_ids[0].get("credits")

                if self.px_id1_credits:
                    self.px_id1_credits_null='False'
                else:
                    credit_db_api_=credit_db_api%self.px_id1
                    credit_resp_db=lib_common_modules().fetch_response_for_api_(credit_db_api_,token)
                    if credit_resp_db:
                        self.px_id1_db_credit_present='True'

                if self.px_id1_aliases!=[] and self.px_id1_aliases is not None:
                    for alias in self.px_id1_aliases:
                        if alias.get("source_name")==source and (alias.get("language")=='ENG' or alias.get("language")=='en'):
                            self.px_id1_alias.append(unidecode.unidecode(alias.get("alias")))
                        if alias.get("source_name")=='Rovi' and alias.get("type")=='alias_title' and (alias.get("language")=='ENG' or alias.get("language")=='en'):
                            self.px_id1_alias.append(unidecode.unidecode(alias.get("alias")))
                else:
                    self.px_id1_alias_comment='Null'

                self.px_id2=data_resp_ids[1].get("id")
                self.px_id2_show_type=data_resp_ids[1].get("show_type").encode('utf-8')
                self.px_id2_variant_parent_id=data_resp_ids[1].get("variant_parent_id")
                self.px_id2_is_group_language_primary=data_resp_ids[1].get("is_group_language_primary")
                self.px_id2_long_title=unidecode.unidecode(data_resp_ids[1].get("long_title"))
                self.px_id2_original_title=unidecode.unidecode(data_resp_ids[1].get("original_title"))
                self.px_id2_run_time=data_resp_ids[1].get("run_time")
                self.px_id2_release_year=data_resp_ids[1].get("release_year")
                self.px_id2_record_language=data_resp_ids[1].get("record_language").encode('utf-8')
                self.px_id2_aliases=data_resp_ids[1].get("aliases")
                self.px_id2_credits=data_resp_ids[1].get("credits")

                if self.px_id2_credits:
                    self.px_id2_credits_null='False'
                else:
                    credit_db_api_=credit_db_api%self.px_id2
                    credit_resp_db=lib_common_modules().fetch_response_for_api_(credit_db_api_,token)
                    if credit_resp_db:
                        self.px_id2_db_credit_present='True'

                if self.px_id2_aliases!=[] and self.px_id2_aliases is not None:
                    for alias in self.px_id2_aliases:
                        if alias.get("source_name")==source and alias.get("language")=='ENG':
                            self.px_id2_alias.append(unidecode.unidecode(alias.get("alias")))
                        if alias.get("source_name")=='Rovi' and alias.get("type")=='alias_title' and alias.get("language")=='ENG':
                            self.px_id2_alias.append(unidecode.unidecode(alias.get("alias")))
                else:
                    self.px_id2_alias_comment='Null'

                if self.px_id1_long_title.upper() in self.px_id2_long_title.upper():
                    self.long_title_match='True'
                else:
                    if self.px_id2_long_title.upper() in self.px_id1_long_title.upper():
                        self.long_title_match='True'
                    else:
                        ratio_title=fuzz.ratio(self.px_id1_long_title.upper(),self.px_id2_long_title.upper())
                        if ratio_title >=70:
                            self.long_title_match='True'
                        else:
                            self.long_title_match='False'

                if self.px_id1_original_title.upper() in self.px_id2_original_title.upper():
                    self.original_title_match='True'
                else:
                    if self.px_id2_original_title.upper() in self.px_id1_original_title.upper():
                        self.original_title_match='True'
                    else:
                        ratio_title=fuzz.ratio(self.px_id1_original_title.upper(),self.px_id2_original_title.upper())
                        if ratio_title >=70:
                            self.original_title_match='True'
                        else:
                            self.original_title_match='False'

                if self.px_id1_run_time==self.px_id2_run_time:
                    self.runtime_match='True'
                else:
                    self.runtime_match='False'

                if self.px_id1_release_year is not None and self.px_id2_release_year is not None:
                    if self.px_id1_release_year==self.px_id2_release_year:
                        self.release_year_match='True'
                    else:
                        r_y=self.px_id1_release_year
                        r_y=r_y+1
                        if r_y==self.px_id2_release_year:
                            self.release_year_match='True'
                        else:
                            r_y=r_y-2
                            if r_y==self.px_id2_release_year:
                                self.release_year_match='True'
                            else:
                                self.release_year_match='False'

                if self.px_id1_aliases and self.px_id2_aliases:
                    for alias in self.px_id1_alias:
                        if alias in self.px_id2_long_title:
                            self.alias_title_match='True'
                            break
                        elif self.px_id2_long_title in alias:
                            self.alias_title_match='True'
                            break
                        elif self.px_id2_original_title in alias:
                            self.alias_title_match='True'
                            break
                        elif alias in self.px_id2_original_title:
                            self.alias_title_match='True'
                            break
                        else:
                            ratio_title=fuzz.ratio(self.px_id2_long_title.upper(),alias.upper())
                            if ratio_title >=70:
                                self.alias_title_match='True'
                                break
                            else:
                                ratio_title=fuzz.ratio(self.px_id2_original_title.upper(),alias.upper())
                                if ratio_title >=70:
                                    self.alias_title_match='True'
                                    break
                                else:
                                    self.alias_title_match='False'

                return {"px_id1":self.px_id1,"px_id1_show_type":self.px_id1_show_type,"px_id1_variant_parent_id":self.px_id1_variant_parent_id,
                      "px_id1_is_group_language_primary":self.px_id1_is_group_language_primary,"px_id1_record_language":self.px_id1_record_language,"px_id2":self.px_id2,
                    "px_id2_show_type":self.px_id2_show_type,"px_id2_variant_parent_id":self.px_id2_variant_parent_id,"px_id2_is_group_language_primary":self.px_id2_is_group_language_primary
                    ,"px_id2_record_language":self.px_id2_record_language,"px_id1_credits_null":self.px_id1_credits_null,"px_id1_db_credit_present":self.px_id1_db_credit_present,
                    "px_id2_credits_null":self.px_id2_credits_null,"px_id2_db_credit_present":self.px_id2_db_credit_present,"long_title_match":self.long_title_match,
                    "original_title_match":self.original_title_match,"runtime_match":self.runtime_match,"release_year_match":self.release_year_match,"alias_title_match":
                     self.alias_title_match,"comment":self.comment}

            except Exception as e:
                print ("exception caught (checking_same_program).................",type(e),[self.px_id1,self.px_id2])
                print ("\n")
                print ("Retrying.............")
                print ("\n")
                self.checking_same_program(duplicate_id,projectx_preprod_api,
                        credit_db_api,source,token)

        def meta_data_validation(self,duplicate_ids,projectx_preprod_api,
                        credit_db_api,source,token):
            #import pdb;pdb.set_trace()
            self.cleanup()
            print("Checking same program of duplicate cases..............")
            return self.checking_same_program(duplicate_ids,projectx_preprod_api,
                        credit_db_api,source,token)


class ott_meta_data_validation_modules:

    retry_count=0

    def init(self):
        self.px_long_title=''
        self.px_video_link=[]
        self.px_credit_present='False'
        self.px_video_link_present='False'
        self.px_original_title=''
        self.px_episode_title=''
        self.px_run_time=0
        self.px_release_year=0
        self.px_record_language=''
        self.px_description=''
        self.px_images_details=[]
        self.px_genres=[]
        self.px_aliases=[]
        self.launch_id=[]
        self.px_response='Null'
        self.px_credits=[]
        self.px_season_number=0
        self.px_episode_number=0

    def px_image_details(self,px_show_id,data_images_px,show_type,projectx_programs_api,token):
        #import pdb;pdb.set_trace()
        px_images_details=[]
        for images in data_images_px:
            px_images_details.append({'url':images.get("url")})
        if show_type=='SE' and px_images_details==[]:
            projectx_api=projectx_programs_api%px_show_id
            data_px_images=lib_common_modules().fetch_response_for_api_(projectx_api,token)
            for images in data_px_images:
                px_images_details.append({'url':images.get("url")})
        return px_images_details

    def getting_px_credits(self,data_credits):
        #import pdb;pdb.set_trace()
        px_credits=[]

        for credits in data_credits:
            px_credits.append(unidecode.unidecode(pinyin.get(credits.get("full_credit_name"))))
        return px_credits

    def px_aliases_(self,data_aliases,source):
        #import pdb;pdb.set_trace()
        px_aliases=[]
        for aliases in data_aliases:
            if aliases.get("type")=='alias' and aliases.get("source_name")==source:
                px_aliases.append(unidecode.unidecode(pinyin.get(aliases.get("alias"))))
        return px_aliases

    #TODO: to get meta details of projectx ids
    def getting_projectx_details(self,projectx_id,show_type,source,thread_name,projectx_programs_api,token):
        #import pdb;pdb.set_trace()
        self.init()
        try:
            projectx_api=projectx_programs_api%projectx_id
            data_px_resp=lib_common_modules().fetch_response_for_api_(projectx_api,token)
            if data_px_resp!=[]:

                for data in data_px_resp:
                    if data.get("long_title") is not None and data.get("long_title")!="":
                        self.px_long_title=unidecode.unidecode(pinyin.get(data.get("long_title")))
                    if data.get("original_title") is not None and data.get("original_title")!="":
                        self.px_original_title=unidecode.unidecode(pinyin.get(data.get("original_title")))
                    if data.get("original_episode_title")!="":
                        self.px_episode_title=unidecode.unidecode(pinyin.get(data.get("original_episode_title")))
                    elif data.get("episode_title")!="":
                        px_episode_title=unidecode.unidecode(pinyin.get(data.get("episode_title")))

                    self.px_record_language= data.get("record_language")
                    self.px_release_year=data.get("release_year")
                    self.px_run_time=data.get("run_time")
                    px_show_id=data.get("series_id")
                    try:
                        self.px_description=pinyin.get(data.get("description")[0].get("program_description").lower().replace("\\",""))
                    except Exception:
                        return self.px_description

                    if data.get("genres"):
                        for genres in data.get("genres"):
                            self.px_genres.append(genres.lower())

                    if data.get("aliases"):
                        self.px_aliases=self.px_aliases_(data.get("aliases"),source)
                    try:
                        self.px_season_number=data.get("episode_season_number")
                    except Exception:
                        return self.px_season_number
                    #import pdb;pdb.set_trace()
                    try:
                        self.px_episode_number= data.get("episode_season_sequence")
                    except Exception:
                        return self.px_episode_number

                    if data.get("credits"):
                        self.px_credits=self.getting_px_credits(data.get("credits"))
                        self.px_credit_present='True'

                    self.px_video_link= data.get("videos")
                    if self.px_video_link:
                        self.px_video_link_present='True'
                        for linkid in self.px_video_link:
                            #if linkid.get("source_id")=='hulu':
                            self.launch_id.append(linkid.get("launch_id"))

                    if data.get("images"):
                        self.px_images_details=self.px_image_details(px_show_id,data.get("images")
                                              ,show_type,projectx_programs_api,token)

                    return {"px_credits":self.px_credits,"px_credit_present":self.px_credit_present,"px_long_title":self.px_long_title,"px_episode_title":self.px_episode_title,
                           "px_original_title":self.px_original_title,"px_description":self.px_description,"px_genres":self.px_genres,"px_aliases":self.px_aliases,
                            "px_release_year":self.px_release_year,"px_run_time":self.px_run_time,"px_season_number":self.px_season_number,"px_episode_number":self.px_episode_number,
                            "px_video_link_present":self.px_video_link_present,"px_images_details":self.px_images_details,"launch_id":self.launch_id}
            else:
                return self.px_response

        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError,) as e:
            self.retry_count+=1
            print ("exception caught getting_projectx_details func..................",type(e),projectx_id,show_type,source,thread_name)
            print ("\n")
            print ("Retrying.............",self.retry_count)
            if self.retry_count<=5:
                self.getting_projectx_details(projectx_id,show_type,source,thread_name,projectx_programs_api,token)
            else:
                self.retry_count=0

    #TODO: to get Px_id from mapping DB
    def getting_mapped_px_id(self,_id,show_type,source,px_mappingdb_cur):
        #import pdb;pdb.set_trace()
        try:
            px_id=[]
            any_source_flag='False'
            source_flag='False'
            source_map=[]
            query="select projectxId from projectx_mapping where data_source=%s and sourceId =%s and sub_type=%s"
            px_mappingdb_cur.execute(query,(source,_id,show_type))
            data_resp_mapping=px_mappingdb_cur.fetchall()

            for data in data_resp_mapping:
                px_id.append(data[0])

            if px_id:
                #import pdb;pdb.set_trace()
                query="select count(*) from projectx_mapping where projectxId =%s"
                px_mappingdb_cur.execute(query,(px_id[0],))
                data_resp_px_mapping=px_mappingdb_cur.fetchall()
                for resp in data_resp_px_mapping:
                    if int(resp[0])>1:
                        any_source_flag='True(Rovi+others)'
                    else:
                        source_flag='True'
                if  source_flag=='True' and any_source_flag=='False':
                    return (px_id[0],_id,source_flag)
                elif source_flag=='False' and any_source_flag=='True(Rovi+others)':
                    return (px_id[0],_id,any_source_flag)
                elif source_flag=='True' and any_source_flag=='True(Rovi+others)':
                    return (px_id[0],_id,any_source_flag)
            else:
                return (px_id,_id,any_source_flag)
            px_mappingdb_cur.close()
        except (Exception,MySQLdb.Error, MySQLdb.Warning,socket.error,RuntimeError) as e:
            self.retry_count+=1
            print ("exception caught getting_mapped_px_id.................",type(e),_id,source,show_type)
            print ("\n")
            print ("Retrying.............",self.retry_count)
            if self.retry_count<=5:
                self.getting_mapped_px_id(_id,show_type,source,px_mappingdb_cur)
            else:
                self.retry_count=0

    #TODO: to get mapped PX_id from the mapping API
    def getting_mapped_px_id_mapping_api_vudu(self,_id,source_mapping_api,projectx_mapping_api,show_type,source,token):
        try:
            #import pdb;pdb.set_trace()
            px_id=[]
            source_map=[]
            any_source_flag='False'
            source_flag='False'

            #import pdb;pdb.set_trace()
            source_mapping_api_resp=source_mapping_api%(eval(_id),source,show_type)
            data_resp_mapping=lib_common_modules().fetch_response_for_api_(source_mapping_api_resp,token)

            for data in data_resp_mapping:
                if data.get("data_source")==source and data.get("type")=='Program' and data.get("sub_type")==show_type:
                    px_id.append(data.get("projectx_id"))
            print("\n")
            print ({"px_id":px_id})
            if px_id:
                #import pdb;pdb.set_trace()
                px_mapping_api=projectx_mapping_api%px_id[0]
                data_resp_px_mapping=lib_common_modules().fetch_response_for_api_(px_mapping_api,token)

                for resp in data_resp_px_mapping:
                    if (resp.get("data_source")=='Rovi' or resp.get("data_source")=='GuideBox' or resp.get("data_source")=='Hulu') and resp.get("type")=='Program':
                        source_map.append({str(resp.get("data_source")):resp.get("source_id")})
                        any_source_flag='True(Rovi+others)'
                    elif resp.get("data_source")==source and resp.get("type")=='Program' and resp.get("sub_type")==show_type:
                        source_map.append({str(resp.get("data_source")):resp.get("source_id")})
                        source_flag='True'

                # separate PX_ids with flag which is only mapped to source and mapped to others
                if  source_flag=='True' and any_source_flag=='False':
                    return {"px_id":px_id[0],"source_id":_id,"source_flag":source_flag,"source_map":source_map}
                elif source_flag=='False' and any_source_flag=='True(Rovi+others)':
                    return {"px_id":px_id[0],"source_id":_id,"source_flag":any_source_flag,"source_map":source_map}
                elif source_flag=='True' and any_source_flag=='True(Rovi+others)':
                    return {"px_id":px_id[0],"source_id":_id,"source_flag":any_source_flag,"source_map":source_map}
            else:
                return {"px_id":px_id,"source_id":_id,"source_flag":any_source_flag,"source_map":source_map}
        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError) as e:
            #import pdb;pdb.set_trace()
            self.retry_count+=1
            if self.retry_count<=5:
                print ("exception caught in getting_mapped_px_id func.........................",type(e),_id,source,show_type)
                print ("\n")
                print ("Retrying.............",self.retry_count)
                print ("\n")
                self.getting_mapped_px_id_mapping_api_vudu(_id,source_mapping_api,projectx_mapping_api,show_type,source,token)
            else:
                self.retry_count=0

    #TODO: to get mapped PX_id from the mapping API
    def getting_mapped_px_id_mapping_api_hulu(self,_id,source_mapping_api,projectx_mapping_api,show_type,source,token):
        try:
            #import pdb;pdb.set_trace()
            px_id=[]
            source_map=[]
            any_source_flag='False'
            source_flag='False'

            #import pdb;pdb.set_trace()
            source_mapping_api_resp=source_mapping_api%(eval(_id),source,show_type)
            data_resp_mapping=lib_common_modules().fetch_response_for_api_(source_mapping_api_resp,token)

            for data in data_resp_mapping:
                if data.get("data_source")==source and data.get("type")=='Program' and data.get("sub_type")==show_type:
                    px_id.append(data.get("projectx_id"))
            print("\n")
            print ({"px_id":px_id})
            if px_id:
                #import pdb;pdb.set_trace()
                px_mapping_api=projectx_mapping_api%px_id[0]
                data_resp_px_mapping=lib_common_modules().fetch_response_for_api_(px_mapping_api,token)

                for resp in data_resp_px_mapping:
                    if (resp.get("data_source")=='Rovi' or resp.get("data_source")=='GuideBox' or resp.get("data_source")=='Vudu') and resp.get("type")=='Program':
                        source_map.append({str(resp.get("data_source")):resp.get("source_id")})
                        any_source_flag='True(Rovi+others)'
                    elif resp.get("data_source")==source and resp.get("type")=='Program' and resp.get("sub_type")==show_type:
                        source_map.append({str(resp.get("data_source")):resp.get("source_id")})
                        source_flag='True'

                # separate PX_ids with flag which is only mapped to source and mapped to others
                if  source_flag=='True' and any_source_flag=='False':
                    return {"px_id":px_id[0],"source_id":_id,"source_flag":source_flag,"source_map":source_map}
                elif source_flag=='False' and any_source_flag=='True(Rovi+others)':
                    return {"px_id":px_id[0],"source_id":_id,"source_flag":any_source_flag,"source_map":source_map}
                elif source_flag=='True' and any_source_flag=='True(Rovi+others)':
                    return {"px_id":px_id[0],"source_id":_id,"source_flag":any_source_flag,"source_map":source_map}
            else:
                return {"px_id":px_id,"source_id":_id,"source_flag":any_source_flag,"source_map":source_map}
        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError) as e:
            #import pdb;pdb.set_trace()
            self.retry_count+=1
            if self.retry_count<=5:
                print ("exception caught in getting_mapped_px_id func.........................",type(e),_id,source,show_type)
                print ("\n")
                print ("Retrying.............",self.retry_count)
                print ("\n")
                self.getting_mapped_px_id_mapping_api_hulu(_id,source_mapping_api,projectx_mapping_api,show_type,source,token)
            else:
                self.retry_count=0
    #TODO: to get mapped px_id which is only mapped to requested sources
    def getting_mapped_px_id_mapping_api(self,_id,source_mapping_api,projectx_mapping_api,show_type,source,token):
        try:
            #import pdb;pdb.set_trace()
            px_id=[]
            source_map=[]
            any_source_flag='False'
            source_flag='False'

            #import pdb;pdb.set_trace()
            source_mapping_api_resp=source_mapping_api%(_id,source,show_type)
            data_resp_mapping=lib_common_modules().fetch_response_for_api_(source_mapping_api_resp,token)

            for data in data_resp_mapping:
                if data.get("data_source")==source and data.get("type")=='Program' and data.get("sub_type")==show_type:
                    px_id.append(data.get("projectx_id"))
            print("\n")
            print ({"px_id":px_id})
            if px_id:
                #import pdb;pdb.set_trace()
                px_mapping_api=projectx_mapping_api%px_id[0]
                data_resp_px_mapping=lib_common_modules().fetch_response_for_api_(px_mapping_api,token)

                for resp in data_resp_px_mapping:
                    if resp.get("data_source")!=source and resp.get("type")=='Program':
                        source_map.append({str(resp.get("data_source")):resp.get("source_id")})
                        any_source_flag='True(Rovi+others)'
                    elif resp.get("data_source")==source and resp.get("type")=='Program' and resp.get("sub_type")==show_type:
                        source_map.append({str(resp.get("data_source")):resp.get("source_id")})
                        source_flag='True'

                # separate PX_ids with flag which is only mapped to source and mapped to others
                if  source_flag=='True' and any_source_flag=='False':
                    return {"px_id":px_id[0],"source_id":_id,"source_flag":source_flag,"source_map":source_map}
                elif source_flag=='False' and any_source_flag=='True(Rovi+others)':
                    return {"px_id":px_id[0],"source_id":_id,"source_flag":any_source_flag,"source_map":source_map}
                elif source_flag=='True' and any_source_flag=='True(Rovi+others)':
                    return {"px_id":px_id[0],"source_id":_id,"source_flag":any_source_flag,"source_map":source_map}
            else:
                return {"px_id":px_id,"source_id":_id,"source_flag":any_source_flag,"source_map":source_map}
        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError) as e:
            #import pdb;pdb.set_trace()
            self.retry_count+=1
            if self.retry_count<=5:
                print ("exception caught in getting_mapped_px_id_mapping_api func..........."
                             ,type(e),_id,source,show_type)
                print ("\n")
                print ("Retrying.............",self.retry_count)
                print ("\n")
                self.getting_mapped_px_id_mapping_api(_id,source_mapping_api,
                    projectx_mapping_api,show_type,source,token)
            else:
                self.retry_count=0

    #TODO: to check images population
    def images_validation(self,source_images,projectx_images):
        #import pdb;pdb.set_trace()
        image_url_match=''
        image_url_missing=''
        wrong_url=[]

        if projectx_images!='Null':
            source_images_url=source_images["source_images_details"]
            projectx_images=projectx_images["px_images_details"]

            if source_images_url!=[] and projectx_images!=[]:
                for images in projectx_images:
                    if images in source_images_url:
                        image_url_match="True"
                        break
                    else:
                        image_url_missing="True"
                        wrong_url.append(images.get("url"))
            elif projectx_images==[] and source_images_url!=[]:
                image_url_missing="True"

        return (image_url_missing,wrong_url)

    def ott_validation(self,projectx_details,source_id):
 
        comment_link='Null'
        #import pdb;pdb.set_trace()
        try:
            if projectx_details["launch_id"]:
                if str(source_id) in projectx_details["launch_id"]:
                    comment_link='Present'
                else:
                    comment_link='Not_Present'
                return comment_link
            else:
                return comment_link
        except Exception:
            return comment_link

    def credits_validation(self,source_details,projectx_details):
        #import pdb;pdb.set_trace()
        credit_match='True'#by default
        credit_mismatch=[]
        counter=0
        if projectx_details!='Null':
            if source_details["source_credit_present"]=='True' and projectx_details["px_credit_present"]=='True':
                for px_credits in projectx_details["px_credits"]:
                    if px_credits in source_details["source_credits"]:
                        credit_match='True'
                        counter+=counter
                    else:
                        if counter< len(projectx_details["px_credits"]):
                            for source_credits in source_details["source_credits"]:
                                credit_ratio=fuzz.ratio(px_credits.upper(),source_credits.upper())
                                if credit_ratio >=70:
                                    credit_match='True'
                                    counter+=counter
                                    break
                                else:
                                    counter+=counter
                        else:
                            credit_match='False'
                            credit_mismatch.append(px_credits)

            if source_details["source_credit_present"]=='Null' and projectx_details["px_credit_present"]=='False':
                credit_match='True'
            if source_details["source_credit_present"]=='True' and projectx_details["px_credit_present"]=='False':
                credit_match='False'

        return (credit_match,credit_mismatch)

    class meta_data_validate_hulu:
        #initilization
        def __init__(self):
            self.title_match='False'
            self.description_match='False'
            self.genres_match='Null'
            self.release_year_match='False'
            self.season_number_match=''
            self.episode_number_match=''
            self.px_video_link_present=''
            self.source_link_present=''

        def cleanup(self):
            self.title_match='False'
            self.description_match='False'
            self.genres_match='Null'
            self.release_year_match='False'
            self.season_number_match=''
            self.episode_number_match=''
            self.px_video_link_present=''
            self.source_link_present=''

        # meta_data_validation
        def meta_data_validation(self,_id,source_details,projectx_details,show_type):
            #import pdb;pdb.set_trace()
            if projectx_details!='Null':
                if show_type=='MO' or show_type=='SM':
                    if projectx_details["px_original_title"]!='' or projectx_details["px_original_title"] is not None:
                        if projectx_details["px_original_title"].upper() in source_details["source_title"].upper():
                            self.title_match='True'
                        elif projectx_details["px_original_title"].upper() in source_details["source_title"].upper():
                            self.title_match='True'
                        else:
                            ratio_title=fuzz.ratio(projectx_details["px_original_title"].upper(),source_details["source_title"].upper())
                            if ratio_title >=70:
                                self.title_match='True'
                            else:
                                ratio_title=fuzz.ratio(projectx_details["px_original_title"].upper(),source_details["source_title"].upper())
                                if ratio_title >=70:
                                    self.title_match='True'
                                else:
                                    self.title_match='False'
                    else:
                        if projectx_details["px_long_title"].upper() in source_details["source_title"].upper():
                            self.title_match='True'
                        elif source_details["px_long_title"].upper() in projectx_details["source_title"].upper():
                            self.title_match='True'
                        else:
                            ratio_title=fuzz.ratio(source_details["source_title"].upper(),projectx_details["px_long_title"].upper())
                            if ratio_title >=70:
                                self.title_match='True'
                else:
                    if projectx_details["px_episode_title"].upper() in source_details["source_title"].upper():
                        self.title_match='True'
                    elif source_details["source_title"].upper() in projectx_details["px_episode_title"].upper():
                        self.title_match='True'
                    else:
                        ratio_title=fuzz.ratio(projectx_details["px_episode_title"].upper(),source_details["source_title"].upper())
                        if ratio_title >=70:
                            self.title_match='True'

                if projectx_details["px_description"]==source_details["source_description"]:
                    self.description_match='True'
                try:
                    if eval(source_details["source_release_year"]) == projectx_details["px_release_year"]:
                        self.release_year_match='True'
                    elif projectx_details["px_release_year"]-1 == eval(source_details["source_release_year"]):
                        self.release_year_match='True'
                    elif projectx_details["px_release_year"] ==  eval(source_details["source_release_year"])-1:
                        self.release_year_match='True'
                except Exception:
                    if projectx_details["px_release_year"] == source_details["source_release_year"]:
                        self.release_year_match='True'

                if show_type=='SE':
                    if source_details["source_season_number"]==projectx_details["px_season_number"]:
                        self.season_number_match='True'
                    else:
                        self.season_number_match='False'
                    #import pdb;pdb.set_trace()
                    if projectx_details["px_episode_number"]=="" and source_details[11]!="":
                        projectx_details["px_episode_number"]="0"
                        if source_details["source_episode_number"]==projectx_details["px_episode_number"]:
                            self.episode_number_match='True'
                        else:
                            self.episode_number_match='False'
                    else:
                         if source_details["source_episode_number"]==projectx_details["px_episode_number"]:
                             self.episode_number_match='True'
                         else:
                             self.episode_number_match='False'

                self.px_video_link_present=projectx_details["px_video_link_present"]
                self.source_link_present=source_details["source_link_present"]

            return {"title_match":self.title_match,"description_match":self.description_match,"genres_match":self.genres_match,"release_year_match":self.release_year_match,
                    "season_number_match":self.season_number_match,"episode_number_match":self.episode_number_match,"px_video_link_present":self.px_video_link_present,
                    "source_link_present":self.source_link_present}

    class meta_data_validate_vudu:

        def meta_data_validation(self,_id,source_details,projectx_details,show_type):
            #default:
            #import pdb;pdb.set_trace()
            title_match='False'
            description_match='False'
            genres_match='Null'
            aliases_match='Null'
            release_year_match='False'
            duration_match='False'
            season_number_match=''
            episode_number_match=''
            px_video_link_present=''
            source_link_present=''

            if projectx_details!='Null':

                if show_type=='MO' or show_type=='SM':
                    if projectx_details["px_original_title"]!='' or projectx_details["px_original_title"] is not None:
                        if projectx_details["px_original_title"].upper() in source_details["source_title"].upper():
                            title_match='True'
                        elif projectx_details["px_original_title"].upper() in source_details["source_title"].upper():
                            title_match='True'
                        else:
                            ratio_title=fuzz.ratio(projectx_details["px_original_title"].upper(),source_details["source_title"].upper())
                            if ratio_title >=70:
                                title_match='True'
                            else:
                                ratio_title=fuzz.ratio(projectx_details["px_original_title"].upper(),source_details["source_title"].upper())
                                if ratio_title >=70:
                                    title_match='True'
                                else:
                                    title_match='False'
                    else:
                        if projectx_details["px_long_title"].upper() in source_details["source_title"].upper():
                            title_match='True'
                        elif source_details["source_title"].upper() in projectx_details["px_long_title"].upper():
                            title_match='True'
                        else:
                            ratio_title=fuzz.ratio(source_details["source_title"].upper(),projectx_details["px_long_title"].upper())
                            if ratio_title >=70:
                                title_match='True'
                else:
                    if projectx_details["px_episode_title"].upper() in source_details["source_title"].upper():
                        title_match='True'
                    elif source_details["source_title"].upper() in projectx_details["px_episode_title"].upper():
                        title_match='True'
                    else:
                        ratio_title=fuzz.ratio(projectx_details["px_episode_title"].upper(),source_details["source_title"].upper())
                        if ratio_title >=70:
                            title_match='True'

                if projectx_details["px_description"]==source_details["source_description"]:
                    description_match='True'

                if source_details["source_genres"] is not None or source_details["source_genres"]:
                    if source_details["source_genres"]== projectx_details["px_genres"]:
                        genres_match='True'

                if source_details["source_alternate_titles"]!=[] or source_details["source_alternate_titles"]!='':
                    if source_details["source_alternate_titles"]==projectx_details["px_aliases"]:
                        aliases_match='True'
                    elif source_details["source_alternate_titles"] in projectx_details["px_aliases"]:
                        aliases_match='True'

                try:
                    if str(projectx_details["px_release_year"]) in str(source_details["source_release_year"]):
                        release_year_match='True'
                    elif eval(source_details["source_release_year"]) == projectx_details["px_release_year"]:
                        release_year_match='True'
                    elif projectx_details["px_release_year"]-1 == eval(source_details["source_release_year"]):
                        release_year_match='True'
                    elif projectx_details["px_release_year"] ==  eval(source_details["source_release_year"])-1:
                        release_year_match='True'
                except Exception:
                    if source_details["source_release_year"]=="":
                        source_details["source_release_year"]=0
                        if projectx_details["px_release_year"] == source_details["source_release_year"]:
                            release_year_match='True'


                #import pdb;pdb.set_trace()
                if source_details["source_duration"] is not None or source_details["source_duration"]==0:
                    if source_details["source_duration"]== projectx_details["px_run_time"]:
                        duration_match='True'
                    elif eval(source_details["source_duration"])== projectx_details["px_run_time"]:
                        duration_match='True'
                else:
                    duration_match='True'
                #import pdb;pdb.set_trace()
                if show_type=='SE':
                    if eval(source_details["source_season_number"])==projectx_details["px_season_number"]:
                        season_number_match='True'
                    else:
                        season_number_match='False'

                    #import pdb;pdb.set_trace()
                    if (projectx_details["px_episode_number"]!="" or projectx_details["px_episode_number"] is not None) and source_details["source_episode_number"].encode()!='':
                        try:
                            if eval(source_details["source_episode_number"])==eval(projectx_details["px_episode_number"]):
                                episode_number_match='True'
                            else:
                                episode_number_match='False'
                        except Exception:
                            if eval(source_details["source_episode_number"])==projectx_details["px_episode_number"]:
                                episode_number_match='True'
                            else:
                                episode_number_match='False'


                px_video_link_present=projectx_details["px_video_link_present"]
                source_link_present=source_details["source_link_present"]

            return {"title_match":title_match,"description_match":description_match,"genres_match":genres_match,"aliases_match":aliases_match,"release_year_match":release_year_match,
                    "duration_match":duration_match,"season_number_match":season_number_match,"episode_number_match":episode_number_match,
                    "px_video_link_present":px_video_link_present,"source_link_present":source_link_present}

    class meta_data_validate_headrun:

        # meta_data_validation
        def meta_data_validation(self,_id,source_details,projectx_details,show_type):
            #default:
            #import pdb;pdb.set_trace()
            title_match='False'
            description_match='False'
            genres_match='Null'
            aliases_match='Null'
            release_year_match='False'
            duration_match='False'
            season_number_match=''
            episode_number_match=''
            px_video_link_present=''
            source_link_present=''

            if projectx_details!='Null':

                if show_type=='MO' or show_type=='SM':
                    if projectx_details["px_original_title"]!='' or projectx_details["px_original_title"] is not None:
                        if projectx_details["px_original_title"].upper() in source_details["source_title"].upper():
                            title_match='True'
                        elif projectx_details["px_original_title"].upper() in source_details["source_title"].upper():
                            title_match='True'
                        else:
                            ratio_title=fuzz.ratio(projectx_details["px_original_title"].upper(),source_details["source_title"].upper())
                            if ratio_title >=70:
                                title_match='True'
                            else:
                                ratio_title=fuzz.ratio(projectx_details["px_original_title"].upper(),source_details["source_title"].upper())
                                if ratio_title >=70:
                                    title_match='True'
                                else:
                                    title_match='False'
                    else:
                        if projectx_details["px_long_title"].upper() in source_details["source_title"].upper():
                            title_match='True'
                        elif source_details["source_title"].upper() in projectx_details["px_long_title"].upper():
                            title_match='True'
                        else:
                            ratio_title=fuzz.ratio(source_details["source_title"].upper(),projectx_details["px_long_title"].upper())
                            if ratio_title >=70:
                                title_match='True'
                else:
                    if projectx_details["px_episode_title"].upper() in source_details["source_title"].upper():
                        title_match='True'
                    elif source_details["source_title"].upper() in projectx_details["px_episode_title"].upper():
                        title_match='True'
                    else:
                        ratio_title=fuzz.ratio(projectx_details["px_episode_title"].upper(),source_details["source_title"].upper())
                        if ratio_title >=70:
                            title_match='True'

                if projectx_details["px_description"]==source_details["source_description"]:
                    description_match='True'
                #import pdb;pdb.set_trace()
                if source_details["source_genres"] is not None or source_details["source_genres"]:
                    if source_details["source_genres"]== projectx_details["px_genres"]:
                        genres_match='True'
                    else:
                        genres_match='False'

                try:
                    if str(projectx_details["px_release_year"]) == str(source_details["source_release_year"]):
                        release_year_match='True'
                    elif eval(source_details["source_release_year"]) == projectx_details["px_release_year"]:
                        release_year_match='True'
                    elif projectx_details["px_release_year"]-1 == eval(source_details["source_release_year"]):
                        release_year_match='True'
                    elif projectx_details["px_release_year"] ==  eval(source_details["source_release_year"])-1:
                        release_year_match='True'
                except Exception:
                    if source_details["source_release_year"]=="":
                        source_details["source_release_year"]=0
                        if projectx_details["px_release_year"] == source_details["source_release_year"]:
                            release_year_match='True'


                #import pdb;pdb.set_trace()
                if source_details["source_duration"]!='0':
                    if eval(source_details["source_duration"])== projectx_details["px_run_time"]:
                        duration_match='True'
                    elif eval(source_details["source_duration"])*60== projectx_details["px_run_time"]:
                        duration_match='True'
                elif eval(source_details["source_duration"])== projectx_details["px_run_time"]:
                    duration_match='True'
                else:
                    duration_match='False'

                #import pdb;pdb.set_trace()
                if show_type=='SE':
                    try:
                        if eval(source_details["source_season_number"])==projectx_details["px_season_number"]:
                            season_number_match='True'
                        else:
                            season_number_match='False'
                    except Exception:
                        if source_details["source_season_number"] == projectx_details["px_season_number"]:
                            season_number_match='True'
                        else:
                            season_number_match='False'


                    #import pdb;pdb.set_trace()
                    if (projectx_details["px_episode_number"]!="" or projectx_details["px_episode_number"] is not None) and source_details["source_episode_number"]!='':
                        try:
                            if eval(source_details["source_episode_number"])==projectx_details["px_episode_number"]:
                                episode_number_match='True'
                            else:
                                episode_number_match='False'
                        except Exception:
                            if source_details["source_episode_number"]==projectx_details["px_episode_number"]:
                                episode_number_match='True'
                            elif "0" in source_details["source_episode_number"]:
                                source_details["source_episode_number"]=source_details["source_episode_number"].strip('0')
                                if eval(source_details["source_episode_number"])==projectx_details["px_episode_number"]:
                                    episode_number_match='True'
                                else:
                                    episode_number_match='False'
                            else:
                                episode_number_match='False'


                px_video_link_present=projectx_details["px_video_link_present"]
                source_link_present=source_details["source_link_present"]

            return {"title_match":title_match,"description_match":description_match,"genres_match":genres_match,"aliases_match":aliases_match,"release_year_match":release_year_match,
                    "duration_match":duration_match,"season_number_match":season_number_match,"episode_number_match":episode_number_match,
                    "px_video_link_present":px_video_link_present,"source_link_present":source_link_present}

    class meta_data_validate_gracenote:
        def meta_data_validation(self,_id,source_details,projectx_details,show_type):
            #import pdb;pdb.set_trace()
            title_match='False'
            description_match='False'
            genres_match='Null'
            aliases_match='Null'
            release_year_match='False'
            duration_match='False'
            season_number_match=''
            episode_number_match=''
            px_video_link_present=''
            source_link_present=''

            if projectx_details!='Null':

                if show_type=='MO' or show_type=='SM':
                    if projectx_details["px_original_title"]!='' or projectx_details["px_original_title"] is not None:
                        if projectx_details["px_original_title"].upper() in source_details["source_title"].upper():
                            title_match='True'
                        elif projectx_details["px_original_title"].upper() in source_details["source_title"].upper():
                            title_match='True'
                        else:
                            ratio_title=fuzz.ratio(projectx_details["px_original_title"].upper(),source_details["source_title"].upper())
                            if ratio_title >=70:
                                title_match='True'
                            else:
                                ratio_title=fuzz.ratio(projectx_details["px_original_title"].upper(),source_details["source_title"].upper())
                                if ratio_title >=70:
                                    title_match='True'
                                else:
                                    title_match='False'
                    else:
                        if projectx_details["px_long_title"].upper() in source_details["source_title"].upper():
                            title_match='True'
                        elif source_details["source_title"].upper() in projectx_details["px_long_title"].upper():
                            title_match='True'
                        else:
                            ratio_title=fuzz.ratio(source_details["source_title"].upper(),projectx_details["px_long_title"].upper())
                            if ratio_title >=70:
                                title_match='True'
                else:
                    if projectx_details["px_episode_title"].upper() in source_details["source_title"].upper():
                        title_match='True'
                    elif source_details["source_title"].upper() in projectx_details["px_episode_title"].upper():
                        title_match='True'
                    else:
                        ratio_title=fuzz.ratio(projectx_details["px_episode_title"].upper(),source_details["source_title"].upper())
                        if ratio_title >=70:
                            title_match='True'

                if projectx_details["px_description"]==source_details["source_description"]:
                    description_match='True'

                if source_details["source_genres"] is not None or source_details["source_genres"]:
                    if source_details["source_genres"]== projectx_details["px_genres"]:
                        genres_match='True'
                    else:
                        genres_match='False'
                try:
                    if projectx_details["px_release_year"] == source_details["source_release_year"]:
                        release_year_match='True'
                    elif projectx_details["px_release_year"] == eval(source_details["source_release_year"]):
                        release_year_match='True'
                    elif projectx_details["px_release_year"]-1 == source_details["source_release_year"]:
                        release_year_match='True'
                    elif projectx_details["px_release_year"] == source_details["source_release_year"]-1:
                        release_year_match='True'
                except Exception:
                    if projectx_details["px_release_year"]-1 == eval(source_details["source_release_year"]):
                        release_year_match='True'
                    elif projectx_details["px_release_year"] == eval(source_details["source_release_year"])-1:
                        release_year_match='True'

                #import pdb;pdb.set_trace()
                if source_details["source_duration"]!='0':
                    if source_details["source_duration"]== projectx_details["px_run_time"]:
                        duration_match='True'
                    elif source_details["source_duration"]== projectx_details["px_run_time"]:
                        duration_match='True'
                else:
                    duration_match='True'
                #import pdb;pdb.set_trace()
                if show_type=='SE':
                    try:
                        if projectx_details["px_season_number"] is None and source_details["source_season_number"]==0:
                            season_number_match='True'
                        elif source_details["source_season_number"]==projectx_details["px_season_number"]:
                            season_number_match='True'
                        elif eval(source_details["source_season_number"])==projectx_details["px_season_number"]:
                            season_number_match='True'
                    except Exception:
                        season_number_match='False'

                    #import pdb;pdb.set_trace()
                    if (projectx_details["px_episode_number"]!="" or projectx_details["px_episode_number"] is not None) and source_details["source_episode_number"] !=0:
                        try:
                            if source_details["source_episode_number"]==projectx_details["px_episode_number"]:
                                episode_number_match='True'
                            elif eval(source_details["source_episode_number"])==projectx_details["px_episode_number"]:
                                episode_number_match='True'
                        except Exception:
                            episode_number_match='False'
                    else:
                        if source_details["source_episode_number"]==projectx_details["px_episode_number"]:
                            episode_number_match='True'
                        else:
                            episode_number_match='False'


                px_video_link_present=projectx_details["px_video_link_present"]
                source_link_present=source_details["source_link_present"]

            return {"title_match":title_match,"description_match":description_match,"genres_match":genres_match,"aliases_match":aliases_match,"release_year_match":release_year_match,
                    "duration_match":duration_match,"season_number_match":season_number_match,"episode_number_match":episode_number_match,
                    "px_video_link_present":px_video_link_present,"source_link_present":source_link_present}



class checking_any_two_px_programs:

    def __init__(self):
        self.px_id_alias=[]
        self.px_id_alias_comment=''
        self.px_id_credits_null='True'

        self.px_id=0
        self.px_id_show_type=''
        self.px_id_variant_parent_id=0
        self.px_id_is_group_language_primary=''
        self.px_id_long_title=''
        self.px_id_original_title=''
        self.px_id_run_time=''
        self.px_id_release_year=''
        self.px_id_record_language=''
        self.px_id_aliases=[]
        self.px_id_credits=[]
        self.px_id_db_credit_present='False'
        self.px_id_videos_launch_id=[]
        self.comment=''

        self.long_title_match=''
        self.original_title_match=''
        self.runtime_match=''
        self.release_year_match=''
        self.alias_title_match=''
        self.video_match=''
        self.match_launch_id=[]
        self.credit_match=''

    def cleanup(self):
        self.px_id_alias=[]
        self.px_id_alias_comment=''
        self.px_id_credits_null='True'
        self.comment=''

        self.px_id=0
        self.px_id_show_type=''
        self.px_id_variant_parent_id=0
        self.px_id_is_group_language_primary=''
        self.px_id_long_title=''
        self.px_id_original_title=''
        self.px_id_run_time=''
        self.px_id_release_year=''
        self.px_id_record_language=''
        self.px_id_aliases=[]
        self.px_id_credits=[]
        self.px_id_db_credit_present='False'
        self.px_id_videos_launch_id=[]

        self.long_title_match=''
        self.original_title_match=''
        self.runtime_match=''
        self.release_year_match=''
        self.alias_title_match=''
        self.video_match=''
        self.match_launch_id=[]
        self.credit_match=''

    def getting_px_credits(self,data_credits):
        #import pdb;pdb.set_trace()
        px_credits=[]
        for credits in data_credits:
            px_credits.append(unidecode.unidecode(pinyin.get(credits.get("full_credit_name"))))
        return px_credits

    def long_title_validtion(self,px_id1_long_title,px_id2_long_title):
        #import pdb;pdb.set_trace()
        if px_id1_long_title.upper()!='' and px_id2_long_title.upper()!='':
            if px_id1_long_title.upper() in px_id2_long_title.upper():
                self.long_title_match='True'
            else:
                if px_id2_long_title.upper() in px_id1_long_title.upper():
                    self.long_title_match='True'
                else:
                    ratio_title=fuzz.ratio(px_id1_long_title.upper(),px_id2_long_title.upper())
                    if ratio_title >=70:
                        self.long_title_match='True'
                    else:
                        self.long_title_match='False'
        elif px_id1_long_title.upper()=='' and px_id2_long_title.upper()=='':
            self.long_title_match='True'
        else:
            self.long_title_match='False'

    def original_title_validation(self,px_id1_original_title,px_id2_original_title):
        #import pdb;pdb.set_trace()
        if px_id1_original_title.upper()!='' and px_id2_original_title.upper()!='':
            if px_id1_original_title.upper() in px_id2_original_title.upper():
                self.original_title_match='True'
            else:
                if px_id2_original_title.upper() in px_id1_original_title.upper():
                    self.original_title_match='True'
                else:
                    ratio_title=fuzz.ratio(px_id1_original_title.upper(),px_id2_original_title.upper())
                    if ratio_title >=70:
                        self.original_title_match='True'
                    else:
                        self.original_title_match='False'
        elif px_id1_original_title.upper()=='' and px_id2_original_title.upper()=='':
            self.original_title_match='True'
        else:
            self.original_title_match='False'

    def runtime_validation(self,px_id1_run_time,px_id2_run_time):
        #import pdb;pdb.set_trace()
        if px_id1_run_time==px_id2_run_time:
            self.runtime_match='True'
        else:
            self.runtime_match='False'

    def release_year_validation(self,px_id1_release_year,px_id2_release_year):
        #import pdb;pdb.set_trace()
        if px_id1_release_year is not None and px_id2_release_year is not None:
            if px_id1_release_year==px_id2_release_year:
                self.release_year_match='True'
            else:
                r_y=px_id1_release_year
                r_y=r_y+1
                if r_y==px_id2_release_year:
                    self.release_year_match='True'
                else:
                    r_y=r_y-2
                    if r_y==px_id2_release_year:
                        self.release_year_match='True'
                    else:
                        self.release_year_match='False'

    def alias_title_validation(self,px_id1_aliases,px_id1_alias,px_id2_aliases
                                       ,px_id2_long_title,px_id2_original_title ):
        #import pdb;pdb.set_trace()
        if px_id1_aliases and px_id2_aliases:
            for alias in px_id1_alias:
                if alias in px_id2_long_title:
                    self.alias_title_match='True'
                    break
                elif px_id2_long_title in alias:
                    self.alias_title_match='True'
                    break
                elif px_id2_original_title in alias:
                    self.alias_title_match='True'
                    break
                elif alias in px_id2_original_title:
                    self.alias_title_match='True'
                    break
                else:
                    ratio_title=fuzz.ratio(px_id2_long_title.upper(),alias.upper())
                    if ratio_title >=70:
                        self.alias_title_match='True'
                        break
                    else:
                        ratio_title=fuzz.ratio(px_id2_original_title.upper(),alias.upper())
                        if ratio_title >=70:
                            self.alias_title_match='True'
                            break
                        else:
                            self.alias_title_match='False'

    def ott_link_id_validation(self,px_id1_videos_launch_id,px_id2_videos_launch_id):
        #import pdb;pdb.set_trace()
        if px_id1_videos_launch_id and px_id2_videos_launch_id:
            for launch_id in px_id1_videos_launch_id[0]:
                if launch_id in px_id2_videos_launch_id[0]:
                    self.video_match='True'
                    self.match_launch_id.append(launch_id)
                    break
                else:
                    self.video_match='False'
        elif px_id1_videos_launch_id==[] and px_id2_videos_launch_id:
            self.video_match='px_id1_video_link_null'
        elif px_id1_videos_launch_id and px_id2_videos_launch_id==[]:
            self.video_match='px_id2_video_link_null'
        else:
            self.video_match='Both_ott_link_null'

    def credit_validation(self,px_id1_credits,px_id2_credits):
        #import pdb;pdb.set_trace()
        if px_id1_credits and px_id2_credits:
            for credits in px_id1_credits:
                if credits in px_id2_credits:
                    self.credit_match='True'
                    break
                else:
                    self.credit_match='False'

    def projectx_id_details(self,data_resp_ids,credit_db_api,source,token):
        #import pdb;pdb.set_trace()
        self.px_id=data_resp_ids.get("id")
        self.px_id_show_type=data_resp_ids.get("show_type").encode('utf-8')
        self.px_id_variant_parent_id=data_resp_ids.get("variant_parent_id")
        self.px_id_is_group_language_primary=data_resp_ids.get("is_group_language_primary")
        self.px_id_long_title=unidecode.unidecode(data_resp_ids.get("long_title"))
        self.px_id_original_title=unidecode.unidecode(data_resp_ids.get("original_title"))
        self.px_id_run_time=data_resp_ids.get("run_time")
        self.px_id_release_year=data_resp_ids.get("release_year")
        self.px_id_record_language=data_resp_ids.get("record_language")
        self.px_id_aliases=data_resp_ids.get("aliases")
        if data_resp_ids.get("credits"):
            self.px_id_credits=self.getting_px_credits(data_resp_ids.get("credits"))
        #import pdb;pdb.set_trace()
        if data_resp_ids.get("videos"):
            self.px_id_videos_launch_id.append([str(launch_id["launch_id"]) for launch_id in data_resp_ids.get("videos")])

        if self.px_id_credits:
            self.px_id_credits_null='False'
        else:
            credit_db_api_=credit_db_api%self.px_id
            credit_resp_db=lib_common_modules().fetch_response_for_api_(credit_db_api_,token)
            if credit_resp_db:
                self.px_id_credits=self.getting_px_credits(credit_resp_db)
                self.px_id_db_credit_present='True'

        if self.px_id_aliases!=[] and self.px_id_aliases is not None:
            for alias in self.px_id_aliases:
                if alias.get("source_name")==source and alias.get("language")=='ENG':
                    self.px_id_alias.append(unidecode.unidecode(alias.get("alias")))
                if alias.get("source_name")=='Rovi' and alias.get("type")=='alias_title' and alias.get("language")=='ENG':
                    self.px_id_alias.append(unidecode.unidecode(alias.get("alias")))
        else:
            self.px_id_alias_comment='Null'

        return {str(self.px_id):{"px_id_show_type":self.px_id_show_type,"px_id_variant_parent_id":self.px_id_variant_parent_id,
              "px_id_is_group_language_primary":self.px_id_is_group_language_primary,"px_id_record_language":self.px_id_record_language,
              "px_id_credits_null":self.px_id_credits_null,"px_id_db_credit_present":self.px_id_db_credit_present,"px_id_long_title":self.px_id_long_title,
              "px_id_original_title":self.px_id_original_title,"px_id_run_time":self.px_id_run_time,"px_id_release_year":self.px_id_release_year,"px_id_aliases":self.px_id_aliases
              ,"px_credits":self.px_id_credits,"px_id_videos_launch_id":self.px_id_videos_launch_id,"px_id_credits_null":self.px_id_credits_null,
              "px_id_alias":self.px_id_alias,"px_id_alias_comment":self.px_id_alias_comment}}

    def checking_same_program(self,duplicate_id,projectx_api,
                                         credit_db_api,source,token):

        details=[]
        #import pdb;pdb.set_trace()
        try:
            projectx_api_=projectx_api%'{}'.format(",".join([str(i) for i in duplicate_id]))
            data_resp_ids=lib_common_modules().fetch_response_for_api_(projectx_api_,token)
            for data in data_resp_ids:
                self.cleanup()
                details.append(self.projectx_id_details(data,credit_db_api,source,token))
                # self.projectx_id2_details(data_resp_ids[1],credit_db_api,source,token)
            if len(details)>1:

                self.long_title_validtion(details[0][str(duplicate_id[0])]["px_id_long_title"],details[1][str(duplicate_id[1])]["px_id_long_title"])
                self.original_title_validation(details[0][str(duplicate_id[0])]["px_id_original_title"],details[1][str(duplicate_id[1])]["px_id_original_title"])
                self.runtime_validation(details[0][str(duplicate_id[0])]["px_id_run_time"],details[1][str(duplicate_id[1])]["px_id_run_time"])
                self.release_year_validation(details[0][str(duplicate_id[0])]["px_id_release_year"],details[1][str(duplicate_id[1])]["px_id_release_year"])
                self.alias_title_validation(details[0][str(duplicate_id[0])]["px_id_aliases"],details[0][str(duplicate_id[0])]["px_id_alias"]
                                        ,details[1][str(duplicate_id[1])]["px_id_aliases"],details[1][str(duplicate_id[1])]["px_id_long_title"],
                                                        details[1][str(duplicate_id[1])]["px_id_original_title"])
                self.ott_link_id_validation(details[0][str(duplicate_id[0])]["px_id_videos_launch_id"],details[1][str(duplicate_id[1])]["px_id_videos_launch_id"])
                self.credit_validation(details[0][str(duplicate_id[0])]["px_credits"],details[1][str(duplicate_id[1])]["px_credits"])

                return {"px_id1":duplicate_id[0],"px_id1_show_type":details[0][str(duplicate_id[0])]["px_id_show_type"],"px_id1_variant_parent_id":details[0][str(duplicate_id[0])]["px_id_variant_parent_id"],
                      "px_id1_is_group_language_primary":details[0][str(duplicate_id[0])]["px_id_is_group_language_primary"],"px_id1_record_language":details[0][str(duplicate_id[0])]["px_id_record_language"],
                      "px_id2":duplicate_id[1],"px_id2_show_type":details[1][str(duplicate_id[1])]["px_id_show_type"],"px_id2_variant_parent_id":details[1][str(duplicate_id[1])]["px_id_variant_parent_id"],
                      "px_id2_is_group_language_primary":details[1][str(duplicate_id[1])]["px_id_is_group_language_primary"],"px_id2_record_language":details[1][str(duplicate_id[1])]["px_id_record_language"],
                      "px_id1_credits_null":details[0][str(duplicate_id[0])]["px_id_credits_null"],"px_id1_db_credit_present":details[0][str(duplicate_id[0])]["px_id_db_credit_present"],
                    "px_id2_credits_null":details[1][str(duplicate_id[1])]["px_id_credits_null"],"px_id2_db_credit_present":details[1][str(duplicate_id[1])]["px_id_db_credit_present"],"long_title_match":self.long_title_match,
                    "original_title_match":self.original_title_match,"runtime_match":self.runtime_match,"release_year_match":self.release_year_match,"alias_title_match":
                     self.alias_title_match,"credit_match":self.credit_match,"match_link_id":self.match_launch_id,"link_match":self.video_match}
            else:
                self.comment="Any of them px_id have no program_API response"
                return {"px_id1":'',"px_id1_show_type":'',"px_id1_variant_parent_id":'',
                      "px_id1_is_group_language_primary":'',"px_id1_record_language":'',
                      "px_id2":'',"px_id1_show_type":'',"px_id2_variant_parent_id":'',
                      "px_id2_is_group_language_primary":'',"px_id2_record_language":'',
                      "px_id1_credits_null":'',"px_id1_db_credit_present":'',
                    "px_id2_credits_null":'',"px_id2_db_credit_present":'',"long_title_match":'',
                    "original_title_match":'',"runtime_match":'',"release_year_match":'',"alias_title_match":
                     '',"credit_match":'',"match_link_id":'',"link_match":'',"comment":self.comment}




        except Exception as e:
            print ("exception caught (checking_same_program).................",type(e),[duplicate_id[0],duplicate_id[1]])
            print ("\n")
            print ("Retrying.............")
            print ("\n")
            self.checking_same_program(duplicate_id,projectx_api,credit_db_api,source,token)
