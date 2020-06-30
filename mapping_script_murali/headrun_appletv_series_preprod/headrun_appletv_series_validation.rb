require 'rest_client'
require 'mongo'
require 'json'
require 'csv'
require 'date'
require 'fuzzystringmatch'
require 'logger'

run_date = Date.today
run_date = run_date.strftime("%Y%m%d")
$link_not_available = 0
$link_mismatch_count = 0
$link_match_count = 0
File.delete("logs/log_#{run_date}.txt") if File.exist?("logs/log_#{run_date}.txt")
$log = Logger.new("logs/log_#{run_date}.txt")
CSV.open("#{run_date}Headrun_validation_Preprod.csv","w+") do |cs|
    cs << ["SM_Title", "rel_yr", "APPLETV_SM_ID", "SM_Map_Status","Duplicate_PX_id","Blind_ID","OZ_SM_ID","Blind_Status",
     "EP_Title","SN_NO","EP_NO", "Launch_ID", "status", "mapped_by", "OZ_EP_ID", "OZ_SN_NO", "OZ_EP_NO", "OZ_OTT"]
end

def check_blind_projectx_ingested(series_id)
    retry_cnt = 3
    begin
        pjx_body = JSON.parse(RestClient.get("http://#{$host_ip}/projectx/mappingfromsource?sourceIds=#{series_id}&sourceName=#{$source}&showType=SM", {:authorization => "#{$token}",:user_agent => "#{$user_agent}",:host => "#{$host_ip}"}))
        ingestion_status = []
        if !pjx_body.empty?            
            ingestion_status << "Pass"
            ingestion_status << pjx_body[0]["projectx_id"]   
            return ingestion_status         
        else 
            r = check_projectx_duplicate(series_id);
            if r == nil
                ingestion_status << "Fail"
                ingestion_status << "NA"
                return ingestion_status
            else
                ingestion_status << "part of duplictes"
                ingestion_status << "#{r}"
                return ingestion_status
            end           
            ingestion_status << "Fail"
            ingestion_status << "NA"            
        end    
    rescue Exception => err 
        $log.info "Exception in Blind ProjectX Ingestion Method"
        $log.info "Error!!!: #{err}"
        $log.info "Error!!!: #{err.backtrace}"
        retry_cnt -= 1
        if retry_cnt > 0
            sleep 2
            retry
        else
            $log.info "Exceeded retry count in ProjectX Ingested Method: #{retry_cnt}"
        end
    end
end

def check_projectx_duplicate(series_id)
    retry_cnt = 3
    dup_id=[]
    begin
        dup_body = JSON.parse(RestClient.get("http://#{$host_ip}/projectx/duplicate?sourceId=#{series_id}&sourceName=#{$source}&showType=SM", {:authorization => $token,:user_agent => $user_agent,:host => $host_ip}))
        if !dup_body.empty?
            dup_body.each do |resp|
                dup_id << resp["projectx_id"]                
            end
            return dup_id
        else
            return nil
        end
    rescue Exception => err
        $log.info "Exception in ProjectX Duplicate Method"
        $log.info "Error!!!: #{err}"
        $log.info "Error!!!: #{err.backtrace}"
        retry_cnt -= 1
        if retry_cnt > 0
            sleep 2
            retry
        else
            $log.info "Exceeded retry count in ProjectX Duplicate Method: #{retry_cnt}"
        end
    end
end

def check_ott_deleted(vu)
    retry_cnt = 3
    begin
        del_status = JSON.parse(RestClient.get("http://#{$host_ip}/projectx/#{vu}/Netflixusa/isDeletedOttprojectx", {:authorization => 'Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3',:user_agent => 'Caavo_Fyra_v1.1.199',:host => '34.231.212.186:81'}))
        if !del_status.empty?
            return "true"
        else
            return "false"
        end
    rescue Exception => err
        $log.info "Exception in Ott Deleted Method"
        $log.info "Error!!!: #{err}"
        $log.info "Error!!!: #{err.backtrace}"
        if retry_cnt > 0
            sleep 2
            retry
        else
            $log.info "Exceeded retry count in Ott Deleted Method: #{retry_cnt}"
        end
    end
end

def check_ott_expiry(source_id)
    retry_cnt = 3
    begin
        exp_status = JSON.parse(RestClient.get("#{$env_domain}expired_ott/is_available?source_program_id=#{source_id}&service_short_name=#{$service}", {:authorization => $expired_token,:user_agent => $user_agent}))
        if !exp_status.empty?
            if exp_status["is_available"] == "false"
                return "false"
            elsif exp_status["is_available"] == "true"
                return "true"
            end            
        else
            return "empty"
        end
    rescue Exception => err
        $log.info "Exception in Ott Expiry Method"
        $log.info "Error!!!: #{err}"
        $log.info "Error!!!: #{err.backtrace}"
        if retry_cnt > 0
            sleep 2
            retry
        else
            $log.info "Exceeded retry count in Ott Expiry Method: #{retry_cnt}"
        end
    end
end

def process(source_ott, px)
  map_flag = 0
  $log.info "Video links under test are..... #{source_ott}"
  result_vid = []
  px_vids_array = Array.new
  px.keys.each do |s|
    if s == $service
      px[s].each do |values|
        b = values
        px_vids_array << b
      end
    end            
    $log.info "Ozone links are.....#{px_vids_array}"
    if px_vids_array.length == 0
        $link_not_available = $link_not_available + 1
        result_vid << "Links not available"
        result_vid << source_ott
        result_vid << "NA"
        # ot_del = check_ott_deleted(source_ott);
        # if ot_del == "true"
        #     result_vid << "Not Deleted"            
        # elsif ot_del == "true"
        #     result_vid << "Deleted"
        # end
        ot_exp = check_ott_expiry(source_ott);
        if ot_exp == "false"
            result_vid << "Not Expired"            
        elsif ot_exp == "true"
            result_vid << "Expired"
        elsif ot_exp == "empty"
            result_vid << "empty"
        end
    else                                        
        ind = px_vids_array.index(source_ott)
        if ind == nil
            $link_mismatch_count = $link_mismatch_count + 1
            result_vid << "links didn't matched"
            result_vid.concat px_vids_array
        else
            $link_match_count = $link_match_count + 1
            result_vid << "links matched"
            result_vid.concat px_vids_array
        end
    end
  end
    return result_vid
end

def get_index_of_ott_search_object(json_body)
  $log.info "using my code only indexof search object"
  index_of_ott_search_obj = nil
  no_of_arr_in_complete_resp = json_body.length
  for i in 0..no_of_arr_in_complete_resp-1
    $log.info "action type: #{json_body[i]["action_type"]}"
    if json_body[i]["action_type"] == "ott_search"
      index_of_ott_search_obj = i
      $log.info "Index of ott search object is: #{i}"
      break
    end
  end
  index_of_ott_search_obj
end

def voice_search_pagination(search_term,tab)
  $log.info "using my code only voicesearch pagination"
  total_response = Array.new
  results_array = Array.new
  retry_cnt = 3
    begin
    uri = URI.encode("#{$env_domain}v3/voice_search?web=false&aliases=true&q=#{search_term}")
    json_body = JSON.parse(RestClient.get(uri, {:authorization => $search_token,:user_agent => $user_agent}))
    rescue Exception => err 
      $log.info "Error in getting response <br>"
      $log.info "Error!!!: #{err} <br"
      $log.info err.backtrace
      retry_cnt -= 1
      if retry_cnt > 0
        sleep 10
        retry
      else
        $log.info "retry count: #{retry_cnt}"
      end
    end  
  results_array = json_body["results"]
  if results_array.length > 0
    $log.info "json_body_results_length : #{(json_body["results"]).length}"
    if tab == "ott_search"
      ott_search_index = get_index_of_ott_search_object(results_array)
    elsif tab == "epg_search"
      $log.info "not required"
    elsif tab == "upcoming_epg_search"
      $log.info "not required"
    elsif tab == "web_results"
      $log.info "not required"
    end     
    other_responses = Array.new
    if ott_search_index != nil
      $log.info "#{results_array[ott_search_index]}"
      if results_array[ott_search_index].key?("next_page_params")
        $log.info "page_params key exists"
        total_response =  results_array[ott_search_index]["results"]
        query = results_array[ott_search_index]["next_page_params"]["query"]
        search_id = results_array[ott_search_index]["next_page_params"]["search_id"]
        page = results_array[ott_search_index]["next_page_params"]["page"]
        filter = results_array[ott_search_index]["next_page_params"]["filter"]        
        final_next_url = nil
        final_next_url = "/v3/voice_search?query=" + "#{query}" +"&search_id=" + "#{search_id}" + "&page=" + "#{page}" + "&filter=" + "#{filter}" + "&aliases=true" + "&web=false"
        other_responses = collect_all_pages_info(final_next_url)
        total_response = total_response + other_responses
      else
        $log.info "No page_params key present ; hence getting existing results"
        total_response =  results_array[ott_search_index]["results"]
      end
    else
      $state = "No requested object found in results"
      $log.info "#{$state}"
    end    
  else
    $state = "empty results from cloud"
    $log.info "#{$state}"
  end      
    total_response
end  

def collect_all_pages_info(url)
    $log.info "using my code only collect all pages info"
    rest_results_array = Array.new
    next_key = true
    while (next_key)
      retry_cnt = 3
      begin
        $log.info "next page url to query: #{url}"
        uri = URI.encode($env_domain+"#{url}")
        json_body = JSON.parse(RestClient.get(uri, {:authorization => $search_token,:user_agent => $user_agent}))
      rescue Exception => err 
        $log.info "Error in getting response <br>"
        $log.info "Error!!!: #{err} <br"
        $log.info err.backtrace
        retry_cnt -= 1
        if retry_cnt > 0
          sleep 10
          retry
        else
          $log.info "retry count: #{retry_cnt}"
        end
      end
      if (json_body["results"]).length > 0
        $log.info "current next_page results response length: #{((json_body["results"])[0]["results"]).length}"
        rest_results_array = rest_results_array + (json_body["results"])[0]["results"]
        if json_body["results"][0].key?("next_page_params")
          query = json_body["results"][0]["next_page_params"]["query"]
          search_id = json_body["results"][0]["next_page_params"]["search_id"]
          page = json_body["results"][0]["next_page_params"]["page"]
          filter = json_body["results"][0]["next_page_params"]["filter"]
          url = "/v3/voice_search?query=" + "#{query}" +"&search_id=" + "#{search_id}" + "&page=" + "#{page}" + "&filter=" + "#{filter}" + "&aliases=true" + "&web=false"
          next_key = true
        else
           next_key = false
        end 
      else
        $log.info "No results found in the URL obtained"
        next_key = false
      end   
    end
    $log.info "length of next pages response obtained: #{rest_results_array.length}"  
    rest_results_array
end

def mod_title(title)
    title_mod = title.downcase
    title_mod = title_mod.gsub(/[;|:|\-|,|.|'|"|?|!|@|#| |]/,'')
    title_mod = title_mod.gsub(/&/,'and')
    return title_mod
end

def get_aliases(input_array)
    final_alias_array = []
    input_array.each do |aliases|
        if aliases["source_name"] == "Rovi"
            if aliases["type"] == "long_title"
                final_alias_array << aliases["alias"]
            elsif aliases["type"] == "original_title"
                final_alias_array << aliases["alias"]
            elsif aliases["type"] == "alias_title"
                final_alias_array << aliases["alias"]
            elsif aliases["type"] == "alias_title2"
                final_alias_array << aliases["alias"]
            elsif aliases["type"] == "alias_title3"
                final_alias_array << aliases["alias"]
            elsif aliases["type"] == "alias_title4"
                final_alias_array << aliases["alias"]
            end
        elsif aliases["source_name"] == "Vudu"
            if aliases["type"] == "title"
                final_alias_array << aliases["alias"]
            end
        elsif aliases["source_name"] == "Hulu"
            if aliases["type"] == "title"
                final_alias_array << aliases["alias"]
            end           
        elsif aliases["source_name"] == "GuideBox"
            if aliases["type"] == "title"
                final_alias_array << aliases["alias"]
            elsif aliases["type"] == "original_title"
                final_alias_array << aliases["alias"]
            end
        end
    end
    return final_alias_array
end

def total_series(service)
    arr_valid = []
    arr_invalid = []
    arr = []
    series = $collection.find({"item_type":"tvshow","service":"#{service}"}).projection({"_id":0,"id":1})
    series.each do |series_details|
        series_details = series_details.to_json
        series_details = JSON.parse(series_details)
        arr << series_details["id"]
    end
    arr = arr.uniq
    $log.info "total no.of series available : #{arr.length}"
    arr.each do |series_id|
        series_validity = $collection.count({"item_type":"episode","service":"#{service}","series_id":"#{series_id}"})
        $log.info "-------------------------------------------------------------------------"
        $log.info "episode_count: #{series_validity}"
        $log.info "-------------------------------------------------------------------------"
        if series_validity != 0
            arr_valid << series_id
        else
            arr_invalid << series_id
        end
    end
    $log.info "valid series are #{arr_valid}"
    $log.info "invalid series are #{arr_invalid}"
    return arr_valid
end

def check_title_match(title_totest,program_title)
    title_match='False'
    jarow = FuzzyStringMatch::JaroWinkler.create( :pure )
    ratio=jarow.getDistance(program_title.upcase, title_totest.upcase)*100
    if ratio.to_i >70
        title_match="True"
    end
    return title_match
end    

def print_count()
    $log.info "series test count is #{$series_count_test}"
    $log.info "counts are as follows ..........................."
    $log.info "total no.of series are #{$total_series_to_be_tested}"
    $log.info "Series mapped count is #{$series_mapped_count}"
    $log.info "Series unmapped count is #{$series_unmapped_count}"
    $log.info "Series release year nil count is #{$series_rel_year_nil_count}"
    $log.info "Empty search results count is #{$empty_search_results_count}"
    $log.info "Exceptions in series are #{$exceptions_series}"
    $log.info "Total Episodes count is #{$total_episodes_count}"
    $log.info "Total Episodes mapped count is #{$episodes_mapped_count}"
    $log.info "Total Episodes unmapped count is #{$episodes_unmapped_count}"
    $log.info "Exceptions in Episodes are #{$exceptions_episode}"
    $log.info "Blind Ingestion pass count is  #{$blind_ingestion_pass_count}"
    $log.info "Blind Ingestion fail count is  #{$blind_ingestion_fail_count}"
    $log.info "Links match count is  #{$link_match_count}"    
    $log.info "Wrong Links count is  #{$link_mismatch_count}"    
    $log.info "Links Ingestion fail count is  #{$link_not_available}"    
end

# mongo connection
client = Mongo::Client.new(['192.168.86.10:27017'],:database => 'qadb') #Connecting to Mongo and creating a DB 'sample'.....use 
$collection = client[:headrun]

#Global Initialization
$service="apple_tv_plus"
$source="AppleTvPlus"
$mongo_source="appletv"
$env_domain="https://preprod.caavo.com/"
$token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
$expired_token='Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
$search_token='Token token=ddfa7110e3562b2a87314d383e31d9af5d8283bfe08d74940bc54c54858b0d11'
$user_agent='Caavo_Fyra_v1.1.199'
$host_ip="34.231.212.186:81"
$series_count_test = 0
$exceptions_occured_series = []
$exceptions_occured_episode = []
$total_series_to_be_tested = 0
$series_mapped_count = 0
$series_unmapped_count = 0
$empty_search_results_count = 0
$exceptions_series = 0
$exceptions_episode = 0
$total_episodes_count = 0
$episodes_mapped_count = 0
$episodes_unmapped_count = 0
$blind_ingestion_pass_count = 0
$blind_ingestion_fail_count = 0
$series_rel_year_nil_count=0

#starting
#valid_series = ["9-1-1-lone-star"]
valid_series = total_series($mongo_source);
$total_series_to_be_tested = valid_series.length
$log.info "-------------------------------------------------------------------------"
$log.info "valid_series #{valid_series}"
$log.info "-------------------------------------------------------------------------"

valid_series.each do |series_id|
    ser_id = series_id
    query_series_id = $collection.find({"item_type":"tvshow","service":$mongo_source,"id":"#{ser_id}"})
                                .projection({"_id":0,"service":1,"title":1,"release_year":1,"id":1,})
    #create new array                            
    ne = Array.new
    query_series_id.each do |details|
        begin
            $series_count_test = $series_count_test +1
            print_count();
            $log.info "counts finished for this round ..........................."
            details = details.to_json
            details = JSON.parse(details)
            ne << details
            title_totest = ne[0]["title"]
            title_totest_m = mod_title(title_totest);
            $log.info "series title under test is #{title_totest_m}"
            rel_year_totest = ne[0]["release_year"]
            rel_year_totest = rel_year_totest.to_i
            show_type_totest = "SM"
            launch_id_totest = ne[0]["id"]
            $log.info "series ID under test is #{launch_id_totest}"
            temp_seasonno = 0
            temp_relyr = 0
            rel_year_totest = rel_year_totest.to_i
            $log.info "release year before computing is #{rel_year_totest}"
            $log.info rel_year_totest.inspect
            if rel_year_totest == 0
                $log.info "creating release year by my own because it is 0..."
                mon_sns = []
                mon_sns = $collection.distinct(("season_number"),{"item_type":"episode","service":$mongo_source,"series_id":"#{launch_id_totest}"})
                $log.info "mongo seasons are #{mon_sns}"
                $log.info mon_sns.inspect
                #mon_sns.map(&:to_i)
                mon_sns = mon_sns.collect{|i| i.to_i}
                temp_seasonno = mon_sns.max
                $log.info "temporary season number before computing is #{temp_seasonno}"
                mon_relyr = $collection.find({"item_type":"episode","service":$mongo_source,"series_id":"#{launch_id_totest}","season_number":"#{temp_seasonno}"}).projection({"_id":0,"release_year":1}).limit(1)
                mon_relyr.each do |ry|
                    ry = ry.to_json
                    ry = JSON.parse(ry)
                    temp_relyr = (ry["release_year"]).to_i
                    $log.info "temporary release year to test is #{temp_relyr}"
                end
                if temp_seasonno == temp_relyr
                    rel_year_totest = temp_relyr
                else
                    temp_seasonno = temp_seasonno - 1
                    rel_year_totest = temp_relyr - temp_seasonno
                end
            end
            $log.info "release year in test after computing is #{rel_year_totest}"
            if rel_year_totest > 0
                search_response = voice_search_pagination(title_totest,"ott_search")
                search_result = JSON.parse((search_response.to_json))
                #$log.info "total responses from all the pages: #{r}"
                $log.info "total responses from all the pages came"
                $series_mapped_flag = 0
                $title_mapped_flag = 0
                blind_ingest_status = []
                blind_ingest_status = check_blind_projectx_ingested(launch_id_totest);
                $log.info blind_ingest_status
                if blind_ingest_status[0] == "Pass"
                    $log.info "blind ingestion pass"
                    $blind_ingestion_pass_count = $blind_ingestion_pass_count + 1
                    blind_ingest_id = blind_ingest_status[1]
                    if !search_result.empty?
                        $log.info "Got search results... Not empty#{title_totest_m}"
                        search_result.each do |obj|
                            $log.info "beginning....*******************************"
                            #begin
                            aliases_array = []
                            aliases_temp_array = []
                            alias_match_flag = 0
                            program_longtitle = obj["object"]["long_title"]
                            $log.info "#{program_longtitle}"
                            program_longtitle = mod_title(program_longtitle);
                            $log.info "program longtitle is #{program_longtitle}"
                            program_originaltitle = obj["object"]["original_title"]
                            program_originaltitle = mod_title(program_originaltitle);
                            $log.info "program originaltitle is #{program_originaltitle}"
                            if obj["object"]["aliases"] != nil
                                aliases_temp_array = obj["object"]["aliases"]
                                $log.info "px aliases are #{aliases_temp_array}"
                                aliases_array = get_aliases(aliases_temp_array);
                                if aliases_array.include? title_totest
                                    alias_match_flag = 1
                                end
                            else
                                alias_match_flag = 0
                            end

                          # title_match_result=check_title_match(title_totest,program_longtitle)  
                          # if title_match_result== "True"
                          #    $log.info "first level series mapped"
                          # else
                          #    title_match_result=check_title_match(title_totest,program_originaltitle)  
                          #    if title_match_result == "True"
                          #       $log.info "first level series mapped" 
                            if ((program_longtitle == title_totest_m) || (program_originaltitle == title_totest_m) || (alias_match_flag == 1))
                                $log.info "first level series mapped"
                                if obj["object"]["show_type"] == "SM"
                                    $title_mapped_flag =1
                                    $log.info "second level series mapped"
                                    if (obj["object"]["release_year"] == rel_year_totest) || (obj["object"]["release_year"] == rel_year_totest-1) || (obj["object"]["release_year"] == rel_year_totest+1)
                                        $series_mapped_flag = 1
                                        $series_mapped_count = $series_mapped_count + 1
                                        $log.info "Series got mapped and going for episode ID"
                                        $log.info "Mapped Px_series is #{obj["object"]["series_id"]}"
                                        #$log.info "episode title from input is #{episode_title_totest_m}"
                                        $series_id = obj["object"]["series_id"]
                                        dist_seasons_q = $collection.distinct(("season_id"),{"item_type":"episode","service":$mongo_source,"series_id":"#{ser_id}"})
                                        dist_seasons = []
                                        $log.info dist_seasons_q.inspect
                                        $log.info "distinct sesons for this series are #{dist_seasons_q}"
                                        dist_seasons_q.each do |id|
                                            dist_seasons << id
                                        end
                                        dist_seasons.each do |eps|
                                            $log.info "season under test is #{eps}"
                                            mon_ep = $collection.find({"item_type":"episode","series_id":"#{ser_id}","service":$mongo_source,"season_id":"#{eps}"}).projection({"_id":0,"episode_title":1,"season_number":1,"episode_number":1,"id":1,})
                                            episodes_season = []
                                            mon_ep.each do |des|
                                                des = des.to_json
                                                des = JSON.parse(des)
                                                episodes_season << des
                                            end                                            
                                            $log.info "episodes from mongo db are #{episodes_season}"
                                            empty_season_api_flag = 1
                                            empty_all_api_flag = 1
                                            episodes_oz = JSON.parse(RestClient.get("#{$env_domain}programs/#{$series_id}/episodes?ott=true&service=#{$service}&season_number=#{episodes_season[0]["season_number"]}", {:authorization => $search_token,:user_agent => $user_agent}))
                                            episodes_all_oz = JSON.parse(RestClient.get("#{$env_domain}programs/#{$series_id}/episodes?ott=true&service=#{$service}", {:authorization => $search_token,:user_agent => $user_agent}))
                                            $total_episodes_count = $total_episodes_count + episodes_season.length
                                            episodes_season.each do |start|
                                                begin
                                                    final_result = []
                                                    episode_title_to_test = start["episode_title"]
                                                    season_number_to_test = start["season_number"]
                                                    episode_number_to_test = start["episode_number"]
                                                    episode_ott_to_test = start["id"]
                                                    $log.info "episode under test is #{episode_title_to_test}"
                                                    $log.info "season number under test is #{season_number_to_test}"
                                                    $log.info "episode number under test is #{episode_number_to_test}"
                                                    $log.info "episode video link under test is #{episode_ott_to_test}"
                                                    final_result << title_totest
                                                    final_result << rel_year_totest
                                                    final_result << ser_id
                                                    final_result << "series mapped"
                                                    final_result << "NA"
                                                    final_result << blind_ingest_id
                                                    final_result << $series_id
                                                    if $series_id == blind_ingest_id
                                                        final_result << "Search result ID is same as Ingested ID"
                                                    else
                                                        final_result << "Search result ID is not same as Ingested ID"
                                                    end
                                                    final_result << episode_title_to_test
                                                    final_result << season_number_to_test
                                                    final_result << episode_number_to_test
                                                    final_result << episode_ott_to_test
                                                    episode_mapped_flag = 0
                                                    if episodes_oz.length > 0
                                                        episodes_oz.each do |resp|
                                                            $log.info "PX episode title under test is #{resp["episode_title"]}"
                                                            $log.info "PX Original episode title under test is #{resp["original_episode_title"]}"
                                                            if (resp["episode_title"].downcase == episode_title_to_test.downcase) || (resp["original_episode_title"].downcase == episode_title_to_test.downcase)
                                                                flag = 1
                                                                $log.info "episode ID under test is #{resp["id"]}"
                                                                episode_mapped_flag = 1
                                                                final_result << "true"
                                                                final_result << "Ep Title"
                                                                final_result << resp["id"]
                                                                final_result <<  resp["episode_season_number"]
                                                                final_result <<  resp["episode_season_sequence"]
                                                                $episodes_mapped_count = $episodes_mapped_count + 1 
                                                                ro_vid = Array.new
                                                                ro_vid = resp["videos"]
                                                                rvid = Array.new
                                                                ro_vid.each do |rv|
                                                                    hvid = Hash.new
                                                                    hvid["service"] = rv["source_id"]
                                                                    if hvid["service"] == "amazon"
                                                                        hvid["url"] = rv["source_program_id"]
                                                                        rvid << hvid
                                                                    else
                                                                        # hvid["url"] = rv["link"]["uri"]
                                                                        # rvid << hvid
                                                                        hvid["url"] = rv["source_program_id"]
                                                                        rvid << hvid
                                                                    end
                                                                end
                                                                rvid = rvid.collect {|x| [x["service"], x["url"]]}.inject({}) {|memo, (x,y)| memo[x].nil? ? memo[x] = [y] : memo[x] << y; memo}
                                                                ret = process(episode_ott_to_test,rvid);
                                                                final_result += ret
                                                                $log.info "///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////"
                                                                $log.info "final result is #{final_result}"
                                                                $log.info "///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////"
                                                                CSV.open("#{run_date}Headrun_validation_Preprod.csv","a+") do |cs|
                                                                    cs << final_result
                                                                end
                                                            end
                                                            if episode_mapped_flag == 1
                                                                break
                                                            else
                                                                $log.info "Episode didnt got mapped in season_number API"
                                                            end
                                                        end
                                                    else
                                                        $log.info "Empty results in Season number API"
                                                        empty_season_api_flag = 1
                                                    end
                                                    if episode_mapped_flag == 0
                                                        episode_mapped_flag_all = 0
                                                        if episodes_all_oz.length >0
                                                            episodes_all_oz.each do |resp|
                                                                #$log.info "entered All Episodes API for episode matching........"
                                                                if (resp["episode_title"].downcase == episode_title_to_test.downcase) || (resp["original_episode_title"].downcase == episode_title_to_test.downcase)
                                                                    flag =1
                                                                    $log.info "episode ID under test is #{resp["id"]}" 
                                                                    episode_mapped_flag_all = 1
                                                                    $episodes_mapped_count = $episodes_mapped_count + 1
                                                                    final_result << "true"
                                                                    final_result << "All Episodes"
                                                                    final_result << resp["id"]
                                                                    final_result <<  resp["episode_season_number"]
                                                                    final_result <<  resp["episode_season_sequence"]                                                
                                                                    ro_vid = Array.new
                                                                    ro_vid = resp["videos"]
                                                                    rvid = Array.new
                                                                    ro_vid.each do |rv|
                                                                        hvid = Hash.new
                                                                        hvid["service"] = rv["source_id"]
                                                                        if hvid["service"] == "amazon"
                                                                            hvid["url"] = rv["source_program_id"]
                                                                            rvid << hvid
                                                                        else
                                                                            hvid["url"] = rv["source_program_id"]
                                                                            rvid << hvid
                                                                        end
                                                                    end
                                                                    rvid = rvid.collect {|x| [x["service"], x["url"]]}.inject({}) {|memo, (x,y)| memo[x].nil? ? memo[x] = [y] : memo[x] << y; memo}
                                                                    ret = process(episode_ott_to_test,rvid);
                                                                    final_result += ret
                                                                    $log.info "///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////"
                                                                    $log.info "final result is #{final_result}"
                                                                    $log.info "///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////"
                                                                    CSV.open("#{run_date}Headrun_validation_Preprod.csv","a+") do |cs|
                                                                        cs << final_result
                                                                    end
                                                                end
                                                                if episode_mapped_flag_all == 1
                                                                    break
                                                                else
                                                                    $log.info "Episode didnt got mapped in ALl episodes API also "
                                                                end
                                                            end
                                                        else
                                                            $log.info "Empty results in All Episodes API"
                                                            empty_all_api_flag = 1
                                                        end
                                                    end
                                                    if (episode_mapped_flag == 0 && episode_mapped_flag_all == 0)
                                                        $episodes_unmapped_count = $episodes_unmapped_count + 1
                                                        final_result << "false"
                                                        CSV.open("#{run_date}Headrun_validation_Preprod.csv","a+") do |cs|
                                                            cs << final_result
                                                        end
                                                    end
                                                rescue Exception => ex
                                                    $log.info "!!!Danger!!!...!!!Danger!!!...!!!Danger!!!...!!!Danger!!!...!!!episode!!!"
                                                    $log.info ex
                                                    $log.info ex.backtrace
                                                    $exceptions_occured_episode << ex
                                                    #$log.info "exception: #{exc}"
                                                    $exceptions_episode = $exceptions_episode + 1
                                                    $episodes_unmapped_count = $episodes_unmapped_count + episodes_season.length
                                                    next
                                                end
                                            end
                                        end
                                    end
                                end
                            else
                                #########################
                                $log.info "Under Implementation"
                            end
                            if $series_mapped_flag == 1
                                break
                            end
                        end
                    else
                        $log.info "Got search results... but empty"
                        $empty_search_results_count = $empty_search_results_count + 1
                        $series_unmapped_count = $series_unmapped_count + 1
                        mon_ep = $collection.find({"item_type":"episode","service":$mongo_source,"series_id":"#{ser_id}"}).projection({"_id":0,"id":1})
                        episodes_series = []
                        mon_ep.each do |des|
                            des = des.to_json
                            des = JSON.parse(des)
                            episodes_series << des
                        end
                        $total_episodes_count = $total_episodes_count + episodes_series.length
                        $episodes_unmapped_count = $episodes_unmapped_count + episodes_series.length
                        final_result = []
                        final_result << title_totest
                        final_result << rel_year_totest
                        final_result << launch_id_totest
                        final_result << "Empty search results"
                        final_result << "NA"
                        final_result << blind_ingest_id
                        CSV.open("#{run_date}Headrun_validation_Preprod.csv","a+") do |cs|
                            cs << final_result
                        end
                        $log.info "Empty search results"
                    end
                    if $series_mapped_flag == 0 && !search_result.empty?
                        $series_unmapped_count = $series_unmapped_count + 1
                        mon_ep = $collection.find({"item_type":"episode","service":$mongo_source,"series_id":"#{ser_id}"}).projection({"_id":0,"id":1})
                        episodes_series = []
                        mon_ep.each do |des|
                            des = des.to_json
                            des = JSON.parse(des)
                            episodes_series << des
                        end
                        $total_episodes_count = $total_episodes_count + episodes_series.length
                        $episodes_unmapped_count = $episodes_unmapped_count + episodes_series.length
                        final_result = []
                        final_result << title_totest
                        final_result << rel_year_totest
                        final_result << launch_id_totest
                        final_result << "series not mapped"
                        final_result << "NA"
                        final_result << blind_ingest_id
                        CSV.open("#{run_date}Headrun_validation_Preprod.csv","a+") do |cs|
                            cs << final_result
                        end
                        $log.info "titles not matched"
                    end
                else
                    mon_ep = $collection.find({"item_type":"episode","service":$mongo_source,"series_id":"#{ser_id}"}).projection({"_id":0,"id":1})
                    episodes_series = []
                    mon_ep.each do |des|
                        des = des.to_json
                        des = JSON.parse(des)
                        episodes_series << des
                    end
                    $total_episodes_count = $total_episodes_count + episodes_series.length
                    $episodes_unmapped_count = $episodes_unmapped_count + episodes_series.length
                    $series_unmapped_count = $series_unmapped_count + 1
                    final_result = []
                    $log.info "blind ingestion failed"
                    $blind_ingestion_fail_count = $blind_ingestion_fail_count + 1
                    final_result << title_totest
                    final_result << rel_year_totest
                    final_result << launch_id_totest
                    final_result << "Blind Ingestion Failure"
                    final_result << blind_ingest_status[1] 
                    CSV.open("#{run_date}Headrun_validation_Preprod.csv","a+") do |cs|
                        cs << final_result
                    end
                end
            else
                $log.info "under implementation as release year is nil from source(headrun)........."
                $series_rel_year_nil_count = $series_rel_year_nil_count + 1
            end    
        rescue Exception => ex
            $log.info "!!!Danger!!!...!!!Danger!!!...!!!Danger!!!...!!!Danger!!!...!!!series!!!"
            $log.info ex
            $log.info ex.backtrace
            $exceptions_occured_series << ex
            $exceptions_series = $exceptions_series + 1
            mon_ep = $collection.find({"item_type":"episode","service":$mongo_source,"series_id":"#{ser_id}"}).projection({"_id":0,"id":1})
            episodes_series = []
            mon_ep.each do |des|
                des = des.to_json
                des = JSON.parse(des)
                episodes_series << des
            end
            $total_episodes_count = $total_episodes_count + episodes_series.length
            $episodes_unmapped_count = $episodes_unmapped_count + episodes_series.length
            next
        end
    end
end
print_count();
client.close