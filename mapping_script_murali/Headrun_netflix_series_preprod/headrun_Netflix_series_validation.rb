require 'rest_client'
require 'mongo'
require 'json'
require 'csv'
require 'date'
run_date = Date.today
run_date = run_date.strftime("%Y%m%d")
$link_not_available = 0
$link_mismatch_count = 0
$link_match_count = 0
CSV.open("#{run_date}Headrun_validation_Preprod.csv","w+") do |cs|
    cs << ["SM_Title", "rel_yr", "NF_SM_ID", "SM_Map_Status","Blind_ID","OZ_SM_ID","Blind_Status", "EP_Title","SN_NO","EP_NO", "Launch_ID", "status", "mapped_by", "OZ_EP_ID", "OZ_SN_NO", "OZ_EP_NO", "OZ_OTT"]
end

def check_projectx_ingested(vu_ser_id)
    retry_cnt = 3
    begin
        pjx_body = JSON.parse(RestClient.get("http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=#{vu_ser_id}&sourceName=Netflixusa&showType=SM", {:authorization => 'Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3',:user_agent => 'Caavo_Fyra_v1.1.199',:host => '34.231.212.186:81'}))
        if !pjx_body.empty?
            ingestion_status = []
            ingestion_status << "Ingested properly but not available in search"
            ingestion_status << pjx_body[0]["projectx_id"]
            return ingestion_status
        else
            r = check_projectx_duplicate(vu_ser_id);
            if r == nil
                ingestion_status = []
                ingestion_status << "Not Ingested"
                ingestion_status << "NA"
                return ingestion_status
            else
                ingestion_status = []
                ingestion_status << "part of duplictes"
                ingestion_status << "#{r}"
                return ingestion_status
            end
        end
    rescue Exception => err
        puts "Exception in ProjectX Ingested Method"
        puts "Error!!!: #{err}"
        puts "Error!!!: #{err.backtrace}"
        retry_cnt -= 1
        if retry_cnt > 0
            sleep 2
            retry
        else
            puts "Exceeded retry count in ProjectX Ingested Method: #{retry_cnt}"
        end
    end
end

def check_blind_projectx_ingested(vu_ser_id)
    retry_cnt = 3
    begin
        #pjx_body = JSON.parse(RestClient.get("http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=#{vu_ser_id}&sourceName=Vudu&showType=SM", {:authorization => 'Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3',:user_agent => 'Caavo_Fyra_v1.1.199',:host => '34.231.212.186:81'}))
        pjx_body = JSON.parse(RestClient.get("http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=#{vu_ser_id}&sourceName=Netflixusa&showType=SM", {:authorization => 'Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3',:user_agent => 'Caavo_Fyra_v1.1.199',:host => '34.231.212.186:81'}))
        ingestion_status = []
        if !pjx_body.empty?            
            ingestion_status << "Pass"
            ingestion_status << pjx_body[0]["projectx_id"]   
            return ingestion_status         
        else 
            r = check_projectx_duplicate(vu_ser_id);
            if r == nil
                ingestion_status = []
                ingestion_status << "Fail"
                ingestion_status << "NA"
                return ingestion_status
            else
                ingestion_status = []
                ingestion_status << "part of duplictes"
                ingestion_status << "#{r}"
                return ingestion_status
            end           
            ingestion_status << "Fail"
            ingestion_status << "NA"            
        end    
    rescue Exception => err 
        puts "Exception in Blind ProjectX Ingestion Method"
        puts "Error!!!: #{err}"
        puts "Error!!!: #{err.backtrace}"
        retry_cnt -= 1
        if retry_cnt > 0
            sleep 2
            retry
        else
            puts "Exceeded retry count in ProjectX Ingested Method: #{retry_cnt}"
        end
    end
end

def check_projectx_duplicate(vu_ser_id)
    retry_cnt = 3
    begin
        dup_body = JSON.parse(RestClient.get("http://34.231.212.186:81/projectx/duplicate?sourceId=#{vu_ser_id}&sourceName=Netflixusa&showType=SM", {:authorization => 'Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3',:user_agent => 'Caavo_Fyra_v1.1.199',:host => '34.231.212.186:81'}))
        if !dup_body.empty?
            return dup_body[0]["projectx_id"]
        else
            return nil
        end
    rescue Exception => err
        puts "Exception in ProjectX Duplicate Method"
        puts "Error!!!: #{err}"
        puts "Error!!!: #{err.backtrace}"
        retry_cnt -= 1
        if retry_cnt > 0
            sleep 2
            retry
        else
            puts "Exceeded retry count in ProjectX Duplicate Method: #{retry_cnt}"
        end
    end
end

def check_ott_deleted(vu)
    retry_cnt = 3
    begin
        del_status = JSON.parse(RestClient.get("http://34.231.212.186:81/projectx/#{vu}/Netflixusa/isDeletedOttprojectx", {:authorization => 'Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3',:user_agent => 'Caavo_Fyra_v1.1.199',:host => '34.231.212.186:81'}))
        if !del_status.empty?
            return "true"
        else
            return "false"
        end
    rescue Exception => err
        puts "Exception in Ott Deleted Method"
        puts "Error!!!: #{err}"
        puts "Error!!!: #{err.backtrace}"
        if retry_cnt > 0
            sleep 2
            retry
        else
            puts "Exceeded retry count in Ott Deleted Method: #{retry_cnt}"
        end
    end
end

def check_ott_expiry(vu)
    retry_cnt = 3
    begin
        exp_status = JSON.parse(RestClient.get("https://preprod.caavo.com/expired_ott/is_available?source_program_id=#{vu}&service_short_name=Netflixusa", {:authorization => 'Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7',:user_agent => 'Caavo_Fyra_v1.1.199'}))
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
        puts "Exception in Ott Expiry Method"
        puts "Error!!!: #{err}"
        puts "Error!!!: #{err.backtrace}"
        if retry_cnt > 0
            sleep 2
            retry
        else
            puts "Exceeded retry count in Ott Expiry Method: #{retry_cnt}"
        end
    end
end

def process(source_ott, rovi)
  map_flag = 0
  puts "Video links under test are..... #{source_ott}"
  result_vid = []
  rovi_vids_array = Array.new
  rovi.keys.each do |s|
    if s == "netflixusa"
      rovi[s].each do |values|
        b = values
        rovi_vids_array << b
        # rovi_vids_array << b.match(/http[s]?:\/\/(www.)?netflix\.com\/([a-zA-Z?\/=]+)([0-9]+)/)[3]
      end
    end            
    puts "Ozone links are.....#{rovi_vids_array}"
    if rovi_vids_array.length == 0
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
  		ind = rovi_vids_array.index(source_ott)
  		if ind == nil
  			$link_mismatch_count = $link_mismatch_count + 1
  			result_vid << "links didn't matched"
  			result_vid.concat rovi_vids_array
  		else
  			$link_match_count = $link_match_count + 1
  			result_vid << "links matched"
  			result_vid.concat rovi_vids_array
  		end
  	end
  end
	return result_vid
end

def get_index_of_ott_search_object(json_body)
  puts "using my code only indexof search object"
  index_of_ott_search_obj = nil
  no_of_arr_in_complete_resp = json_body.length
  for i in 0..no_of_arr_in_complete_resp-1
    puts "action type: #{json_body[i]["action_type"]}"
    if json_body[i]["action_type"] == "ott_search"
      index_of_ott_search_obj = i
      puts "Index of ott search object is: #{i}"
      break
    end
  end
  index_of_ott_search_obj
end

def voice_search_pagination(search_term,tab)
  puts "using my code only voicesearch pagination"
  total_response = Array.new
  results_array = Array.new
  retry_cnt = 3
    begin
    uri = URI.encode("https://preprod.caavo.com/v3/voice_search?web=false&aliases=true&q=#{search_term}")
    #json_body = JSON.parse(RestClient.get("https://preprod.caavo.com/v3/voice_search?aliases=true&q=#{search_term}", {:authorization => 'Token token=ddfa7110e3562b2a87314d383e31d9af5d8283bfe08d74940bc54c54858b0d11',:user_agent => 'Caavo_Fyra_v1.1.199'}))
    json_body = JSON.parse(RestClient.get(uri, {:authorization => 'Token token=ddfa7110e3562b2a87314d383e31d9af5d8283bfe08d74940bc54c54858b0d11',:user_agent => 'Caavo_Fyra_v1.1.199'}))
    rescue Exception => err 
      puts "Error in getting response <br>"
      puts "Error!!!: #{err} <br"
      puts err.backtrace
      retry_cnt -= 1
      if retry_cnt > 0
        sleep 10
        retry
      else
        puts "retry count: #{retry_cnt}"
      end
    end  
  results_array = json_body["results"]
  if results_array.length > 0
    puts "json_body_results_length : #{(json_body["results"]).length}"
    if tab == "ott_search"
      ott_search_index = get_index_of_ott_search_object(results_array)
    elsif tab == "epg_search"
      puts "not required"
    elsif tab == "upcoming_epg_search"
      puts "not required"
    elsif tab == "web_results"
      puts "not required"
    end     
    other_responses = Array.new
    if ott_search_index != nil
      puts "#{results_array[ott_search_index]}"
      if results_array[ott_search_index].key?("next_page_params")
        puts "page_params key exists"
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
        puts "No page_params key present ; hence getting existing results"
        total_response =  results_array[ott_search_index]["results"]
      end
    else
      $state = "No requested object found in results"
      puts "#{$state}"
    end    
  else
    $state = "empty results from cloud"
    puts "#{$state}"
  end      
    total_response
end  

def collect_all_pages_info(url)
    puts "using my code only collect all pages info"
    rest_results_array = Array.new
    next_key = true
    while (next_key)
      retry_cnt = 3
      begin
        puts "next page url to query: #{url}"
        uri = URI.encode("https://preprod.caavo.com/#{url}")
        json_body = JSON.parse(RestClient.get(uri, {:authorization => 'Token token=ddfa7110e3562b2a87314d383e31d9af5d8283bfe08d74940bc54c54858b0d11',:user_agent => 'Caavo_Fyra_v1.1.199'}))
      rescue Exception => err 
        puts "Error in getting response <br>"
        puts "Error!!!: #{err} <br"
        puts err.backtrace
        retry_cnt -= 1
        if retry_cnt > 0
          sleep 10
          retry
        else
          puts "retry count: #{retry_cnt}"
        end
      end
      if (json_body["results"]).length > 0
        #puts "current next_page response : #{json_body}"
        puts "current next_page results response length: #{((json_body["results"])[0]["results"]).length}"
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
        puts "No results found in the URL obtained"
        next_key = false
      end   
    end
    puts "length of next pages response obtained: #{rest_results_array.length}"  
    rest_results_array
end

def mod_title(title)
	title_mod = title.downcase
	#title_mod = title_mod.gsub(/^(the |an |a )/,'')
	title_mod = title_mod.gsub(/[;|:|\-|,|.|'|"|?|!|@|#| |]/,'')
	title_mod = title_mod.gsub(/&/,'and')
	return title_mod
end

def get_aliases(input_array)
    final_alias_array = []
    input_array.each do |al|
        if al["source_name"] == "Rovi"
            if al["type"] == "long_title"
                final_alias_array << al["alias"]
            elsif al["type"] == "original_title"
                final_alias_array << al["alias"]
            elsif al["type"] == "alias_title"
                final_alias_array << al["alias"]
            elsif al["type"] == "alias_title2"
                final_alias_array << al["alias"]
            elsif al["type"] == "alias_title3"
                final_alias_array << al["alias"]
            elsif al["type"] == "alias_title4"
                final_alias_array << al["alias"]
            end
        elsif al["source_name"] == "Vudu"
            if al["type"] == "title"
                final_alias_array << al["alias"]
            end
        elsif al["source_name"] == "Hulu"
            if al["type"] == "title"
                final_alias_array << al["alias"]
            end           
        elsif al["source_name"] == "GuideBox"
            if al["type"] == "title"
                final_alias_array << al["alias"]
            elsif al["type"] == "original_title"
                final_alias_array << al["alias"]
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
	#series = $collection.find({"item_type":"tvshow","service":"#{service}"}).projection({"_id":0,"id":1}).limit(2)
	series.each do |a|
		a = a.to_json
		a = JSON.parse(a)
		arr << a["id"]
	end
	arr = arr.uniq
	#puts "avaialble series are #{arr}"
	puts "total no.of series available : #{arr.length}"
	arr.each do |b|
		series_validity = $collection.count({"item_type":"episode","service":"#{service}","series_id":"#{b}"})
		puts "-------------------------------------------------------------------------"
		p series_validity
		puts "-------------------------------------------------------------------------"
		if series_validity != 0
			arr_valid << b
		else
			arr_invalid << b
		end
	end
	#puts "total no.of valid series available : #{arr_valid.length}"
	#puts "total no.of invalid series available : #{arr_invalid.length}"
	puts "valid series are #{arr_valid}"
	puts "invalid series are #{arr_invalid}"
	return arr_valid
end

client = Mongo::Client.new(['127.0.0.1:27017'],:database => 'qadb') #Connecting to Mongo and creating a DB 'sample'.....use 
$collection = client[:headrun]
# series = $collection.find({"item_type":"tvshow"}).projection({"_id":0,"id":1})
# series.each do |

series_count_test = 0
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

valid_series = ["70304718"]
# valid_series = []
# valid_series = total_series("netflix");
$total_series_to_be_tested = valid_series.length
#puts "-------------------------------------------------------------------------"
#p valid_series
#puts "-------------------------------------------------------------------------"

valid_series.each do |tp|
	ser_id = tp
	mon = $collection.find({"item_type":"tvshow","service":"netflix","id":"#{ser_id}"}).projection({"_id":0,"service":1,"title":1,"release_year":1,"id":1,})
    ne = Array.new
    mon.each do |tt|
    	begin
	    	series_count_test = series_count_test +1
    		puts "series test count is #{series_count_test}"
	    	puts "counts are as follows ..........................."
  			puts "total no.of series are #{$total_series_to_be_tested}"
  			puts "Series mapped count is #{$series_mapped_count}"
  			puts "Series unmapped count is #{$series_unmapped_count}"
  			puts "Empty search results count is #{$empty_search_results_count}"
  			puts "Exceptions in series are #{$exceptions_series}"
  			puts "Total Episodes count is #{$total_episodes_count}"
  			puts "Total Episodes mapped count is #{$episodes_mapped_count}"
  			puts "Total Episodes unmapped count is #{$episodes_unmapped_count}"
  			puts "Exceptions in Episodes are #{$exceptions_episode}"
        puts "Blind Ingestion pass count is  #{$blind_ingestion_pass_count}"
        puts "Blind Ingestion fail count is  #{$blind_ingestion_fail_count}"
			puts "counts finished for this round ..........................."
	        tt = tt.to_json
	        tt = JSON.parse(tt)
	        ne << tt
	        title_totest = ne[0]["title"]
	        title_totest_m = mod_title(title_totest);
	        puts "series title under test is #{title_totest_m}"
	        rel_year_totest = ne[0]["release_year"]
	        rel_year_totest = rel_year_totest.to_i
	        show_type_totest = "SM"
	        launch_id_totest = ne[0]["id"]
            puts "series ID under test is #{launch_id_totest}"
	        r = voice_search_pagination(title_totest,"ott_search")
	        r = JSON.parse((r.to_json))
	        #puts "total responses from all the pages: #{r}"
	        puts "total responses from all the pages came"
	        $series_mapped_flag = 0
	        $title_mapped_flag = 0
            blind_ingest_status = []
            blind_ingest_status = check_blind_projectx_ingested(launch_id_totest);
            puts blind_ingest_status
            if blind_ingest_status[0] == "Pass"
                puts "blind ingestion pass"
                $blind_ingestion_pass_count = $blind_ingestion_pass_count + 1
                blind_ingest_id = blind_ingest_status[1]
    	        if !r.empty?
    	            puts "Got search results... Not empty"
    	            r.each do |obj|
    	            	puts "beginning....*******************************"
    	                #begin
                        aliases_array = []
                        aliases_temp_array = []
                        alias_match_flag = 0
    	                rovi_longtitle = obj["object"]["long_title"]
    	                puts "#{rovi_longtitle}"
    	                rovi_longtitle = mod_title(rovi_longtitle);
    	                puts "rovi longtitle is #{rovi_longtitle}"
    	                rovi_originaltitle = obj["object"]["original_title"]
    	                rovi_originaltitle = mod_title(rovi_originaltitle);
    	                puts "rovi longtitle is #{rovi_originaltitle}"
                        if obj["object"]["aliases"] != nil
                            aliases_temp_array = obj["object"]["aliases"]
                            puts "rovi aliases are #{aliases_temp_array}"
                            aliases_array = get_aliases(aliases_temp_array);
                            if aliases_array.include? title_totest
                                alias_match_flag = 1
                            end
                        else
                            alias_match_flag = 0
                        end
    	                if ((rovi_longtitle == title_totest_m) || (rovi_originaltitle == title_totest_m) || (alias_match_flag == 1))
    	                	puts "first level series mapped"
    	            		if obj["object"]["show_type"] == "SM"
    	            			$title_mapped_flag =1
    	            			puts "second level series mapped"
    	            			if (obj["object"]["release_year"] == rel_year_totest) || (obj["object"]["release_year"] == rel_year_totest-1) || (obj["object"]["release_year"] == rel_year_totest+1)
    	            				$series_mapped_flag = 1
    	            				$series_mapped_count = $series_mapped_count + 1
    	            				puts "Series got mapped and going for episode ID"
    	            				puts "Mapped series is #{obj["object"]["series_id"]}"
    	            				#puts "episode title from input is #{episode_title_totest_m}"
    	            				$series_id = obj["object"]["series_id"]
    	            				dist_seasons_q = $collection.distinct(("season_id"),{"item_type":"episode","service":"netflix","series_id":"#{ser_id}"})
    	            				dist_seasons = []
    	            				p dist_seasons_q.inspect
    	            				puts "distinct sesons for this series are #{dist_seasons_q}"
    	            				dist_seasons_q.each do |a|
    	            					# p a.inspect
    									# a = a.to_json
    									# a = JSON.parse(a)
    									#dist_seasons << a["id"]
    									dist_seasons << a
    								end
    								dist_seasons.each do |eps|
    									puts "season under test is #{eps}"
    									mon_ep = $collection.find({"item_type":"episode","service":"netflix","season_id":"#{eps}"}).projection({"_id":0,"episode_title":1,"season_number":1,"episode_number":1,"id":1,})
    									episodes_season = []
    									mon_ep.each do |des|
    										des = des.to_json
    										des = JSON.parse(des)
    										episodes_season << des
    									end
    									$total_episodes_count = $total_episodes_count + episodes_season.length
    									puts "episodes from mongo db are #{episodes_season}"
    									empty_season_api_flag = 1
    			                    	empty_all_api_flag = 1
    									episodes_oz = JSON.parse(RestClient.get("https://preprod.caavo.com/programs/#{$series_id}/episodes?ott=true&service=netflixusa&season_number=#{episodes_season[0]["season_number"]}", {:authorization => 'Token token=ddfa7110e3562b2a87314d383e31d9af5d8283bfe08d74940bc54c54858b0d11',:user_agent => 'Caavo_Fyra_v1.1.199'}))
    									episodes_all_oz = JSON.parse(RestClient.get("https://preprod.caavo.com/programs/#{$series_id}/episodes?ott=true&service=netflixusa", {:authorization => 'Token token=ddfa7110e3562b2a87314d383e31d9af5d8283bfe08d74940bc54c54858b0d11',:user_agent => 'Caavo_Fyra_v1.1.199'}))
    									episodes_season.each do |start|
    										begin
    											final_result = []
    											episode_title_to_test = start["episode_title"]
    											season_number_to_test = start["season_number"]
    											episode_number_to_test = start["episode_number"]
    											episode_ott_to_test = start["id"]
    											puts "episode under test is #{episode_title_to_test}"
    											puts "season number under test is #{season_number_to_test}"
    											puts "episode number under test is #{episode_number_to_test}"
    											puts "episode video link under test is #{episode_ott_to_test}"
    											final_result << title_totest
    											final_result << rel_year_totest
    											final_result << ser_id
    											final_result << "series mapped"
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
    			                                    episodes_oz.each do |ro|
    			                                        if (ro["episode_title"].downcase == episode_title_to_test.downcase) || (ro["original_episode_title"].downcase == episode_title_to_test.downcase)
    		                                            	flag = 1
    		                                            	puts "episode ID under test is #{ro["id"]}"
    		                                            	episode_mapped_flag = 1
    		                                            	final_result << "true"
    		                                            	final_result << "Ep Title"
    		                                            	final_result << ro["id"]
    				                                    	final_result <<  ro["episode_season_number"]
    				                                    	final_result <<  ro["episode_season_sequence"]
    				                                    	$episodes_mapped_count = $episodes_mapped_count + 1                                          
    		                                            	ro_vid = Array.new
    		                                            	ro_vid = ro["videos"]
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
    		                                            	puts "///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////"
    		                                            	puts "final result is #{final_result}"
    		                                            	puts "///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////"
    		                                            	CSV.open("#{run_date}Headrun_validation_Preprod.csv","a+") do |cs|
    		                                            	    cs << final_result
    		                                            	end
    		                                            end
    			                                        if episode_mapped_flag == 1
    			                                        	break
    			                                        else
    				                                    	puts "Episode didnt got mapped in season_number API"
    				                                    end
    			                                    end
    			                                else
    			                                	puts "Empty results in Season number API"
    				                            	empty_season_api_flag = 1
    				                            end
    				                            if episode_mapped_flag == 0
    			                                    episode_mapped_flag_all = 0
    			                                    if episodes_all_oz.length >0
    				                                    episodes_all_oz.each do |ro|
    				                                    	#puts "entered All Episodes API for episode matching........"
    				                                		if (ro["episode_title"].downcase == episode_title_to_test.downcase) || (ro["original_episode_title"].downcase == episode_title_to_test.downcase)
    					                                        flag =1
    					                                        puts "episode ID under test is #{ro["id"]}" 
    					                                        episode_mapped_flag_all = 1
    					                                        $episodes_mapped_count = $episodes_mapped_count + 1
    					                                        final_result << "true"
    					                                        final_result << "All Episodes"
    					                                        final_result << ro["id"]
    					                                    	final_result <<  ro["episode_season_number"]
    					                                    	final_result <<  ro["episode_season_sequence"]                                                
    					                                        ro_vid = Array.new
    					                                        ro_vid = ro["videos"]
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
    					                                        puts "///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////"
    					                                        puts "final result is #{final_result}"
    					                                        puts "///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////"
    					                                        CSV.open("#{run_date}Headrun_validation_Preprod.csv","a+") do |cs|
    					                                            cs << final_result
    					                                        end
    					                                    end
    					                                    if episode_mapped_flag_all == 1
    				                                        	break
    				                                        else
    				                                        	puts "Episode didnt got mapped in ALl episodes API also "
    				                                        end
    					                                end
    					                            else
    					                            	puts "Empty results in All Episodes API"
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
    											puts "!!!Danger!!!...!!!Danger!!!...!!!Danger!!!...!!!Danger!!!...!!!episode!!!"
    			    						    puts ex
    			    						    puts ex.backtrace
    			    						    $exceptions_occured_episode << ex
    			    						    #puts "exception: #{exc}"
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
    						puts "Under Implementation"
    					end
                        if $series_mapped_flag == 1
                            break
                        end
    				end
    			else
    				puts "Got search results... but empty"
    				$empty_search_results_count = $empty_search_results_count + 1
    				$series_unmapped_count = $series_unmapped_count + 1
    				mon_ep = $collection.find({"item_type":"episode","service":"netflix","series_id":"#{ser_id}"}).projection({"_id":0,"id":1})
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
                    final_result << blind_ingest_id
    				CSV.open("#{run_date}Headrun_validation_Preprod.csv","a+") do |cs|
    	                cs << final_result
    	            end
    				puts "Empty search results"
    			end
    			if $series_mapped_flag == 0 && !r.empty?
    				$series_unmapped_count = $series_unmapped_count + 1
    				mon_ep = $collection.find({"item_type":"episode","service":"netflix","series_id":"#{ser_id}"}).projection({"_id":0,"id":1})
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
                    final_result << blind_ingest_id
    				CSV.open("#{run_date}Headrun_validation_Preprod.csv","a+") do |cs|
    	                cs << final_result
    	            end
    				puts "titles not matched"
    			end
            else
                final_result = []
                puts "blind ingestion failed"
                $blind_ingestion_fail_count = $blind_ingestion_fail_count + 1
                final_result << title_totest
                final_result << rel_year_totest
                final_result << launch_id_totest
                final_result << "Blind Ingestion Failure"
                CSV.open("#{run_date}Headrun_validation_Preprod.csv","a+") do |cs|
                    cs << final_result
                end
            end
		rescue Exception => ex
			puts "!!!Danger!!!...!!!Danger!!!...!!!Danger!!!...!!!Danger!!!...!!!series!!!"
    	    puts ex
    	    puts ex.backtrace
    	    $exceptions_occured_series << ex
    	    #puts "exception: #{exc}"
    	    $exceptions_series = $exceptions_series + 1
    	    #$seasons_unmapped_count = $seasons_unmapped_count + 1
			mon_ep = $collection.find({"item_type":"episode","service":"netflix","series_id":"#{ser_id}"}).projection({"_id":0,"id":1})
			episodes_series = []
			mon_ep.each do |des|
				des = des.to_json
				des = JSON.parse(des)
				# bonus = 0
				# bonus = des["duration"]
				# bonus = bonus.to_i
				# if bonus > 180
				# 	episodes_season << des
				# end
				episodes_series << des
			end
			$total_episodes_count = $total_episodes_count + episodes_series.length
			$episodes_unmapped_count = $episodes_unmapped_count + episodes_series.length
    	    next
    	end
	end
end


puts "Final counts are as follows ..........................."
puts "total no.of series are #{$total_series_to_be_tested}"
puts "Series mapped count is #{$series_mapped_count}"
puts "Series unmapped count is #{$series_unmapped_count}"
puts "Empty search results count is #{$empty_search_results_count}"
puts "Exceptions in series are #{$exceptions_series}"
puts "Total Episodes count is #{$total_episodes_count}"
puts "Total Episodes mapped count is #{$episodes_mapped_count}"
puts "Total Episodes unmapped count is #{$episodes_unmapped_count}"
puts "Exceptions in Episodes are #{$exceptions_episode}"
puts "Links match count is  #{$link_match_count}"    
puts "Wrong Links count is  #{$link_mismatch_count}"    
puts "Links Ingestion fail count is  #{$link_not_available}"