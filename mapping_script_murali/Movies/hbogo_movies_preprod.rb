require 'rest_client'
require 'mysql2'
require 'csv'
require 'json'
require 'date'
run_date = Date.today
run_date_csv = run_date.strftime("%Y%m%d")
def mod_title (title)
    title_mod = title.downcase
    title_mod = title_mod.gsub(/^(the |an |a )/,'')
    title_mod = title_mod.gsub(/[;|:|\-|,|.|'|?|!|@|#]/,'')
    title_mod = title_mod.gsub(/&/,'and')
    return title_mod
end

def get_index_of_ott_search_object(json_body)
  #puts "using my code only indexof search object"
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
  #puts "using my code only voicesearch pagination"
  total_response = Array.new
  results_array = Array.new
  retry_cnt = 3
    begin
    json_body = JSON.parse(RestClient.get("https://preprod.caavo.com/v3/voice_search?ott=true&service=hbogo&aliases=true&q=#{URI.escape("#{search_term}")}", {:authorization => 'Token token=ddfa7110e3562b2a87314d383e31d9af5d8283bfe08d74940bc54c54858b0d11',:user_agent => 'Caavo_Fyra_v1.1.199'}))
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
        final_next_url = "/v3/voice_search?query=" + "#{query}" +"&search_id=" + "#{search_id}" + "&page=" + "#{page}" + "&filter=" + "#{filter}" + "&ott=true" + "&service=hbogo" + "&aliases=true"
        other_responses = collect_all_pages_info(final_next_url);
        #total_response << other_responses
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
        json_body = JSON.parse(RestClient.get("https://preprod.caavo.com/#{url}", {:authorization => 'Token token=ddfa7110e3562b2a87314d383e31d9af5d8283bfe08d74940bc54c54858b0d11',:user_agent => 'Caavo_Fyra_v1.1.199'}))
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
        #puts "current next_page results response length: #{((json_body["results"])[0][:results]).length}"
        rest_results_array = rest_results_array + (json_body["results"])[0]["results"]
        if json_body["results"][0].key?("next_page_url")
          #url = "/v3/voice_search?query=" + "#{query}" +"&search_id=" + "#{search_id}" + "&page=" + "#{page}" + "&filter=" + "#{filter}" +"&ott=true" + "&aliases=true"
          url = json_body["results"][0]["next_page_url"] + "&ott=true" + "&service=hbogo" + "&aliases=true"
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

def check_projectx_ingested(link_ser_id)
    retry_cnt = 3
    begin
        pjx_body = JSON.parse(RestClient.get("http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=#{link_ser_id}&sourceName=HBONOW&showType=MO", {:authorization => 'Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3',:user_agent => 'Caavo_Fyra_v1.1.199',:host => '54.175.96.97:81'}))
        if !pjx_body.empty?
            ingestion_status = []
            ingestion_status << "Ingested properly but not available in search"
            ingestion_status << pjx_body[0]["projectx_id"]
            return ingestion_status
        else
            r = check_projectx_duplicate(link_ser_id);
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

def check_blind_projectx_ingested(link_ser_id)
    retry_cnt = 3
    begin
        pjx_body = JSON.parse(RestClient.get("http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=#{link_ser_id}&sourceName=HBONOW&showType=MO", {:authorization => 'Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3',:user_agent => 'Caavo_Fyra_v1.1.199',:host => '54.175.96.97:81'}))
        ingestion_status = []
        if !pjx_body.empty?            
            ingestion_status << "Pass"
            ingestion_status << pjx_body[0]["projectx_id"]   
            return ingestion_status         
        else 
            r = check_projectx_duplicate(link_ser_id);
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
            ingestion_status = []
            return ingestion_status
        end
    end
end

def check_projectx_duplicate(link_ser_id)
    retry_cnt = 3
    begin
        dup_body = JSON.parse(RestClient.get("http://34.231.212.186:81/projectx/duplicate?sourceId=#{link_ser_id}&sourceName=HBONOW&showType=MO", {:authorization => 'Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3',:user_agent => 'Caavo_Fyra_v1.1.199',:host => '54.175.96.97:81'}))
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

def process(link,rovi)
    puts "hbogo links : #{link}"
    puts "rovi links : #{rovi}"
    rovi = rovi.uniq
    temp = []
    if rovi.length == 0
        p "links not available"
        $link_not_available = $link_not_available + 1
        temp << "links not available"
        temp << link
        temp << "NA"
        ot_del = check_ott_deleted(link);
        if ot_del == "false"
            temp << "Not Deleted"
        elsif ot_del == "error"
            temp << "Not Deleted"
        else
            temp << "Deleted"
        end
        ot_exp = check_ott_expiry(link);
        if ot_exp == "false"
            temp << "Not Expired"            
        elsif ot_exp == "true"
            temp << "Expired"
        elsif ot_exp == "empty"
            temp << "empty"
        elsif ot_exp == "error"
            temp << "error"
        end
    else                                        
        ind = rovi.index(link)
        if ind == nil
            p "links mismatch"
            $link_mismatch_count = $link_mismatch_count + 1
            temp << "links didn't matched"
            temp << link
            temp.concat rovi
        else
            p "links matched"
            $link_match_count = $link_match_count + 1
            temp << "links matched"
            temp << link
            temp.concat rovi
        end
    end
    return temp
end

def check_ott_deleted(link)
    retry_cnt = 3
    begin
        del_status = JSON.parse(RestClient.get("http://34.231.212.186:81/projectx/#{link}/hbogo/isDeletedOttprojectx", {:authorization => 'Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3',:user_agent => 'Caavo_Fyra_v1.1.199',:host => '54.175.96.97:81'}))
        if !del_status.empty?
            return "true"
        else
            return "false"
        end
    rescue Exception => err 
        puts "Exception in Ott Deleted Method"
        puts "Error!!!: #{err}"
        puts "Error!!!: #{err.backtrace}"
        retry_cnt -= 1
        if retry_cnt > 0
            sleep 2
            retry
        else
            puts "Exceeded retry count in Ott Deleted Method: #{retry_cnt}"
            return "error"
        end
    end
end

def check_ott_expiry(link)
    retry_cnt = 3
    begin
        exp_status = JSON.parse(RestClient.get("https://preprod.caavo.com/expired_ott/is_available?source_program_id=#{link}&service_short_name=hbogo", {:authorization => 'Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7',:user_agent => 'Caavo_Fyra_v1.1.199'}))
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
        retry_cnt -= 1
        if retry_cnt > 0
            sleep 2
            retry
        else
            puts "Exceeded retry count in Ott Expiry Method: #{retry_cnt}"
            return "error"
        end
    end
end

def remove_special_ascii(title)
    title = title.gsub(/é/,'e')
    title = title.gsub(/ë/,'e')
    title = title.gsub(/ú/,'u')
    title = title.gsub(/á/,'a')
    title = title.gsub(/ä/,'a')
    title = title.gsub(/ñ/,'n')
    title = title.gsub(/:/,'')
    title = title.gsub(/#/,'')
    title = title.gsub(/é/,'e')
    title = title.gsub(/à/,'a')
    return title
end


CSV.open("#{run_date_csv}hbogo_preprod.csv","w+") do |cs|
    cs << ["hbogo_title","rel_yr","hbogo_id","resp_code","rel_yr_status","matched_status","oz_title","rel_yr","program_id","blind_id","search_status","links_status","hbogo_id","rovi_id"]
end
CSV.open("#{run_date_csv}movie_notmapped_preprod.csv","w+") do |cs|
end
CSV.open("#{run_date_csv}hbogo_movies_empty_search_results_Preprod.csv","w+") do |cs|
end
CSV.open("#{run_date_csv}hbogo_Blind_Ingestion_issues.csv","w+") do |cs|
end
total_count = 0
total_movies = 0
$link_match_count = 0
$link_mismatch_count = 0
$link_not_available = 0
$title_match_count = 0
$title_mismath_count = 0
$empty_search_results = 0
$blind_ingestion_fail_count = 0
$blind_ingest_duplicates = 0
total_errors = 0
arr = Array.new 
#client = Mongo::Client.new(['127.0.0.1:27017'],:database => 'qadb') #Connecting to Mongo and creating a DB 'sample'.....use 
$client = Mysql2::Client.new(:host => "192.168.86.10", :username => "root", :database =>"branch_service", :password => "branch@123") #Connecting to Mysql and creating a DB 'sample'.....use 
$db_table='hbogo_programs'
$run_date = Time.now.strftime("%Y-%m-%d %H:%M:%S")
results = $client.query("select launch_id,title,release_year from #{$db_table} where show_type in ('MO','OT') and (expired_at is null or expired_at > '#{$run_date}');")
results.each do |doc|
    doc = doc.to_json
    doc = JSON.parse(doc)
    arr << doc
end
total_movies = arr.length
p total_movies
#CSV.foreach("#{run_date}hbogodata.csv") do |row|
arr.each do |row|
    #p row
    hbogo_id = row["launch_id"]
    puts "hbogo dump videos are #{hbogo_id}"
    hbogo_title = row["title"]
    p hbogo_title
    hbogo_title = remove_special_ascii(hbogo_title);
    hbogo_title = mod_title(hbogo_title);
    hbogo_title = hbogo_title.gsub(/\P{ASCII}/,'')
    #p hbogo_title
    hbogo_showtype = "MO"
    hbogo_releaseyear = row["release_year"]
    hbogo_releaseyear = hbogo_releaseyear.to_i    
    blind_ingest_status = []
    blind_ingest_status = check_blind_projectx_ingested(hbogo_id);
    puts "ingestion_status #{hbogo_id} #{blind_ingest_status}"
    if blind_ingest_status[0] == "Pass"
        blind_ingest_id = blind_ingest_status[1]
        begin
            oz_req = voice_search_pagination(hbogo_title,"ott_search")
            oz_req = JSON.parse((oz_req.to_json))
            flag = 0
            rlyr_flg = 0
            empty_search_results_flag = 0
            total_count = total_count + 1
            if !oz_req.empty?
                oz_req.each do |res|
                    aliases_array = []
                    aliases_temp_array = []
                    alias_match_flag = 0
                    #puts res.inspect
                    puts res["object"]["show_type"]
                    puts res["object"]["release_year"]
                    puts res["object"]["id"]
                    if res["object"]["show_type"] == "MO"
                        if (res["object"]["release_year"] == hbogo_releaseyear)
                            #p "rel year success..."
                            rlyr_flg = 1
                            oz_title = res["object"]["long_title"]
                            oz_origtitle = res["object"]["original_title"]                      
                            oz_title_mod = mod_title(oz_title);
                            oz_title_mod = remove_special_ascii(oz_title_mod);
                            oz_title_mod = oz_title_mod.gsub(/\P{ASCII}/,'')
                            oz_origtitle_mod = mod_title(oz_origtitle);
                            oz_origtitle_mod = remove_special_ascii(oz_origtitle_mod);
                            oz_origtitle_mod = oz_origtitle_mod.gsub(/\P{ASCII}/,'')
                            aliases_temp_array = res["object"]["aliases"]
                            aliases_temp_array.each do |al|
                                if al["source_name"] == "Rovi"
                                    if al["type"] == "long_title"
                                        aliases_array << al["alias"]
                                    elsif al["type"] == "original_title"
                                        aliases_array << al["alias"]
                                    elsif al["type"] == "alias_title"
                                        aliases_array << al["alias"]
                                    elsif al["type"] == "alias_title2"
                                        aliases_array << al["alias"]
                                    elsif al["type"] == "alias_title3"
                                        aliases_array << al["alias"]
                                    elsif al["type"] == "alias_title4"
                                        aliases_array << al["alias"]
                                    end
                                elsif al["source_name"] == "Rovi"
                                    if al["type"] == "title"
                                        aliases_array << al["alias"]
                                    elsif al["type"] == "original_title"
                                        aliases_array << al["alias"]
                                    end
                                elsif al["source_name"] == "Vudu"
                                    if al["type"] == "title"
                                        aliases_array << al["alias"]
                                    end
                                elsif al["source_name"] == "Hulu"
                                    if al["type"] == "title"
                                        aliases_array << al["alias"]
                                    end
                                end
                            end
                            if aliases_array.include? hbogo_title
                                alias_match_flag = 1
                            end
                            if (oz_title_mod == hbogo_title) || (oz_origtitle_mod == hbogo_title) || (alias_match_flag == 1)
                                flag = 1
                                $title_match_count = $title_match_count + 1
                                #p "...1..."
                                oz_hbogo = Array.new
                                oz_hbogo_lid_arr = Array.new
                                oz_hbogo << hbogo_title.downcase
                                oz_hbogo << hbogo_releaseyear
                                oz_hbogo << hbogo_id
                                oz_hbogo << "200"
                                oz_hbogo << "exact release year"
                                oz_hbogo << "title matched"                     
                                oz_hbogo << oz_title.downcase
                                oz_hbogo << res["object"]["release_year"]
                                mapped_id = res["object"]["id"]
                                oz_hbogo << mapped_id
                                oz_hbogo << blind_ingest_id
                                if mapped_id == blind_ingest_id
                                    oz_hbogo << "Search result ID is same as Ingested ID"
                                else
                                    oz_hbogo << "Search result ID is not same as Ingested ID"
                                end
                                res["object"]["videos"].each do |vid|
                                    #p "...2..."
                                    oz_hbogo_lid_arr << vid["launch_id"]
                                end
                                ret = process(hbogo_id,oz_hbogo_lid_arr);
                                oz_hbogo += ret
                                CSV.open("#{run_date_csv}hbogo_preprod.csv","a+") do |cs|
                                    cs << oz_hbogo
                                end
                                break
                            end                                 
                        end
                    end
                end
                if rlyr_flg == 0
                    oz_req.each do |res|
                        #p res
                        aliases_array = []
                        aliases_temp_array = []
                        alias_match_flag = 0
                        #puts res.inspect
                        puts res["object"]["show_type"]
                        puts res["object"]["release_year"]
                        puts res["object"]["id"]
                        if res["object"]["show_type"] == "MO"
                            if (res["object"]["release_year"] == hbogo_releaseyear - 1 || res["object"]["release_year"] == hbogo_releaseyear + 1)
                                #p "rel year success..."
                                rlyr_flg = 1
                                oz_title = res["object"]["long_title"]
                                oz_origtitle = res["object"]["original_title"]                      
                                oz_title_mod = mod_title(oz_title);
                                oz_origtitle_mod = mod_title(oz_origtitle);
                                aliases_temp_array = res["object"]["aliases"]
                                    aliases_temp_array.each do |al|
                                    aliases_array << al["alias"]
                                end
                                p aliases_array
                                p hbogo_title                        
                                if aliases_array.include? hbogo_title
                                    puts "alias title matched"
                                    alias_match_flag = 1
                                end
                                if (oz_title_mod == hbogo_title) || (oz_origtitle_mod == hbogo_title) || (alias_match_flag == 1)
                                    flag = 1
                                    $title_match_count = $title_match_count + 1
                                    #p "...1..."
                                    oz_hbogo = Array.new
                                    oz_hbogo_lid_arr = Array.new
                                    oz_hbogo << hbogo_title.downcase
                                    oz_hbogo << hbogo_releaseyear
                                    oz_hbogo << hbogo_id
                                    oz_hbogo << "200"
                                    oz_hbogo << "tentative release year"
                                    oz_hbogo << "title matched"                      
                                    oz_hbogo << oz_title.downcase
                                    oz_hbogo << res["object"]["release_year"]
                                    mapped_id = res["object"]["id"]
                                    oz_hbogo << mapped_id
                                    oz_hbogo << blind_ingest_id
                                    if mapped_id == blind_ingest_id
                                        oz_hbogo << "Search result ID is same as Ingested ID"
                                    else
                                        oz_hbogo << "Search result ID is not same as Ingested ID"
                                    end
                                    res["object"]["videos"].each do |vid|
                                        #p "...2..."
                                        oz_hbogo_lid_arr << vid["launch_id"]
                                    end
                                    ret = process(hbogo_id,oz_hbogo_lid_arr);
                                    oz_hbogo += ret
                                    CSV.open("#{run_date_csv}hbogo_preprod.csv","a+") do |cs|
                                        cs << oz_hbogo
                                    end
                                    break
                                end                                 
                            end
                        end
                    end
                end
            elsif oz_req.empty?
                empty_search_results_flag = 1
                pjx_id = nil
                $empty_search_results = $empty_search_results + 1
                pjx_id = check_projectx_ingested(hbogo_id);
                final_result = []
                final_result << hbogo_title.downcase
                final_result << hbogo_releaseyear
                final_result << hbogo_id            
                final_result << "NA"
                final_result << "NA"
                final_result << "NA"
                final_result << "Empty_search_results"
                CSV.open("#{run_date_csv}hbogo_preprod.csv","a+") do |cs|
                    cs << final_result
                end
                final_result << "#{pjx_id[0]}"
                final_result << "#{pjx_id[1]}"
                CSV.open("#{run_date_csv}hbogo_movies_empty_search_results_Preprod.csv","a+") do |cs|
                    cs << final_result
                end
            end
            if flag == 0 && empty_search_results_flag == 0
                $title_mismath_count =$title_mismath_count +1
                #p "...4..."                            
                oz_hbogo = Array.new
                oz_hbogo << hbogo_title.downcase
                oz_hbogo << hbogo_releaseyear
                oz_hbogo << hbogo_id
                oz_hbogo << "200"
                oz_hbogo << "title didn't matched"
                CSV.open("#{run_date_csv}hbogo_preprod.csv","a+") do |cs|
                    cs << oz_hbogo
                end
                CSV.open("#{run_date_csv}movie_notmapped_preprod.csv","a+") do |cs|
                    cs << oz_hbogo
                end
            end
        rescue RestClient::InternalServerError
            total_errors = total_errors + 1
            oz_hbogo = Array.new
            oz_hbogo << hbogo_title.downcase
            oz_hbogo << hbogo_releaseyear
            oz_hbogo << hbogo_id
            oz_hbogo << "500"
            CSV.open("#{run_date_csv}hbogo_preprod.csv","a+") do |cs|
                cs << oz_hbogo
            end
        rescue RestClient::BadGateway
            total_errors = total_errors + 1
            oz_hbogo = Array.new
            oz_hbogo << hbogo_title.downcase
            oz_hbogo << hbogo_releaseyear
            oz_hbogo << hbogo_id
            oz_hbogo << "502"
            CSV.open("#{run_date_csv}hbogo_preprod.csv","a+") do |cs|
                cs << oz_hbogo
            end     
        rescue RestClient::GatewayTimeout
            total_errors = total_errors + 1
            oz_hbogo = Array.new
            oz_hbogo << hbogo_title.downcase
            oz_hbogo << hbogo_releaseyear
            oz_hbogo << hbogo_id
            oz_hbogo << "504"
            CSV.open("#{run_date_csv}hbogo_preprod.csv","a+") do |cs|
                cs << oz_hbogo
            end
        rescue Exception => ex
            puts ex
            puts ex.backtrace
            total_errors = total_errors + 1
            oz_hbogo = Array.new
            oz_hbogo << hbogo_title.downcase
            oz_hbogo << hbogo_releaseyear
            oz_hbogo << hbogo_id
            oz_hbogo << "505"
            CSV.open("#{run_date_csv}hbogo_preprod.csv","a+") do |cs|
                cs << oz_hbogo
            end     
        end
    elsif blind_ingest_status[0] == "Fail"
        $blind_ingestion_fail_count = $blind_ingestion_fail_count + 1
        oz_hbogo = Array.new
        oz_hbogo << hbogo_title.downcase
        oz_hbogo << hbogo_releaseyear
        oz_hbogo << hbogo_id
        oz_hbogo << "200"
        oz_hbogo << "Blind Ingestion Failure"
        CSV.open("#{run_date_csv}hbogo_Blind_Ingestion_issues.csv","a+") do |cs|
            cs << oz_hbogo
        end
        CSV.open("#{run_date_csv}hbogo_preprod.csv","a+") do |cs|
            cs << oz_hbogo
        end
    elsif blind_ingest_status[0] == "part of duplictes"
        $blind_ingest_duplicates = $blind_ingest_duplicates + 1
        oz_hbogo = Array.new
        oz_hbogo << hbogo_title.downcase
        oz_hbogo << hbogo_releaseyear
        oz_hbogo << hbogo_id
        oz_hbogo << "200"
        oz_hbogo << "Blind Ingestion"
        CSV.open("#{run_date_csv}hbogo_preprod.csv","a+") do |cs|
            cs << oz_hbogo
        end
        oz_hbogo << blind_ingest_status[1]
        CSV.open("#{run_date_csv}hbogo_Blind_Ingestion_issues.csv","a+") do |cs|
            cs << oz_hbogo
        end
    else 
        $blind_ingest_errors = $blind_ingest_errors + 1
        oz_hbogo = Array.new
        oz_hbogo << hbogo_title.downcase
        oz_hbogo << hbogo_releaseyear
        oz_hbogo << hbogo_id
        oz_hbogo << "200"
        oz_hbogo << "Blind Ingestion Failure_Errors"
        CSV.open("#{run_date_csv}hbogo_preprod.csv","a+") do |cs|
            cs << oz_hbogo
        end
        oz_hbogo << blind_ingest_status[1]
        CSV.open("#{run_date_csv}hbogo_Blind_Ingestion_issues.csv","a+") do |cs|
            cs << oz_hbogo
        end
    end
end
puts "total movies count: #{total_movies}"
puts "total blind Ingestion failures: #{$blind_ingestion_fail_count}"
puts "total blind Ingestion Duplicates: #{$blind_ingest_duplicates}"
puts "Total errors: #{total_errors}"
puts "total count: #{total_count}"
puts "total empty search results count: #{$empty_search_results}"
puts "Titles matched: #{$title_match_count}"
puts "Titles Not matched: #{$title_mismath_count}"
puts "links matched: #{$link_match_count}"
puts "Links Not matched: #{$link_mismatch_count}"
puts "Ingestion Failures: #{$link_not_available}"
#puts "Total errors: #{total_errors}"