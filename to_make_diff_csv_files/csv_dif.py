"""Writer: Saayan"""

import csv
import os,sys
#import pdb;pdb.set_trace()
homedir=os.path.expanduser("~")
sys.path.insert(0,'%s/common_lib'%homedir)
from lib import lib_common_modules
sys.setrecursionlimit(1500)




def main(csv_file1,csv_file2):
    result_sheet='/output/ids.csv'
    output_file=lib_common_modules().create_csv(result_sheet)
    with output_file as mycsvfile:
        writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
        writer.writerow(["ids"])
        #import pdb;pdb.set_trace()
        print (input_file1)
        print ("\n")
        print (input_file2)
        for id_ in input_file2:
            if id_ not in input_file1:
                print (id_)
                writer.writerow(id_)



input_file1= lib_common_modules().read_csv("/input/netflix1")
input_file2= lib_common_modules().read_csv("/input/netflix2")

main(input_file1,input_file2)