# -*- coding: utf-8 -*-
import numpy as np 

#define MapReduce function part A
def MapReduceA():
    
    #set confint and start key 
    confint = 0.00000114   #confint equal to 1/875713 
    last_key = 10          #start key is not equal to the first key "0"  
    out_node=[]            #the list which will contant out nodes
 

    data={}
    
    #read source 
    old_arr = open("web-Google.txt")
    
    #get key and value, value start with confint and out nodes
    for i in range(5105039):# there are 5105039 lines in the sourse, so do 5105039 times for-loop
        cur_line = old_arr.readline()
        cur_number = cur_line.split()

        if last_key != cur_number[0]:
            out_node=[confint]
            out_node.append(cur_number[1])
            data[cur_number[0]]=out_node
            last_key=cur_number[0]

        elif cur_number[0] == last_key:
            out_node.append(cur_number[1])
            data[cur_number[0]]=out_node
            last_key=cur_number[0]
        
    old_arr.close()

    return data

#define MapReduce function part B
def MapReduceB(data):
    out_data={}
    cur_pr=0;
    cur_out=[]
    for i in range(len(data)):
        cur_out = data[i]
        cur_pr = cur_out[0]
        del cur_our[0]
        data[i]=cur_out
        if cur_out is not None:
            for i in range(len(cur_out)):
                out_data[list[i-1]] = cur_pr/(len(cur_out))
        elif cur_out is None:
            out_data[data[i]] = ' '
        else:
            out_data[data[i]] = cur_out


    n=875713
    lamada=0.8
    pr=(1-lamada)/n
    for i in range(len(out_data)):
        reduce_value=out_data[i]
        if type(reduce_value) == 'list':
            for j in range(len(reduce_value)):
                out_data[reduce_value[j]] = ' '
        else:
            reduce_value = reduce_value +lamada*reduce_value
            out_data[i] = reduce_value


    for i in range(len(out_data)):
        reduce_value=out_data[i]
        if reduce_value == ' ':
            out_data[i]=pr
        elif type(reduce_value) == 'list':
            reduce_value.insert(0,pr)
            data[i] = reduce_value

    return out_data




# main entry
if __name__ =="__main__":
    #data_o=datasetList()

    data_p = MapReduceA()

    for i in range(15):
        data_p = MapReduceB(data_p)
    print(data_p)





