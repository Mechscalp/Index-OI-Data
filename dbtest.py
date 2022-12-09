# -*- coding: utf-8 -*-
"""
Created on Fri Jun 10 11:43:15 2022

@author: Scalper
"""

import requests
import numpy
import json
import pandas as pd
import time
import mysql.connector
import sqlalchemy
import threading
from datetime import datetime



 
def insert():
 global ddf, engine
 
 engine = sqlalchemy.create_engine('mysql+pymysql://root:asd123@127.0.0.1:3306/optdata1')

 sym = "NIFTY"
 exp_date = "03-Nov-2022"
 

 a = {}
 url = "https://www.nseindia.com/api/option-chain-indices?symbol="+sym
 headers = {"accept-encoding":"gzip, deflate, br",
               "accept-language":"en-US,en;q=0.9",
               "referer": "https://www.nseindia.com/get-quotes/derivatives?symbol="+sym,
               "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"}
    
 response = requests.get(url, headers = headers).text
    
 data = json.loads(response)
    
 exp_list = data['records']['expiryDates']


 ce = {}
 pe = {}
 n=0
 m=0 
 for i in data['records']['data']:
        if i ['expiryDate']== exp_date:
            try:
                    ce[n] = i['CE']
                    n=n+1
            except:
                pass
            try:
                    pe[m] = i['PE']
                    m=m+1
            except:
                pass

 time = datetime.now().time().strftime("%H:%M")  
         
 ce_df = pd.DataFrame.from_dict(ce).transpose()
 ce_df.columns += "_CE"
 #to add time col
 ce_df.insert(loc=1, column='Time', value=time)
# ce_df.insert(0, 'Index', range(1, 1 + len(ce_df)))      # insertion of index

 pe_df = pd.DataFrame.from_dict(pe).transpose()
 pe_df.columns += "_PE"
 #to add time col
 pe_df.insert(loc=1, column='Time', value=time)
# pe_df.insert(0, 'Index', range(1, 1 + len(pe_df)))      # insertion of index

 #mycursor = engine.cursor()
 #ce_df.to_sql(con=engine,name='ce_df',if_exists='append',index=False)
 
 sql_ce = "SELECT * FROM oc_ce "
 ddf_ce= pd.read_sql_query(sql_ce,engine)
 sql_pe = "SELECT * FROM oc_pe "
 ddf_pe= pd.read_sql_query(sql_pe,engine)
 
 
 
 if (ddf.strikePrice_CE == 0).any():                         # insertion check in database
  pe_df.to_sql(name='oc_pe', con=engine, index=False, if_exists='append')
  ce_df.to_sql(name='oc_ce', con=engine, index=False, if_exists='append')
  print('Entry Done for ', datetime.now())
    
  df = pd.concat([ce_df,pe_df],axis = 1)
  
 else:
   for i in ce_df['strikePrice_CE']:
  
       if (i == ddf.loc[:,'strikePrice_CE']).any():
            index_no = ce_df.index[ce_df['strikePrice_CE'] == i].tolist()
            ind = index_no[0]
                     
            try:
                print('done')
                           
                a = pd.DataFrame(ce_df.loc[ind,:])
                a = numpy.transpose(a)
                 
                ddf = ddf.append(a,ignore_index = True)   
                ddf.sort_values(by=['strikePrice_CE'],inplace=True,ignore_index = True) 
                print('Entry Done for ',time , datetime.now())
          
                         
            except:
                   pass
       else:
                   print('none')
                  
 return ddf

while True:
    try:
        data = insert()
        ddf.to_sql(name='oc_ce', con=engine, index=False, if_exists='replace')
        time.sleep(180)
        
    except:
        print("Retrying")
        time.sleep(5)
