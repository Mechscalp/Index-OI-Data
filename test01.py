# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 11:45:23 2022

@author: Scalper
"""

import requests
import json
import pandas as pd
import numpy as np
import altair as alt
import streamlit as st
import time
from datetime import datetime

sym = "NIFTY"
exp_date = "17-Nov-2022"
res = []
fnl = [] 
s=0 


def oc(sym,exp_dat) :
    
 time_ = datetime.now().time().strftime("%H:%M")  
 date = datetime.today().strftime("%d/%m/%Y")
   
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
            
 ce_df = pd.DataFrame.from_dict(ce).transpose()
 ce_df.columns += "_CE"
 pe_df = pd.DataFrame.from_dict(pe).transpose()
 pe_df.columns += "_PE"
 global df   
 df = pd.concat([ce_df,pe_df],axis = 1)
 df.insert(loc=1, column='Time', value=time_)
 df.insert(loc=1, column='Date', value=date)
 
 # end of data scrapping 
 
 # start data manupl
 

  
 #return exp_list,df

#def report(df,fnl):
   
 df = df.replace(0,0)
    
 df.loc['Column_Total']= df.sum(numeric_only=True, axis=0) #numeric_only=True missing as argument
 df["Date"] = df["Date"].replace([df.iloc[-1,1]],date)
 df["Time"] = df["Time"].replace([df.iloc[-1,2]],time_)
 df["expiryDate_CE"] = df["expiryDate_CE"].replace([df.iloc[-1,3]],df.loc [5,"expiryDate_CE"])

 rep = df.tail(1)
 global fnl
 fnl = rep.append(fnl,ignore_index=True)
  
 return exp_list,df,fnl

with st.empty(): 
 while True:
     try:
        data = oc(sym,exp_date)
        #rpt = report(df,fnl)
        p = st.empty()
        s = s + 1
        #p.dataframe(res)
        
        a = alt.Chart(fnl).mark_line(color='Red').encode(x='Time', y='openInterest_CE',tooltip = ['Time','openInterest_CE'])

        b = alt.Chart(fnl).mark_line(color='Green').encode(x='Time', y='openInterest_PE',tooltip = ['Time','openInterest_PE'])

        c = alt.layer(a, b)

        st.altair_chart(c, use_container_width=True)
        time.sleep(180)
     except:
        print("Retrying")
        time.sleep(5)