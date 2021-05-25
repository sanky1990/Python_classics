###
### EXTRACT Email events data from Hubspot events API using Python
###
import pandas as pd
import numpy as np
import random
import string
import requests
import os
from pandas.io.json import json_normalize
from io import StringIO


#part A: Yesterday midnight to 11.59.59 whole day in miliseconds epoch generator

import datetime
def get_yesterday_epoch() -> (int, int):

    today = datetime.datetime.now()
    yesterday = today.date() - datetime.timedelta(days=1)

    yesterday_midnight = datetime.datetime.combine(yesterday, datetime.time.min)
    yesterday_235959 = datetime.datetime.combine(yesterday, datetime.time.max)

    yesterday_midnight_millis = int(yesterday_midnight.timestamp() * 1000)
    yesterday_235959_millis = int(yesterday_235959.timestamp() * 1000)
    return yesterday_midnight_millis, yesterday_235959_millis
timerange1 = get_yesterday_epoch()
start1 = timerange1[0]
end1 = timerange1[1]

#Part B: Extract events= open , bounce, delivered, statuschange that wont have URL

List = ['OPEN', 'BOUNCE', 'DELIVERED', 'STATUSCHANGE']
sizeofList = len(List)
i=0
final_data= pd.DataFrame(columns=['event_id', 'created','email','event_type','emailCampaignId','url'])
Api_key= "" # enter hubspot events api key
while i < sizeofList :
    response = requests.get("https://api.hubapi.com/email/public/v1/events?hapikey="+Api_key+"&eventType="+List[i]+"&startTimestamp="+str(start1)+"&endTimestamp="+str(end1)+"&limit=1000")
    events = response.json()
    events1=events['offset']
    hasMore = events['hasMore']
    new_data=events['events']
    main = pd.DataFrame(columns=['event_id', 'created','email','event_type','emailCampaignId'])
    for x in range(0, len(new_data), 1):
        event_id=new_data[x]['id']
        created= new_data[x]['created']
        email = new_data[x]['recipient']
        event_type= new_data[x]['type']
        emailCampaignId= new_data[x]['emailCampaignId']
        d = {'event_id': [event_id], 'created' : [created] , 'email' : [email] , 'event_type' : [event_type] , 'emailCampaignId' : [emailCampaignId]}
        df = pd.DataFrame(data=d)
        main=main.append(df)

    stack= pd.DataFrame(columns=['event_id', 'created','email','event_type','emailCampaignId'])
    while hasMore:
        response = requests.get("https://api.hubapi.com/email/public/v1/events?hapikey="+Api_key+"&offset="+events1+"&eventType="+List[i]+"&startTimestamp="+str(start1)+"&endTimestamp="+str(end1)+"&limit=1000")
        events = response.json()
        events1=events['offset']
        hasMore = events['hasMore']
        new_data2=events['events']
        stack2= pd.DataFrame(columns=['event_id', 'created','email','event_type','emailCampaignId'])
        for x in range(0, len(new_data2), 1):
            event_id=new_data2[x]['id']
            created= new_data2[x]['created']
            email = new_data2[x]['recipient']
            event_type= new_data2[x]['type']
            emailCampaignId= new_data2[x]['emailCampaignId']
            d = {'event_id': [event_id] , 'created' : [created] , 'email' : [email] , 'event_type' : [event_type] , 'emailCampaignId' : [emailCampaignId]}
            df = pd.DataFrame(data=d)
            stack2= stack2.append(df)
        stack=stack.append(stack2)
    data= main.append(stack)
    from datetime import datetime
    data['created']= pd.to_datetime(data['created'], unit='ms')
    data=data.reset_index(drop=True)
    data=data[['event_id', 'created','email','event_type','emailCampaignId']]
    data['url'] = ""
    data['created']=data['created'].astype('datetime64[s]')
    data['created']=data['created'] - pd.Timedelta(hours=7)
    final_data=final_data.append(data)
    i += 1


# Part C Extract Click events with URL having Campaign info

response = requests.get("https://api.hubapi.com/email/public/v1/events?hapikey="+Api_key+"&eventType=CLICK&startTimestamp="+str(start1)+"&endTimestamp="+str(end1)+"&limit=1000")
events = response.json()
events1=events['offset']
hasMore = events['hasMore']
new_data=events['events']
main = pd.DataFrame(columns=['event_id', 'url', 'created','email','event_type','emailCampaignId'])
for x in range(0, len(new_data), 1):
    event_id=new_data[x]['id']
    url= new_data[x]['url']
    created= new_data[x]['created']
    email = new_data[x]['recipient']
    event_type= new_data[x]['type']
    emailCampaignId= new_data[x]['emailCampaignId']
    d = {'event_id': [event_id], 'url': [url] , 'created' : [created] , 'email' : [email] , 'event_type' : [event_type] , 'emailCampaignId' : [emailCampaignId] }
    df = pd.DataFrame(data=d)
    main=main.append(df)

stack= pd.DataFrame(columns=['event_id', 'url', 'created','email','event_type','emailCampaignId'])
while hasMore:
    response = requests.get("https://api.hubapi.com/email/public/v1/events?hapikey="+Api_key+"&offset="+events1+"&eventType=CLICK&startTimestamp="+str(start1)+"&endTimestamp="+str(end1)+"&limit=1000")
    events = response.json()
    events1=events['offset']
    hasMore = events['hasMore']
    new_data2=events['events']
    stack2= pd.DataFrame(columns=['event_id', 'url', 'created','email','event_type','emailCampaignId'])
    for x in range(0, len(new_data2), 1):
        event_id=new_data2[x]['id']
        url= new_data2[x]['url']
        created= new_data2[x]['created']
        email = new_data2[x]['recipient']
        event_type= new_data2[x]['type']
        emailCampaignId= new_data2[x]['emailCampaignId']
        d = {'event_id': [event_id], 'url': [url] , 'created' : [created] , 'email' : [email] , 'event_type' : [event_type] , 'emailCampaignId' : [emailCampaignId] }
        df = pd.DataFrame(data=d)
        stack2= stack2.append(df)
    stack=stack.append(stack2)
data= main.append(stack)
data['created']= pd.to_datetime(data['created'], unit='ms')
click_data=data.reset_index(drop=True)
click_data =click_data[['event_id', 'url', 'created','email','event_type','emailCampaignId']]
click_data['created']=click_data['created'].astype('datetime64[s]')
click_data['created']= click_data['created'] - pd.Timedelta(hours=7)

#print(click_data)
# merging clicks data with other events data

MSW_events_yesterday = final_data.append(click_data)
MSW_events_yesterday = MSW_events_yesterday[MSW_events_yesterday.emailCampaignId != 0]
#print(MSW_events_yesterday)
#MSW_events_yesterday.to_csv("msw_events_Apr30.csv",date_format='%Y-%m-%d %H:%M:%S',index=False)
