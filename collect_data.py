#import
print("Progress: Importing")

import scrapetube
from googleapiclient.discovery import build
import pandas as pd
import numpy as np
from datetime import datetime
import time
import math



#variables
print("Progress: Setting Variables")

#user must input API key for Youtube Data API
api_key = input("What is your API key:")

#youtube links of channels
links = ["https://www.youtube.com/@NCT127/videos",
           "https://www.youtube.com/@NCTDREAM/videos",
           "https://www.youtube.com/@WayV/videos",
           "https://www.youtube.com/@NCTWISH/videos",
           "https://www.youtube.com/@nctmusic/videos",
           "https://www.youtube.com/@nctdaily/videos",
           "https://www.youtube.com/@NCTsmtown/videos",]

youtube = build('youtube', 'v3', developerKey = api_key)



#finding video titles and IDs of NCT videos
print("Progress: Scraping Video Titles")

#manually entering sixteen video IDs and titles that are not added through other sources

videoTitles = ["[STATION 3] TAEYONG 태용 'Long Flight' MV Teaser", "[STATION 3] TAEYONG 태용 'Long Flight' #비하인더스테이션", "[STATION 3] NCT DREAM 엔시티 드림 '사랑한단 뜻이야 (Candle Light)' MV Teaser", "[STATION X] NCT U 엔시티 유 'Coming Home' #비하인더스테이션", "[STATION X] NCT U 엔시티 유 'Coming Home (Sung by 태일, 도영, 재현, 해찬)' MV Teaser", "[STATION X] NCT U 엔시티 유 'Coming Home' Teaser Clip #JAEHYUN", "[STATION X] NCT U 엔시티 유 'Coming Home' Teaser Clip #TAEIL", "[STATION X] NCT U 엔시티 유 'Coming Home' Teaser Clip #DOYOUNG", "[STATION X] NCT U 엔시티 유 'Coming Home' 꿈만 같은 뮤비 현장 NG Cut (Feat. 텐데)", "[STATION X] ⛄ 2019 SMTOWN Winter Trailer ⛄｜4 LOVEs for Winter", "[STATION] 태용 (TAEYONG) X 원슈타인 (Wonstein) 'Love Theory' UNBOXING V-log #1", "[STATION] TEN 텐 'Paint Me Naked' Performance Video #비하인더스테이션", "[STATION] TEN 텐 'Paint Me Naked' MV", "[STATION 3] 예리X런쥔X제노X재민 'Hair in the Air' #비하인더스테이션", "[STATION 3] NCT DREAM '사랑한단 뜻이야 (Candle Light)' #비하인더스테이션", "[STATION X] NCT U 엔시티 유 'Coming Home' Teaser Clip #HAECHAN"]

videoIds = ['dBFVH882FmM','zkxnXEyX5P4','q3vP-LpesoI','Tw6C8N_Qj58','qPI4jKKnncI','LkqNlIW9Rus','Ni0Rh_gZ8Dw','EoS97CRIsk0','38xi_SsCig8','wLp9H2hQh00','6_xtt2kjy6Y','SqsyOM9sw0A','dOZyZiJ9n_8','D3iHhkZSccM','ikZnF7BHb2o','JBDmYRpcYnU']


#scraping all the official NCT channels
for yt in links:
	print('Progress: Scraping',yt)
	videos = scrapetube.get_channel(channel_url = yt, sort_by = "newest",limit=1000,content_type='videos')
	for video in videos:
		videoTitles += [video['title']['runs'][0]['text']]
		videoIds += [video["videoId"]]


#scraping from a playlist titled 'NCT' from the company's channel
print('Progress: Scraping NCT Playlist from SMTOWN Channel')

playlist_id = "PLA91TLEzZINu1FlHuX76oNJ2SptNMzHUL"
request = youtube.playlistItems().list(part=['snippet'], maxResults=50, playlistId=playlist_id)
response = request.execute()
for item in response['items']:
	videoIds += [item['snippet']['resourceId']['videoId']]
	videoTitles += [item['snippet']['title']]
runtimes = math.ceil((response['pageInfo']['totalResults']) / (response['pageInfo']['resultsPerPage'])) - 1 
for i in np.arange(runtimes): 
	pt = response['nextPageToken']
	request = youtube.playlistItems().list(part="snippet", maxResults=50, playlistId=playlist_id, pageToken = pt)
	response = request.execute()
	for item in response['items']:
		videoIds += [item['snippet']['resourceId']['videoId']]
		videoTitles += [item['snippet']['title']]



#import data using Youtube API
print("Progress: Importing Youtube data")

#organizes data about each video into appropriate lists
def organize_data(response, duration, views, likes, comments, date, title, channel):
	stat = response['items'][0]['statistics']
	snip = response['items'][0]['snippet']
	duration += [response['items'][0]['contentDetails']['duration']]
	views += [stat['viewCount']]
	if 'commentCount' in stat:
		comments += [stat['commentCount']]
	else:
		comments += ['NA']
	if 'likeCount' in stat:
		likes += [stat['likeCount']]
	else:
		likes += ['NA']
	date += [snip['publishedAt']]
	title += [snip['title']]
	channel += [snip['channelTitle']]

#set up lists to store data in
views = []
likes = []
comments = []
date = []
title = []
channel = []
duration = []
remove_idx = []

#request data and organize into appropriate list for each video
for ids in videoIds:
	request = youtube.videos().list(id = ids, part = ['statistics','snippet','contentDetails'])
	response = request.execute()
	if response['items']==[]:
		remove_idx += [videoIds.index(ids)] #list of indices for privated videos
	else:
		organize_data(response=response, duration=duration, views=views, likes=likes, comments=comments, date=date, title=title, channel=channel)



#clean data (part 1)
print("Progress: Cleaning Data (Part 1)")

#remove private videos which hold no data
for i in sorted(remove_idx, reverse=True):
    del videoIds[i]
    del videoTitles[i]

#format release date into datetime object
date2 = [datetime.strptime(date[i], '%Y-%m-%dT%H:%M:%SZ') for i in np.arange(len(date))]



#create dataframe
print("Progress: Creating Dataframe")

df = pd.DataFrame({
    'ID': videoIds,
    'Channel': channel,
    'Title': videoTitles,
    'Duration': duration,
    'Release Date': date2,
    'Views': views,
    'Likes': likes,
    'Comments': comments,
})



#clean data (part 2)
print("Progress: Cleaning Data (Part 2)")

#clean/prep Comments columns
df.loc[:,'Comments'] = df.loc[:,'Comments'].str.replace('NA', '0')
df.loc[:,'Comments'] = pd.to_numeric(df.loc[:,'Comments'])

#clean/prep Views and Likes columns
df.loc[:,'Views'] = pd.to_numeric(df.loc[:,'Views'])
df.loc[:,'Likes'] = df.loc[:,'Likes'].str.replace('NA', '0')
df.loc[:,'Likes'] = pd.to_numeric(df.loc[:,'Likes'])

#clean/prep Duration column
tlist = []
for item in df.loc[:,'Duration'].str.replace('PT',''):
	if 'H' in item:
		tlist += [item.split('H')]
	else:
		tlist += [['0', item]]
for item in tlist:
	if 'M' in item[1]:
		var = item[1].split('M')[0]
		item += [item[1].split('M')[1][:-1]]
		item[1] = var
	else:
		item += [item[1][:-1]]
		item[1] = '0'
	if item[2] == '':
		item[2] = '0'
df2 = pd.DataFrame([tlist]).transpose()
for i in np.arange(len(df2)):
	item = df2.iloc[i,0]
	var = item[0] + 'H' + item[1] + 'M' + item[2] + 'S'
	var2 = datetime.strptime(var, '%HH%MM%SS')
	df.iloc[i,3] = var2.time()

#remove videos that aren't from an NCT channel or from NCT's company's channels that aren't music videos
remove_idx = []
for i in np.arange(len(df)):
	if df.loc[i,'Channel'] in ['BuzzFeed Celeb','Zach Sang Show','WiLD 949','ALL THE K-POP','REACT','MOMO X','KBS WORLD TV','Tim Milgram']:
		remove_idx += [i]
df = df.drop(remove_idx).reset_index(drop=True)

#sort data frame by date (increasing order) and reset index
df = df.sort_values(by=['Release Date']).reset_index(drop=True)



#saving file
print("Progress: Saving File")
save = input("save data as excel file, csv, or both?")
if save in ['excel file','excel']:
	df.to_excel("nct_youtube_data.xlsx")
elif save in ['csv','csv file']:
	df.to_csv("nct_data.csv")
elif save == 'both':
	df.to_csv("nct_data.csv")
	df.to_excel("nct_youtube_data.xlsx")
print("Complete")
