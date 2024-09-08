##import
print("Progress: Importing")
import numpy as np
import pandas as pd
import math
import time
import json
from datetime import datetime, timedelta

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

df = pd.read_csv('nct_data.csv')
df = df.iloc[:,1:]



##variables
print("Progress: Setting Variables")
maxResults = 100
hourCount = 24
dayCount = 7
monthCount = 12
emptyList = ['NA'] * (hourCount + dayCount + monthCount)

##user must input API key for Youtube Data API
api_key = input("What is your API key:")
youtube = build('youtube', 'v3', developerKey = api_key)



##creating dataframe for comments' data
print("Progress: Setting Up nct_comments_data.csv")
try:
    commentsdf = pd.read_csv('nct_comments_data.csv')
    commentsdf = commentsdf.iloc[:,commentsdf.columns.get_loc('ID'):]
except FileNotFoundError:
    cols = list(df.columns)
    for i in range(1,hourCount + 1):
        cols.append('Hour ' + str(i))
    for i in range(1,dayCount + 1):
        cols.append('Day ' + str(i))
    for i in range(1,monthCount + 1):
        cols.append('Month ' + str(i))
    commentsdf = pd.DataFrame(columns=cols)



##creating functions for importing and organizing data
print("Progress: Setting Up Functions")
##import comments based on a video ID and a comment page number (pt)
##since we can only import 100 comments per query, we loop over multiple pages of comments in commentsStats().
##note: this does not import comments replying to other comments. So, for comment threads, only the first (AKA parent) comment will be imported.
def commentThreads(ids, pt=None, maxResults=100):
    if pt is None:
        request = youtube.commentThreads().list(part='snippet,replies', videoId=ids, maxResults=maxResults)
    else:
        request = youtube.commentThreads().list(part='snippet,replies', videoId=ids, maxResults=maxResults,pageToken=pt)
    return request.execute()

##import comment replies for a given parent comment and comment reply page number (pt)
def commentReplies(parentId, pt=None, maxResults=100):
    if pt is None:
        request = youtube.comments().list(part="id,snippet",parentId=parentId,maxResults=maxResults)
    else:
        request = youtube.comments().list(part="id,snippet",parentId=parentId,maxResults=maxResults,pageToken=pt)
    return request.execute()

##get dates of each of the parent comments
##the commented-out section gets the dates for the comment replies as well.
def getCommentDates(threads):
    dates = []
    for item in threads['items']:
        dates.append(item['snippet']['topLevelComment']['snippet']['publishedAt'])
        #if item['snippet']['totalReplyCount'] != 0:
            #replies = commentReplies(item['id'])
            #for replyItem in replies['items']:
                #dates.append(replyItem['snippet']['publishedAt'])
            #while 'nextPageToken' in replies.keys():
                #replies = commentReplies(item['id'],replies['nextPageToken'])
                #for replyItem in replies['items']:
                    #dates.append(replyItem['snippet']['publishedAt'])
    return dates

##gets all the comment dates for a video, looping over all page numbers
##organizes comment dates into three lists: number of comments published hourly, daily, and monthly after video release date
def commentsStats(idx,hours=hourCount,days=dayCount,months=monthCount):
	dates = []
	ids = df.loc[idx,'ID']
    #pt = 0 #variable for debugging
	threads = commentThreads(ids)
    #print('page ' + str(pt))
	dates += getCommentDates(threads)
	while 'nextPageToken' in threads.keys():
		threads = commentThreads(ids,threads['nextPageToken'])
		#pt += 1
        #print('page ' +str(pt))
		dates += getCommentDates(threads)
    ##prepping date data into datetime objects to compare
	dates = [datetime.strptime(dates[i], '%Y-%m-%dT%H:%M:%SZ') for i in np.arange(len(dates))]
	releaseDate = datetime.strptime(df.loc[idx,'Release Date'], '%Y-%m-%d %H:%M:%S')
	oneHour = timedelta(hours=1)
	oneDay = timedelta(days=1)
	oneMonth = timedelta(days=30.437)
	hourlyComments = [0] * hours
	dailyComments = [0] * days
	monthlyComments = [0] * months
	for date in dates:
        ##how many comments are published every hour for 24 hours after release time
		for i in range(hours):
			if releaseDate + hours*oneHour >= datetime.now():
				hourlyComments[i] = 'NA'
			elif releaseDate + i*oneHour <= date < releaseDate + (i+1)*oneHour:
				hourlyComments[i] += 1
        ##how many comments are published every day for 7 days after release date
		for i in range(days):
			if releaseDate + days*oneDay >= datetime.now():
				dailyComments[i] = 'NA'
			elif releaseDate + i*oneDay <= date < releaseDate + (i+1)*oneDay:
				dailyComments[i] += 1
        ##how many comments are published every month for 12 months after release date
		for i in range(months):
			if releaseDate + months*oneMonth >= datetime.now():
				monthlyComments[i] = 'NA'
			elif releaseDate + i*oneMonth <= date < releaseDate + (i+1)*oneMonth:
				monthlyComments[i] += 1
	return hourlyComments, dailyComments, monthlyComments



print("Progress: Recording Comment Data for Video #")
try:
    for i in range(len(commentsdf),len(df)): ##looping over all videos
        print(i) ##track which video we're extracting comment data from
		##put 'NA' for all comment data columns if the video has no comments
		##if there are comments, get the hourly, daily, and monthly comments lists and put them in the respective comment data columns
        if df.loc[i,'Comments'] == 0:
            commentsdf.loc[i,:] = list(df.loc[i,:]) + emptyList
        else:
            hourlyComments, dailyComments, monthlyComments = commentsStats(i)
            commentsdf.loc[i,:] = list(df.loc[i,:]) + hourlyComments + dailyComments + monthlyComments
##save the dataframe to a csv if we exceed our quota for queries
except HttpError as e:
	if json.loads(e.content)['error']['errors'][0]['reason'] == 'quotaExceeded':
		print("Progress: Saving File")
		commentsdf.to_csv("nct_comments_data.csv")
	else:
		raise
