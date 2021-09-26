from airflow.hooks.base import BaseHook
import requests
import json

class CustomYoutubeHook(BaseHook):

    def __init__(self,yt_conn):
        
        self.conn = self.get_connection(yt_conn)
        
        extras = self.conn.get_extra()

        extras = json.loads(extras)
        self._client_id = extras['client_id']
        self._client_secrets = extras['client_secret']
        self._refresh_token= extras['refresh_key']
        print(extras)
        
    def YT_to_local(self,playlist_id):
        
        print('yes in the function')
        
        def accessToken(client_id, client_secret, refresh_key):
            
            params = {
                    "grant_type": "refresh_token",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "refresh_token": refresh_key
            }
    
    
            authorization_url = "https://www.googleapis.com/oauth2/v4/token"
    
            response = requests.post(authorization_url, data=params)

            if response.status_code == 200:
                    return response.json()['access_token']
            else:
                    return print(f"You've got response code : {response.status_code} with {response.text}")

    
        access_token = accessToken(client_id=self._client_id,client_secret=self._client_secrets,refresh_key=self._refresh_token)

        print(f"access token is : {access_token}")

        url = 'https://www.googleapis.com/youtube/v3/playlistItems'

        param_ = {
            'access_token' : access_token,
            'part' : 'id,contentDetails',
            'playlistId' : playlist_id,
            'maxResults' : 150
            } 

        response = requests.get(url,params=param_).json()


        #Content Details

        videoId = list(map(lambda x: x['contentDetails']['videoId'],response['items']))

        videoPublishedAt = list(map(lambda x: x['contentDetails']['videoPublishedAt'],response['items']))

        content_details = {'videoId': videoId,'Date':videoPublishedAt}


        #video Stats
        url2 = "https://www.googleapis.com/youtube/v3/videos"
        video_response =[]

        for i in videoId:
            param2 = {
                'access_token':access_token,
                'part':'id,statistics',
                'id' : i
                }
            
            
            video_response.append(requests.get(url2,params=param2).json())

        video_stats = list(map(lambda x: x['items'][0]['statistics'],video_response))
        video_id = list(map(lambda x: x['items'][0]['id'],video_response))

        Stats = {'id':video_id, 
                 'Views' : list(map(lambda x: x['viewCount'],video_stats)),
                 'Likes' : list(map(lambda x: x['likeCount'],video_stats)),
                 'Dislikes' : list(map(lambda x: x['dislikeCount'],video_stats)),
                 'Favorites' : list(map(lambda x: x['favoriteCount'],video_stats)),
                 'Number_of_Comments' : list(map(lambda x: x['commentCount'],video_stats))

                 }
    
        return Stats,content_details
        