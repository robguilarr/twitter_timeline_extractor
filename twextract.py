# -------------------------------------------------------------------------------------------------------------------------------
# List of dependecies to run the Timeline Extrator
# -------------------------------------------------------------------------------------------------------------------------------

import tweepy
import json
import pandas as pd
import re
import string

# -------------------------------------------------------------------------------------------------------------------------------
#   twextract module description
# -------------------------------------------------------------------------------------------------------------------------------
#   Miner class (Parent class of "tlminer"): This class is used to extract and subdivide the user timeline into Tweet, Retweet,
#                                           Reply, Quoted as dictionaries without subdictionaries
#   tlminer class: Class to transform each list of dictionaries into dataframes
#
#   !!Input
#   We have to use as arguments:
#       - username: Screen name of the user from we want to extract the timeline
#       - max_length: Max number of tweets to request. These are extracted from lastest to earliest
#       - consumerKey: Consumer key provided by Twitter Dev API
#       - consumerSecret: Consumer secret provided by Twitter Dev API
#       - accessToken: Access token provided by Twitter Dev API
#       - accessTokenSecret: Access token secret provided by Twitter Dev API
#
#   !!Output
#   This class has no output, all final dataframes can be extracted as objects using "tlminer" class
#
#   Dependecies required: tweepy json pandas (Base operations) 
#                         re string (For cleanText method)
#
#   About tweepy library:
#       - This script was tested using version 4.4.0
# -------------------------------------------------------------------------------------------------------------------------------

# Class to start mining tweets and keep output dictionaries at one level
class Miner():
    # Init constructor
    def __init__(self, username, max_length,
                        consumerKey, consumerSecret,
                        accessToken, accessTokenSecret):
        #--------------------------------------------------------------------------------------------------------------------------
        # Tweepy API connection
        #--------------------------------------------------------------------------------------------------------------------------
        # Save screename variable
        self.screen_name = username
        # Save max_length variable
        self.max_length = max_length
        # Authorize Access to the API using .OAuthHandler().
        authenticate = tweepy.OAuthHandler(consumerKey, consumerSecret)
        # Set the Access tokens.
        authenticate.set_access_token(accessToken, accessTokenSecret)
        # Create the API object while passing in the auth information.
        self.api = tweepy.API(authenticate, wait_on_rate_limit = True) 

        #--------------------------------------------------------------------------------------------------------------------------
        # Subsetting variables
        #--------------------------------------------------------------------------------------------------------------------------
        # Columns fixes
        self.in_user_cols = ['name','screen_name','followers_count',
                            'friends_count','statuses_count','favourites_count']
        self.in_entities_cols = ['hashtags','user_mentions']

        # Keys for subsetting
        # For basic tweets
        self.tweets_keys = ['id','full_text','user','favorited','entities']
        # For replies
        self.replies_keys = self.tweets_keys.copy()
        # For retweets
        self.retweets_keys = self.tweets_keys.copy()
        self.retweets_keys.append('retweeted_status')
        # For quoted tweets
        self.quotes_keys = self.tweets_keys.copy()
        self.quotes_keys.append('quoted_status')

        #--------------------------------------------------------------------------------------------------------------------------
        # Final step: extract DFs
        #--------------------------------------------------------------------------------------------------------------------------
         # Lists to allocate tweets, replied, retweets and quoted tweets
        self.tweets = []
        self.replies = []
        self.retweets = []
        self.quotes = []

        # Columns fixes
        in_user_cols = self.in_user_cols
        in_entities_cols = self.in_entities_cols

        # Keys for subsetting
        tweets_keys = self.tweets_keys
        replies_keys = self.replies_keys
        retweets_keys = self.retweets_keys
        quotes_keys = self.quotes_keys

        # Get user timeline 
        for tweet in tweepy.Cursor(method= self.api.user_timeline, screen_name= self.screen_name,
                                    tweet_mode = "extended").items(self.max_length):

            # Convert twitter status into a dictionary and validate if contains an extended version
            try:
                newtweet = self.jsonify_tweepy(tweet.extended_tweet)
            except AttributeError:
                newtweet = self.jsonify_tweepy(tweet)

            # Subset quoted tweets
            if 'quoted_status' in newtweet.keys():
                # Create Empty dictionary
                quoted_status = dict()
                # Loop over quoted tweet keys
                for key in quotes_keys:
                        # Add quoted_status user info to dict
                        if key == 'quoted_status':
                            # Loop over quoted user info
                            for i in in_user_cols:
                                quoted_status['quoted_status.user.'+ i] = newtweet[key]['user'][i]
                        # Add entities info to dict
                        elif key == 'entities':
                            # Loop over entities items
                            for i in in_entities_cols:
                                quoted_status['entities.'+ i] = newtweet[key][i]
                        # Add source node info
                        elif key == 'user':
                            # Loop source user info
                            for i in in_user_cols:
                                quoted_status['user.'+ i] = newtweet[key][i]
                        # Add any other key to dict
                        else:
                            # If we don't have an extended tweet, it will append the 'text' value
                            if key == "full_text":
                                try:
                                    quoted_status[key] = newtweet[key]
                                except:
                                    quoted_status[key] = newtweet['text']
                            else:
                                quoted_status[key] = newtweet[key]                            
                # Append new dictionary to quoted list
                self.quotes.append(quoted_status) 


            # Subset retweeted tweets
            elif 'retweeted_status' in newtweet.keys():
                # Create Empty dictionary
                retweeted_status = dict()
                # Loop over retweet keys
                for key in retweets_keys:
                        # Add retweeted_status user info to dict
                        if key == 'retweeted_status':
                            # Loop over retweeted user info
                            for i in in_user_cols:
                                retweeted_status['retweeted_status.user.'+ i] = newtweet[key]['user'][i]
                        # Add entities info to dict
                        elif key == 'entities':
                            # Loop over entities items
                            for i in in_entities_cols:
                                retweeted_status['entities.'+ i] = newtweet[key][i] 
                        # Add source node info
                        elif key == 'user':
                            # Loop source user info
                            for i in in_user_cols:
                                retweeted_status['user.'+ i] = newtweet[key][i]
                        # Add any other key to dict
                        else:
                            # If we don't have an extended tweet, it will append the 'text' value
                            if key == "full_text":
                                try:
                                    retweeted_status[key] = newtweet[key]
                                except:
                                    retweeted_status[key] = newtweet['text']
                            else:
                                retweeted_status[key] = newtweet[key]                       
                # Append new dictionary to retweeted list
                self.retweets.append(retweeted_status)

            
            # Subset replies
            elif newtweet['in_reply_to_status_id'] != None:
                # Create Empty dictionary
                replied_status = dict()
                # Return replied user info in a dictionary
                replied_user_info = self.get_user_info(user_id = newtweet['in_reply_to_user_id_str'])
                # Loop over replied tweet keys
                for key in replies_keys:
                        # Add entities info to dict
                        if key == 'entities':
                            # Loop over entities items
                            for i in in_entities_cols:
                                replied_status['entities.'+ i] = newtweet[key][i]
                        # Add source node info
                        elif key == 'user':
                            # Loop source user info
                            for i in in_user_cols:
                                replied_status['user.'+ i] = newtweet[key][i]
                        # Add any other key to dict
                        else:
                            # If we don't have an extended tweet, it will append the 'text' value
                            if key == "full_text":
                                try:
                                    replied_status[key] = newtweet[key]
                                except:
                                    replied_status[key] = newtweet['text']
                            else:
                                replied_status[key] = newtweet[key]
                # Merge status dictionary with replies dictionary
                replied_status = {**replied_status,**replied_user_info}
                # Append new dictionary to replies list
                self.replies.append(replied_status)


            # Subset regular tweets
            else:
                # Create Empty dictionary
                normal_status = dict()
                # Loop over basic tweet keys
                for key in tweets_keys:
                        # Add entities info to dict
                        if key == 'entities':
                            # Loop over entities items
                            for i in in_entities_cols:
                                normal_status['entities.'+ i] = newtweet[key][i]
                        # Add source node info
                        elif key == 'user':
                            # Loop source user info
                            for i in in_user_cols:
                                normal_status['user.'+ i] = newtweet[key][i]
                        # Add any other key to dict
                        else:
                            # If we don't have an extended tweet, it will append the 'text' value
                            if key == "full_text":
                                try:
                                    normal_status[key] = newtweet[key]
                                except:
                                    normal_status[key] = newtweet['text']
                            else:
                                normal_status[key] = newtweet[key]                    
                # Append new dictionary to tweets list
                self.tweets.append(normal_status)

    
    #--------------------------------------------------------------------------------------------------------------------------
    # Function to transform a 'tweepy.models.Status' object into a string and then into a Dictionary 
    #--------------------------------------------------------------------------------------------------------------------------
    def jsonify_tweepy(self, tweepy_object):
        # Write : Transform the tweepy's json object and transform into a dictionary 
        json_str = json.dumps(tweepy_object._json, indent = 2)
        # Read : Transform the json into a Python Dictionary
        self.finaljson = json.loads(json_str)
        return self.finaljson

    #--------------------------------------------------------------------------------------------------------------------------
    # Function to get info from users, in this case will be useful for replied users
    #--------------------------------------------------------------------------------------------------------------------------
    def get_user_info(self, user_id):
        # Create dictionary from tweepy oject 
        user_dict = self.jsonify_tweepy(self.api.get_user(id = user_id))
        # Create new comprehensive dictionary with required columns
        user_dict = {'in_reply_to_status.'+ key : user_dict[key] for key in self.in_user_cols}
        return user_dict


# Class to transform each list of dictionaries into dataframes
class tlminer(Miner):
    def __init__(self, username, max_length,
                        consumerKey, consumerSecret,
                        accessToken, accessTokenSecret):
        super().__init__(username, max_length,
                        consumerKey, consumerSecret,
                        accessToken, accessTokenSecret)
        # Transform each list of dictionaries into dataframes
        if self.tweets != []:
            self.tweetsDF = pd.json_normalize(self.tweets)
        elif self.retweets != []:
            self.retweetsDF = pd.json_normalize(self.retweets)
        elif self.quotes != []:
            self.quotedDF = pd.json_normalize(self.quotes)
        elif self.replies != []:
            self.repliesDF = pd.json_normalize(self.replies)


# Twitter text cleaner, additional method for further use
def cleanText(text):
    # Remove @mentions
    text = re.sub(r'@[A-Za-z0-9]+', '', text)
    # Remove hashtags, just the numeral
    text = re.sub(r'#', '', text)
    # Remove tweets with Retweets followed by one or more whitespaces
    text = re.sub(r'RT[\s]+', '', text)
    # Get rid of an URL or hypelink
    text = re.sub(r'https?://(www\.)?(\w+)(\.\w+)', '', text)
    # Remove words with punctuations
    text = re.sub(r'[%s]' % re.escape(string.punctuation), '', text)
    # Remove words with numbers 
    text = re.sub(r'\w*\d\w*', '', text)
    # Remove emojis
    text = re.sub(r"/[^\u1F600-\u1F6FF\s]/i", '', text)
    # Remove emails
    text = re.sub(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', '', text)

    return text
        
# -------------------------------------------------------------------------------------------------------------------------------
# About script
# -------------------------------------------------------------------------------------------------------------------------------
#   Developed by @robguilarr on March 2022, tested using tweepy 4.4.0
    