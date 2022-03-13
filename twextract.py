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

# -------------------------------------------------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------------------------------------------------
# Class to start mining tweets and keep output dictionaries at one level
# -------------------------------------------------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------------------------------------------------
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
        self.tweets_keys = ['id','created_at','full_text','user','favorited','entities']
        # For replies
        self.replies_keys = self.tweets_keys.copy()
        # For retweets
        self.retweets_keys = self.tweets_keys.copy()
        self.retweets_keys.append('retweeted_status')
        # For quoted tweets
        self.quotes_keys = self.tweets_keys.copy()
        self.quotes_keys.append('quoted_status')

        #--------------------------------------------------------------------------------------------------------------------------
        # Variable to rename columns
        #--------------------------------------------------------------------------------------------------------------------------
        self.dict_changer = {'user.name':'source_node.name','user.screen_name':'source_node.screen_name',
        # For source Node
        'user.followers_count':'source_node.followers_count','user.friends_count':'source_node.friends_count',
        'user.statuses_count':'source_node.statuses_count','user.favourites_count':'source_node.favourites_count',
        'favorited':'favorited_by_source', 'id':'tweet_id',
        # For Target Node (Replies)
        'in_reply_to_status.name':'target_node.name','in_reply_to_status.screen_name':'target_node.screen_name',
        'in_reply_to_status.followers_count':'target_node.followers_count','in_reply_to_status.friends_count':'target_node.friends_count',
        'in_reply_to_status.statuses_count':'target_node.statuses_count','in_reply_to_status.favourites_count':'target_node.favourites_count',
        # For Target Node (Retweets)
        'retweeted_status.user.name':'target_node.name','retweeted_status.user.screen_name':'target_node.screen_name',
        'retweeted_status.user.followers_count':'target_node.followers_count','retweeted_status.user.friends_count':'target_node.friends_count',
        'retweeted_status.user.statuses_count':'target_node.statuses_count','retweeted_status.user.favourites_count':'target_node.favourites_count',
        # For Target Node (Quoted)
        'quoted_status.user.name':'target_node.name','quoted_status.user.screen_name':'target_node.screen_name',
        'quoted_status.user.followers_count':'target_node.followers_count','quoted_status.user.friends_count':'target_node.friends_count',
        'quoted_status.user.statuses_count':'target_node.statuses_count','quoted_status.user.favourites_count':'target_node.favourites_count'
        }

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
                replied_user_info = self.get_user_info(user_id = newtweet['in_reply_to_user_id'])
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
        user_dict = self.jsonify_tweepy(self.api.get_user(user_id = user_id))
        # Create new comprehensive dictionary with required columns
        user_dict = {'in_reply_to_status.'+ key : user_dict[key] for key in self.in_user_cols}
        return user_dict

# -------------------------------------------------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------------------------------------------------
# Class to transform each list of dictionaries into dataframes
# -------------------------------------------------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------------------------------------------------
class tlminer(Miner):
    def __init__(self, username, max_length,
                        consumerKey, consumerSecret,
                        accessToken, accessTokenSecret):
        super().__init__(username, max_length,
                        consumerKey, consumerSecret,
                        accessToken, accessTokenSecret)

        # Transform each list of dictionaries into dataframes
        try:
            # Individual transformations
            tweetsDF = self.transformer(tweets_list = self.tweets, kind = 'Tweet')
        except:
            tweetsDF = pd.DataFrame()
            print('No tweets')

        try:
            # Individual transformations
            retweetsDF = self.transformer(tweets_list = self.retweets, kind = 'Retweet')
        except:
            retweetsDF = pd.DataFrame()
            print('No retweets')

        try:
            # Individual transformations
            quotedDF = self.transformer(tweets_list = self.quotes, kind = 'Quoted')
        except:
            quotedDF = pd.DataFrame()
            print('No quoted tweets')

        try:
            # Individual transformations
            repliesDF = self.transformer(tweets_list = self.replies, kind = 'Replied')
        except:
            repliesDF = pd.DataFrame()
            print('No replied tweets')

        # Final output -- MERGE all dataframes into one by equal columns
        data  = pd.concat([tweetsDF, quotedDF, repliesDF, retweetsDF], axis = 0)
        # Fix index repetition issue
        self.data = data.reset_index().drop(columns=['index'])


    #--------------------------------------------------------------------------------------------------------------------------
    # Function to make last transformations on individual Dataframes 
    #--------------------------------------------------------------------------------------------------------------------------
    def transformer(self, tweets_list, kind):
        # Transform into dataframe
        df = pd.json_normalize(tweets_list)
        # Rename columns (To source and target node)
        df = df.rename(columns=self.dict_changer)
        # Set label for type of tweet
        df['type'] = [kind for i in range(df.shape[0])]
        # Clean text label
        df['full_text'] = df['full_text'].apply(self.cleanText)

        return df

    # -------------------------------------------------------------------------------------------------------------------------------
    # Twitter text cleaner, additional method
    # -------------------------------------------------------------------------------------------------------------------------------
    def cleanText(self, text):
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
        text = re.sub(r'’', '', text)
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
    