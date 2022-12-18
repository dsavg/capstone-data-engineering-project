"""

source code: https://towardsdatascience.com/how-to-use-the-reddit-api-in-python-5e05ddfd1e5c
"""
import os
import requests
from airflow.models import Variable

# GET Reddit configs from Airflow environment
reddit_client_id = Variable.get('reddit_client_id')
reddit_client_secret = Variable.get('reddit_client_secret')
reddit_username = Variable.get('reddit_username')
reddit_password = Variable.get('reddit_password')


class SubredditAPI():
    """Class to connect to Reddit API and request data."""
    client_id = reddit_client_id
    client_secret = reddit_client_secret
    headers = {'User-Agent': 'MyBot/0.0.1'}
    data = {
        'grant_type': 'password',
        'username': reddit_username,
        'password': reddit_password}


    def __init__(self):
        """
        Class initialization.

        :param client_id: client id for Reddit API requests.
        :param client_secret: client secret for Reddit API requests.
        :param data: data for Reddit API requests.
        :param headers: headers for Reddit API requests.
        """
        self.params = None
        self.__auth = None
        self.__request = None
        self.__token = None
        self.__headers = None

    def get(self,
            subreddit_name: str,
            subreddit_type: str,
            limit: int = 500):
        """
        Get response from Reddit API.

        :param name: subreddit name (str)
        :param type: subreddit type (str). Use `hot` for most popular and `new` for most recent
        :return: json object holding request response from Reddit
        """
        self.params = {'limit': limit}
        self.__auth = requests.auth.HTTPBasicAuth(SubredditAPI.client_id,
                                                  SubredditAPI.client_secret)
        # send request for an OAuth token
        self.__request = requests.post('https://www.reddit.com/api/v1/access_token',
                                       auth=self.__auth,
                                       data=SubredditAPI.data,
                                       headers=SubredditAPI.headers,
                                       timeout=5)
        # convert response to JSON and pull access_token value
        self.__token = self.__request.json()['access_token']
        # add authorization to our headers dictionary
        self.__headers = {**self.headers, **{'Authorization': f"bearer {self.__token}"}}

        return (requests.get(f"https://oauth.reddit.com/r/{subreddit_name}/{subreddit_type}",
                             headers=self.__headers,
                             params=self.params,
                             timeout=5))
