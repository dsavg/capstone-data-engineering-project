# fetch reddit data
import os
import json
from request_reddit_data import SubredditAPI
from datetime import datetime, timezone


def store_json_data(json_obj,
                    folder_name):
    """
    source code: https://stackoverflow.com/questions/38915183/python-conversion-from-json-to-jsonl
    """
    posts = json_obj.json()['data']['children']
    with open(f"./data/reddit/{folder_name}.json", "w") as outfile:
        for entry in posts:
            json.dump(entry, outfile)
            outfile.write('\n')

if __name__ == "__main__":

    reddit = SubredditAPI()
    # collect top 300 trending world news data
    res = reddit.get('worldnews', 'hot', 300)
    # get the current utc date
    now = datetime.now(timezone.utc)
    current_date = str(now.date())
    # store data in s3
    # os.mkdir(f'./data/reddit/{current_date}/')
    store_json_data(res, f'{current_date}/reddit-worldnews')
