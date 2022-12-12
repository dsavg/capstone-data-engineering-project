# fetch reddit data
import os
import json
import configparser
from request_reddit_data import SubredditAPI
from datetime import datetime, timezone

# config = configparser.ConfigParser()
# config.read('dl.cfg')
# # set aws configs
# os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

# Extract Functions
def store_json_data(json_obj,
                    folder_name,
                    output_data_path):
    """
    source code: https://stackoverflow.com/questions/38915183/python-conversion-from-json-to-jsonl
    """
    posts = json_obj.json()['data']['children']
    with open(f"{output_data_path}data/json_logs/{folder_name}.json", "w") as outfile:
        for entry in posts:
            json.dump(entry, outfile)
            outfile.write('\n')

def fetch_reddit_data(output_data_path):
    reddit = SubredditAPI()
    # collect top 300 trending world news data
    res = reddit.get('worldnews', 'hot', 50)
    # get the current utc date
    now = datetime.now(timezone.utc)
    current_date = str(now.date())
    # current_date = '2022-12-10'
    # store data in s3
    if not os.path.exists(f'{output_data_path}data/json_logs/{current_date}/'):
        os.mkdir(f'{output_data_path}data/json_logs/{current_date}/')
    store_json_data(res, f'{current_date}/reddit-worldnews', output_data_path)


if __name__ == "__main__":
    # extract data
    s3_path = ''
    fetch_reddit_data(s3_path)

