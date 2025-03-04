import praw
from dotenv import load_dotenv
import os
from datetime import datetime
import pytz
import mysql.connector
from pprint import pprint
from mysql_ingestion import create_post_table_function, data_prep_and_insert
load_dotenv()

TIMEZONE = pytz.timezone('Asia/Kolkata')
UTC_TIMEZONE = pytz.timezone('UTC')

connection = mysql.connector.connect(
    host=os.getenv('HOSTNAME'), 
    database=os.getenv('DATABASE'), 
    user=os.getenv('USERNAME'), 
    password=os.getenv('PASSWORD')
)
cursor = connection.cursor()


reddit = praw.Reddit(
    client_id = os.getenv('CLIENT_ID'),
    client_secret = os.getenv('CLIENT_SECRET'),
    user_agent = os.getenv('USER_AGENT'),
    username=os.getenv('REDDIT_USERNAME'),
    password=os.getenv('REDDIT_PASSWORD'),
)

# time taken in normal fn --> 17 sec.
def normal_fn():
    l = []
    for j in ['1e830zt']:
        comments = reddit.submission(id = j).comments.list()
        for i in comments:
            commentObj = reddit.comment(id = i.id)
            l.append(
                {
                    'id' : i.id,
                    'body' : commentObj.body,
                    'body_html' : commentObj.body_html,
                    'score' : commentObj.score,
                    'distinguished' : commentObj.distinguished,
                    'edited' : commentObj.edited,
                    'post_id' : commentObj.submission.id,
                    'is_submitter' : commentObj.is_submitter,
                    # The submission ID that the comment belongs to.
                    'link_id' : commentObj.link_id,
                    # The ID of the parent comment (prefixed with t1_). If it is a top-level comment, this returns the submission ID instead (prefixed with t3_).
                    'parent_id' : commentObj.parent_id,
                    'created_at' : datetime.fromtimestamp(commentObj.created_utc, tz=TIMEZONE).strftime('%Y-%m-%d %H:%M:%S'),
                }
            )
    return l
start = datetime.now()
print(normal_fn())
end = datetime.now()
print(end - start)




