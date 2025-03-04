import praw
from dotenv import load_dotenv
import os
from datetime import datetime
import pytz
import mysql.connector
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

print('Creating Post Table...')
create_query = """
    CREATE TABLE IF NOT EXISTS r_posts (
        id VARCHAR(50) PRIMARY KEY,
        sub_reddit VARCHAR(100),
        post_type VARCHAR(10),
        title TEXT,
        author VARCHAR(100),
        text_content TEXT,  
        url TEXT,
        score BIGINT,
        num_comments INT,
        upvote_ratio DECIMAL(5, 2),
        over_18 VARCHAR(10),
        edited TIMESTAMP,
        created_at TIMESTAMP,
        fetched_at TIMESTAMP,
        etl_insert_date TIMESTAMP
    );
    """

create_post_table_function(
    cursor=cursor,
    create_query=create_query
)
print('Post Table Created !!')


print('Creating Comments Table...')
create_query = """
    CREATE TABLE IF NOT EXISTS r_comments (
        id VARCHAR(20) PRIMARY KEY,
        body TEXT,
        body_html TEXT,
        score INT,
        distinguished VARCHAR(20),
        edited TIMESTAMP,
        post_id VARCHAR(20),
        is_submitter BOOLEAN,
        link_id VARCHAR(20),
        parent_id VARCHAR(20),
        created_at DATETIME,
        fetched_at TIMESTAMP,
        etl_insert_date TIMESTAMP
    );
    """

create_post_table_function(
    cursor=cursor,
    create_query=create_query
)
print('Comments Table Created !!')

start = datetime.now()

subRedditList = ['python', 'career', 'dataengineering', 'data']
for subreddit in subRedditList:
    subredditObj = reddit.subreddit(subreddit)

    new_posts = subredditObj.new(limit=10000)
    top_posts = subredditObj.top(limit=1000)

    print(f'Inserting Data... for {subreddit}')
    
    data_prep_and_insert(
        conn=connection,
        posts=top_posts,
        postType='top', 
        subreddit=subreddit,
        schema=os.getenv('DATABASE'),
        cursor=cursor,
        timezone=TIMEZONE,
        redditInstance=reddit
    )

    data_prep_and_insert(
        conn=connection,
        posts=new_posts,
        postType='new', 
        subreddit=subreddit,
        schema=os.getenv('DATABASE'),
        cursor=cursor,
        timezone=TIMEZONE,
        redditInstance=reddit
    )

connection.close()

end = datetime.now()
print('Data Inserted !!')
print('Time Taken to insert', end - start)