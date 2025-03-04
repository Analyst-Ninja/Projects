from datetime import datetime

def create_post_table_function(cursor, create_query):
    cursor.execute(create_query)
    return 0

def post_data_insert_function(cursor, schema, tableName, payload, timezone):
    
    cursor.execute("SELECT id FROM r_posts WHERE id IN (%s)",(payload['id'],))
    exists = cursor.fetchone()
    
    if not exists:
        sql = f"INSERT INTO {schema}.{tableName} VALUES({'%s,'*len(payload)+'%s'})"
        dataToInsert = {k: (lambda x: x)(v) for k, v in payload.items()}
        dataToInsert['etl_insert_date'] = str(datetime.now(tz = timezone))

        cursor.execute(sql, tuple(dataToInsert.values()))

    return 0

def submission_data_insert_function(cursor, schema, tableName, payload, timezone):
    cursor.execute("SELECT id FROM r_comments WHERE id IN (%s)",(payload['id'],))
    exists = cursor.fetchone()
    if not exists:
        sql = f"INSERT INTO {schema}.{tableName} VALUES({'%s,'*len(payload)+'%s'})"
        dataToInsert = {k: (lambda x: x)(v) for k, v in payload.items()}
        dataToInsert['etl_insert_date'] = str(datetime.now(tz = timezone))

        cursor.execute(sql, tuple(dataToInsert.values()))

    return 0

def data_prep_and_insert(conn,cursor,posts,postType,subreddit,timezone,schema,redditInstance):
    for post in posts:
        post_dict = {
            'id' : post.id,
            'sub_reddit' : subreddit,
            'post_type' : postType,
            'title' : post.title,
            'author' : post.author.name if post.author else None,
            'text_content' : post.selftext if post.selftext else None,
            'url' : post.url,
            'score' : int(post.score),
            'num_comments' : post.num_comments,
            'upvote_ratio' : post.upvote_ratio,
            'over_18' : post.over_18,
            'edited' : datetime.fromtimestamp(post.edited, tz=timezone).strftime('%Y-%m-%d %H:%M:%S') if post.edited != 0 else None ,
            'created_at' : datetime.fromtimestamp(post.created_utc, tz=timezone).strftime('%Y-%m-%d %H:%M:%S'),
            'fetched_at' : datetime.now(tz = timezone).strftime('%Y-%m-%d %H:%M:%S'),
        }

        post_data_insert_function(
            cursor=cursor, 
            schema=schema, 
            tableName='r_posts',
            payload=post_dict, 
            timezone=timezone
        )

        # try:
        #     comments = redditInstance.submission(id = post.id).comments.list()
        # except:
        #     print(f'Comments not found for {post.id}')
        # for i in comments:
        #     try:
        #         commentObj = redditInstance.comment(id = i.id)
        #         comment_dict = {
        #                 'id' : i.id,
        #                 'body' : commentObj.body,
        #                 'body_html' : commentObj.body_html,
        #                 'score' : commentObj.score,
        #                 'distinguished' : commentObj.distinguished,
        #                 'edited' : datetime.fromtimestamp(commentObj.edited, tz=timezone).strftime('%Y-%m-%d %H:%M:%S') if post.edited != 0 else None,
        #                 'post_id' : commentObj.submission.id,
        #                 'is_submitter' : commentObj.is_submitter,
        #                 # The submission ID that the comment belongs to.
        #                 'link_id' : commentObj.link_id,
        #                 # The ID of the parent comment (prefixed with t1_). If it is a top-level comment, this returns the submission ID instead (prefixed with t3_).
        #                 'parent_id' : commentObj.parent_id,
        #                 'created_at' : datetime.fromtimestamp(commentObj.created_utc, tz=timezone).strftime('%Y-%m-%d %H:%M:%S'),
        #                 'fetched_at' : datetime.now(tz = timezone).strftime('%Y-%m-%d %H:%M:%S'),
        #             }

        #         submission_data_insert_function(
        #             cursor=cursor, 
        #             schema=schema, 
        #             tableName='r_comments',
        #             payload=comment_dict, 
        #             timezone=timezone
        #         )
        #         conn.commit()
        #     except:
        #         print(f'Data not found for commentId: {i.id}')
        conn.commit()
def main():
    pass

if __name__ == "__main__":
    main()