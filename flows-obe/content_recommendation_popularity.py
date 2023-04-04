import logging
#import interactions.snowflake_interaction as si
#import prefect
from prefect import task, flow
#from prefect_aws.ecs import ECSTask
#ecs_task_block = ECSTask.load("ecs-fargate")

from snowflake.connector import DictCursor


from prefect import flow, task
from prefect_snowflake import SnowflakeConnector, SnowflakeCredentials
#from prefect_snowflake.database import snowflake_query



from sqlalchemy import create_engine



from datetime import date
import sys, os, yaml
import pandas as pd
import math
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import UUID
from sklearn.model_selection import train_test_split
import boto3

log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

for l in ['snowflake.connector', 'interactions.snowflake_interaction']:
    logger = logging.getLogger(l)
    logger.setLevel('INFO')
    log_stream = logging.StreamHandler(sys.stdout)
    log_stream.setFormatter(log_format)
    logger.addHandler(log_stream)

event_df_fields = [
    '_timestamp'
    , 'event_type'
    , 'content_id'
    , 'user_id'	
]
event_query = """
with view_events as (
    select s.stream_start_time :: timestamp as _timestamp , 'VIEW' as event_type , s.content_id as content_id , s.user_id as user_id 
    from core.fct_streams s 

    inner join core.fct_videos v on s.content_id = v.content_id 
    and s.stream_start_time :: timestamp <= dateadd(day,14,v.created_at) 
    
    where is_completion = 1 and s.stream_type in ('vod') ) 
    
, rating_events as (
    select created_at :: timestamp as _timestamp , 'RATE' as event_type , content_id as content_id , user_id as user_id 
    from core.fct_class_ratings where overall_rating > 3 ) 
    
, bookmark_events as (
    select created_at :: timestamp as _timestamp , 'BOOKMARK' as event_type , content_id as content_id , user_id as user_id 
    from core.fct_class_bookmarks ) 
    
, favorite_events as (
    select created_at :: timestamp as _timestamp , 'FAVORITE' as event_type , content_id as content_id , user_id as user_id 
    from core.fct_class_favorites ) 
    
, calendar_events as (
    select created_at :: timestamp as _timestamp , 'CALENDAR' as event_type , content_id as content_id , user_id as user_id 
    from core.fct_class_added_to_calendar ) 
    
, final as (
    select * from view_events 
    union all 
    select * from rating_events 
    union all 
    select * from bookmark_events  
) 
select {} from final
limit 10
""".format(','.join(event_df_fields))

content_df_fields = [
        'content_id'
        , 'stream_type'
        , 'class_type'
        , 'created_at'
    ]
content_query = """
with content as (
    select * from core.fct_videos 
) 
select {} from content c 
limit 10
""".format(','.join(content_df_fields))

@task
def smooth_user_preference(x):
    return math.log(1 + x, 2)

@task
def get_data_from_query(query):
    result = []
    connector = SnowflakeConnector.load("snowflake-mb")
    with connector.get_connection() as conn:
        try:
            cur = conn.cursor(DictCursor)
            cur.execute(query)
            while True:
                rows = cur.fetchmany(1000)
                if not rows:
                    break
                result.extend(rows)
            cur.close()
        except Exception as e:
            logger.exception(e)
            raise e
        else:
            logger.info("get query success: returned {} rows".format(len(result)))

        result = [{k.lower(): v for k, v in row.items()} for row in result]
        result_df = pd.DataFrame.from_dict(result)

        return result_df


@task
def preprocess_dataframes(event_df, content_df):

    user_df = pd.DataFrame(event_df['user_id'].unique(), columns=['user_id'])
    user_lookup = {y: x for x, y in zip(user_df.index, user_df['user_id'])}

    content_df = content_df[content_df_fields].dropna()
    event_df = event_df[event_df['content_id'].isin(content_df['content_id'])]
    content_lookup = {y: x for x, y in zip(content_df.index, content_df['content_id'])}

    event_type_strength = {
        'VIEW': 1.0
        , 'RATE': 3.0
        , 'BOOKMARK': 5.0
    }

    event_df = event_df[event_df['event_type'].isin(event_type_strength)]

    event_df['event_strength'] = event_df['event_type'].apply(lambda x: event_type_strength[x])
    event_df['content_id_num'] = event_df['content_id'].apply(lambda x: content_lookup[x])
    event_df['user_id_num'] = event_df['user_id'].apply(lambda x: user_lookup[x])

    content_df['content_id_num'] = content_df['content_id'].apply(lambda x: content_lookup[x])
    content_df['class_type'] = content_df['class_type'].apply(lambda x: x.lower())

    return event_df, content_df, content_lookup

@task
def preprocess_interactions(df):
    users_interactions_count_df = df.groupby(['user_id_num', 'content_id_num']).size().groupby('user_id_num').size()
    logger.info('# users: %d' % len(users_interactions_count_df))

    users_with_enough_interactions_df = users_interactions_count_df[users_interactions_count_df >= 5].reset_index()[['user_id_num']]
    logger.info('# users with at least 5 interactions: %d' % len(users_with_enough_interactions_df))

    logger.info('# of interactions: %d' % len(df))

    interactions_from_selected_users_df = df.merge(users_with_enough_interactions_df,
                                                   how='right',
                                                   left_on='user_id_num',
                                                   right_on='user_id_num')

    logger.info('# of interactions from users with at least 5 interactions: %d' % len(interactions_from_selected_users_df))


    interactions_full_df = interactions_from_selected_users_df \
        .groupby(['user_id_num', 'content_id_num'])['event_strength'].sum() \
        .apply(smooth_user_preference).reset_index()

    logger.info('# of unique user/item interactions: %d' % len(interactions_full_df))

    return interactions_full_df


@task
def get_popularity_df(interactions_full_df, content_lookup):
    content_lookup_iv = {v: k for k, v in content_lookup.items()}

    item_popularity_df = interactions_full_df.groupby('content_id_num')['event_strength'].sum().sort_values(
        ascending=False).reset_index()
    item_popularity_df['content_id'] = item_popularity_df['content_id_num'].apply(lambda x: content_lookup_iv[x])

    item_popularity_writeout_df = item_popularity_df[['content_id', 'event_strength']]

    return item_popularity_writeout_df

@task
def process_event_type_strength(item_popularity_df):

    event_type_strength = {
        'VIEW': 1.0
        , 'RATE': 3.0
        , 'BOOKMARK': 5.0
    }
    for s in event_type_strength:
        item_popularity_df['weight_{event}'.format( event = s.lower().replace(' ','_' ) ) ] = event_type_strength[s]

    return item_popularity_df

@task
def insert_dataframe_into_snowflake(df,table_name,database,schema):
    connector = SnowflakeConnector.load("snowflake-mb")
    credentials = SnowflakeCredentials.load("snowflake-mb") # connector.credentials #
    try:
        engine = create_engine(
            'snowflake://{user}:{password}@{account}/{db}/{schema}?warehouse={warehouse}'.format(
                user=credentials.user,
                password=credentials.password,
                account=credentials.account,
                db=database,
                schema=schema,
                warehouse=connector.warehouse
            ))
        logger.info("rows to be inserted: {}".format(len(df)))
        logger.info("inserting rows...")
        df.to_sql(table_name, con=engine, index=False, if_exists='append', chunksize=5000)

    except Exception as e:
        logger.exception(e)
        raise e
    else:
        logger.info("{} rows inserted into table_name: {}".format(len(df), table_name))


@flow(name="Get Content and Event Data")
def get_data():
    content_df = get_data_from_query(content_query)
    event_df = get_data_from_query(event_query)
    return content_df, event_df

@flow(name="Preprocess Content and Event Data")
def preprocess(event_df,content_df):
    pre_event_df, pre_content_df, content_lookup = preprocess_dataframes(event_df, content_df)
    interactions_full_df = preprocess_interactions(df=pre_event_df)
    return interactions_full_df, content_lookup

@flow(name="Build Popularity DF")
def get_process_pop_df(interactions_full_df,content_lookup):
    item_popularity_df = get_popularity_df(interactions_full_df=interactions_full_df, content_lookup=content_lookup)
    item_popularity_df_processed = process_event_type_strength(item_popularity_df=item_popularity_df)
    return item_popularity_df_processed

@flow
def content_recommendation_popularity_dev():
    content_df, event_df = get_data()
    interactions_full_df, content_lookup = preprocess(event_df,content_df)
    item_popularity_df_processed = get_process_pop_df(interactions_full_df,content_lookup)
    insert_dataframe_into_snowflake(item_popularity_df_processed,table_name='content_ratings_popularity',database='OBE_ANALYTICS',schema='DATASCIENCE_OUTPUT')
    print (item_popularity_df_processed.head())
    
    
    #insert_data_snowflake(df=item_popularity_df_processed, table='content_ratings_popularity')
    # s3 = get_s3_session(sa_id,sa_key,sa_role)
    # insert_dataframe_s3(df=item_popularity_df_processed,s3=s3,bucket = 'obe-analytics-export-staging', prefix = 'content_ratings_popularity')
    # insert_dataframe_postgres(df=item_popularity_df_processed, table='content_ratings_popularity')


content_recommendation_popularity_dev()
#if __name__ == "main":
#    content_recommendation_popularity_dev()