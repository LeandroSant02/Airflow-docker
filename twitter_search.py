from asyncore import write
from importlib.resources import path
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
import config
import tweepy as tw
import json
from pathlib import Path
import os


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 1
}


def twitter_search_tweets():
    client = tw.Client(config.bearer_token)
    query = '#covid19 -is:retweet lang:pt'
    pesquisa = client.search_recent_tweets(query=query, max_results=20, tweet_fields=['author_id', 'id', 'created_at'], user_fields=['id','username'], expansions=['author_id'])
    tweets = pesquisa.data
    users = {u['id']: u for u in pesquisa.includes['users']}
    for tweet in tweets:
        if users[tweet.author_id]:
            user = users[tweet.author_id]
            user_id = tweet['id']
            texto = tweet['text']
            author_id = tweet['author_id']
            created_at = tweet['created_at']
            print('User_id:', tweet.id)
            print('Tweet:', tweet.text)
            print('author_id:', tweet.author_id)
            print('Created_at:', tweet.created_at)
            print('user_name:', user.username)
            with open("tweet.json", "+a") as output:
                data = {"User_id": user_id,
                        "tweet": texto,
                        "author_id": author_id,
                        "created_at": created_at,
                        "user_name": user
                        }
                output.write("{}\n,".format((json.dumps(data, default=str, ensure_ascii=True, sort_keys=True))))

with DAG("twitter_search", schedule_interval='@daily', catchup=False, start_date=datetime.now()) as dag:

    Task_twitter_search = PythonOperator(
       task_id='twitter_search',
       python_callable=twitter_search_tweets
    )

    Processing_user = SparkSubmitOperator(
        application=(r'C:\Users\leand\Desktop\Case Verx\airflow-docker\spark\transformation.py'),
        conn_id="spark_default",
        task_id="transformation",
        env_vars=['JAVA_HOME']
    )

    Task_twitter_search >> Processing_user
