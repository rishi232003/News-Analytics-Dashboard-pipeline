# Using Pyspark to fetch news Articles form NEWS API

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,count,explode,split,lower,regexp_replace
from pymongo import MongoClient
from dotenv import load_dotenv
import os
from datetime import datetime
import time

# Load environment variables from .env file
load_dotenv()

# Configuration from environment variables
NEWS_API_KEY = os.getenv('NEWS_API_KEY')
MONGO_HOST = os.getenv("MONGO_HOST", 'localhost')
MONGO_PORT = os.getenv("MONGO_PORT", '27017')
MONGO_USER = os.getenv("MONGO_USER",'admin')
MONGO_PASS = os.getenv("MONGO_PASSWORD",'password')
MONGO_DB = os.getenv('MONGO_DATABASE','news_analytics')

print("=" * 70)
print("NEWS ANALYTICS PYSPARK JOB")
print("=" * 70)
print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# STEP 1: Fetch articles from News API
def fetch_news_articles():
    # Fetch articles from News API across multiple categories
    # Returns a list of article dictionaries
    
    print('\nFetching articles from News API...')
    url = "https://newsapi.org/v2/top-headlines"

    categories = ['technology', 'business','sports','entertainment']
    all_articles = [] 
    
    for i,category in enumerate(categories):
        params = {
            'country' :'us',
            'category' : category,
            'pageSize' : 25,
            'apiKey' : NEWS_API_KEY }

        try:
            response = requests.get(url,params = params)
            data = response.json()

            if response.status_code == 200:
                articles = data.get('articles',[])
                print(f"  Successfully fetched {len(articles)} articles from '{category}' category")

                for i,article in enumerate(articles):
                    article['category'] = category
                
                all_articles.extend(articles)
            else:
                print(f"  Error fetching '{category}': {data.get('message', 'Unknown error')}")

        except Exception as e:
            print(f"  Exception occurred for '{category}': {e}")
    print(f"\n  Total articles fetched: {len(all_articles)}")
    return all_articles


# STEP 2: Create Spark session for data processing

def create_spark_session():
    print("\nInitializing Spark session...")

    spark = SparkSession.builder \
        .appName('NewsAnalytics') \
        .config("spark.driver.memory",'2g') \
        .config('spark.executor.memory', '2g') \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel('ERROR')

    print('  Spark session created successfully')
    return spark

# STEP 3: Process articles with Spark for analytics
def process_articles_with_spark(spark,articles):
    print('\nProcessing articles with PySpark...')

    cleaned_articles = []
    for article in articles:
        # Only keep articles with valid title and source
        if article.get('title') and article.get('source',{}).get('name'):
            cleaned_articles.append({
                'title' : article.get('title',''),
                'author' : article.get('author','Unknown'),
                'source' : article.get('source',{}).get('name','Unknown'),
                'description' : article.get('description',''),
                'url' : article.get('url',''),
                'category' : article.get('category','general'),
                'published_at' : article.get('publishedAt', ''),
                'content' : article.get('content','')
            })
    print(f"  Cleaned {len(cleaned_articles)} articles (removed {len(articles) - len(cleaned_articles)} incomplete)")

    # Create Spark DataFrame from cleaned articles
    df = spark.createDataFrame(cleaned_articles)

    print(f"  Created Spark DataFrame with {df.count()} rows")

    # Display a sample of the data
    print('\nSample of data:')
    df.select('title','source','category').show(5,truncate = 50)

    
    print('\nComputing articles per source...')
    source_stats = df.groupBy("source") \
        .agg(count('*').alias('article_count')) \
        .orderBy(col('article_count').desc())
    
    print("Top Sources:")
    source_stats.show(10,truncate = False)


    print("\nComputing articles per category...")
    category_stats = df.groupBy('category') \
        .agg(count('*').alias('article_count')) \
        .orderBy(col('article_count').desc())
    
    print("\nArticles by category:")
    category_stats.show(truncate=False)


    keywords_df = df.select(
        explode(split(lower(col('title')), ' ')).alias('word')
    )
    
    # Remove common stop words and clean
    stop_words = ['the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 
                  'of', 'with', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
                  'as', 'by', 'from', 'it', 'its', 'this', 'that', 'has', 'have']
    
    keywords_df = keywords_df.filter(~col('word').isin(stop_words)) \
        .filter(col('word').rlike('^[a-z]{3,}$'))  # Only words with 3+ letters
    
    keyword_counts = keywords_df.groupBy('word') \
        .agg(count('*').alias('count')) \
        .orderBy(col('count').desc())
    
    print("\nTop keywords:")
    keyword_counts.show(15, truncate=False)
    
    return df, source_stats, category_stats, keyword_counts


# STEP 4: Store processed results in MongoDB
def store_in_mongodb(df,source_stats,category_stats,keyword_counts):

    print("\nStoring results in MongoDB...")

    try:
        client = MongoClient(f'mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/')
        db = client[MONGO_DB]

        print(f'  Connected to database: {MONGO_DB}')
        # Store raw articles in collection
        articles_collection = db['articles']
        articles_data = df.collect()
        articles_list = [row.asDict() for row in articles_data]
        
        # Add timestamp to track when data was ingested
        timestamp = datetime.now()
        for article in articles_list:
            article['ingested_at'] = timestamp 
        
        articles_collection.delete_many({})  # Clear old data before inserting new

        if articles_list:
            articles_collection.insert_many(articles_list)
            print(f"  Inserted {len(articles_list)} articles into 'articles' collection")

        
        # Store source statistics
        source_stats_collection = db['source_stats']
        source_data = source_stats.collect()
        source_list = [{'source': row['source'], 
                       'article_count': row['article_count'],
                       'updated_at': timestamp} 
                        for row in source_data]
        
        source_stats_collection.delete_many({})
        if source_list:
            source_stats_collection.insert_many(source_list)
            print(f"  Inserted {len(source_list)} sources into 'source_stats' collection")

        # Store category statistics
        category_stats_collection = db['category_stats']

        category_data = category_stats.collect()

        category_list = [{'category' : row['category'],
                        'article_count': row['article_count'],
                        'updated_at' : timestamp}
                        for row in category_data]
        category_stats_collection.delete_many({})

        if category_list:
            category_stats_collection.insert_many(category_list)
            print(f"  Inserted {len(category_list)} categories into 'category_stats' collection")
        
        # Store top keywords from article titles
        keywords_collection = db['keywords']
        keyword_data = keyword_counts.limit(50).collect()
        keyword_list = [{ 'keyword' : row['word'],
                          'count' : row['count'],
                          'updated_at' : timestamp}
                          for row in keyword_data]

        keywords_collection.delete_many({})
        if keyword_list:
            keywords_collection.insert_many(keyword_list)
            print(f"  Inserted {len(keyword_list)} keywords into 'keywords' collection")
        
        client.close()
        print("\nAll data stored successfully in MongoDB")
        
    except Exception as e:
        print(f"\nError storing in MongoDB: {e}")

# STEP 5: Verify data was stored correctly in MongoDB

def verify_mongodb_data():

    print('\nVerifying data in MongoDB...')

    try:
        client = MongoClient(f'mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/')
        db = client[MONGO_DB]

        collections = ['articles','source_stats','category_stats','keywords']

        for coll_name in collections:
            count = db[coll_name].count_documents({})
            print(f'  {coll_name}: {count} documents')
        
        client.close()
    
    except Exception as e:
        print(f'  Error verifying data: {e}')

# Main pipeline execution
def main():
    """
    Runs the complete news analytics pipeline:
    1. Fetches articles from News API
    2. Processes data with Spark
    3. Stores results in MongoDB
    4. Verifies the data
    """
    start_time = time.time()
    
    try:
        # Step 1: Fetch articles from News API
        articles = fetch_news_articles()
        
        if not articles:
            print("\nNo articles fetched. Exiting.")
            return
        
        # Step 2: Create Spark session
        spark = create_spark_session()

        # Step 3: Process articles with Spark
        df, source_stats, category_stats, keyword_counts = process_articles_with_spark(spark, articles)

        # Step 4: Store results in MongoDB
        store_in_mongodb(df, source_stats, category_stats,keyword_counts)
        
        # Step 5: Verify data integrity
        verify_mongodb_data()

        # Clean up Spark session
        spark.stop()

        # Print job summary
        elapsed = time.time() - start_time
        print("\n" + "=" * 70)
        print("JOB COMPLETED SUCCESSFULLY")
        print("=" * 70)
        print(f"Total time: {elapsed:.2f} seconds")
        print(f"Articles processed: {len(articles)}")
        print(f"Data stored in MongoDB database: {MONGO_DB}")
        print("\nYou can now:")
        print("  1. Check Mongo Express: http://localhost:8081")
        print("=" * 70)
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__': 
    main()






