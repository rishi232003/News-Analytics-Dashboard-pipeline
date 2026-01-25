from flask import Flask,jsonify,render_template
from flask_cors import CORS
from pymongo import MongoClient
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

#LOAD CONFIGS
MONGO_HOST = os.getenv("MONGO_HOST",'localhost')
MONGO_PORT = os.getenv('MONGO_PORT','27017')
MONGO_USER = os.getenv("MONGO_USERNAME",'admin')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD','password')
MONGO_DB = os.getenv("MONGO_DATABASE",'news_analytics')


app = Flask(__name__)
CORS(app)

#GETTING MONGDB CONNECTION
def get_db():
    client = MongoClient(f'mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/')
    return client[MONGO_DB]



#API ENDPOINTS
@app.route('/')
def index():
    """
    Home page / Dashboard
    """
    return render_template('index.html')

@app.route('/api/health', methods=['GET'])
def health_check():
    # health check to verify every endpoint is up and running 

    return jsonify({
        'status' : 'healthy',
        'timestamp' : datetime.now().isoformat(),
        'database' : MONGO_DB
    }),200


@app.route('/api/stats', methods=['GET'])
def get_stats():
    # Get overall stats amd return total counts for each collection 
    try:
        db = get_db()
        stats = {
            'total_articles' : db['articles'].count_documents({}),
            'total_sources' : db['source_stats'].count_documents({}),
            'total_categories' : db['category_stats'].count_documents({}),
            'total_keywords' : db['keywords'].count_documents({}),
            'last_updated' : None
            }
        
        latest_article = db['articles'].find_one(
            sort = [('ingested_at',-1)]
        )
        if latest_article and 'ingested_at' in latest_article:
            stats['last_updated'] = latest_article['ingested_at'].isoformat()
        return jsonify(stats), 200

    except Exception as e:
        return jsonify({'error' : str(e)}), 500


@app.route('/api/sources',methods = ['GET'])
def get_top_sources():
    """
    Get top news sources by article count
    Returns: List of sources sorted by article count (descending)
    """
    try:
        db = get_db()

        sources = list(db['source_stats'].find(
        {},
        {'_id' : 0, 'source' : 1, 'article_count': 1}).sort('article_count' , -1).limit(15))
        
        

        return jsonify({
            'sources' : sources,
            'count' : len(sources)
        }) , 200

    except Exception as e:
        return jsonify({'error' : str(e)}),500

@app.route("/api/categories", methods = ['GET'])
def get_categories():
    """Gets article count by category"""
    """Returns : List of categories with count"""

    try:
        db = get_db()

        categories = list(db['category_stats'].find(
        {},
        {"_id" : 0 , 'category' : 1, 'article_count' : 1}).sort('article_count',-1).limit(15))
        
        

        return jsonify({
            'categories' : categories,
            'count' : len(categories)
        }), 200

    except Exception as e:
        return jsonify({'error' : str(e)}),500

@app.route('/api/keywords', methods=['GET'])
def get_top_keywords():
    """
    Get top keywords from article titles
    Returns: List of top 20 keywords
    """
    try:
        db = get_db()
        
        keywords = list(db['keywords'].find(
            {},
            {'_id': 0, 'keyword': 1, 'count': 1}
        ).sort('count', -1).limit(20))
        
        return jsonify({
            'keywords': keywords,
            'count': len(keywords)
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/articles', methods=['GET'])
def get_recent_articles():
    """
    Get recent articles
    Returns: List of 20 most recent articles
    """
    try:
        db = get_db()
        
        articles = list(db['articles'].find(
            {},
            {
                '_id': 0,
                'title': 1,
                'source': 1,
                'author': 1,
                'description': 1,
                'url': 1,
                'published_at': 1,
                'category': 1
            }
        ).sort('published_at', -1).limit(20))
        
        return jsonify({
            'articles': articles,
            'count': len(articles)
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/articles/category/<category>', methods=['GET'])
def get_articles_by_category(category):
    """
    Get articles for a specific category
    Returns: List of articles in that category
    """
    try:
        db = get_db()
        
        articles = list(db['articles'].find(
            {'category': category},
            {
                '_id': 0,
                'title': 1,
                'source': 1,
                'author': 1,
                'url': 1,
                'published_at': 1
            }
        ).sort('published_at', -1).limit(20))
        
        return jsonify({
            'category': category,
            'articles': articles,
            'count': len(articles)
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500



@app.route('/api/articles/source/<source>', methods=['GET'])
def get_articles_by_source(source):
    """
    Get articles for a specific source
    Returns: List of articles from that source
    """
    try:
        db = get_db()
        
        articles = list(db['articles'].find(
            {'source': source},
            {
                '_id': 0,
                'title': 1,
                'author': 1,
                'url': 1,
                'published_at': 1,
                'category': 1
            }
        ).sort('published_at', -1).limit(20))
        
        return jsonify({
            'source': source,
            'articles': articles,
            'count': len(articles)
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    print("=" * 70)
    print("STARTING FLASK API SERVER")
    print("=" * 70)
    print(f"MongoDB: {MONGO_DB}")
    print(f"API will be available at: http://localhost:5001")
    print(f"Dashboard will be at: http://localhost:5001")
    print("=" * 70)
    print("\nEndpoints:")
    print("  GET /api/health          - Health check")
    print("  GET /api/stats           - Overall statistics")
    print("  GET /api/sources         - Top sources")
    print("  GET /api/categories      - Categories breakdown")
    print("  GET /api/keywords        - Top keywords")
    print("  GET /api/articles        - Recent articles")
    print("  GET /api/articles/category/<name>  - Articles by category")
    print("  GET /api/articles/source/<name>    - Articles by source")
    print("=" * 70)
    
    app.run(debug=True, host='0.0.0.0', port=5001)