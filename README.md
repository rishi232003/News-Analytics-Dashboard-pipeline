# News Analytics Dashboard

I built this to see what's actually trending in the news. It grabs articles from a bunch of sources, processes them to find patterns, and shows everything on a dashboard.

## What does it do?

Pulls news articles from News API (tech, business, science, health stuff), runs them through PySpark to figure out which sources publish the most and what keywords keep showing up, then displays it all on a web page. The dashboard updates automatically so you can see what's happening right now.

## Tools I used

- **PySpark** - handles the data processing (counting articles, finding trends, etc.)
- **MongoDB** - stores everything (the articles + all the stats I calculated)
- **Flask** - backend API that serves the data
- **Docker** - runs MongoDB locally without installation headaches
- **JavaScript/CSS** - built the actual dashboard you see in your browser

## What you can see on the dashboard

- Which news sources are publishing the most articles
- What keywords are trending in headlines
- How articles are distributed across different categories
- Recent articles with links to read the full story
- Everything updates in real-time

## How to run it yourself

**You'll need:**
- Python 3.11 or newer
- Docker Desktop installed
- A free API key from [newsapi.org](https://newsapi.org)

**Steps:**

1. Clone this repo
```bash
   git clone https://github.com/YOUR_USERNAME/news-stream-analyzer.git
   cd news-stream-analyzer
```

2. Install the Python stuff
```bash
   pip install -r requirements.txt
```

3. Set up your API key
```bash
   cp .env.example .env
   # Open .env and paste your News API key
```

4. Start MongoDB
```bash
   docker-compose -f docker/docker-compose.yml up -d
```

5. Run the data pipeline (this fetches and processes articles)
```bash
   python spark_jobs/news_spark_job.py
```

6. Start the API server
```bash
   python api/flask_app.py
```

7. Open your browser to `http://localhost:5001`

## API endpoints if you want to use them directly

- `/api/stats` - overall numbers (total articles, sources, etc.)
- `/api/sources` - top news sources ranked by article count
- `/api/categories` - breakdown by category
- `/api/keywords` - what words keep showing up in headlines
- `/api/articles` - recent articles
- `/api/articles/category/technology` - just tech articles
- `/api/articles/source/CNN` - just CNN articles

## How it works

The PySpark job grabs articles from News API, counts how many come from each source, extracts keywords from the titles, and groups everything by category. All that gets saved to MongoDB in 4 different collections so the API can query it quickly. The dashboard pulls from the API every few seconds to stay updated.

## Project structure
```
├── spark_jobs/news_spark_job.py    # The PySpark pipeline
├── api/flask_app.py                # Flask API backend
├── api/templates/index.html        # Dashboard page
├── api/static/css/style.css        # Styling
├── api/static/js/app.js            # Frontend code
├── docker/docker-compose.yml       # MongoDB setup
└── requirements.txt                # Python packages
```

## Stuff I might add later

- Sentiment analysis to show if news is positive/negative
- Charts showing trends over time
- Search bar to find specific articles
- Automatic scheduling so it updates without me running it manually
- link to relevant youtube video to watch

## What I learned building this

Getting PySpark to work with MongoDB took some figuring out. Also learned a lot about REST API design and how to structure a NoSQL database for analytics queries instead of just storing data. The Docker setup made development way easier since I didn't have to install MongoDB directly on my machine.
