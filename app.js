// API Base URL
const API_BASE = 'http://localhost:5001/api';

// Fetch and display overall stats
async function loadStats() {
    try {
        const response = await fetch(`${API_BASE}/stats`);
        const data = await response.json();
        
        document.getElementById('total-articles').textContent = data.total_articles;
        document.getElementById('total-sources').textContent = data.total_sources;
        document.getElementById('total-categories').textContent = data.total_categories;
        document.getElementById('total-keywords').textContent = data.total_keywords;
        
        if (data.last_updated) {
            const date = new Date(data.last_updated);
            document.getElementById('last-updated').textContent = date.toLocaleString();
        }
    } catch (error) {
        console.error('Error loading stats:', error);
    }
}

// Fetch and display top sources
async function loadSources() {
    try {
        const response = await fetch(`${API_BASE}/sources`);
        const data = await response.json();
        
        const container = document.getElementById('sources-list');
        container.innerHTML = '';
        
        data.sources.forEach(source => {
            const div = document.createElement('div');
            div.className = 'source-item';
            div.innerHTML = `
                <span class="source-name">${source.source}</span>
                <span class="source-count">${source.article_count}</span>
            `;
            container.appendChild(div);
        });
    } catch (error) {
        console.error('Error loading sources:', error);
        document.getElementById('sources-list').innerHTML = '<p class="loading">Error loading sources</p>';
    }
}

// Fetch and display categories
async function loadCategories() {
    try {
        const response = await fetch(`${API_BASE}/categories`);
        const data = await response.json();
        
        const container = document.getElementById('categories-list');
        container.innerHTML = '';
        
        data.categories.forEach(category => {
            const div = document.createElement('div');
            div.className = 'category-item';
            div.innerHTML = `
                <span class="category-name">${category.category}</span>
                <span class="category-count">${category.article_count}</span>
            `;
            container.appendChild(div);
        });
    } catch (error) {
        console.error('Error loading categories:', error);
        document.getElementById('categories-list').innerHTML = '<p class="loading">Error loading categories</p>';
    }
}

// Fetch and display keywords
async function loadKeywords() {
    try {
        const response = await fetch(`${API_BASE}/keywords`);
        const data = await response.json();
        
        const container = document.getElementById('keywords-list');
        container.innerHTML = '';
        
        data.keywords.forEach(keyword => {
            const span = document.createElement('span');
            span.className = 'keyword-tag';
            span.innerHTML = `
                ${keyword.keyword}
                <span class="keyword-count">${keyword.count}</span>
            `;
            container.appendChild(span);
        });
    } catch (error) {
        console.error('Error loading keywords:', error);
        document.getElementById('keywords-list').innerHTML = '<p class="loading">Error loading keywords</p>';
    }
}

// Fetch and display recent articles
async function loadArticles() {
    try {
        const response = await fetch(`${API_BASE}/articles`);
        const data = await response.json();
        
        const container = document.getElementById('articles-list');
        container.innerHTML = '';
        
        data.articles.forEach(article => {
            const div = document.createElement('div');
            div.className = 'article-item';
            
            const publishedDate = article.published_at ? 
                new Date(article.published_at).toLocaleDateString() : 'Unknown date';
            
            div.innerHTML = `
                <div class="article-title">
                    <a href="${article.url}" target="_blank">${article.title}</a>
                </div>
                <div class="article-meta">
                    <span>📰 ${article.source}</span>
                    <span>✍️ ${article.author || 'Unknown'}</span>
                    <span>📅 ${publishedDate}</span>
                    <span class="category-badge">${article.category}</span>
                </div>
                ${article.description ? `<div class="article-description">${article.description}</div>` : ''}
            `;
            container.appendChild(div);
        });
    } catch (error) {
        console.error('Error loading articles:', error);
        document.getElementById('articles-list').innerHTML = '<p class="loading">Error loading articles</p>';
    }
}

// Load all data when page loads
document.addEventListener('DOMContentLoaded', () => {
    loadStats();
    loadSources();
    loadCategories();
    loadKeywords();
    loadArticles();
    
    // Refresh data every 5 minutes
    setInterval(() => {
        loadStats();
        loadSources();
        loadCategories();
        loadKeywords();
        loadArticles();
    }, 300000); // 5 minutes
});