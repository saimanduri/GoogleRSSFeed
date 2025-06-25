# Google News RSS Ingestion Pipeline

An offline-friendly Python-based pipeline that fetches and processes Google News RSS feeds based on multiple keywords, deduplicates articles, and formats them for downstream LLM integration.

## Features

- **Keyword-Based RSS Fetching**: Uses configurable keywords to dynamically construct Google News RSS URLs
- **Scheduled Execution**: Runs at specified times with configurable intervals between keyword groups
- **Single Port Communication**: All external traffic funneled through one configurable port
- **Robust Error Handling**: Implements retries and timeouts for resilient fetching
- **Intelligent Deduplication**: Prevents duplicate articles across keyword fetches
- **LLM-Friendly Output**: Produces clean, consistent JSON for easy LLM processing
- **Comprehensive Logging**: Records execution details, statistics, and errors
- **Offline Compatibility**: Designed to run on air-gapped systems with limited connectivity

## Directory Structure

```
/rss_collector/
├── feeds/            # Stores daily JSON feed data
├── logs/             # Stores execution logs
├── config/           # Configuration files
│   ├── feeds.json    # Keywords configuration
│   └── settings.json # General settings
├── main.py           # Main execution script
├── utils/            # Utility modules
│   ├── __init__.py
│   ├── fetcher.py    # RSS fetching functionality
│   ├── parser.py     # RSS parsing functionality
│   └── storage.py    # Data storage functionality
├── requirements.txt  # Python dependencies
└── .env              # Environment variables (ports, paths, etc.)
```

## Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd rss_collector
   ```

2. Create a virtual environment and install dependencies:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. Configure the `.env` file:
   ```
   # .env example
   HTTP_PROXY=http://localhost:8081
   HTTPS_PROXY=http://localhost:8081
   RSS_BASE_URL=https://news.google.com/rss/search
   LANGUAGE=en-IN
   REGION=IN
   CEID=IN:en
   RETRY_ATTEMPTS=3
   RETRY_DELAY=5
   REQUEST_TIMEOUT=30
   ```

4. Set up the keyword configuration in `config/feeds.json`:
   ```json
   {
     "keywords": [
       {
         "name": "finance",
         "terms": ["budget", "inflation", "interest rates"]
       },
       {
         "name": "technology",
         "terms": ["artificial intelligence", "machine learning", "cloud computing"]
       }
     ]
   }
   ```

5. Configure the general settings in `config/settings.json`:
   ```json
   {
     "schedule": {
       "times": ["05:00", "14:00"],
       "timezone": "Asia/Kolkata"
     },
     "fetch": {
       "group_interval_minutes": 5,
       "retry_attempts": 3,
       "retry_delay_seconds": 5,
       "timeout_seconds": 30
     },
     "storage": {
       "feeds_dir": "feeds",
       "archive_days": 30,
       "enable_stats": true
     },
     "proxy": {
       "enabled": true,
       "port": 8081
     },
     "logging": {
       "level": "INFO",
       "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
     }
   }
   ```

## Usage

### Running Manually

```
python main.py
```

### Setting Up as a Scheduled Task

Create a cron job (Linux/RHEL) to run the script at the specified times:

```
# Run at 5 AM and 2 PM IST daily
0 5,14 * * * cd /path/to/rss_collector && /path/to/python main.py
```

Or use the built-in APScheduler for more complex scheduling needs.

## Proxy Configuration

### Environment Variables

The system uses the following environment variables for proxy configuration:
- `HTTP_PROXY`: HTTP proxy URL (e.g., http://localhost:8081)
- `HTTPS_PROXY`: HTTPS proxy URL (e.g., http://localhost:8081)

### System-Level Configuration (RHEL)

To enforce all traffic through a specific port using iptables:

```bash
# Allow outbound traffic only through port 8081
iptables -A OUTPUT -p tcp --dport 8081 -j ACCEPT
# Block all other outbound traffic
iptables -A OUTPUT -j DROP
```

## Output Format

Example of daily JSON output (`feeds/2025-05-18.json`):

```json
[
  {
    "fetched_at": "2025-05-18T08:00:00Z",
    "query": "budget",
    "source_url": "https://news.google.com/rss/search?q=budget&hl=en-IN&gl=IN&ceid=IN:en",
    "articles": [
      {
        "title": "Government Announces New Budget Plans",
        "link": "https://news.example.com/article1",
        "published": "2025-05-18T06:30:00Z",
        "source": "Example News",
        "snippet": "The government today announced new budget plans..."
      }
    ]
  }
]
```

## Troubleshooting

Check the logs directory for detailed execution logs:
```
cat logs/2025-05-18.log
```

Common issues:
- Proxy connectivity problems
- RSS feed format changes
- Permissions issues with file storage

## License

[MIT License](LICENSE)