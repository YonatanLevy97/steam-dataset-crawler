"""
SteamCharts Crawler Configuration Settings
"""

# Network settings
DEFAULT_DELAY_RANGE = (0.3, 0.5)  # Faster than Steam crawler as SteamCharts is less sensitive
REQUEST_TIMEOUT = 15
MAX_RETRIES = 3

# User Agent
DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'

# Headers
DEFAULT_HEADERS = {
    'User-Agent': DEFAULT_USER_AGENT,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

# SteamCharts URLs
STEAMCHARTS_URL = "https://steamcharts.com/app/{app_id}"

# Data export settings
DEFAULT_DATA_DIR = 'data'
DEFAULT_CSV_FIELDS = [
    "appid", "name", "month", "avg_players", "peak_players", "change_percent"
]

# Batch processing settings
DEFAULT_BATCH_SIZE = 1000
DEFAULT_CHECKPOINT_INTERVAL = 100  # Save checkpoint every N apps

# Checkpoint settings
CHECKPOINT_FILE_PREFIX = 'checkpoint_batch_'
CHECKPOINT_FILE_EXTENSION = '.json'

# Logging
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

# CSV input settings
METADATA_CSV_COLUMNS = ['appid', 'name']
