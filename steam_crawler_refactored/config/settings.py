"""
Steam Crawler Configuration Settings
"""

# Network settings
DEFAULT_DELAY_RANGE = (1, 3)
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

# Steam URLs
STEAM_STORE_URL = "https://store.steampowered.com/app/{app_id}"

# Age verification settings
AGE_VERIFICATION_INDICATORS = [
    'agegate_birthday_selector',
    'please enter your birth date',
    'viewproductpage',
    'checkagegatesubmit',
    'wants_mature_content',
    'not appropriate for all ages'
]

# Currency symbols for price detection
CURRENCY_SYMBOLS = ['₪', '$', '€', '£', '¥', '¢', '₹', '₽']

# Data export settings
DEFAULT_DATA_DIR = 'data'
DEFAULT_CSV_FIELDS = [
    "appid", "name", "type", "short_description", "is_free", "required_age",
    "release_date", "coming_soon", "developers", "publishers", "categories", "genres", "tags",
    "windows", "mac", "linux", "initial_price", "final_price", "discount_percent",
    "metacritic_score", "recommendations_total", "achievements_total",
    "supported_languages", "pc_min_requirements", "controller_support",
    "has_dlc", "dlc_count"
]

# Logging
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
