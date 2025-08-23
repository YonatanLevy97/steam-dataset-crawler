"""
Main Steam Crawler Class
"""

import logging
from typing import Optional, Dict, Any
from bs4 import BeautifulSoup

from steam_crawler_refactored.core.web_client import WebClient
from steam_crawler_refactored.extractors import BasicInfoExtractor, PriceExtractor, TechnicalExtractor
from steam_crawler_refactored.config.settings import STEAM_STORE_URL

class SteamCrawler:
    """Main Steam Crawler that orchestrates data extraction"""
    
    def __init__(self, delay_range=(1, 3)):
        """
        Initialize Steam Crawler.
        
        Args:
            delay_range: Min and max seconds to wait between requests
        """
        self.web_client = WebClient(delay_range)
        self.basic_extractor = BasicInfoExtractor()
        self.price_extractor = PriceExtractor()
        self.technical_extractor = TechnicalExtractor()

    def crawl_app(self, app_id: str) -> Optional[Dict[str, Any]]:
        """
        Crawl Steam store page for a specific app ID.
        
        Args:
            app_id: Steam app ID
            
        Returns:
            dict: Extracted app information or None if failed
        """
        # Reset age verification flag for each new app
        self.web_client.reset_age_verification_flag()

        url = STEAM_STORE_URL.format(app_id=app_id)
        logging.info(f"Crawling Steam app {app_id}: {url}")

        # First, check if this app requires age verification
        requires_age_verification = self.web_client.check_age_verification_required(app_id)
        if requires_age_verification:
            logging.info(f"App {app_id} requires age verification")

        response = self.web_client.get_page(url)
        if not response:
            return None

        soup = BeautifulSoup(response.content, 'html.parser')

        # Check if page exists and is not an error page
        if self._is_error_page(soup):
            logging.warning(f"App {app_id} not found or access denied")
            return None

        # Double-check if we still have age verification after our bypass attempt
        still_has_age_gate = self._is_age_verification_page_by_soup(soup)
        if still_has_age_gate:
            if requires_age_verification:
                logging.warning(f"Age verification could not be bypassed for app {app_id}")
            else:
                # This shouldn't happen - we got an age gate but didn't detect it initially
                logging.warning(f"Unexpected age verification page encountered for app {app_id}")
                # Update our flag since we discovered this app needs age verification
                requires_age_verification = True
            # Still try to extract what we can from the age check page
        elif requires_age_verification:
            # We expected age verification but got the content page - verification was successful
            logging.info(f"Successfully accessed age-restricted content for app {app_id}")

        # Extract all information using specialized extractors
        app_data = self._extract_all_data(soup, app_id, requires_age_verification)
        
        return app_data

    def _extract_all_data(self, soup: BeautifulSoup, app_id: str, requires_age_verification: bool = False) -> Dict[str, Any]:
        """Extract all data using specialized extractors"""
        
        # Basic information
        basic_info = {
            'appid': int(app_id),
            'name': self.basic_extractor.extract_title(soup),
            'type': self.basic_extractor.extract_app_type(soup),
            'short_description': self.basic_extractor.extract_description(soup),
            'release_date': self.basic_extractor.extract_release_date(soup),
            'coming_soon': self.basic_extractor.extract_coming_soon(soup),
            'developers': self.basic_extractor.extract_developer(soup),
            'publishers': self.basic_extractor.extract_publisher(soup),
        }

        # Categories and tags
        categories = self.basic_extractor.extract_categories(soup)
        basic_info.update({
            'categories': ', '.join(categories) if categories else '',
            'genres': self.basic_extractor.extract_genre(soup),
            'tags': ', '.join(self.basic_extractor.extract_tags(soup)),
        })

        # Price information
        price_info = self.price_extractor.extract_price_info(soup)
        basic_info.update({
            'is_free': self.price_extractor.is_free_game(soup),
            'initial_price': self.price_extractor.extract_initial_price(soup),
            'final_price': self.price_extractor.extract_final_price(soup),
            'discount_percent': self.price_extractor.extract_discount_percent(price_info),
        })

        # Technical information
        platform_info = self.technical_extractor.extract_platform_support(soup)
        basic_info.update({
            'windows': platform_info.get('windows', False),
            'mac': platform_info.get('mac', False),
            'linux': platform_info.get('linux', False),
        })

        # More technical details
        basic_info.update({
            'metacritic_score': self.technical_extractor.extract_metacritic_score(soup),
            'recommendations_total': self.technical_extractor.extract_recommendations_total(soup),
            'achievements_total': self.technical_extractor.extract_achievements_count(soup),
            'pc_min_requirements': self.technical_extractor.extract_system_requirements(soup),
            'controller_support': self.technical_extractor.extract_controller_support(soup),
        })

        # Language support
        language_info = self.technical_extractor.extract_languages(soup)
        basic_info['supported_languages'] = ', '.join(language_info.get('supported_languages', []))

        # DLC information
        dlc_count = self.technical_extractor.extract_dlc_count(soup)
        basic_info.update({
            'has_dlc': dlc_count > 0,
            'dlc_count': dlc_count,
        })

        # Age rating - improved logic
        extracted_age = self.technical_extractor.extract_age_rating_numeric(soup)
        
        # If the app originally required age verification, it's an 18+ game
        # regardless of whether we bypassed it or not
        if requires_age_verification:
            basic_info['required_age'] = 18
            logging.debug(f"App {app_id} set to 18+ due to age verification requirement")
        elif self.web_client.bypassed_age_verification:
            # Fallback: if we bypassed verification during this session but 
            # didn't detect it in the initial check
            basic_info['required_age'] = 18
            logging.debug(f"App {app_id} set to 18+ due to bypassed verification in session")
        else:
            # Use extracted age rating from the page
            basic_info['required_age'] = extracted_age
            logging.debug(f"App {app_id} required_age set to extracted value: {extracted_age}")

        return basic_info

    def _is_error_page(self, soup: BeautifulSoup) -> bool:
        """Check if the page is an error page (404, access denied, etc.)"""
        error_indicators = [
            soup.find('div', class_='error'),
            soup.find('div', {'id': 'global_header'}) is None,
            'Sorry' in soup.get_text(),
            'not available' in soup.get_text().lower()
        ]

        # Don't treat age verification as an error page
        if self._is_age_verification_page_by_soup(soup):
            return False

        return any(error_indicators)

    def _is_age_verification_page_by_soup(self, soup: BeautifulSoup) -> bool:
        """Check if the soup represents an age verification page."""
        if not soup:
            return False

        # Check for age gate elements
        age_indicators = [
            soup.find('div', class_='agegate_birthday_selector'),
            soup.find('select', {'id': 'ageDay'}),
            soup.find('select', {'id': 'ageMonth'}),
            soup.find('select', {'id': 'ageYear'}),
            soup.find('a', {'onclick': lambda x: x and 'ViewProductPage' in x})
        ]

        return any(age_indicators)
