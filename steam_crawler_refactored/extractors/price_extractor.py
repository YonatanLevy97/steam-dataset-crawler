"""
Price Information Extractor for Steam Games
"""

import re
from typing import Dict, Optional
from bs4 import BeautifulSoup

from steam_crawler_refactored.config.settings import CURRENCY_SYMBOLS

class PriceExtractor:
    """Extracts pricing information including discounts and free-to-play status"""
    
    @staticmethod
    def is_free_game(soup: BeautifulSoup) -> bool:
        """Check if game is free"""
        # Find the main purchase price element
        main_price = soup.find('div', class_='game_purchase_price price')
        if main_price:
            price_text = main_price.get_text(strip=True)
            # Check if it explicitly says "Free To Play" or "Free"
            if 'Free To Play' in price_text or price_text == 'Free':
                # Double check it doesn't have a data-price-final attribute with a non-zero value
                data_price = main_price.get('data-price-final')
                if not data_price or data_price == '0':
                    return True
        
        # Fallback: check if all price elements show free
        all_price_elements = soup.find_all('div', class_='game_purchase_price')
        for price_elem in all_price_elements:
            price_text = price_elem.get_text(strip=True)
            if any(currency in price_text for currency in CURRENCY_SYMBOLS):
                return False
        
        return False

    @staticmethod
    def extract_price_info(soup: BeautifulSoup) -> Dict[str, str]:
        """Extract current price information"""
        price_info = {}

        # Try different price selectors
        price_elem = soup.find('div', class_='game_purchase_price')
        if not price_elem:
            price_elem = soup.find('div', class_='discount_final_price')
        if not price_elem:
            price_elem = soup.find('div', class_='price')

        if price_elem:
            price_info['current_price'] = price_elem.get_text(strip=True)

        # Check for discount
        discount_elem = soup.find('div', class_='discount_pct')
        if discount_elem:
            price_info['discount_percent'] = discount_elem.get_text(strip=True)

        original_price_elem = soup.find('div', class_='discount_original_price')
        if original_price_elem:
            price_info['original_price'] = original_price_elem.get_text(strip=True)

        # Check if free
        if PriceExtractor.is_free_game(soup):
            price_info['is_free'] = True

        return price_info

    @staticmethod
    def extract_discount_percent(price_info: Dict[str, str]) -> int:
        """Extract discount percentage from price info"""
        discount = price_info.get('discount_percent', '')
        if discount:
            # Remove % sign and convert to number
            discount_num = re.sub(r'[^\d]', '', discount)
            return int(discount_num) if discount_num else 0
        return 0

    @staticmethod
    def extract_final_price(soup: BeautifulSoup) -> str:
        """Extract the final price (after discount if any)"""
        # Look for the main purchase price
        main_price = soup.find('div', class_='game_purchase_price price')
        if main_price:
            return main_price.get_text(strip=True)
        
        # Fallback to discount final price
        discount_price = soup.find('div', class_='discount_final_price')
        if discount_price:
            return discount_price.get_text(strip=True)
            
        return ""

    @staticmethod
    def extract_initial_price(soup: BeautifulSoup) -> str:
        """Extract the original price (before discount)"""
        original_price_elem = soup.find('div', class_='discount_original_price')
        if original_price_elem:
            return original_price_elem.get_text(strip=True)
        
        # If no discount, the initial price is the same as final price
        return PriceExtractor.extract_final_price(soup)
