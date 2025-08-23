"""
Web Client for handling HTTP requests to Steam
"""

import requests
import time
import random
import logging
from typing import Optional, Tuple
from urllib.parse import urlparse, parse_qs
import re

from steam_crawler_refactored.config.settings import (
    DEFAULT_HEADERS, DEFAULT_DELAY_RANGE, REQUEST_TIMEOUT, MAX_RETRIES,
    AGE_VERIFICATION_INDICATORS
)

class WebClient:
    """Handles all web requests to Steam with proper rate limiting and age verification bypass"""
    
    def __init__(self, delay_range: Tuple[float, float] = DEFAULT_DELAY_RANGE):
        """
        Initialize Web Client with configurable delay range.
        
        Args:
            delay_range: Min and max seconds to wait between requests
        """
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.delay_range = delay_range
        self._bypassed_age_verification = False

    def get_page(self, url: str, retries: int = MAX_RETRIES) -> Optional[requests.Response]:
        """
        Get a page with retry logic, polite delays, and age verification bypass.
        
        Args:
            url: URL to fetch
            retries: Number of retry attempts
            
        Returns:
            requests.Response or None
        """
        for attempt in range(retries):
            try:
                # Add random delay to be polite
                if attempt > 0:
                    delay = random.uniform(*self.delay_range) * (attempt + 1)
                    logging.info(f"Waiting {delay:.1f} seconds before retry {attempt + 1}")
                    time.sleep(delay)
                else:
                    time.sleep(random.uniform(*self.delay_range))

                response = self.session.get(url, timeout=REQUEST_TIMEOUT)

                if response.status_code == 200:
                    # Check if we hit an age verification page
                    if self._is_age_verification_page(response):
                        logging.info(f"Age verification detected for {url}, attempting bypass...")
                        bypassed_response = self._bypass_age_verification(url, response)
                        if bypassed_response:
                            return bypassed_response
                        else:
                            logging.warning(f"Failed to bypass age verification for {url}")
                            return response  # Return original response as fallback
                    return response
                elif response.status_code == 429:
                    logging.warning(f"Rate limited, waiting longer before retry...")
                    time.sleep(random.uniform(5, 10))
                    continue
                elif response.status_code in [403, 404]:
                    logging.warning(f"HTTP {response.status_code} for URL: {url}")
                    return None
                else:
                    logging.warning(f"HTTP {response.status_code} for URL: {url}")

            except requests.exceptions.Timeout:
                logging.warning(f"Timeout for URL: {url}, attempt {attempt + 1}")
            except requests.exceptions.RequestException as e:
                logging.warning(f"Request error for URL: {url}, attempt {attempt + 1}: {e}")

        logging.error(f"Failed to fetch URL after {retries} attempts: {url}")
        return None

    def _is_age_verification_page(self, response: requests.Response) -> bool:
        """
        Check if the response is an age verification page.
        
        Args:
            response: requests.Response object
            
        Returns:
            bool: True if this is an age verification page
        """
        if not response:
            return False

        # Check URL for agecheck
        if 'agecheck' in response.url:
            return True

        # Check content for age verification indicators
        content = response.text.lower()
        return any(indicator in content for indicator in AGE_VERIFICATION_INDICATORS)

    def _bypass_age_verification(self, original_url: str, age_check_response: requests.Response) -> Optional[requests.Response]:
        """
        Attempt to bypass age verification by setting appropriate cookies.
        
        Args:
            original_url: The original URL we wanted to access
            age_check_response: The response containing age verification
            
        Returns:
            requests.Response or None: Response after bypassing age check
        """
        try:
            # Extract app ID from URL
            app_id_match = re.search(r'/app/(\d+)', original_url)
            if not app_id_match:
                return None

            app_id = app_id_match.group(1)

            # Set the mature content cookie for this specific app
            cookie_path = f'/app/{app_id}'
            self.session.cookies.set(
                'wants_mature_content',
                '1',
                domain='.steampowered.com',
                path=cookie_path
            )

            # Also set a general mature content cookie
            self.session.cookies.set(
                'wants_mature_content',
                '1',
                domain='.steampowered.com',
                path='/'
            )

            # Add birthtime cookie (simulate being over 18)
            birth_timestamp = str(int(time.time()) - (25 * 365 * 24 * 60 * 60))  # 25 years ago
            self.session.cookies.set(
                'birthtime',
                birth_timestamp,
                domain='.steampowered.com',
                path='/'
            )

            # Wait a moment for cookies to take effect
            time.sleep(1)

            # Try to access the original URL again
            logging.info(f"Attempting to access {original_url} with mature content cookies...")
            response = self.session.get(original_url, timeout=REQUEST_TIMEOUT)

            # Check if we successfully bypassed the age gate
            if response.status_code == 200 and not self._is_age_verification_page(response):
                logging.info("Successfully bypassed age verification!")
                self._bypassed_age_verification = True
                return response
            else:
                # If still on age check page, try the direct approach with SNR parameter
                direct_url = f"{original_url}?snr=1_direct-navigation__"
                direct_response = self.session.get(direct_url, timeout=REQUEST_TIMEOUT)
                if direct_response.status_code == 200 and not self._is_age_verification_page(direct_response):
                    logging.info("Successfully bypassed age verification with direct URL!")
                    self._bypassed_age_verification = True
                    return direct_response

                return None

        except Exception as e:
            logging.warning(f"Error during age verification bypass: {e}")
            return None

    def check_age_verification_required(self, app_id: str) -> bool:
        """
        Check if an app requires age verification by accessing it with a clean session.
        
        Args:
            app_id: Steam app ID
            
        Returns:
            bool: True if age verification is required
        """
        # Create a temporary clean session for testing (no cookies)
        test_session = requests.Session()
        test_session.headers.update(DEFAULT_HEADERS)

        try:
            url = f"https://store.steampowered.com/app/{app_id}"
            
            # Add a small delay to avoid seeming like a bot
            time.sleep(0.5)
            
            response = test_session.get(url, timeout=10)

            if response.status_code == 200:
                is_age_gate = self._is_age_verification_page(response)
                if is_age_gate:
                    logging.debug(f"App {app_id} requires age verification (detected via clean session)")
                else:
                    logging.debug(f"App {app_id} does not require age verification")
                return is_age_gate
            else:
                logging.debug(f"Failed to check age verification for app {app_id}, status: {response.status_code}")

        except Exception as e:
            logging.debug(f"Error checking age verification for app {app_id}: {e}")

        return False

    @property
    def bypassed_age_verification(self) -> bool:
        """Check if age verification was bypassed in this session"""
        return self._bypassed_age_verification

    def reset_age_verification_flag(self):
        """Reset age verification flag for new app"""
        self._bypassed_age_verification = False
