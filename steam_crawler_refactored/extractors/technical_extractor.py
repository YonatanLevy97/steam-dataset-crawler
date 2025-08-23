"""
Technical Information Extractor for Steam Games
"""

import re
import json
from typing import Dict, List, Optional
from bs4 import BeautifulSoup

class TechnicalExtractor:
    """Extracts technical information like platform support, requirements, etc."""
    
    @staticmethod
    def extract_platform_support(soup: BeautifulSoup) -> Dict[str, bool]:
        """Extract platform support information"""
        platforms = {'windows': False, 'mac': False, 'linux': False}

        # Look for platform icons/indicators
        platform_area = soup.find('div', class_='sysreq_tabs')
        if platform_area:
            tabs = platform_area.find_all('div', class_='sysreq_tab')
            for tab in tabs:
                tab_text = tab.get_text().lower()
                if 'windows' in tab_text or 'pc' in tab_text:
                    platforms['windows'] = True
                elif 'mac' in tab_text or 'osx' in tab_text:
                    platforms['mac'] = True
                elif 'linux' in tab_text:
                    platforms['linux'] = True

        # Fallback: look for platform icons
        if not any(platforms.values()):
            platform_icons = soup.find_all('span', class_=['platform_img', 'win', 'mac', 'linux'])
            for icon in platform_icons:
                class_names = icon.get('class', [])
                if 'win' in class_names:
                    platforms['windows'] = True
                elif 'mac' in class_names:
                    platforms['mac'] = True
                elif 'linux' in class_names:
                    platforms['linux'] = True

        return platforms

    @staticmethod
    def extract_system_requirements(soup: BeautifulSoup) -> str:
        """Extract system requirements"""
        req_elem = soup.find('div', class_='game_area_sys_req')
        if req_elem:
            return req_elem.get_text(strip=True)
        return ""

    @staticmethod
    def extract_languages(soup: BeautifulSoup) -> Dict[str, List[str]]:
        """Extract supported languages with detailed support info"""
        languages = {
            'supported_languages': [],
            'interface_languages': [],
            'full_audio_languages': [],
            'subtitle_languages': []
        }

        # Find the language table
        lang_table = soup.find('table', class_='game_language_options')
        if lang_table:
            rows = lang_table.find_all('tr')[1:]  # Skip header row

            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 4:
                    lang_name = cols[0].get_text(strip=True)
                    interface = '✔' in cols[1].get_text() or cols[1].find('span', class_='supported')
                    audio = '✔' in cols[2].get_text() or cols[2].find('span', class_='supported')
                    subtitles = '✔' in cols[3].get_text() or cols[3].find('span', class_='supported')

                    if lang_name:
                        languages['supported_languages'].append(lang_name)
                        if interface:
                            languages['interface_languages'].append(lang_name)
                        if audio:
                            languages['full_audio_languages'].append(lang_name)
                        if subtitles:
                            languages['subtitle_languages'].append(lang_name)

        # Fallback to simple text extraction if table not found
        if not languages['supported_languages']:
            lang_elem = soup.find('div', class_='game_language_options')
            if lang_elem:
                lang_text = lang_elem.get_text(strip=True)
                languages['supported_languages'] = [lang_text]

        return languages

    @staticmethod
    def extract_achievements_count(soup: BeautifulSoup) -> int:
        """Extract number of achievements"""
        achievement_elem = soup.find('div', {'id': 'achievement_block'})
        if achievement_elem:
            count_text = achievement_elem.get_text()
            # Extract number from text like "42 achievements"
            match = re.search(r'(\d+)', count_text)
            if match:
                return int(match.group(1))
        return 0

    @staticmethod
    def extract_metacritic_score(soup: BeautifulSoup) -> Optional[int]:
        """Extract Metacritic score"""
        # Look for metacritic score in various possible locations
        score_selectors = [
            ('div', {'id': 'game_area_metascore'}),
            ('div', {'class': 'score'}),
            ('div', {'class': 'metacritic_score'}),
        ]

        for tag, attrs in score_selectors:
            score_elem = soup.find(tag, attrs)
            if score_elem:
                score_text = score_elem.get_text(strip=True)
                # Extract number from text
                match = re.search(r'(\d+)', score_text)
                if match:
                    score = int(match.group(1))
                    # Validate score is in reasonable range
                    if 0 <= score <= 100:
                        return score

        # Alternative: search for "metacritic" text and nearby numbers
        page_text = soup.get_text().lower()
        if 'metacritic' in page_text:
            # Find numbers near metacritic mentions
            metacritic_pattern = re.compile(r'metacritic[^\d]*?(\d{1,3})', re.IGNORECASE)
            match = metacritic_pattern.search(soup.get_text())
            if match:
                score = int(match.group(1))
                if 0 <= score <= 100:
                    return score

        return None

    @staticmethod
    def extract_age_rating_numeric(soup: BeautifulSoup) -> int:
        """Extract numeric age rating"""
        age_elem = soup.find('div', class_='game_rating')
        if age_elem:
            age_text = age_elem.get_text()
            # Extract number from ratings like "18+" or "ESRB: M"
            numbers = re.findall(r'\d+', age_text)
            if numbers:
                return int(numbers[0])
        return 0

    @staticmethod
    def extract_recommendations_total(soup: BeautifulSoup) -> int:
        """Extract total recommendations count"""
        # Look for reviews/recommendations section
        reviews_area = soup.find('div', class_='user_reviews')
        if reviews_area:
            summary = reviews_area.find('span', class_='responsive_hidden')
            if summary:
                text = summary.get_text()
                # Extract number from text like "(1,234 reviews)"
                numbers = re.findall(r'([\d,]+)', text)
                if numbers:
                    return int(numbers[0].replace(',', ''))
        return 0

    @staticmethod
    def extract_controller_support(soup: BeautifulSoup) -> str:
        """Extract controller support information"""
        # Look for controller support info in data-props
        controller_elem = soup.find('div', attrs={'data-featuretarget': 'store-sidebar-controller-support-info'})
        if controller_elem:
            data_props = controller_elem.get('data-props')
            if data_props:
                try:
                    props = json.loads(data_props)
                    controller_info = []
                    
                    if props.get('bFullXboxControllerSupport'):
                        controller_info.append('Full Xbox Controller Support')
                    elif props.get('bPartialXboxControllerSupport'):
                        controller_info.append('Partial Xbox Controller Support')
                    
                    if props.get('bPS4ControllerSupport'):
                        controller_info.append('PS4 Controller Support')
                    if props.get('bPS5ControllerSupport'):
                        controller_info.append('PS5 Controller Support')
                    if props.get('bSteamInputAPISupport'):
                        controller_info.append('Steam Input API Support')
                    
                    return ', '.join(controller_info) if controller_info else ''
                except (json.JSONDecodeError, KeyError):
                    pass

        # Fallback method
        controller_elem = soup.find('div', class_='game_area_details_specs')
        if controller_elem and 'controller' in controller_elem.get_text().lower():
            return controller_elem.get_text(strip=True)
        return ""

    @staticmethod
    def extract_dlc_count(soup: BeautifulSoup) -> int:
        """Extract DLC count"""
        # Try camelCase id first (found in HTML analysis)
        dlc_elem = soup.find('div', {'id': 'gameAreaDLCSection'})
        if not dlc_elem:
            # Fallback to original snake_case id
            dlc_elem = soup.find('div', {'id': 'game_area_dlc_section'})
        
        if dlc_elem:
            # Try to extract count from "Browse all (X)" link
            browse_link = dlc_elem.find('a', href=re.compile(r'/dlc/'))
            if browse_link:
                browse_text = browse_link.get_text()
                match = re.search(r'\((\d+)\)', browse_text)
                if match:
                    return int(match.group(1))
            
            # Fallback: count DLC rows directly
            dlc_items = dlc_elem.find_all('div', class_='game_area_dlc_row')
            return len(dlc_items)
        return 0
