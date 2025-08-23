"""
Basic Information Extractor for Steam Games
"""

import re
from typing import List, Optional
from bs4 import BeautifulSoup

class BasicInfoExtractor:
    """Extracts basic game information like title, description, developers, etc."""
    
    @staticmethod
    def extract_title(soup: BeautifulSoup) -> str:
        """Extract game title"""
        # First try the main app name element
        title_elem = soup.find('div', class_='apphub_AppName')
        if title_elem:
            return title_elem.get_text(strip=True)

        # Try page title
        page_title = soup.find('title')
        if page_title:
            title_text = page_title.get_text(strip=True)
            # Clean up title from Steam page title format
            if 'on Steam' in title_text:
                title_text = title_text.replace(' on Steam', '')
            if title_text and title_text != 'Steam':
                return title_text

        # Try h1 or other headers
        for tag in ['h1', 'h2']:
            elem = soup.find(tag)
            if elem:
                text = elem.get_text(strip=True)
                if text and len(text) > 3:  # Avoid very short titles
                    return text

        return ""

    @staticmethod
    def extract_description(soup: BeautifulSoup) -> str:
        """Extract game description"""
        desc_elem = soup.find('div', class_='game_description_snippet')
        if not desc_elem:
            desc_elem = soup.find('div', {'id': 'game_area_description'})

        return desc_elem.get_text(strip=True) if desc_elem else ""

    @staticmethod
    def extract_developer(soup: BeautifulSoup) -> str:
        """Extract developer information"""
        developers = []

        # Look for developers in the details block
        details_block = soup.find('div', class_='details_block')
        if details_block:
            # Find the Developer: line
            dev_line = details_block.find('b', string='Developer:')
            if dev_line and dev_line.parent:
                # Get all links after the Developer: label
                dev_links = dev_line.parent.find_all('a')
                for link in dev_links:
                    dev_name = link.get_text(strip=True)
                    if dev_name and dev_name not in developers:
                        developers.append(dev_name)

        # Fallback methods
        if not developers:
            dev_elem = soup.find('div', {'id': 'developers_list'})
            if dev_elem:
                developers.append(dev_elem.get_text(strip=True))
            else:
                dev_link = soup.find('a', href=re.compile(r'/developer/'))
                if dev_link:
                    developers.append(dev_link.get_text(strip=True))

        return ', '.join(developers) if developers else ""

    @staticmethod
    def extract_publisher(soup: BeautifulSoup) -> str:
        """Extract publisher information"""
        publishers = []

        # Look for publishers in the details block
        details_block = soup.find('div', class_='details_block')
        if details_block:
            # Find the Publisher: line
            pub_line = details_block.find('b', string='Publisher:')
            if pub_line and pub_line.parent:
                # Get all links after the Publisher: label
                pub_links = pub_line.parent.find_all('a')
                for link in pub_links:
                    pub_name = link.get_text(strip=True)
                    if pub_name and pub_name not in publishers:
                        publishers.append(pub_name)

        # Fallback method
        if not publishers:
            pub_elem = soup.find('div', class_='summary')
            if pub_elem:
                links = pub_elem.find_all('a', href=re.compile(r'/publisher/'))
                for link in links:
                    pub_name = link.get_text(strip=True)
                    if pub_name and pub_name not in publishers:
                        publishers.append(pub_name)

        return ', '.join(publishers) if publishers else ""

    @staticmethod
    def extract_release_date(soup: BeautifulSoup) -> str:
        """Extract release date"""
        # Look for the release date specifically in the format from the webpage
        # Try finding 'Released' followed by the date
        released_elem = soup.find('div', class_='release_date')
        if released_elem:
            date_elem = released_elem.find('div', class_='date')
            if date_elem:
                return date_elem.get_text(strip=True)

        # Try finding in details block with 'Release Date:' label
        details_block = soup.find('div', class_='details_block')
        if details_block:
            # Look for the specific pattern
            release_text = details_block.get_text()
            # Use regex to find date after 'Release Date:'
            date_match = re.search(r'Release Date:\s*([^\n]+)', release_text)
            if date_match:
                return date_match.group(1).strip()

        # Alternative: look for date in specific format
        date_pattern = re.compile(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}\b')
        page_text = soup.get_text()
        date_match = date_pattern.search(page_text)
        if date_match:
            return date_match.group(0)

        return ""

    @staticmethod
    def extract_app_type(soup: BeautifulSoup) -> str:
        """Extract application type (game, software, etc.)"""
        # Look for breadcrumbs to determine type
        breadcrumbs = soup.find('div', class_='breadcrumbs')
        if breadcrumbs:
            links = breadcrumbs.find_all('a')
            for link in links:
                text = link.get_text(strip=True).lower()
                if 'software' in text:
                    return 'software'
                elif 'game' in text:
                    return 'game'

        # Default assumption
        return 'game'

    @staticmethod
    def extract_coming_soon(soup: BeautifulSoup) -> bool:
        """Check if game is coming soon"""
        coming_soon_indicators = [
            soup.find(string=re.compile('Coming Soon', re.I)),
            soup.find(string=re.compile('Pre-Purchase', re.I)),
            soup.find('div', class_='coming_soon')
        ]
        return any(coming_soon_indicators)

    @staticmethod
    def extract_categories(soup: BeautifulSoup) -> List[str]:
        """Extract game categories/genres"""
        categories = []
        
        # Look for genres in the details block
        details_block = soup.find('div', class_='details_block')
        if details_block:
            # Find the Genre: line
            genre_line = details_block.find('b', string='Genre:')
            if genre_line and genre_line.parent:
                # Get all links after the Genre: label that contain /genre/
                genre_links = genre_line.parent.find_all('a', href=re.compile(r'/genre/'))
                for link in genre_links:
                    category = link.get_text(strip=True)
                    if category and category not in categories:
                        categories.append(category)
        
        # Fallback: look for game area details specs (original method)
        if not categories:
            category_elems = soup.find_all('div', class_='game_area_details_specs')
            for cat_elem in category_elems:
                cat_text = cat_elem.get_text(strip=True)
                if cat_text and cat_text not in categories:
                    categories.append(cat_text)

        return categories

    @staticmethod
    def extract_genre(soup: BeautifulSoup) -> str:
        """Extract genre information (same as categories but as string)"""
        categories = BasicInfoExtractor.extract_categories(soup)
        return ', '.join(categories) if categories else ""

    @staticmethod
    def extract_tags(soup: BeautifulSoup) -> List[str]:
        """Extract popular user-defined tags"""
        tags = []

        # Look for tags in the popular tags section
        tag_elems = soup.find_all('a', class_='app_tag')
        for tag_elem in tag_elems:
            tag_text = tag_elem.get_text(strip=True)
            # Clean up any extra whitespace or symbols
            tag_text = re.sub(r'[+\s]+$', '', tag_text)
            if tag_text and tag_text not in tags and len(tag_text) > 1:
                tags.append(tag_text)

        # If no tags found, try alternative selectors
        if not tags:
            # Try looking for tags in other possible locations
            for tag_class in ['popular_tags', 'game_tag', 'tag']:
                tag_section = soup.find('div', class_=tag_class)
                if tag_section:
                    links = tag_section.find_all('a')
                    for link in links:
                        tag_text = link.get_text(strip=True)
                        if tag_text and tag_text not in tags:
                            tags.append(tag_text)

        return tags[:20]  # Return up to 20 tags
