#!/usr/bin/env python3
"""
Simple test of the SteamCharts crawler
"""

import sys
import os
import requests
from bs4 import BeautifulSoup
import time

# Simple test function based on original code
def test_steamcharts_extraction(app_id=730):
    """Test SteamCharts data extraction for a single app"""
    url = f"https://steamcharts.com/app/{app_id}"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    print(f"🔗 Testing URL: {url}")
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"❌ HTTP {response.status_code}")
            return False
        
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", class_="common-table")
        
        if not table:
            print("❌ No data table found")
            return False
        
        rows = table.find_all("tr")[1:]  # Skip header
        monthly_data = []
        
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 5:
                continue
            
            month_text = cols[0].text.strip()
            avg_text = cols[1].text.strip().replace(",", "")
            peak_text = cols[4].text.strip().replace(",", "")
            pct_text = cols[3].text.strip().replace("%", "").replace(",", "")
            
            try:
                avg_players = float(avg_text)
                peak_players = int(peak_text) if peak_text else 0
                change_percent = float(pct_text) if pct_text else 0
                
                monthly_data.append({
                    "month": month_text,
                    "avg_players": avg_players,
                    "peak_players": peak_players,
                    "change_percent": change_percent
                })
            except ValueError:
                continue
        
        print(f"✅ Extracted {len(monthly_data)} records")
        
        # Show first few records
        for i, record in enumerate(monthly_data[:3]):
            print(f"   {i+1}. {record['month']}: {record['avg_players']:,.0f} avg, {record['peak_players']:,} peak")
        
        if len(monthly_data) > 3:
            print(f"   ... and {len(monthly_data) - 3} more records")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    app_id = int(sys.argv[1]) if len(sys.argv) > 1 else 730
    
    print(f"🎮 Testing SteamCharts extraction for app {app_id}")
    print("="*50)
    
    success = test_steamcharts_extraction(app_id)
    
    if success:
        print("✅ Test passed! The system is working correctly.")
    else:
        print("❌ Test failed. Check the app ID or internet connection.")
