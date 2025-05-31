#!/usr/bin/env python3
"""
Electronics Ecommerce Market Analysis Scraper
Following CRISP-DM methodology for business understanding and data collection

Author: Business Analytics Team
Purpose: Market analysis for integrated circuits, sensors, and microcontrollers
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
from urllib.parse import urljoin
import re
from typing import Dict, List, Optional, Tuple
import csv
from datetime import datetime
import os

class EcommerceMarketScraper:
    """
    A comprehensive scraper for electronics ecommerce market analysis
    """
    
    def __init__(self, base_url: str = "https://store.nerokas.co.ke/SKU-"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # Initialize logging
        self.setup_logging()
        
        # Counters for monitoring
        self.successful_scrapes = 0
        self.failed_scrapes = 0
        self.consecutive_failures = 0
        self.products_data = []
        
    def setup_logging(self):
        """Setup comprehensive logging for monitoring scraping progress"""
        log_filename = f"ecommerce_scraping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def is_valid_product_page(self, soup: BeautifulSoup, url: str) -> bool:
        """
        Comprehensive validation to check if page contains valid product data
        """
        try:
            # Check for specific product page indicators
            product_title = soup.find('h1', class_='title page-title')
            product_price = soup.find('div', class_='product-price')
            product_container = soup.find('div', id='product-product')
            
            # Check for error indicators
            error_indicators = [
                soup.find('div', class_='error'), 
                soup.find('h1', string=re.compile(r'404|not found|error', re.I)),
                soup.find('title', string=re.compile(r'404|not found|error', re.I))
            ]
            
            # Basic validation checks
            has_product_elements = bool(product_title and product_container)
            has_no_errors = not any(error_indicators)
            has_meaningful_title = bool(product_title and len(product_title.get_text().strip()) > 3)
            
            return has_product_elements and has_no_errors and has_meaningful_title
            
        except Exception as e:
            self.logger.warning(f"Error validating page {url}: {str(e)}")
            return False
    
    def extract_product_data(self, soup: BeautifulSoup, sku: int, url: str) -> Dict:
        """
        Extract comprehensive product data from the HTML soup
        """
        product_data = {
            'sku': sku,
            'url': url,
            'scraped_at': datetime.now().isoformat(),
            'product_name': '',
            'price': '',
            'price_numeric': 0.0,
            'currency': 'KES',
            'product_description': '',
            'product_features': '',
            'stock_status': '',
            'model': '',
            'brand': '',
            'manufacturer': '',
            'category': '',
            'tags': '',
            'rating': 0,
            'review_count': 0,
            'reviews_text': '',
            'image_urls': [],
            'main_image_url': '',
            'product_labels': '',
            'specifications': {}
        }
        
        try:
            # Extract product name
            title_elem = soup.find('h1', class_='title page-title')
            if title_elem:
                product_data['product_name'] = title_elem.get_text().strip()
            
            # Extract price information
            price_elem = soup.find('div', class_='product-price')
            if price_elem:
                price_text = price_elem.get_text().strip()
                product_data['price'] = price_text
                
                # Extract numeric price
                price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                if price_match:
                    try:
                        product_data['price_numeric'] = float(price_match.group())
                    except ValueError:
                        pass
            
            # Extract product description and features
            desc_tab = soup.find('div', id=re.compile(r'product_tabs.*'))
            if desc_tab:
                desc_content = desc_tab.find('div', class_='block-content')
                if desc_content:
                    # Get full description text
                    product_data['product_description'] = desc_content.get_text(strip=True)
                    
                    # Extract structured features
                    features = []
                    for p in desc_content.find_all('p'):
                        text = p.get_text().strip()
                        if text and ('●' in text or 'Features:' in text):
                            features.append(text)
                    product_data['product_features'] = ' | '.join(features)
            
            # Extract stock status
            stock_elem = soup.find('li', class_='product-stock')
            if stock_elem:
                stock_span = stock_elem.find('span')
                if stock_span:
                    product_data['stock_status'] = stock_span.get_text().strip()
            
            # Extract model
            model_elem = soup.find('li', class_='product-model')
            if model_elem:
                model_span = model_elem.find('span')
                if model_span:
                    product_data['model'] = model_span.get_text().strip()
            
            # Extract brand/manufacturer
            brand_elem = soup.find('div', class_='brand-image product-manufacturer')
            if brand_elem:
                brand_link = brand_elem.find('a')
                if brand_link:
                    brand_span = brand_link.find('span')
                    if brand_span:
                        product_data['brand'] = brand_span.get_text().strip()
                        product_data['manufacturer'] = product_data['brand']
            
            # Extract category from breadcrumb
            breadcrumb = soup.find('ul', class_='breadcrumb')
            if breadcrumb:
                breadcrumb_items = [li.get_text().strip() for li in breadcrumb.find_all('li')[1:]]  # Skip home
                if breadcrumb_items:
                    product_data['category'] = ' > '.join(breadcrumb_items[:-1])  # Exclude product name
            
            # Extract tags
            tags_div = soup.find('div', class_='tags')
            if tags_div:
                tag_links = tags_div.find_all('a')
                tags = [link.get_text().strip() for link in tag_links]
                product_data['tags'] = ', '.join(tags)
            
            # Extract rating information
            rating_div = soup.find('div', class_='rating rating-page')
            if rating_div:
                # Count filled stars
                filled_stars = len(rating_div.find_all('i', class_='fa-star'))
                product_data['rating'] = filled_stars
                
                # Extract review count from text
                review_text = rating_div.get_text()
                review_match = re.search(r'(\d+)\s+reviews?', review_text, re.I)
                if review_match:
                    product_data['review_count'] = int(review_match.group(1))
            
            # Extract image URLs
            image_urls = []
            main_image = soup.find('img', {'alt': product_data['product_name']})
            if main_image and main_image.get('src'):
                main_url = urljoin(url, main_image['src'])
                product_data['main_image_url'] = main_url
                image_urls.append(main_url)
            
            # Get additional images
            additional_images = soup.find_all('img', {'alt': product_data['product_name']})
            for img in additional_images[1:]:  # Skip first (main) image
                if img.get('src'):
                    img_url = urljoin(url, img['src'])
                    if img_url not in image_urls:
                        image_urls.append(img_url)
            
            product_data['image_urls'] = image_urls
            
            # Extract product labels
            labels = soup.find_all('span', class_='product-label')
            if labels:
                label_texts = [label.get_text().strip() for label in labels]
                product_data['product_labels'] = ', '.join(label_texts)
            
            # Extract additional specifications from product stats
            stats_list = soup.find('ul', class_='list-unstyled')
            if stats_list:
                specs = {}
                for li in stats_list.find_all('li'):
                    text = li.get_text()
                    if ':' in text:
                        key, value = text.split(':', 1)
                        specs[key.strip().replace(':', '')] = value.strip()
                product_data['specifications'] = specs
                
        except Exception as e:
            self.logger.error(f"Error extracting data for SKU {sku}: {str(e)}")
            
        return product_data
    
    def scrape_product(self, sku: int) -> Optional[Dict]:
        """
        Scrape a single product by SKU
        """
        url = f"{self.base_url}{sku}"
        
        try:
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 404:
                self.logger.debug(f"SKU {sku}: 404 Not Found")
                return None
            elif response.status_code != 200:
                self.logger.warning(f"SKU {sku}: HTTP {response.status_code}")
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            if not self.is_valid_product_page(soup, url):
                self.logger.debug(f"SKU {sku}: Invalid product page")
                return None
            
            product_data = self.extract_product_data(soup, sku, url)
            
            if product_data['product_name']:  # Basic validation
                self.successful_scrapes += 1
                self.consecutive_failures = 0
                self.logger.info(f"Successfully scraped SKU {sku}: {product_data['product_name']}")
                return product_data
            else:
                self.logger.warning(f"SKU {sku}: No product name found")
                return None
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network error for SKU {sku}: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error for SKU {sku}: {str(e)}")
            return None
    
    def save_to_csv(self, filename: str = None):
        """
        Save collected data to CSV with optimal structure for ecommerce analysis
        """
        if not filename:
            filename = f"ecommerce_products_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        if not self.products_data:
            self.logger.warning("No product data to save")
            return
        
        # Flatten specifications for CSV
        flattened_data = []
        for product in self.products_data:
            flat_product = product.copy()
            
            # Convert specifications dict to separate columns
            if 'specifications' in flat_product and isinstance(flat_product['specifications'], dict):
                for key, value in flat_product['specifications'].items():
                    flat_product[f'spec_{key.lower().replace(" ", "_")}'] = value
                del flat_product['specifications']
            
            # Convert image URLs list to string
            if isinstance(flat_product.get('image_urls'), list):
                flat_product['image_urls'] = '|'.join(flat_product['image_urls'])
            
            flattened_data.append(flat_product)
        
        df = pd.DataFrame(flattened_data)
        
        # Optimize column order for analysis
        priority_columns = [
            'sku', 'product_name', 'price', 'price_numeric', 'currency',
            'stock_status', 'brand', 'manufacturer', 'category', 'model',
            'rating', 'review_count', 'product_labels', 'tags',
            'product_description', 'product_features', 'main_image_url',
            'url', 'scraped_at'
        ]
        
        # Reorder columns
        other_columns = [col for col in df.columns if col not in priority_columns]
        ordered_columns = [col for col in priority_columns if col in df.columns] + other_columns
        df = df[ordered_columns]
        
        df.to_csv(filename, index=False, encoding='utf-8')
        self.logger.info(f"Saved {len(df)} products to {filename}")
        
        # Create summary statistics
        self.create_summary_report(df, filename.replace('.csv', '_summary.txt'))
    
    def create_summary_report(self, df: pd.DataFrame, filename: str):
        """
        Create a summary report for business analysis
        """
        with open(filename, 'w') as f:
            f.write("ECOMMERCE MARKET ANALYSIS SUMMARY REPORT\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("DATASET OVERVIEW:\n")
            f.write(f"Total products scraped: {len(df)}\n")
            f.write(f"Successful scrapes: {self.successful_scrapes}\n")
            f.write(f"Failed scrapes: {self.failed_scrapes}\n\n")
            
            if len(df) > 0:
                f.write("PRICE ANALYSIS:\n")
                f.write(f"Average price: {df['price_numeric'].mean():.2f} KES\n")
                f.write(f"Median price: {df['price_numeric'].median():.2f} KES\n")
                f.write(f"Price range: {df['price_numeric'].min():.2f} - {df['price_numeric'].max():.2f} KES\n\n")
                
                f.write("BRAND DISTRIBUTION:\n")
                brand_counts = df['brand'].value_counts().head(10)
                for brand, count in brand_counts.items():
                    f.write(f"{brand}: {count} products\n")
                f.write("\n")
                
                f.write("CATEGORY DISTRIBUTION:\n")
                category_counts = df['category'].value_counts().head(10)
                for category, count in category_counts.items():
                    f.write(f"{category}: {count} products\n")
                f.write("\n")
                
                f.write("STOCK STATUS:\n")
                stock_counts = df['stock_status'].value_counts()
                for status, count in stock_counts.items():
                    f.write(f"{status}: {count} products\n")
    
    def run_market_analysis(self, start_sku: int = 0, max_sku: int = 50000, max_consecutive_failures: int = 100):
        """
        Main method to run the comprehensive market analysis
        """
        self.logger.info(f"Starting market analysis scraping from SKU {start_sku} to {max_sku}")
        self.logger.info(f"Will stop after {max_consecutive_failures} consecutive failures")
        
        page_count = 0
        
        for sku in range(start_sku, max_sku + 1):
            # Rate limiting: 3 second delay after every 100 pages
            if page_count > 0 and page_count % 100 == 0:
                self.logger.info(f"Rate limiting: Sleeping for 3 seconds after {page_count} requests")
                time.sleep(3)
            
            product_data = self.scrape_product(sku)
            
            if product_data:
                self.products_data.append(product_data)
                self.consecutive_failures = 0
            else:
                self.failed_scrapes += 1
                self.consecutive_failures += 1
            
            page_count += 1
            
            # Progress reporting
            if page_count % 50 == 0:
                self.logger.info(f"Progress: {page_count} pages processed, {len(self.products_data)} products found")
            
            # Stop condition
            if self.consecutive_failures >= max_consecutive_failures:
                self.logger.info(f"Stopping: {max_consecutive_failures} consecutive failures reached")
                break
            
            # Small delay between requests
            time.sleep(0.5)
        
        # Save results
        if self.products_data:
            self.save_to_csv()
            self.logger.info(f"Market analysis complete! Collected {len(self.products_data)} products")
        else:
            self.logger.warning("No products were successfully scraped")

def main():
    """
    Main execution function following CRISP-DM methodology
    """
    print("Electronics Ecommerce Market Analysis Tool")
    print("Following CRISP-DM Methodology for Business Understanding")
    print("-" * 60)
    
    # Business Understanding Phase
    print("PHASE 1: Business Understanding")
    print("Market: Integrated circuits, sensors, and microcontrollers")
    print("Goal: Comprehensive market analysis for new business entry")
    print("Data Source: Electronics ecommerce platform")
    print()
    
    # Data Understanding Phase
    print("PHASE 2: Data Understanding & Collection")
    
    # Initialize scraper
    scraper = EcommerceMarketScraper()
    
    # Allow user customization
    start_sku = int(input("Enter starting SKU (default 0): ") or "0")
    max_sku = int(input("Enter maximum SKU to check (default 50000): ") or "50000")
    max_failures = int(input("Enter max consecutive failures before stopping (default 100): ") or "100")
    
    print(f"\nStarting scraping from SKU {start_sku} to {max_sku}")
    print(f"Will stop after {max_failures} consecutive failures")
    print("Implementing 3-second delay after every 100 requests")
    print("-" * 60)
    
    # Run the analysis
    try:
        scraper.run_market_analysis(start_sku, max_sku, max_failures)
        print("\nMarket analysis completed successfully!")
        print("Check the generated CSV file and summary report for detailed results.")
        
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
        if scraper.products_data:
            scraper.save_to_csv()
            print("Partial data saved to CSV")
    except Exception as e:
        print(f"Error during scraping: {str(e)}")
        if scraper.products_data:
            scraper.save_to_csv()
            print("Partial data saved to CSV")

if __name__ == "__main__":
    main()