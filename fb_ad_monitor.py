# fb_ad_monitor.py
# Copyright (c) 2024, regek
# All rights reserved.

# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import hashlib
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from threading import Lock, RLock
from typing import Any, Dict, List, Optional, Tuple
import shutil
from urllib.parse import urlparse

import PyRSS2Gen
import tzlocal
from apscheduler.jobstores.base import ConflictingIdError, JobLookupError
from apscheduler.schedulers.background import BackgroundScheduler
from bs4 import BeautifulSoup
from dateutil import parser
from flask import Flask, Response, jsonify, request, render_template
from selenium import webdriver
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.firefox import GeckoDriverManager

# --- Constants ---
DEFAULT_DB_NAME = 'fb-rss-feed.db'
DEFAULT_LOG_LEVEL = 'INFO'
CONFIG_FILE_ENV_VAR = 'CONFIG_FILE'
DEFAULT_CONFIG_FILE = 'config.json'
LOG_LEVEL_ENV_VAR = 'LOG_LEVEL'
AD_DIV_SELECTOR = 'div.x78zum5.xdt5ytf.x1iyjqo2.xd4ddsz' # Selector for waiting
AD_LINK_TAG = 'a'
AD_TITLE_SELECTOR_STYLE = '-webkit-line-clamp' # Part of the style attribute for title span
AD_PRICE_SELECTOR_DIR = 'auto' # dir attribute for price span
SELENIUM_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0"
FACEBOOK_BASE_URL = "https://facebook.com"


class fbRssAdMonitor:
    """Monitors Facebook Marketplace search URLs for new ads and generates an RSS feed."""

    def __init__(self, json_file: str):
        """
        Initializes the fbRssAdMonitor instance.

        Args:
            json_file (str): Path to the configuration JSON file.
        """
        self.config_file_path: str = json_file # Store path to config file
        self.config_lock: RLock = RLock() # Lock for config file operations

        self.urls_to_monitor: List[str] = []
        self.url_filters: Dict[str, Dict[str, List[str]]] = {}
        self.database: str = DEFAULT_DB_NAME
        self.local_tz = tzlocal.get_localzone()
        self.log_filename: str = "fb_monitor.log" # Default, will be overwritten by config
        self.server_ip: str = "0.0.0.0" # Default
        self.server_port: int = 5000 # Default
        self.currency: str = "$" # Default
        self.refresh_interval_minutes: int = 15 # Default
        self.driver: Optional[webdriver.Firefox] = None
        # self.logger: logging.Logger = logging.getLogger(__name__) # Placeholder, setup in set_logger
        self.scheduler: Optional[BackgroundScheduler] = None
        self.job_lock: Lock = Lock() # Lock for the ad checking job

        # Initialize logger attribute and perform initial setup
        self.logger = logging.getLogger(__name__)
        self.set_logger() # Initial setup with default self.log_filename

        # Load configuration. This may update self.log_filename.
        # load_from_json can now safely use the initialized self.logger for its own error reporting.
        try:
            original_log_filename = self.log_filename
            self.load_from_json(self.config_file_path)

            # If log_filename was changed by loading config, re-initialize the logger's file handler.
            if self.log_filename != original_log_filename:
                self.logger.info(f"Log filename updated from '{original_log_filename}' to '{self.log_filename}'. Re-initializing logger file handler.")
                self.set_logger() # This will re-setup handlers, including the file handler with the new name.
        except Exception as e:
            # Log critical failure and re-raise to prevent app from running in a bad state
            if self.logger and hasattr(self.logger, 'critical'): # Ensure logger is usable
                self.logger.critical(f"Fatal error during configuration loading: {e}", exc_info=True)
            else:
                # Fallback print if logger itself failed catastrophically
                print(f"CRITICAL FALLBACK: Fatal error during configuration loading and logger unavailable: {e}")
            raise # Important to stop execution if config is broken

        # Initialize Flask app
        self.app: Flask = Flask(__name__, template_folder='templates', static_folder='static')
        self._setup_routes()

        self.rss_feed: PyRSS2Gen.RSS2 = PyRSS2Gen.RSS2(
            title="Facebook Marketplace Ad Feed",
            link=f"http://{self.server_ip}:{self.server_port}/rss", # Use configured IP/Port
            description="An RSS feed to monitor new ads on Facebook Marketplace",
            lastBuildDate=datetime.now(timezone.utc),
            items=[]
        )


    def set_logger(self) -> None:
        """
        Sets up logging configuration with both file and console streaming.
        Log level is fetched from the environment variable LOG_LEVEL.
        """
        self.logger = logging.getLogger(__name__) # Get the logger instance
        log_formatter = logging.Formatter(
            '%(levelname)s:%(asctime)s:%(funcName)s:%(lineno)d::%(message)s',
            datefmt='%m/%d/%Y %I:%M:%S %p'
        )

        # Get log level from environment variable, defaulting to INFO if not set
        log_level_str = os.getenv(LOG_LEVEL_ENV_VAR, DEFAULT_LOG_LEVEL).upper()
        try:
            log_level = logging.getLevelName(log_level_str)
            if not isinstance(log_level, int): # Check if getLevelName returned a valid level
                 # Use basicConfig for logging before logger is fully set up
                 logging.basicConfig(level=logging.WARNING)
                 logging.warning(f"Invalid LOG_LEVEL '{log_level_str}'. Defaulting to {DEFAULT_LOG_LEVEL}.")
                 log_level = logging.INFO
        except ValueError:
            logging.basicConfig(level=logging.WARNING)
            logging.warning(f"Invalid LOG_LEVEL '{log_level_str}'. Defaulting to {DEFAULT_LOG_LEVEL}.")
            log_level = logging.INFO


        # File handler (rotating log)
        try:
            file_handler = RotatingFileHandler(
                self.log_filename, mode='a', maxBytes=10*1024*1024, # Use 'a' for append
                backupCount=2, encoding='utf-8', delay=False # Use utf-8
            )
            file_handler.setFormatter(log_formatter)
            file_handler.setLevel(log_level)
            self.logger.addHandler(file_handler)
        except Exception as e:
             # Use basicConfig for fallback logging if file handler fails
            logging.basicConfig(level=logging.ERROR)
            logging.error(f"Failed to set up file logging handler for {self.log_filename}: {e}. Logging to console only.")


        # Stream handler (console output)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        console_handler.setLevel(log_level)

        # Set the logger level and add handlers
        self.logger.setLevel(log_level)
        # self.logger.addHandler(file_handler) # Added above with error handling
        self.logger.addHandler(console_handler)
        self.logger.info(f"Logger initialized with level {logging.getLevelName(log_level)}")


    def init_selenium(self) -> None:
        """
        Initializes Selenium WebDriver with Firefox options.
        Ensures any existing driver is quit first.
        """
        self.quit_selenium() # Ensure previous instance is closed

        try:
            self.logger.debug("Initializing Selenium WebDriver...")
            firefox_options = FirefoxOptions()
            # Specify Firefox binary path explicitly for Docker compatibility
            # Try multiple possible Firefox binary locations for different environments
            possible_paths = [
                # Homebrew macOS path
                "/opt/homebrew/bin/firefox",
                # Standard macOS paths
                "/Applications/Firefox.app/Contents/MacOS/firefox",
                "/Applications/Firefox Developer Edition.app/Contents/MacOS/firefox",
                # Standard Linux paths
                "/usr/bin/firefox",
                "/usr/lib/firefox/firefox",
                "/snap/bin/firefox",
                "/opt/firefox/firefox",
                "/usr/local/bin/firefox",
                "/app/firefox/firefox",
                "/firefox/firefox",
                # Common Docker/Ubuntu paths
                "/usr/bin/firefox-esr",
                "/usr/lib/firefox-esr/firefox-esr"
            ]
            
            firefox_path = None
            for path in possible_paths:
                if os.path.exists(path):
                    firefox_path = path
                    break
            
            if firefox_path:
                firefox_options.binary_location = firefox_path
                self.logger.debug(f"Using Firefox binary at: {firefox_path}")
            else:
                self.logger.warning("Firefox binary not found in standard locations. Letting webdriver-manager handle it automatically.")
                # Don't set binary_location, let webdriver-manager find it
            firefox_options.add_argument("--no-sandbox")
            firefox_options.add_argument("--disable-dev-shm-usage")
            firefox_options.add_argument("--private")
            firefox_options.add_argument("--headless")
            firefox_options.set_preference("general.useragent.override", SELENIUM_USER_AGENT)
            firefox_options.set_preference("dom.webdriver.enabled", False)
            firefox_options.set_preference("useAutomationExtension", False)
            firefox_options.set_preference("privacy.resistFingerprinting", True)

            # Suppress GeckoDriverManager logs unless logger level is DEBUG
            log_level_gecko = logging.WARNING if self.logger.level > logging.DEBUG else logging.DEBUG
            os.environ['WDM_LOG_LEVEL'] = str(log_level_gecko) # Set env var for WDM logging level
            # Also configure WDM to use the logger's log file if possible
            os.environ['WDM_LOCAL'] = '1' # Try to use local cache
            # Note: WDM might still log to stderr/stdout depending on its internal setup

            gecko_driver_path = GeckoDriverManager().install()
            
            # Redirect selenium service logs to /dev/null (or NUL on windows) to prevent console spam
            service_log_path = 'nul' if os.name == 'nt' else '/dev/null'
            self.driver = webdriver.Firefox(
                service=FirefoxService(gecko_driver_path, log_path=service_log_path),
                options=firefox_options
            )
            self.logger.debug("Selenium WebDriver initialized successfully.")

        except WebDriverException as e:
            self.logger.error(f"WebDriverException during Selenium initialization: {e}")
            self.driver = None # Ensure driver is None if init fails
            raise # Re-raise the exception to be handled by the caller
        except Exception as e:
            self.logger.error(f"Unexpected error initializing Selenium: {e}")
            self.driver = None # Ensure driver is None if init fails
            raise # Re-raise the exception


    def quit_selenium(self) -> None:
        """Safely quits the Selenium WebDriver if it exists."""
        if self.driver:
            self.logger.debug("Quitting Selenium WebDriver...")
            try:
                self.driver.quit()
                self.logger.debug("Selenium WebDriver quit successfully.")
            except WebDriverException as e:
                self.logger.error(f"Error quitting Selenium WebDriver: {e}")
            except Exception as e:
                 self.logger.error(f"Unexpected error quitting Selenium WebDriver: {e}")
            finally:
                self.driver = None # Ensure driver is set to None


    def setup_scheduler(self) -> None:
        """
        Sets up the background job scheduler to check for new ads.
        """
        if self.scheduler and self.scheduler.running:
             self.logger.warning("Scheduler is already running.")
             return

        self.logger.info(f"Setting up scheduler to run every {self.refresh_interval_minutes} minutes.")
        job_id = 'check_ads_job' # Use a fixed ID

        if self.scheduler is None:
            self.scheduler = BackgroundScheduler(timezone=str(self.local_tz))

        try:
            # Remove existing job if it exists, before adding/rescheduling
            try:
                self.scheduler.remove_job(job_id)
                self.logger.info(f"Removed existing job '{job_id}' before rescheduling.")
            except JobLookupError:
                self.logger.debug(f"Job '{job_id}' not found, no need to remove before adding.")

            self.scheduler.add_job(
                self.check_for_new_ads,
                'interval',
                id=job_id,
                minutes=self.refresh_interval_minutes,
                misfire_grace_time=60,
                coalesce=True,
                next_run_time=datetime.now(self.local_tz) + timedelta(seconds=5) # Start soon
            )
            if not self.scheduler.running:
                self.scheduler.start()
            self.logger.info(f"Scheduler configured with job '{job_id}' to run every {self.refresh_interval_minutes} minutes.")

        except ConflictingIdError:
            # This case should ideally be handled by remove_job now, but as a fallback:
            self.logger.warning(f"Job '{job_id}' conflict. Attempting to reschedule.")
            self.scheduler.reschedule_job(job_id, trigger='interval', minutes=self.refresh_interval_minutes)
            if not self.scheduler.running:
                self.scheduler.start(paused=False) # Resume if paused
            self.logger.info(f"Scheduler resumed/rescheduled job '{job_id}'.")
        except Exception as e:
             self.logger.error(f"Failed to setup or start scheduler: {e}")


    def local_time(self, dt: datetime) -> datetime:
        """Converts a UTC datetime object to local time."""
        if dt.tzinfo is None:
             # Assume UTC if no timezone info
             dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(self.local_tz)


    def load_from_json(self, json_file: str) -> None:
        """
        Loads configuration from a JSON file.

        Args:
            json_file (str): Path to the JSON file.

        Raises:
            FileNotFoundError: If the JSON file is not found.
            ValueError: If the JSON file is invalid or missing required keys.
            Exception: For other unexpected errors during loading.
        """
        self.logger.info(f"Loading configuration from {json_file}...")
        try:
            with open(json_file, 'r', encoding='utf-8') as file:
                data = json.load(file)

            # Validate and assign configuration values
            self.server_ip = data.get('server_ip', self.server_ip)
            self.server_port = data.get('server_port', self.server_port)
            self.currency = data.get('currency', self.currency)
            self.refresh_interval_minutes = data.get('refresh_interval_minutes', self.refresh_interval_minutes)
            self.log_filename = data.get('log_filename', self.log_filename)
            self.database = data.get('database_name', self.database) # Allow overriding DB name

            # Validate url_filters structure
            url_filters_raw = data.get('url_filters', {})
            if not isinstance(url_filters_raw, dict):
                 raise ValueError("'url_filters' must be a dictionary in the config file.")

            self.url_filters = url_filters_raw
            self.urls_to_monitor = list(self.url_filters.keys())

            if not self.urls_to_monitor:
                 self.logger.warning("No URLs found in 'url_filters'. Monitoring will be inactive.")

            self.logger.info("Configuration loaded successfully.")
            self.logger.debug(f"Monitoring URLs: {self.urls_to_monitor}")
            self.logger.debug(f"Refresh interval: {self.refresh_interval_minutes} minutes")
            self.logger.debug(f"Log file: {self.log_filename}")
            self.logger.debug(f"Database: {self.database}")


        except FileNotFoundError:
            self.logger.error(f"Configuration file not found: {json_file}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decoding JSON from {json_file}: {e}")
            raise ValueError(f"Invalid JSON format in {json_file}") from e
        except KeyError as e:
             self.logger.error(f"Missing required key in configuration file {json_file}: {e}")
             raise ValueError(f"Missing key '{e}' in {json_file}") from e
        except ValueError as e: # Catch specific validation errors
             self.logger.error(f"Configuration error in {json_file}: {e}")
             raise
        except Exception as e:
            self.logger.exception(f"Unexpected error loading configuration from {json_file}: {e}") # Use exception for stack trace
            raise


    def apply_filters(self, url: str, title: str) -> bool:
        """
        Applies keyword filters specific to the URL to the ad title.
        Filters are defined in levels (e.g., "level1", "level2"). An ad must
        match at least one keyword from *each* defined level for the given URL.

        Args:
            url (str): The URL for which to apply filters.
            title (str): The title of the ad.

        Returns:
            bool: True if the title matches all filter levels for the URL, False otherwise.
        """
        filters = self.url_filters.get(url)
        # If filters is None (URL not in url_filters) or an empty dict {} (URL present but no levels/keywords defined),
        # it means no filtering should be applied for this URL.
        if not filters: # This covers both None and {}
            self.logger.debug(f"No specific keyword filters defined for URL '{url}' (filters: {filters}). Ad '{title}' passes.")
            return True # No filters for this URL, so it passes

        if not isinstance(filters, dict):
             self.logger.warning(f"Filters for URL '{url}' are not a dictionary (type: {type(filters)}). Skipping filters.")
             return True # Invalid filter format, treat as passing

        try:

            title_lower = title.lower()

            exclude_keywords = filters.get('exclude', [])
            if any(keyword.lower() in title_lower for keyword in exclude_keywords):
                self.logger.debug(f"Ad '{title}' for URL '{url}' contains excluded keywords. Keywords: {exclude_keywords}")
                return False

            # Sort levels numerically (level1, level2, ...)
            level_keys = sorted(
                [k for k in filters.keys() if k.startswith('level') and k[5:].isdigit()],
                key=lambda x: int(x[5:])
            )

            if not level_keys:
                 self.logger.debug(f"No valid 'levelX' keys found in filters for URL '{url}'. Ad '{title}' passes.")
                 return True # No valid levels defined

            for level in level_keys:
                keywords = filters.get(level, [])
                if not isinstance(keywords, list):
                     self.logger.warning(f"Keywords for level '{level}' in URL '{url}' are not a list. Skipping level.")
                     continue # Skip invalid level format

                if not keywords:
                     self.logger.debug(f"No keywords defined for level '{level}' in URL '{url}'. Skipping level.")
                     continue # Skip empty level

                # Check if *any* keyword in this level matches
                if not any(keyword.lower() in title_lower for keyword in keywords):
                    self.logger.debug(f"Ad '{title}' failed filter level '{level}' for URL '{url}'. Keywords: {keywords}")
                    return False # Must match at least one keyword per level

            # If all levels passed
            self.logger.debug(f"Ad '{title}' passed all filter levels for URL '{url}'.")
            return True

        except Exception as e:
            self.logger.exception(f"Error applying filters for URL '{url}', title '{title}': {e}")
            return False # Fail safe on error


    def save_html(self, soup: BeautifulSoup, filename: str = 'output.html') -> None:
        """Saves the prettified HTML content of a BeautifulSoup object to a file."""
        try:
            html_content = soup.prettify()
            with open(filename, 'w', encoding='utf-8') as file:
                file.write(html_content)
            self.logger.debug(f"HTML content saved to {filename}")
        except Exception as e:
            self.logger.error(f"Error saving HTML to {filename}: {e}")


    def get_page_content(self, url: str, max_retries: int = 2) -> Optional[str]:
        """
        Fetches the page content using Selenium with retry mechanism.

        Args:
            url (str): The URL of the page to fetch.
            max_retries (int): Maximum number of retry attempts.

        Returns:
            Optional[str]: The HTML content of the page, or None if an error occurred.
        """
        if not self.driver:
             self.logger.error("Selenium driver not initialized. Cannot fetch page content.")
             return None
        
        for attempt in range(max_retries + 1):
            try:
                self.logger.info(f"Requesting URL: {url} (attempt {attempt + 1}/{max_retries + 1})")
                self.driver.get(url)
                
                # Wait for page to load with multiple fallback strategies
                try:
                    # First try the original selector
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, AD_DIV_SELECTOR))
                    )
                except TimeoutException:
                    # Fallback: wait for any div that might contain ads
                    try:
                        WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='main'], div[data-pagelet='MarketplacePDP'], main"))
                        )
                        self.logger.warning(f"Using fallback selector for {url} - AD_DIV_SELECTOR may be outdated")
                    except TimeoutException:
                        # Final fallback: wait for body to be present
                        WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.TAG_NAME, "body"))
                        )
                        self.logger.warning(f"Using body tag fallback for {url} - page structure may have changed")
                
                # Additional wait for JavaScript to complete
                WebDriverWait(self.driver, 5).until(
                    lambda driver: driver.execute_script("return document.readyState") == "complete"
                )
                
                self.logger.debug(f"Page content loaded successfully for {url}")
                return self.driver.page_source
                
            except WebDriverException as e:
                if attempt == max_retries:
                    self.logger.error(f"Selenium error fetching page content for {url} after {max_retries + 1} attempts: {e}")
                    # Save page source for debugging on final failure
                    try:
                        page_source = self.driver.page_source if self.driver else "No driver available"
                        error_filename = f"error_page_{int(time.time())}.html"
                        with open(error_filename, "w", encoding="utf-8") as f:
                            f.write(f"<!-- Error: {e} -->\n")
                            f.write(f"<!-- URL: {url} -->\n")
                            f.write(page_source)
                        self.logger.info(f"Saved error page source to {error_filename}")
                    except Exception as save_err:
                         self.logger.error(f"Could not save page source on error: {save_err}")
                    return None
                else:
                    self.logger.warning(f"Selenium error on attempt {attempt + 1} for {url}: {e}. Retrying...")
                    time.sleep(2)  # Wait before retry
                    
            except Exception as e:
                if attempt == max_retries:
                    self.logger.exception(f"Unexpected error fetching page content for {url} after {max_retries + 1} attempts: {e}")
                    return None
                else:
                    self.logger.warning(f"Unexpected error on attempt {attempt + 1} for {url}: {e}. Retrying...")
                    time.sleep(2)  # Wait before retry


    def get_ads_hash(self, content: str) -> str:
        """
        Generates an MD5 hash for the given content (typically a URL).

        Args:
            content (str): The content to hash.

        Returns:
            str: The MD5 hash of the content.
        """
        return hashlib.md5(content.encode('utf-8')).hexdigest()


    def extract_ad_details(self, content: str, source_url: str) -> List[Tuple[str, str, str, str]]:
        """
        Extracts ad details from the page HTML content and applies URL-specific filters.

        Args:
            content (str): The HTML content of the page.
            source_url (str): The original URL the content was fetched from (used for filtering).

        Returns:
            List[Tuple[str, str, str, str]]: A list of tuples, where each tuple contains
                                             (ad_id_hash, title, price, ad_url).
                                             Only ads matching filters are included.
        """
        ads_found: List[Tuple[str, str, str, str]] = []
        try:
            soup = BeautifulSoup(content, 'html.parser')
            # --- Uncomment to save HTML for debugging ---
            # self.save_html(soup, f"page_content_{source_url.split('/')[-1].split('?')[0]}_{int(time.time())}.html")

            # Find all potential ad links (<a> tags with an href)
            ad_links = soup.find_all(AD_LINK_TAG, href=True)
            self.logger.debug(f"Found {len(ad_links)} potential ad links on {source_url}.")

            processed_urls = set() # Keep track of processed ad URLs to avoid duplicates from the same page

            for ad_link in ad_links:
                href = ad_link.get('href', '')
                # Basic validation of the link (e.g., starts with /marketplace/item/)
                if not href or not href.startswith('/marketplace/item/'):
                    continue

                # Construct full URL and normalize (remove query params)
                full_url = f"{FACEBOOK_BASE_URL}{href.split('?')[0]}"

                if full_url in processed_urls:
                     continue # Skip if already processed this ad URL on this page
                processed_urls.add(full_url)


                # Find title and price with multiple fallback strategies
                title = None
                price = None
                
                # Strategy 1: Original selectors
                title_span = ad_link.find('span', style=lambda value: value and AD_TITLE_SELECTOR_STYLE in value)
                price_span = ad_link.find('span', dir=AD_PRICE_SELECTOR_DIR)
                
                if title_span:
                    title = title_span.get_text(strip=True)
                if price_span:
                    price = price_span.get_text(strip=True)
                
                # Strategy 2: Fallback - look for common title/price patterns
                if not title or not price:
                    # Look for spans with common Marketplace classes
                    all_spans = ad_link.find_all('span')
                    for span in all_spans:
                        span_text = span.get_text(strip=True)
                        if span_text and len(span_text) > 10 and not title:  # Likely title
                            title = span_text
                        elif span_text and (span_text.startswith(self.currency) or 'free' in span_text.lower()) and not price:
                            price = span_text
                
                # Strategy 3: Final fallback - text content analysis
                if not title:
                    # Use link text or nearby text as title
                    link_text = ad_link.get_text(strip=True)
                    if link_text and len(link_text) > 10:
                        title = link_text
                
                if title and price:
                    # Validate price format
                    if price.startswith(self.currency) or 'free' in price.lower():
                        # Generate a unique ID based on the ad's URL
                        ad_id_hash = self.get_ads_hash(full_url)

                        # Apply filters based on the source URL the ad was found on
                        if self.apply_filters(source_url, title):
                            ads_found.append((ad_id_hash, title, price, full_url))
                        # else:
                        #     self.logger.debug(f"Ad '{title}' ({full_url}) skipped due to filters for {source_url}.")
                    # else:
                    #      self.logger.debug(f"Price format invalid for ad '{title}' ({full_url}). Price found: '{price}'")

            self.logger.info(f"Extracted {len(ads_found)} ads matching filters from {source_url}.")
            return ads_found

        except Exception as e:
            self.logger.exception(f"Error extracting ad details from {source_url}: {e}")
            return []


    def get_db_connection(self) -> Optional[sqlite3.Connection]:
        """
        Establishes a connection to the SQLite database.

        Returns:
            Optional[sqlite3.Connection]: The database connection object, or None on error.
        """
        try:
            conn = sqlite3.connect(self.database, timeout=10) # Add timeout
            conn.row_factory = sqlite3.Row
            # Optional: Enable WAL mode for better concurrency
            # try:
            #      conn.execute("PRAGMA journal_mode=WAL;")
            # except sqlite3.Error as e:
            #      self.logger.warning(f"Could not enable WAL mode for database: {e}")
            self.logger.debug(f"Database connection established to {self.database}")
            return conn
        except sqlite3.Error as e:
            self.logger.error(f"Database connection error to {self.database}: {e}")
            return None


    def check_for_new_ads(self) -> None:
        """
        Checks for new ads on monitored URLs, updates the database, and adds new ads to the RSS feed object.
        This method is intended to be run as a scheduled job.
        """
        if not self.job_lock.acquire(blocking=False):
            self.logger.warning("Ad check job is already running. Skipping this execution.")
            return

        self.logger.info("Starting scheduled check for new ads...")
        conn = None
        new_ads_added_count = 0
        processed_urls_count = 0

        try:
            conn = self.get_db_connection()
            if not conn:
                self.logger.error("Failed to get database connection. Aborting ad check.")
                return # Cannot proceed without DB

            cursor = conn.cursor()
            # Keep track of ads added in this run to avoid duplicates if multiple URLs list the same ad
            added_ad_ids_this_run = set()

            for url in self.urls_to_monitor:
                processed_urls_count += 1
                self.logger.info(f"Processing URL ({processed_urls_count}/{len(self.urls_to_monitor)}): {url}")
                try:
                    self.init_selenium() # Initialize driver for this URL
                    if not self.driver:
                         self.logger.warning(f"Skipping URL {url} due to Selenium initialization failure.")
                         continue # Skip to next URL if driver init failed

                    content = self.get_page_content(url)
                    self.quit_selenium() # Quit driver after fetching content for this URL

                    if content is None:
                        self.logger.warning(f"No content received for URL: {url}. Skipping.")
                        continue

                    ads = self.extract_ad_details(content, url)
                    if not ads:
                         self.logger.info(f"No matching ads found or extracted for URL: {url}.")
                         continue


                    for ad_id, title, price, ad_url in ads:
                        if ad_id in added_ad_ids_this_run:
                             self.logger.debug(f"Ad '{title}' ({ad_id}) already processed in this run. Skipping.")
                             continue # Avoid processing the same ad multiple times if found via different source URLs

                        # Check if ad exists in DB (more robust check than just recent)
                        cursor.execute('SELECT ad_id FROM ad_changes WHERE ad_id = ?', (ad_id,))
                        existing_ad = cursor.fetchone()
                        now_utc = datetime.now(timezone.utc)
                        now_iso = now_utc.isoformat()

                        if existing_ad is None:
                            # Ad is completely new
                            self.logger.info(f"New ad detected: '{title}' ({price}) - {ad_url}")
                            new_item = PyRSS2Gen.RSSItem(
                                title=f"{title} - {price}",
                                link=ad_url,
                                description=f"Price: {price} | Title: {title}", # Simpler description
                                guid=PyRSS2Gen.Guid(ad_id, isPermaLink=False), # Use ad_id hash as GUID
                                pubDate=self.local_time(now_utc) # Use local time for pubDate
                            )
                            try:
                                cursor.execute(
                                    'INSERT INTO ad_changes (url, ad_id, title, price, first_seen, last_checked) VALUES (?, ?, ?, ?, ?, ?)',
                                    (ad_url, ad_id, title, price, now_iso, now_iso)
                                )
                                conn.commit()
                                # Prepend to the live RSS feed object
                                self.rss_feed.items.insert(0, new_item)
                                added_ad_ids_this_run.add(ad_id)
                                new_ads_added_count += 1
                                self.logger.debug(f"Successfully added new ad '{title}' to DB and RSS feed.")
                            except sqlite3.IntegrityError:
                                self.logger.warning(f"IntegrityError inserting ad '{title}' ({ad_id}). Might be a race condition. Updating last_checked.")
                                # If insert fails due to constraint (e.g., ad added between SELECT and INSERT), update last_checked
                                cursor.execute('UPDATE ad_changes SET last_checked = ? WHERE ad_id = ?',
                                               (now_iso, ad_id))
                                conn.commit()
                            except sqlite3.Error as db_err:
                                 self.logger.error(f"Database error processing ad '{title}' ({ad_id}): {db_err}")
                                 conn.rollback() # Rollback on error for this specific ad

                        else:
                             # Ad exists, update last_checked timestamp
                             self.logger.debug(f"Existing ad found: '{title}' ({ad_id}). Updating last_checked.")
                             cursor.execute('UPDATE ad_changes SET last_checked = ? WHERE ad_id = ?',
                                            (now_iso, ad_id))
                             conn.commit()


                except Exception as url_proc_err:
                     self.logger.exception(f"Error processing URL {url}: {url_proc_err}")
                     # Ensure driver is quit even if processing fails mid-way for a URL
                     self.quit_selenium()
                finally:
                     # Short delay between processing URLs
                     time.sleep(2)


            # --- Optional: Prune old ads from DB ---
            self.prune_old_ads(conn)

            self.logger.info(f"Finished ad check. Added {new_ads_added_count} new ads.")

        except sqlite3.DatabaseError as e:
            self.logger.error(f"Database error during ad check: {e}")
            if conn:
                 conn.rollback() # Rollback any potential partial changes
        except Exception as e:
            self.logger.exception(f"Unexpected error during ad check: {e}") # Use exception for stack trace
        finally:
            # Ensure driver is quit if the loop finished or broke unexpectedly
            self.quit_selenium()
            if conn:
                conn.close()
                self.logger.debug("Database connection closed.")
            self.job_lock.release()
            self.logger.debug("Ad check job lock released.")


    def prune_old_ads(self, conn: sqlite3.Connection, days_to_keep: int = 14) -> None:
        """Removes ads from the database that haven't been seen for a specified number of days."""
        if not conn:
             self.logger.warning("Cannot prune ads, database connection is not available.")
             return
        try:
             cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
             self.logger.info(f"Pruning ads last checked before {cutoff_date.isoformat()}...")
             cursor = conn.cursor()
             cursor.execute("DELETE FROM ad_changes WHERE last_checked < ?", (cutoff_date.isoformat(),))
             deleted_count = cursor.rowcount
             conn.commit()
             self.logger.info(f"Pruned {deleted_count} old ad entries from the database.")
        except sqlite3.Error as e:
             self.logger.error(f"Error pruning old ads from database: {e}")
             conn.rollback()


    def generate_rss_feed_from_db(self) -> None:
        """
        Generates the RSS feed items list from recent ad changes in the database.
        This replaces the current items in self.rss_feed.items.
        """
        self.logger.debug("Generating RSS feed items from database...")
        conn = None
        new_items: List[PyRSS2Gen.RSSItem] = []
        try:
            conn = self.get_db_connection()
            if not conn:
                 self.logger.error("Cannot generate RSS feed from DB: No database connection.")
                 # Keep existing items if DB fails
                 return

            cursor = conn.cursor()
            # Fetch ads from the last N days (e.g., 7 days) or based on refresh interval for relevance
            # Using a fixed period like 7 days might be more robust than relying on lastBuildDate
            relevant_period_start = datetime.now(timezone.utc) - timedelta(days=7)

            cursor.execute('''
                SELECT ad_id, title, price, url, last_checked
                FROM ad_changes
                WHERE last_checked >= ?
                ORDER BY last_checked DESC
                LIMIT 100
            ''', (relevant_period_start.isoformat(),)) # Limit number of items
            changes = cursor.fetchall()
            self.logger.debug(f"Fetched {len(changes)} ad changes from DB for RSS feed.")

            for change in changes:
                try:
                    # Ensure last_checked is parsed correctly
                    last_checked_dt_utc = parser.isoparse(change['last_checked'])
                    # Convert to local time for pubDate
                    pub_date_local = self.local_time(last_checked_dt_utc)

                    new_item = PyRSS2Gen.RSSItem(
                        title=f"{change['title']} - {change['price']}",
                        link=change['url'],
                        description=f"Price: {change['price']} | Title: {change['title']}", # Consistent description
                        guid=PyRSS2Gen.Guid(change['ad_id'], isPermaLink=False),
                        pubDate=pub_date_local
                    )
                    new_items.append(new_item)
                except (ValueError, TypeError) as e:
                    self.logger.error(f"Error processing ad change for RSS (ID: {change['ad_id']}): {e}. Skipping item.")
                except Exception as item_err:
                     self.logger.exception(f"Unexpected error creating RSS item for ad (ID: {change['ad_id']}): {item_err}. Skipping item.")


            # Update the RSS feed object
            self.rss_feed.items = new_items
            self.rss_feed.lastBuildDate = datetime.now(timezone.utc) # Update last build date
            self.logger.info(f"RSS feed updated with {len(new_items)} items from database.")

        except sqlite3.DatabaseError as e:
            self.logger.error(f"Database error generating RSS feed: {e}")
            # Optionally keep old items on error: `return` instead of `self.rss_feed.items = []`
            self.rss_feed.items = [] # Clear items on DB error to avoid stale data? Or keep old ones? Clearing for now.
        except Exception as e:
            self.logger.exception(f"Unexpected error generating RSS feed: {e}")
            self.rss_feed.items = [] # Clear items on unexpected error
        finally:
            if conn:
                conn.close()
                self.logger.debug("Database connection closed after RSS generation.")


    def rss(self) -> Response:
        """
        Flask endpoint handler. Generates and returns the RSS feed XML.
        """
        self.logger.info("RSS feed requested. Generating fresh feed from database.")
        # Regenerate the feed content from the DB every time it's requested
        # to ensure it's up-to-date, rather than relying solely on the scheduled update.
        self.generate_rss_feed_from_db()

        try:
            rss_xml = self.rss_feed.to_xml(encoding='utf-8')
            return Response(rss_xml, mimetype='application/rss+xml')
        except Exception as e:
             self.logger.exception(f"Error converting RSS feed to XML: {e}")
             return Response("Error generating RSS feed.", status=500, mimetype='text/plain')

    # --- Configuration Management Routes ---
    def _setup_routes(self) -> None:
        """Sets up Flask routes."""
        self.app.add_url_rule('/rss', 'rss', self.rss)
        self.app.add_url_rule('/edit', 'edit_config_page', self.edit_config_page, methods=['GET'])
        self.app.add_url_rule('/api/config', 'get_config_api', self.get_config_api, methods=['GET'])
        self.app.add_url_rule('/api/config', 'update_config_api', self.update_config_api, methods=['POST'])
        # Flask serves static files from 'static_folder' automatically at '/static/...'
        # So an explicit rule for /static/js/edit_config.js is not strictly needed if static_folder is set.

    def edit_config_page(self) -> Any:
        """Serves the HTML page for editing configuration."""
        try:
            return render_template('edit_config.html')
        except Exception as e:
            self.logger.error(f"Error rendering edit_config.html: {e}")
            return "Error loading configuration page.", 500


    def get_config_api(self) -> Response:
        """API endpoint to get the current configuration."""
        with self.config_lock:
            try:
                with open(self.config_file_path, 'r', encoding='utf-8') as f:
                    current_config = json.load(f)
                return jsonify(current_config)
            except FileNotFoundError:
                self.logger.error(f"Config file {self.config_file_path} not found for API.")
                return jsonify({"detail": "Configuration file not found."}), 404
            except json.JSONDecodeError:
                self.logger.error(f"Error decoding config file {self.config_file_path} for API.")
                return jsonify({"detail": "Error reading configuration file."}), 500
            except Exception as e:
                self.logger.exception(f"Unexpected error in get_config_api: {e}")
                return jsonify({"detail": "Internal server error."}), 500


    def _validate_config_data(self, data: Dict[str, Any]) -> Tuple[bool, str]:
        """Validates the structure and types of the configuration data."""
        required_keys = {
            "server_ip": str,
            "server_port": int,
            "currency": str,
            "refresh_interval_minutes": int,
            "url_filters": dict
            # log_filename and database_name are also in config but not strictly validated here for dynamic updates
        }
        for key, expected_type in required_keys.items():
            if key not in data:
                return False, f"Missing required key: {key}"
            if not isinstance(data[key], expected_type):
                return False, f"Invalid type for {key}. Expected {expected_type.__name__}, got {type(data[key]).__name__}"

        if not (0 < data["server_port"] < 65536):
            return False, "Server port must be between 1 and 65535."
        if data["refresh_interval_minutes"] <= 0:
            return False, "Refresh interval must be a positive integer."

        for url, filters in data["url_filters"].items():
            try:
                parsed_url = urlparse(url)
                if not all([parsed_url.scheme, parsed_url.netloc]):
                    return False, f"Invalid URL format: {url}"
                # Optional: more specific facebook marketplace URL check
                # if not "facebook.com/marketplace" in url.lower():
                #     return False, f"URL does not appear to be a Facebook Marketplace URL: {url}"
            except ValueError:
                return False, f"Invalid URL format: {url}"
            if not isinstance(filters, dict):
                return False, f"Filters for URL '{url}' must be a dictionary."
            for level_name, keywords in filters.items():
                if level_name == 'exclude':
                    continue
                if not level_name.startswith("level") or not level_name[5:].isdigit():
                    return False, f"Invalid filter level name '{level_name}' for URL '{url}'."
                if not isinstance(keywords, list):
                    return False, f"Keywords for '{level_name}' in URL '{url}' must be a list."
                if not all(isinstance(kw, str) for kw in keywords):
                    return False, f"All keywords for '{level_name}' in URL '{url}' must be strings."
        return True, ""

    def _write_config(self, data: Dict[str, Any]) -> None:
        """Writes the given data to the configuration file."""
        with open(self.config_file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        self.logger.info(f"Configuration successfully written to {self.config_file_path}")


    def _reload_config_dynamically(self, new_config: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Attempts to apply new configuration settings dynamically.
        Returns (success_status, message).
        """
        self.logger.info("Attempting to reload configuration dynamically...")
        applied_dynamically = []
        requires_restart = [] # Specific items requiring a restart

        # --- Settings that can be applied dynamically ---

        # Update currency
        new_currency = new_config.get("currency", self.currency)
        if self.currency != new_currency:
            self.currency = new_currency
            applied_dynamically.append("Currency")

        # Update URL filters
        new_url_filters = new_config.get("url_filters", self.url_filters)
        if self.url_filters != new_url_filters:
            self.url_filters = new_url_filters
            self.urls_to_monitor = list(self.url_filters.keys())
            applied_dynamically.append("URL filters")
            if not self.urls_to_monitor:
                self.logger.warning("No URLs to monitor after config update. Monitoring will be inactive.")
                # This isn't a restart warning, but a state warning.
                # We can add a separate list for general operational warnings if needed.

        # Update refresh interval and reschedule job
        new_interval = new_config.get("refresh_interval_minutes", self.refresh_interval_minutes)
        if self.refresh_interval_minutes != new_interval:
            old_interval = self.refresh_interval_minutes
            self.refresh_interval_minutes = new_interval
            if self.scheduler and self.scheduler.running:
                try:
                    job_id = 'check_ads_job'
                    self.scheduler.reschedule_job(job_id, trigger='interval', minutes=self.refresh_interval_minutes)
                    self.logger.info(f"Rescheduled ad check job from {old_interval} to {self.refresh_interval_minutes} minutes.")
                    applied_dynamically.append("Refresh interval")
                except Exception as e:
                    self.logger.error(f"Failed to reschedule job for new interval {self.refresh_interval_minutes}: {e}")
                    # Revert interval change if reschedule fails, to maintain consistency
                    self.refresh_interval_minutes = old_interval
                    return False, f"Failed to apply new refresh interval ({new_interval} min): Scheduler error. Interval reverted to {old_interval} min."
            else:
                # If scheduler isn't running, setup_scheduler will pick up the new interval when it's next called
                self.setup_scheduler() # This will now use the new self.refresh_interval_minutes
                applied_dynamically.append("Refresh interval (scheduler re-initialized/will use on next start)")

        # --- Settings that require a restart ---

        # Server IP and Port
        new_server_ip = new_config.get("server_ip", self.server_ip)
        new_server_port = new_config.get("server_port", self.server_port)
        if self.server_ip != new_server_ip or self.server_port != new_server_port:
            # Update instance vars for things like RSS feed link, but actual server binding needs restart
            self.server_ip = new_server_ip
            self.server_port = new_server_port
            self.rss_feed.link = f"http://{self.server_ip}:{self.server_port}/rss"
            requires_restart.append("Server IP/Port")

        # Log Filename
        new_log_filename = new_config.get("log_filename", self.log_filename)
        if self.log_filename != new_log_filename:
            # self.log_filename = new_log_filename # Actual change requires logger re-init, so restart
            requires_restart.append("Log filename")

        # Database Name
        new_database_name = new_config.get("database_name", self.database)
        if self.database != new_database_name:
            # self.database = new_database_name # Actual change requires DB re-init, so restart
            requires_restart.append("Database name")

        # --- Construct message ---
        message_parts = []
        if applied_dynamically:
            self.logger.info(f"Dynamically applied changes for: {', '.join(applied_dynamically)}")
            message_parts.append(f"Dynamically applied: {', '.join(applied_dynamically)}.")

        if requires_restart:
            self.logger.warning(f"Changes requiring restart: {', '.join(requires_restart)}")
            message_parts.append(f"Manual restart required for: {', '.join(requires_restart)}.")

        if not message_parts:
            return True, "Configuration saved. No effective changes made or all changes require a restart and were noted."
        
        final_message = "Configuration saved. " + " ".join(message_parts)
        return True, final_message.strip()


    def update_config_api(self) -> Response:
        """API endpoint to update the configuration."""
        with self.config_lock:
            new_data = request.get_json()
            if not new_data:
                return jsonify({"detail": "No data provided or not JSON."}), 400

            is_valid, validation_msg = self._validate_config_data(new_data)
            if not is_valid:
                self.logger.error(f"Invalid config data received: {validation_msg}")
                return jsonify({"detail": f"Invalid configuration data: {validation_msg}"}), 400

            backup_path = self.config_file_path + ".bak"
            current_config_in_memory = {
                "server_ip": self.server_ip,
                "server_port": self.server_port,
                "currency": self.currency,
                "refresh_interval_minutes": self.refresh_interval_minutes,
                "log_filename": self.log_filename,
                "database_name": self.database,
                "url_filters": self.url_filters.copy() # Ensure a copy
            }

            try:
                # 1. Backup current config file
                if os.path.exists(self.config_file_path):
                    shutil.copy2(self.config_file_path, backup_path)
                    self.logger.info(f"Created backup of config: {backup_path}")

                # 2. Write new config to file
                self.logger.info(f"DEBUG: Data to be written to config by _write_config: {json.dumps(new_data)}")
                self._write_config(new_data)
                try:
                    with open(self.config_file_path, 'r', encoding='utf-8') as f_check:
                        written_data_check = json.load(f_check)
                    self.logger.info(f"DEBUG: Data read back from config immediately after _write_config: {json.dumps(written_data_check)}")
                    if new_data != written_data_check:
                        self.logger.warning("DEBUG: Data in file does NOT match data intended to be written!")
                except Exception as read_check_err:
                    self.logger.error(f"DEBUG: Error reading back config for check: {read_check_err}")

                # 3. Attempt to apply changes dynamically
                # _reload_config_dynamically is responsible for updating the instance variables
                # from new_data and applying any dynamic changes (e.g., rescheduling jobs).
                # We should not call self.load_from_json() here, as that would pre-emptively
                # update the instance variables, causing _reload_config_dynamically to see
                # no difference between its current state and new_data for many fields.
                
                # The comments below were part of the thought process leading to this correction.
                # First, update the instance's view of what the config *should* be,
                # then try to apply. load_from_json updates self. variables.
                # self.load_from_json(self.config_file_path) # This updates self.server_ip etc. from the new file
                
                # Now, _reload_config_dynamically will use these newly loaded self. variables
                # to compare and apply.
                # However, _reload_config_dynamically takes new_config as an argument.
                # Let's pass the `new_data` directly to _reload_config_dynamically
                # and it will update the necessary self attributes.
                
                # The load_from_json above already updated the instance variables.
                # _reload_config_dynamically will now effectively re-apply some of these,
                # like rescheduling. This is okay.
                
                # Let's refine: _reload_config_dynamically should update the instance variables itself.
                # So, we don't call self.load_from_json() here before _reload_config_dynamically.
                # Instead, _reload_config_dynamically will update self.currency, self.url_filters, etc.

                success, reload_msg = self._reload_config_dynamically(new_data)

                if not success:
                    raise Exception(f"Failed to apply new configuration dynamically: {reload_msg}")

                # 4. If successful, remove backup
                if os.path.exists(backup_path):
                    os.remove(backup_path)
                self.logger.info("Configuration updated and applied successfully.")
                return jsonify({"message": reload_msg}), 200

            except Exception as e:
                self.logger.exception(f"Error updating configuration: {e}. Rolling back.")
                # Rollback: Restore from backup if it exists
                if os.path.exists(backup_path):
                    try:
                        shutil.move(backup_path, self.config_file_path) # move is atomic
                        self.logger.info(f"Rolled back configuration from {backup_path}")
                        # Reload the old (rolled-back) configuration into the application state
                        self.load_from_json(self.config_file_path)
                        # Re-apply old settings (e.g., reschedule job with old interval)
                        # This can be done by calling _reload_config_dynamically with the old config
                        # or simply re-calling setup_scheduler() which uses current self.refresh_interval_minutes
                        self.setup_scheduler() # Re-initializes scheduler with current (old) settings
                        self.logger.info("Re-applied previous configuration settings after rollback.")

                    except Exception as rb_err:
                        self.logger.error(f"CRITICAL: Failed to rollback configuration file: {rb_err}. Config may be inconsistent.")
                        return jsonify({"detail": f"Error saving configuration and failed to rollback: {e}. Manual check required."}), 500
                else:
                     # If backup didn't exist (e.g. first write failed), try to write original in-memory config back
                    try:
                        self._write_config(current_config_in_memory)
                        self.logger.info("Wrote original in-memory config back after update failure.")
                        self.load_from_json(self.config_file_path) # Reload this original state
                        self.setup_scheduler()
                    except Exception as write_back_err:
                        self.logger.error(f"CRITICAL: Failed to write original config back: {write_back_err}. Config may be corrupted.")
                        return jsonify({"detail": f"Error saving configuration, failed to write original back: {e}. Manual check required."}), 500


                return jsonify({"detail": f"Error saving configuration: {str(e)}. Configuration has been rolled back to previous state."}), 500


    # --- Main Application Logic ---

    def run(self, debug_opt: bool = False) -> None:
        """
        Starts the Flask application and the background scheduler.

        Args:
            debug_opt (bool): Run Flask in debug mode. Defaults to False.
        """
        self.logger.info(f"Starting Flask server on {self.server_ip}:{self.server_port}...")
        # Start scheduler before Flask app
        self.setup_scheduler()

        try:
            # Use waitress or gunicorn in production instead of Flask's development server
            if debug_opt:
                 self.logger.warning("Running Flask in DEBUG mode.")
                 # When using scheduler, typically disable Flask's reloader
                 self.app.run(host=self.server_ip, port=self.server_port, debug=True, use_reloader=False)
            else:
                 # Consider using a production-ready server like waitress
                 try:
                     from waitress import serve
                     self.logger.info("Running Flask with Waitress production server.")
                     serve(self.app, host=self.server_ip, port=self.server_port, threads=4) # Example with waitress
                 except ImportError:
                     self.logger.warning("Waitress not found. Falling back to Flask development server.")
                     self.logger.warning("Install waitress for a production-ready server: pip install waitress")
                     self.app.run(host=self.server_ip, port=self.server_port, debug=False, use_reloader=False)


        except KeyboardInterrupt:
            self.logger.info("KeyboardInterrupt received. Shutting down...")
        except SystemExit:
            self.logger.info("SystemExit received. Shutting down...")
        except Exception as e:
             self.logger.exception(f"Error running the application: {e}")
        finally:
            self.shutdown()


    def shutdown(self) -> None:
        """Gracefully shuts down the scheduler and Selenium driver."""
        self.logger.info("Initiating shutdown sequence...")
        if self.scheduler and self.scheduler.running:
            self.logger.info("Shutting down scheduler...")
            try:
                 # wait=False allows faster shutdown, True waits for running jobs
                 self.scheduler.shutdown(wait=False)
                 self.logger.info("Scheduler shut down.")
            except Exception as e:
                 self.logger.error(f"Error shutting down scheduler: {e}")
        else:
             self.logger.info("Scheduler not running or not initialized.")

        self.quit_selenium() # Ensure Selenium driver is closed
        self.logger.info("Shutdown sequence complete.")


def initialize_database(db_name: str, logger: logging.Logger) -> None:
    """Initializes the SQLite database and creates the necessary table if it doesn't exist."""
    conn = None
    try:
        logger.info(f"Initializing database: {db_name}")
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ad_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                ad_id TEXT UNIQUE NOT NULL, -- Hash of the ad URL, must be unique
                title TEXT NOT NULL,
                price TEXT NOT NULL,
                first_seen TEXT NOT NULL, -- ISO format datetime string (UTC)
                last_checked TEXT NOT NULL -- ISO format datetime string (UTC)
            )
        ''')
        # Optional: Add index for faster lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ad_id ON ad_changes (ad_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_last_checked ON ad_changes (last_checked)')
        conn.commit()
        logger.info(f"Database '{db_name}' initialized successfully.")
    except sqlite3.Error as e:
        logger.error(f"Error initializing database {db_name}: {e}")
        raise # Re-raise to prevent application start if DB init fails
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    # Basic logger setup for initialization phase before config is loaded
    init_logger = logging.getLogger('init')
    init_handler = logging.StreamHandler()
    init_formatter = logging.Formatter('%(levelname)s:%(asctime)s::%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    init_handler.setFormatter(init_formatter)
    init_logger.addHandler(init_handler)
    init_logger.setLevel(logging.INFO)

    config_file = os.getenv(CONFIG_FILE_ENV_VAR, DEFAULT_CONFIG_FILE)
    init_logger.info(f"Using configuration file: {config_file}")

    if not os.path.exists(config_file):
        init_logger.error(f"Error: Config file '{config_file}' not found!")
        exit(1) # Use non-zero exit code for errors

    monitor_instance = None
    try:
        # Initialize monitor (loads config, sets up detailed logging)
        monitor_instance = fbRssAdMonitor(json_file=config_file)

        # Initialize database using the name from the loaded config
        initialize_database(monitor_instance.database, monitor_instance.logger)

        # Run the monitor (starts scheduler and Flask app)
        # Set debug_opt=True for development/debugging Flask
        monitor_instance.run(debug_opt=False)

    except (FileNotFoundError, ValueError, sqlite3.Error) as e:
         init_logger.error(f"Initialization failed: {e}")
         # Ensure shutdown is called if monitor was partially initialized
         if monitor_instance:
              monitor_instance.shutdown()
         exit(1)
    except Exception as e:
        init_logger.exception(f"An unexpected error occurred during startup or runtime: {e}")
        if monitor_instance:
             monitor_instance.shutdown()
        exit(1)


# Example JSON structure for URL-specific filters (remains the same)
# {
#     "server_ip": "0.0.0.0",
#     "server_port": 5000,
#     "currency": "$",
#     "refresh_interval_minutes": 15,
#     "log_filename": "fb_monitor.log",
#     "database_name": "fb-rss-feed.db", # Example: Allow overriding DB name
#     "url_filters": {
#         "https://www.facebook.com/marketplace/category/search?query=some%20item&exact=false": {
#             "level1": ["keyword1", "keyword2"],
#             "level2": ["must_have_this"]
#         },
#         "https://www.facebook.com/marketplace/brisbane/search?query=another%20search": {
#             "level1": ["brisbane_only_keyword"]
#         }
#     }
# }