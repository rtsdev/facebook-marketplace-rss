import hashlib
import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from threading import Lock, RLock
from typing import Any, Dict, List, Optional, Tuple
import shutil
from urllib.parse import urlparse

import PyRSS2Gen
import requests
import tzlocal
from apscheduler.jobstores.base import ConflictingIdError, JobLookupError
from apscheduler.schedulers.background import BackgroundScheduler
from bs4 import BeautifulSoup
from dateutil import parser
from flask import Flask, Response, jsonify, request, render_template, make_response, abort
from selenium import webdriver
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.firefox import GeckoDriverManager

DEFAULT_DB_NAME = 'fb-rss-feed.db'
DEFAULT_LOG_LEVEL = 'INFO'
CONFIG_FILE_ENV_VAR = 'CONFIG_FILE'
DEFAULT_CONFIG_FILE = 'config.json'
STALE_AD_AGE_ENV_VAR = 'STALE_AD_AGE'
DEFAULT_STALE_AD_AGE = 14
LOG_LEVEL_ENV_VAR = 'LOG_LEVEL'
AD_DIV_SELECTOR = 'div.x78zum5.xdt5ytf.x1iyjqo2.xd4ddsz'
AD_LINK_TAG = "a[href*='/marketplace/item']"
AD_TITLE_SELECTOR_STYLE = '-webkit-line-clamp'
AD_PRICE_SELECTOR_DIR = 'auto'
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
        self.config_file_path: str = json_file
        self.config_lock: RLock = RLock()

        self.urls_to_monitor: List[str] = []
        self.url_filters: Dict[str, Dict[str, List[str]]] = {}
        self.database: str = DEFAULT_DB_NAME
        self.local_tz = tzlocal.get_localzone()
        self.log_filename: str = "fb_monitor.log"
        self.server_ip: str = "0.0.0.0"
        self.server_port: int = 5000
        self.currency: str = "$"
        self.refresh_interval_minutes: int = 15
        self.driver: Optional[webdriver.Firefox] = None
        self.scheduler: Optional[BackgroundScheduler] = None
        self.job_lock: Lock = Lock()

        self.logger = logging.getLogger(__name__)
        self.set_logger()

        try:
            original_log_filename = self.log_filename
            self.load_from_json(self.config_file_path)

            if self.log_filename != original_log_filename:
                self.logger.info(f"Log filename updated from '{original_log_filename}' to '{self.log_filename}'. Re-initializing logger file handler.")
                self.set_logger()
        except Exception as e:
            if self.logger and hasattr(self.logger, 'critical'):
                self.logger.critical(f"Fatal error during configuration loading: {e}", exc_info=True)
            else:
                print(f"CRITICAL FALLBACK: Fatal error during configuration loading and logger unavailable: {e}")
            raise

        self.app: Flask = Flask(__name__, template_folder='templates', static_folder='static')
        self._setup_routes()

        self.rss_feed: PyRSS2Gen.RSS2 = PyRSS2Gen.RSS2(
            title="Facebook Marketplace Ad Feed",
            link=f"http://{self.server_ip}:{self.server_port}/rss",
            description="An RSS feed to monitor new ads on Facebook Marketplace",
            lastBuildDate=datetime.now(timezone.utc),
            items=[]
        )


    def set_logger(self) -> None:
        """
        Sets up logging configuration with both file and console streaming.
        Log level is fetched from the environment variable LOG_LEVEL.
        """
        self.logger = logging.getLogger(__name__)
        log_formatter = logging.Formatter(
            '%(levelname)s:%(asctime)s:%(funcName)s:%(lineno)d::%(message)s',
            datefmt='%m/%d/%Y %I:%M:%S %p'
        )

        log_level_str = os.getenv(LOG_LEVEL_ENV_VAR, DEFAULT_LOG_LEVEL).upper()
        try:
            log_level = logging.getLevelName(log_level_str)
            if not isinstance(log_level, int):
                 logging.basicConfig(level=logging.WARNING)
                 logging.warning(f"Invalid LOG_LEVEL '{log_level_str}'. Defaulting to {DEFAULT_LOG_LEVEL}.")
                 log_level = logging.INFO
        except ValueError:
            logging.basicConfig(level=logging.WARNING)
            logging.warning(f"Invalid LOG_LEVEL '{log_level_str}'. Defaulting to {DEFAULT_LOG_LEVEL}.")
            log_level = logging.INFO


        try:
            file_handler = RotatingFileHandler(
                self.log_filename, mode='a', maxBytes=10*1024*1024,
                backupCount=2, encoding='utf-8', delay=False
            )
            file_handler.setFormatter(log_formatter)
            file_handler.setLevel(log_level)
            self.logger.addHandler(file_handler)
        except Exception as e:
            logging.basicConfig(level=logging.ERROR)
            logging.error(f"Failed to set up file logging handler for {self.log_filename}: {e}. Logging to console only.")

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        console_handler.setLevel(log_level)

        self.logger.setLevel(log_level)
        # self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.info(f"Logger initialized with level {logging.getLevelName(log_level)}")


    def init_selenium(self) -> None:
        """
        Initializes Selenium WebDriver with Firefox options.
        Ensures any existing driver is quit first.
        """
        self.quit_selenium()

        try:
            self.logger.debug("Initializing Selenium WebDriver...")
            firefox_options = FirefoxOptions()
            possible_paths = [
                "/opt/homebrew/bin/firefox",
                "/Applications/Firefox.app/Contents/MacOS/firefox",
                "/Applications/Firefox Developer Edition.app/Contents/MacOS/firefox",
                "/usr/bin/firefox",
                "/usr/lib/firefox/firefox",
                "/snap/bin/firefox",
                "/opt/firefox/firefox",
                "/usr/local/bin/firefox",
                "/app/firefox/firefox",
                "/firefox/firefox",
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
            firefox_options.add_argument("--no-sandbox")
            firefox_options.add_argument("--disable-dev-shm-usage")
            firefox_options.add_argument("--private")
            firefox_options.add_argument("--headless")
            firefox_options.set_preference("general.useragent.override", SELENIUM_USER_AGENT)
            firefox_options.set_preference("dom.webdriver.enabled", False)
            firefox_options.set_preference("useAutomationExtension", False)
            firefox_options.set_preference("privacy.resistFingerprinting", True)

            log_level_gecko = logging.WARNING if self.logger.level > logging.DEBUG else logging.DEBUG
            os.environ['WDM_LOG_LEVEL'] = str(log_level_gecko)
            os.environ['WDM_LOCAL'] = '1'

            gecko_driver_path = GeckoDriverManager().install()

            service_log_path = 'nul' if os.name == 'nt' else '/dev/null'
            self.driver = webdriver.Firefox(
                service=FirefoxService(gecko_driver_path, log_path=service_log_path),
                options=firefox_options
            )
            self.logger.debug("Selenium WebDriver initialized successfully.")

        except WebDriverException as e:
            self.logger.error(f"WebDriverException during Selenium initialization: {e}")
            self.driver = None
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error initializing Selenium: {e}")
            self.driver = None
            raise


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
                self.driver = None


    def setup_scheduler(self) -> None:
        """
        Sets up the background job scheduler to check for new ads.
        """
        if self.scheduler and self.scheduler.running:
             self.logger.warning("Scheduler is already running.")
             return

        self.logger.info(f"Setting up scheduler to run every {self.refresh_interval_minutes} minutes.")
        job_id = 'check_ads_job'

        if self.scheduler is None:
            self.scheduler = BackgroundScheduler(timezone=str(self.local_tz))

        try:
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
            self.logger.warning(f"Job '{job_id}' conflict. Attempting to reschedule.")
            self.scheduler.reschedule_job(job_id, trigger='interval', minutes=self.refresh_interval_minutes)
            if not self.scheduler.running:
                self.scheduler.start(paused=False)
            self.logger.info(f"Scheduler resumed/rescheduled job '{job_id}'.")
        except Exception as e:
             self.logger.error(f"Failed to setup or start scheduler: {e}")


    def local_time(self, dt: datetime) -> datetime:
        """Converts a UTC datetime object to local time."""
        if dt.tzinfo is None:
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

            self.server_ip = data.get('server_ip', self.server_ip)
            self.server_port = data.get('server_port', self.server_port)
            self.currency = data.get('currency', self.currency)
            self.refresh_interval_minutes = data.get('refresh_interval_minutes', self.refresh_interval_minutes)
            self.log_filename = data.get('log_filename', self.log_filename)
            self.database = data.get('database_name', self.database)

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
            self.logger.exception(f"Unexpected error loading configuration from {json_file}: {e}")
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

        if not filters:
            self.logger.debug(f"No specific keyword filters defined for URL '{url}' (filters: {filters}). Ad '{title}' passes.")
            return True

        if not isinstance(filters, dict):
             self.logger.warning(f"Filters for URL '{url}' are not a dictionary (type: {type(filters)}). Skipping filters.")
             return True

        try:

            title_lower = title.lower()

            exclude_keywords = filters.get('exclude', [])
            if any(keyword.lower() in title_lower for keyword in exclude_keywords):
                self.logger.debug(f"Ad '{title}' for URL '{url}' contains excluded keywords. Keywords: {exclude_keywords}")
                return False

            level_keys = sorted(
                [k for k in filters.keys() if k.startswith('level') and k[5:].isdigit()],
                key=lambda x: int(x[5:])
            )

            if not level_keys:
                 self.logger.debug(f"No valid 'levelX' keys found in filters for URL '{url}'. Ad '{title}' passes.")
                 return True

            for level in level_keys:
                keywords = filters.get(level, [])
                if not isinstance(keywords, list):
                     self.logger.warning(f"Keywords for level '{level}' in URL '{url}' are not a list. Skipping level.")
                     continue

                if not keywords:
                     self.logger.debug(f"No keywords defined for level '{level}' in URL '{url}'. Skipping level.")
                     continue

                if not any(keyword.lower() in title_lower for keyword in keywords):
                    self.logger.debug(f"Ad '{title}' failed filter level '{level}' for URL '{url}'. Keywords: {keywords}")
                    return False

            self.logger.debug(f"Ad '{title}' passed all filter levels for URL '{url}'.")
            return True

        except Exception as e:
            self.logger.exception(f"Error applying filters for URL '{url}', title '{title}': {e}")
            return False


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

                try:

                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, AD_DIV_SELECTOR))
                    )
                except TimeoutException:

                    try:
                        WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='main'], div[data-pagelet='MarketplacePDP'], main"))
                        )
                        self.logger.warning(f"Using fallback selector for {url} - AD_DIV_SELECTOR may be outdated")
                    except TimeoutException:

                        WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.TAG_NAME, "body"))
                        )
                        self.logger.warning(f"Using body tag fallback for {url} - page structure may have changed")

                WebDriverWait(self.driver, 5).until(
                    lambda driver: driver.execute_script("return document.readyState") == "complete"
                )
                
                self.logger.debug(f"Page content loaded successfully for {url}")
                return self.driver.page_source
                
            except WebDriverException as e:
                if attempt == max_retries:
                    self.logger.error(f"Selenium error fetching page content for {url} after {max_retries + 1} attempts: {e}")

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
                    time.sleep(2)
                    
            except Exception as e:
                if attempt == max_retries:
                    self.logger.exception(f"Unexpected error fetching page content for {url} after {max_retries + 1} attempts: {e}")
                    return None
                else:
                    self.logger.warning(f"Unexpected error on attempt {attempt + 1} for {url}: {e}. Retrying...")
                    time.sleep(2)


    def get_ads_hash(self, content: str) -> str:
        """
        Generates an MD5 hash for the given content (typically a URL).

        Args:
            content (str): The content to hash.

        Returns:
            str: The MD5 hash of the content.
        """
        return hashlib.md5(content.encode('utf-8')).hexdigest()


    def extract_ad_details(self, content: str, source_url: str) -> List[Tuple[str, str, str, str, str, str]]:
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
        ads_found: List[Tuple[str, str, str, str, str, str]] = []
        try:
            soup = BeautifulSoup(content, 'html.parser')

            ad_links = soup.select(AD_LINK_TAG)
            self.logger.debug(f"Found {len(ad_links)} potential ad links on {source_url}.")

            processed_urls = set()

            for ad_link in ad_links:
                href = ad_link.get('href', '')

                full_url = f"{FACEBOOK_BASE_URL}{href.split('?')[0]}"

                if full_url in processed_urls:
                     continue
                processed_urls.add(full_url)


                title = None
                price = None
                location = None
                image_url = None

                spans = ad_link.select('span')

                for span in spans:
                    span_text = span.get_text(strip=True)

                    if AD_TITLE_SELECTOR_STYLE in span.attrs.get('style', ''):
                        title = span_text
                    elif span.attrs.get('dir', '') == AD_PRICE_SELECTOR_DIR :
                        if span_text.startswith(self.currency) or 'free' in span_text.lower():
                            price = span_text
                    elif re.match(r"^[A-Za-z .'-]+,\s?[A-Z]{2}$", span.get_text(strip=True)):
                        location = span.get_text(strip=True)
                    else:
                        pass

                image_el = ad_link.find('img')
                if image_el:
                    image_url = image_el.get('src')

                if not title or not price:
                    all_spans = ad_link.find_all('span')
                    for span in all_spans:
                        span_text = span.get_text(strip=True)
                        if span_text and len(span_text) > 10 and not title:
                            title = span_text
                        elif span_text and (span_text.startswith(self.currency) or 'free' in span_text.lower()) and not price:
                            price = span_text

                if not title:
                    link_text = ad_link.get_text(strip=True)
                    if link_text and len(link_text) > 10:
                        title = link_text
                
                if title and price:
                    if price.startswith(self.currency) or 'free' in price.lower():
                        ad_id_hash = self.get_ads_hash(full_url)

                        if self.apply_filters(source_url, title):
                            ads_found.append((ad_id_hash, title, price, location, image_url, full_url))

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
                return

            cursor = conn.cursor()
            added_ad_ids_this_run = set()

            for url in self.urls_to_monitor:
                processed_urls_count += 1
                self.logger.info(f"Processing URL ({processed_urls_count}/{len(self.urls_to_monitor)}): {url}")
                try:
                    self.init_selenium()
                    if not self.driver:
                         self.logger.warning(f"Skipping URL {url} due to Selenium initialization failure.")
                         continue

                    content = self.get_page_content(url)
                    self.quit_selenium()

                    if content is None:
                        self.logger.warning(f"No content received for URL: {url}. Skipping.")
                        continue

                    ads = self.extract_ad_details(content, url)
                    if not ads:
                         self.logger.info(f"No matching ads found or extracted for URL: {url}.")
                         continue


                    for ad_id, title, price, location, img_url, ad_url in ads:
                        if ad_id in added_ad_ids_this_run:
                             self.logger.debug(f"Ad '{title}' ({ad_id}) already processed in this run. Skipping.")
                             continue

                        cursor.execute('SELECT ad_id FROM ad_changes WHERE ad_id = ?', (ad_id,))
                        existing_ad = cursor.fetchone()
                        now_utc = datetime.now(timezone.utc)
                        now_iso = now_utc.isoformat()

                        if existing_ad is None:
                            self.logger.info(f"New ad detected: '{title}' ({price}) - {ad_url}")
                            description = f"""
                            Title: {title}
                            Price: {price}
                            Location: {location}
                            """
                            new_item = PyRSS2Gen.RSSItem(
                                title=f"{title} - {price} - {location}",
                                link=ad_url,
                                description=description,
                                enclosure=PyRSS2Gen.Enclosure(img_url, 0, "image/jpeg"),
                                guid=PyRSS2Gen.Guid(ad_id, isPermaLink=False),
                                pubDate=self.local_time(now_utc)
                            )
                            try:
                                cursor.execute(
                                    'INSERT INTO ad_changes (url, ad_id, title, price, location, img_url, first_seen, last_checked) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                                    (ad_url, ad_id, title, price, location, img_url, now_iso, now_iso)
                                )
                                conn.commit()
                                self.rss_feed.items.insert(0, new_item)
                                added_ad_ids_this_run.add(ad_id)
                                new_ads_added_count += 1
                                self.logger.debug(f"Successfully added new ad '{title}' to DB and RSS feed.")
                            except sqlite3.IntegrityError:
                                self.logger.warning(f"IntegrityError inserting ad '{title}' ({ad_id}). Might be a race condition. Updating last_checked.")
                                cursor.execute('UPDATE ad_changes SET last_checked = ? WHERE ad_id = ?',
                                               (now_iso, ad_id))
                                conn.commit()
                            except sqlite3.Error as db_err:
                                 self.logger.error(f"Database error processing ad '{title}' ({ad_id}): {db_err}")
                                 conn.rollback()

                        else:
                             self.logger.debug(f"Existing ad found: '{title}' ({ad_id}). Updating last_checked.")
                             cursor.execute('UPDATE ad_changes SET last_checked = ? WHERE ad_id = ?',
                                            (now_iso, ad_id))
                             conn.commit()


                except Exception as url_proc_err:
                     self.logger.exception(f"Error processing URL {url}: {url_proc_err}")
                     self.quit_selenium()
                finally:
                     time.sleep(2)

            self.prune_old_ads(conn, os.getenv(STALE_AD_AGE_ENV_VAR, DEFAULT_STALE_AD_AGE))

            self.logger.info(f"Finished ad check. Added {new_ads_added_count} new ads.")

        except sqlite3.DatabaseError as e:
            self.logger.error(f"Database error during ad check: {e}")
            if conn:
                 conn.rollback()
        except Exception as e:
            self.logger.exception(f"Unexpected error during ad check: {e}")
        finally:
            self.quit_selenium()
            if conn:
                conn.close()
                self.logger.debug("Database connection closed.")
            self.job_lock.release()
            self.logger.debug("Ad check job lock released.")


    def prune_old_ads(self, conn: sqlite3.Connection, days_to_keep: int = DEFAULT_STALE_AD_AGE) -> None:
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
                 return

            cursor = conn.cursor()
            relevant_period_start = datetime.now(timezone.utc) - timedelta(days=7)

            cursor.execute('''
                SELECT ad_id, title, price, location, img_url, url, last_checked
                FROM ad_changes
                WHERE last_checked >= ?
                ORDER BY last_checked DESC
                LIMIT 100
            ''', (relevant_period_start.isoformat(),))
            changes = cursor.fetchall()
            self.logger.debug(f"Fetched {len(changes)} ad changes from DB for RSS feed.")

            for change in changes:
                try:
                    last_checked_dt_utc = parser.isoparse(change['last_checked'])
                    pub_date_local = self.local_time(last_checked_dt_utc)

                    description = f"""
                    Title: {change['title']}
                    Price: {change['price']}
                    Location: {change['location']}
                    """

                    new_item = PyRSS2Gen.RSSItem(
                        title=f"{change['title']} - {change['price']} - {change['location']}",
                        link=change['url'],
                        description=description,
                        enclosure=PyRSS2Gen.Enclosure(change['img_url'], 0, "image/jpeg"),
                        guid=PyRSS2Gen.Guid(change['ad_id'], isPermaLink=False),
                        pubDate=pub_date_local
                    )
                    new_items.append(new_item)
                except (ValueError, TypeError) as e:
                    self.logger.error(f"Error processing ad change for RSS (ID: {change['ad_id']}): {e}. Skipping item.")
                except Exception as item_err:
                     self.logger.exception(f"Unexpected error creating RSS item for ad (ID: {change['ad_id']}): {item_err}. Skipping item.")

            self.rss_feed.items = new_items
            self.rss_feed.lastBuildDate = datetime.now(timezone.utc)
            self.logger.info(f"RSS feed updated with {len(new_items)} items from database.")

        except sqlite3.DatabaseError as e:
            self.logger.error(f"Database error generating RSS feed: {e}")
            self.rss_feed.items = []
        except Exception as e:
            self.logger.exception(f"Unexpected error generating RSS feed: {e}")
            self.rss_feed.items = []
        finally:
            if conn:
                conn.close()
                self.logger.debug("Database connection closed after RSS generation.")


    def rss(self) -> Response:
        """
        Flask endpoint handler. Generates and returns the RSS feed XML.
        """
        self.logger.info("RSS feed requested. Generating fresh feed from database.")
        self.generate_rss_feed_from_db()

        try:
            rss_xml = self.rss_feed.to_xml(encoding='utf-8')
            return Response(rss_xml, mimetype='application/rss+xml')
        except Exception as e:
             self.logger.exception(f"Error converting RSS feed to XML: {e}")
             return Response("Error generating RSS feed.", status=500, mimetype='text/plain')

    def _setup_routes(self) -> None:
        """Sets up Flask routes."""
        self.app.add_url_rule('/rss', 'rss', self.rss)
        self.app.add_url_rule('/edit', 'edit_config_page', self.edit_config_page, methods=['GET'])
        self.app.add_url_rule('/api/config', 'get_config_api', self.get_config_api, methods=['GET'])
        self.app.add_url_rule('/api/config', 'update_config_api', self.update_config_api, methods=['POST'])
        self.app.add_url_rule('/api/img-proxy', 'img-proxy', self.img_proxy, methods=['GET'])

    def img_proxy(self):
        try:
            img_url = request.args.get("url")
            response = requests.get(img_url, stream=True)
            response.raise_for_status()

            flask_response = make_response(response.content)
            flask_response.headers['Content-Type'] = response.headers['Content-Type']
            flask_response.headers['Content-Length'] = response.headers['Content-Length']
            return flask_response

        except requests.RequestException as e:
            self.logger.error(f"Error generating img proxy request: {e}")
            abort(404)


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
        requires_restart = []

        new_currency = new_config.get("currency", self.currency)
        if self.currency != new_currency:
            self.currency = new_currency
            applied_dynamically.append("Currency")

        new_url_filters = new_config.get("url_filters", self.url_filters)
        if self.url_filters != new_url_filters:
            self.url_filters = new_url_filters
            self.urls_to_monitor = list(self.url_filters.keys())
            applied_dynamically.append("URL filters")
            if not self.urls_to_monitor:
                self.logger.warning("No URLs to monitor after config update. Monitoring will be inactive.")

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
                    self.refresh_interval_minutes = old_interval
                    return False, f"Failed to apply new refresh interval ({new_interval} min): Scheduler error. Interval reverted to {old_interval} min."
            else:
                self.setup_scheduler()
                applied_dynamically.append("Refresh interval (scheduler re-initialized/will use on next start)")

        new_server_ip = new_config.get("server_ip", self.server_ip)
        new_server_port = new_config.get("server_port", self.server_port)
        if self.server_ip != new_server_ip or self.server_port != new_server_port:
            self.server_ip = new_server_ip
            self.server_port = new_server_port
            self.rss_feed.link = f"http://{self.server_ip}:{self.server_port}/rss"
            requires_restart.append("Server IP/Port")

        new_log_filename = new_config.get("log_filename", self.log_filename)
        if self.log_filename != new_log_filename:
            requires_restart.append("Log filename")

        new_database_name = new_config.get("database_name", self.database)
        if self.database != new_database_name:
            requires_restart.append("Database name")

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
                "url_filters": self.url_filters.copy()
            }

            try:
                if os.path.exists(self.config_file_path):
                    shutil.copy2(self.config_file_path, backup_path)
                    self.logger.info(f"Created backup of config: {backup_path}")

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

                success, reload_msg = self._reload_config_dynamically(new_data)

                if not success:
                    raise Exception(f"Failed to apply new configuration dynamically: {reload_msg}")

                if os.path.exists(backup_path):
                    os.remove(backup_path)
                self.logger.info("Configuration updated and applied successfully.")
                return jsonify({"message": reload_msg}), 200

            except Exception as e:
                self.logger.exception(f"Error updating configuration: {e}. Rolling back.")
                if os.path.exists(backup_path):
                    try:
                        shutil.move(backup_path, self.config_file_path)
                        self.logger.info(f"Rolled back configuration from {backup_path}")
                        self.load_from_json(self.config_file_path)
                        self.setup_scheduler()
                        self.logger.info("Re-applied previous configuration settings after rollback.")

                    except Exception as rb_err:
                        self.logger.error(f"CRITICAL: Failed to rollback configuration file: {rb_err}. Config may be inconsistent.")
                        return jsonify({"detail": f"Error saving configuration and failed to rollback: {e}. Manual check required."}), 500
                else:
                    try:
                        self._write_config(current_config_in_memory)
                        self.logger.info("Wrote original in-memory config back after update failure.")
                        self.load_from_json(self.config_file_path)
                        self.setup_scheduler()
                    except Exception as write_back_err:
                        self.logger.error(f"CRITICAL: Failed to write original config back: {write_back_err}. Config may be corrupted.")
                        return jsonify({"detail": f"Error saving configuration, failed to write original back: {e}. Manual check required."}), 500


                return jsonify({"detail": f"Error saving configuration: {str(e)}. Configuration has been rolled back to previous state."}), 500


    def run(self, debug_opt: bool = False) -> None:
        """
        Starts the Flask application and the background scheduler.

        Args:
            debug_opt (bool): Run Flask in debug mode. Defaults to False.
        """
        self.logger.info(f"Starting Flask server on {self.server_ip}:{self.server_port}...")
        self.setup_scheduler()

        try:
            if debug_opt:
                 self.logger.warning("Running Flask in DEBUG mode.")
                 self.app.run(host=self.server_ip, port=self.server_port, debug=True, use_reloader=False)
            else:
                 try:
                     from waitress import serve
                     self.logger.info("Running Flask with Waitress production server.")
                     serve(self.app, host=self.server_ip, port=self.server_port, threads=4)
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
                 self.scheduler.shutdown(wait=False)
                 self.logger.info("Scheduler shut down.")
            except Exception as e:
                 self.logger.error(f"Error shutting down scheduler: {e}")
        else:
             self.logger.info("Scheduler not running or not initialized.")

        self.quit_selenium()
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
                ad_id TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                price TEXT NOT NULL,
                location TEXT NOT NULL,
                img_url TEXT NOT NULL,
                first_seen TEXT NOT NULL,
                last_checked TEXT NOT NULL
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ad_id ON ad_changes (ad_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_last_checked ON ad_changes (last_checked)')
        conn.commit()
        logger.info(f"Database '{db_name}' initialized successfully.")
    except sqlite3.Error as e:
        logger.error(f"Error initializing database {db_name}: {e}")
        raise
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
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
        exit(1)

    monitor_instance = None
    try:
        monitor_instance = fbRssAdMonitor(json_file=config_file)
        initialize_database(monitor_instance.database, monitor_instance.logger)
        monitor_instance.run(debug_opt=False)

    except (FileNotFoundError, ValueError, sqlite3.Error) as e:
         init_logger.error(f"Initialization failed: {e}")
         if monitor_instance:
              monitor_instance.shutdown()
         exit(1)
    except Exception as e:
        init_logger.exception(f"An unexpected error occurred during startup or runtime: {e}")
        if monitor_instance:
             monitor_instance.shutdown()
        exit(1)
