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
from dateutil import parser
from flask import Flask, Response, jsonify, request, render_template, make_response, abort
from playwright.sync_api import sync_playwright, Browser


LOG_LEVEL_ENV_VAR = 'LOG_LEVEL'
DEFAULT_LOG_LEVEL = 'INFO'
CONFIG_DIR = f"{os.getcwd()}/config"
REFRESH_INTERVAL_ENV_VAR = 'REFRESH_INTERVAL'
DEFAULT_REFRESH_INTERVAL = 15
STALE_AD_AGE_ENV_VAR = 'STALE_AD_AGE'
DEFAULT_STALE_AD_AGE = 14
RSS_EXTERNAL_DOMAIN_ENV_VAR = 'RSS_EXTERNAL_DOMAIN'
DEFAULT_RSS_EXTERNAL_DOMAIN = 'http://localhost:5000'
AD_DIV_SELECTOR = 'div.x78zum5.xdt5ytf.x1iyjqo2.xd4ddsz'
AD_LINK_TAG = "a[href*='/marketplace/item']"
AD_TITLE_SELECTOR_STYLE = '-webkit-line-clamp'
AD_PRICE_SELECTOR_DIR = 'auto'
SELENIUM_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0"


class fbRssAdMonitor:
    """Monitors Facebook Marketplace search URLs for new ads and generates an RSS feed."""

    def __init__(self):
        """
        Initializes the fbRssAdMonitor instance.
        """
        self.config_file_path: str = f"{CONFIG_DIR}/config.json"
        self.config_lock: RLock = RLock()

        self.urls_to_monitor: List[str] = []
        self.url_filters: Dict[str, Dict[str, List[str]]] = {}
        self.database: str = f"{CONFIG_DIR}/fbm-rss-feed.db"
        self.local_tz = tzlocal.get_localzone()
        self.log_filename: str = f"{CONFIG_DIR}/fbm-rss-feed.log"
        self.server_ip: str = "0.0.0.0"
        self.rss_external_domain: str = os.getenv(RSS_EXTERNAL_DOMAIN_ENV_VAR, DEFAULT_RSS_EXTERNAL_DOMAIN)
        self.currency: str = "$"
        self.refresh_interval: int = int(os.getenv(REFRESH_INTERVAL_ENV_VAR, DEFAULT_REFRESH_INTERVAL))
        self.stale_ad_age: int = int(os.getenv(STALE_AD_AGE_ENV_VAR, DEFAULT_STALE_AD_AGE))
        self.browser: Optional[Browser] = None
        self.scheduler: Optional[BackgroundScheduler] = None
        self.job_lock: Lock = Lock()

        self.logger = logging.getLogger(__name__)
        self.set_logger()

        self.load_from_json(self.config_file_path)

        self.app: Flask = Flask(__name__, template_folder='templates', static_folder='static')
        self._setup_routes()

        self.rss_feed: PyRSS2Gen.RSS2 = PyRSS2Gen.RSS2(
            title="Facebook Marketplace Ad Feed",
            link=f"{self.rss_external_domain}/rss",
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
        self.logger.addHandler(console_handler)
        self.logger.info(f"Logger initialized with level {logging.getLevelName(log_level)}")


    def init_playwright(self) -> None:
        """
        Initializes Playwright Browser.
        """
        with sync_playwright() as p:
            self.browser = p.chromium.launch(headless=True)


    def close_browser(self) -> None:
        """Safely quits the Playwright browser if it exists."""
        if self.browser:
            self.logger.debug("Quitting Playwright browser...")
            try:
                self.browser.close()
                self.logger.debug("Playwright browser quit successfully.")
            except Exception as ex:
                self.logger.error(f"Error quitting Playwright browser: {ex}")
            finally:
                self.browser = None


    def setup_scheduler(self) -> None:
        """
        Sets up the background job scheduler to check for new ads.
        """
        if self.scheduler and self.scheduler.running:
             self.logger.warning("Scheduler is already running.")
             return

        self.logger.info(f"Setting up scheduler to run every {self.refresh_interval} minutes.")
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
                minutes=self.refresh_interval,
                misfire_grace_time=60,
                coalesce=True,
                next_run_time=datetime.now(self.local_tz) + timedelta(seconds=5) # Start soon
            )
            if not self.scheduler.running:
                self.scheduler.start()
            self.logger.info(f"Scheduler configured with job '{job_id}' to run every {self.refresh_interval} minutes.")

        except ConflictingIdError:
            self.logger.warning(f"Job '{job_id}' conflict. Attempting to reschedule.")
            self.scheduler.reschedule_job(job_id, trigger='interval', minutes=self.refresh_interval)
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

            url_filters_raw = data.get('url_filters', {})
            if not isinstance(url_filters_raw, dict):
                 raise ValueError("'url_filters' must be a dictionary in the config file.")

            self.url_filters = url_filters_raw
            self.urls_to_monitor = list(self.url_filters.keys())

            if not self.urls_to_monitor:
                 self.logger.warning("No URLs found in 'url_filters'. Monitoring will be inactive.")

            self.logger.info("Configuration loaded successfully.")
            self.logger.debug(f"Monitoring URLs: {self.urls_to_monitor}")


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


    def get_page_content(self, url: str) -> list[dict]:
        """
        Fetches the page content using Playwright with retry mechanism.

        Args:
            url (str): The URL of the page to fetch.
            max_retries (int): Maximum number of retry attempts.

        Returns:
            Optional[str]: The HTML content of the page, or None if an error occurred.
        """

        listings = []
        context = self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()

        self.logger.info(f"Requesting URL: {url}")
        page.goto(url, wait_until='networkidle')
        page.wait_for_timeout(5000)

        page.keyboard.press('Escape')

        # Scroll down to load more listings
        self.logger.info("Scrolling to load all listings...")
        max_scrolls = 50
        previous_count = 0
        no_change_count = 0
        max_no_change = 5  # Stop after 5 scrolls with no new listings
        scroll_count = 0

        listing_links = page.locator('a[href*="/marketplace/item/"]')
        listing_links.first.wait_for(timeout=10000)

        while no_change_count < max_no_change and scroll_count < max_scrolls:
            current_count = listing_links.count()

            if current_count > previous_count:
                new_items = current_count - previous_count
                self.logger.info(f"Scroll {scroll_count + 1}: Found {current_count} listings (+{new_items} new)")
                previous_count = current_count
                no_change_count = 0
            else:
                no_change_count += 1
                self.logger.info(f"Scroll {scroll_count + 1}: No new listings ({no_change_count}/{max_no_change})")

            page.keyboard.press('PageDown')
            page.keyboard.press('PageDown')
            page.keyboard.press('PageDown')
            page.wait_for_timeout(5000)
            time.sleep(5)
            scroll_count += 1

        # Final count
        listing_links = page.locator('a[href*="/marketplace/item/"]')
        count = listing_links.count()
        self.logger.info(f"\nTotal listings found after scrolling: {count}")
        self.logger.info("Extracting listing details...\n")

        listings = self._extract_listing_data(page, listing_links, count)

        context.close()

        return listings


    def _extract_listing_data(self, page, listing_links, count: int) -> List[Dict]:
        """
        Helper function to extract data from listing elements

        Args:
            page: Playwright page object
            listing_links: Locator for listing links
            count: Number of listings

        Returns:
            List of listing dictionaries
        """
        listings = []

        for i in range(count):
            try:
                listing = listing_links.nth(i)

                # Extract the URL
                href = listing.get_attribute('href')

                # Extract price
                price_elem = listing.locator('span:has-text("$")').first
                price = price_elem.inner_text() if price_elem.count() > 0 else "N/A"

                # Extract title - look for the longer text that's not the price or location
                # It's in a span with specific classes that shows 2 lines
                title_elem = listing.locator('span[style*="-webkit-line-clamp: 2"]')
                title = title_elem.inner_text() if title_elem.count() > 0 else None

                # If title not found, try to get it from image alt text
                if not title or title == "N/A":
                    img = listing.locator('img').first
                    if img.count() > 0:
                        image_alt = img.get_attribute('alt')
                        if image_alt:
                            # Extract title from alt text (format: "Title in Location")
                            title = image_alt.split(' in ')[0] if ' in ' in image_alt else image_alt

                title = title if title else "N/A"

                # Extract location - usually has comma in it and comes after title
                location_elems = listing.locator('span:has-text(",")').all()
                location = "N/A"
                for loc_elem in location_elems:
                    text = loc_elem.inner_text()
                    # Skip if it's the price or doesn't look like a location
                    if '$' not in text and len(text) < 50:
                        location = text
                        break

                # Extract image URL
                img = listing.locator('img').first
                image_url = img.get_attribute('src') if img.count() > 0 else "N/A"
                image_alt = img.get_attribute('alt') if img.count() > 0 else "N/A"

                # Extract listing ID from the URL
                listing_id = "N/A"
                if href and '/marketplace/item/' in href:
                    parts = href.split('/marketplace/item/')
                    if len(parts) > 1:
                        listing_id = parts[1].split('/')[0].split('?')[0]

                listing_data = {
                    'id': listing_id,
                    'title': title.strip(),
                    'price': price.strip(),
                    'location': location.strip(),
                    'url': f"https://www.facebook.com{href}" if href and not href.startswith(
                        'http') else href if href else "N/A",
                    'image_url': image_url,
                    'image_alt': image_alt
                }

                listings.append(listing_data)

                # Print progress every 10 items
                if (i + 1) % 10 == 0 or i == 0:
                    self.logger.info(f"Extracted {i + 1}/{count}: {listing_data['title'][:50]}... - {listing_data['price']}")

            except Exception as ex:
                self.logger.info(f"Error extracting listing {i}: {str(ex)}")
                continue

        return listings


    def get_ads_hash(self, content: str) -> str:
        """
        Generates an MD5 hash for the given content (typically a URL).

        Args:
            content (str): The content to hash.

        Returns:
            str: The MD5 hash of the content.
        """
        return hashlib.md5(content.encode('utf-8')).hexdigest()


    def extract_ad_details(self, listings: list[dict], source_url: str) -> List[Tuple[str, str, str, str, str, str]]:
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
            processed_urls = set()

            for listing in listings:

                full_url = listing.get('url').split('?')[0]

                if full_url in processed_urls:
                     continue
                processed_urls.add(full_url)

                ad_id_hash = self.get_ads_hash(full_url)

                if self.apply_filters(source_url, listing.get('title')):
                    ads_found.append((ad_id_hash, listing.get('title'), listing.get('price'), listing.get('location'), listing.get('image_url').split('?')[0], full_url))

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
                    with sync_playwright() as p:
                        self.browser = p.chromium.launch(headless=True)
                        if not self.browser:
                            self.logger.warning(f"Skipping URL {url} due to Playwright initialization failure.")
                            continue

                        listings = self.get_page_content(url)

                        self.browser.close()

                    if listings is None:
                        self.logger.warning(f"No content received for URL: {url}. Skipping.")
                        continue

                    ads = self.extract_ad_details(listings, url)
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
                            <br>
                            Price: {price}
                            <br>
                            Location: {location}
                            """
                            new_item = PyRSS2Gen.RSSItem(
                                title=f"{title} | {price} | {location}",
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
                                self.logger.warning(
                                    f"IntegrityError inserting ad '{title}' ({ad_id}). Might be a race condition. Updating last_checked.")
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
                finally:
                     time.sleep(2)

            self.prune_old_ads(conn, self.stale_ad_age)

            self.logger.info(f"Finished ad check. Added {new_ads_added_count} new ads.")

        except sqlite3.DatabaseError as e:
            self.logger.error(f"Database error during ad check: {e}")
            if conn:
                 conn.rollback()
        except Exception as e:
            self.logger.exception(f"Unexpected error during ad check: {e}")
        finally:
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
                    <br>
                    Price: {change['price']}
                    <br>
                    Location: {change['location']}
                    """

                    new_item = PyRSS2Gen.RSSItem(
                        title=f"{change['title']} | {change['price']} | {change['location']}",
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
            "url_filters": dict
        }
        for key, expected_type in required_keys.items():
            if key not in data:
                return False, f"Missing required key: {key}"
            if not isinstance(data[key], expected_type):
                return False, f"Invalid type for {key}. Expected {expected_type.__name__}, got {type(data[key]).__name__}"

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

        new_url_filters = new_config.get("url_filters", self.url_filters)
        if self.url_filters != new_url_filters:
            self.url_filters = new_url_filters
            self.urls_to_monitor = list(self.url_filters.keys())
            applied_dynamically.append("URL filters")
            if not self.urls_to_monitor:
                self.logger.warning("No URLs to monitor after config update. Monitoring will be inactive.")

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
        self.logger.info(f"Starting Flask server...")
        self.setup_scheduler()

        try:
            if debug_opt:
                 self.logger.warning("Running Flask in DEBUG mode.")

            self.app.run(host=self.server_ip, debug=debug_opt, use_reloader=False)


        except KeyboardInterrupt:
            self.logger.info("KeyboardInterrupt received. Shutting down...")
        except SystemExit:
            self.logger.info("SystemExit received. Shutting down...")
        except Exception as ex:
             self.logger.exception(f"Error running the application: {ex}")
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

    monitor_instance = None
    try:
        monitor_instance = fbRssAdMonitor()
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
