# -*- coding: utf-8 -*-
"""
Advanced Telegram Proxy Scraper
===============================

Author: Unavailable User (Conceptual Request)
Developed by: Gemini
Date: 2025-07-12
Version: 1.2.0

Project Overview:
-----------------
This is a specialized version of the multi-stage data pipeline, redesigned
and optimized specifically for discovering, collecting, and organizing
Telegram Proxies (MTProto/SOCKS5).

Its behavior is fully customizable through an external `preferences.json` file,
allowing for fine-grained control over its operations without modifying the
source code. This version refactors the final stage into a smart archival
system that intelligently appends and deduplicates proxies based on their source.

Core Features:
---------------
- **Modular, Multi-Stage Pipeline:** The script is broken down into logical,
  independent stages for clarity and maintainability.
- **Intelligent Scraping & Self-Improvement:** Automatically discovers and
  validates new Telegram channels that share proxies, adding them to the
  scraping list.
- **Stale Source Pruning:** Automatically archives channels that have become
  inactive for a configurable period.
- **Subscription Mining:** Treats URLs found in posts as potential subscription
  links, fetches their content, and extracts any Telegram proxies within.
- **Advanced Proxy Archiving:** Intelligently merges and deduplicates all found
  proxies into source-specific JSON files (`archive_channel_proxies.json` and
  `archive_subscription_proxies.json`), creating a clean, historical database.
- **External Configuration:** All operational parameters are controlled via a
  user-friendly `preferences.json` file.
- **Advanced Colorized Logging:** Provides a clean, professional console output
  with color-coded log levels and informative progress bars.

Pipeline Stages:
----------------
1.  **CLEANUP:** (Optional) Cleans output directories based on a timed interval.
2.  **SCRAPE:** (Optional) Scrapes new messages from public Telegram channels.
3.  **PRUNE:** (Optional) Automatically removes inactive channels.
4.  **EXTRACT:** Processes scraped messages to extract proxies, URLs, etc.
5.  **VALIDATE:** (Optional) Validates new channels based on whether they provide proxies.
6.  **SUBSCRIBE & MINE:** (Optional) Fetches and mines subscription links for proxies.
7.  **ARCHIVE:** (Optional) Intelligently merges, deduplicates, and saves all found proxies
    into source-specific JSON files.

Required Libraries:
-------------------
- requests
- beautifulsoup4
- tqdm (for progress bars)
- jdatetime (for Persian calendar conversion)

You can install them using pip:
`pip install requests beautifulsoup4 tqdm jdatetime`
"""

import json
import logging
import re
import shutil
import sys
import time
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urlsplit

import jdatetime
import requests
from bs4 import BeautifulSoup, Tag
from tqdm import tqdm

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# =============================================================================
# --- A D V A N C E D   L O G G I N G   S E T U P ---
# =============================================================================
class ColorFormatter(logging.Formatter):
    """A custom logging formatter that adds color to log levels for readability."""
    GREY = "\x1b[38;20m"
    YELLOW = "\x1b[33;20m"
    RED = "\x1b[31;20m"
    RESET = "\x1b[0m"
    
    FORMAT = "%(asctime)s [%(levelname)s] - %(message)s"
    FORMATS = {
        logging.DEBUG: GREY + FORMAT + RESET,
        logging.INFO: GREY + FORMAT + RESET,
        logging.WARNING: YELLOW + FORMAT + RESET,
        logging.ERROR: RED + FORMAT + RESET,
        logging.CRITICAL: RED + FORMAT + RESET,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt="%Y-%m-%d %H:%M:%S")
        return formatter.format(record)

def setup_logger() -> logging.Logger:
    """Sets up an isolated, color-coded logger for the application."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    if logger.hasHandlers():
        logger.handlers.clear()
        
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ColorFormatter())
    logger.addHandler(console_handler)
    
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    return logger

# =============================================================================
# --- S E T T I N G S  &  C O N S T A N T S ---
# =============================================================================
# Default settings, will be overwritten by preferences.json if it exists.
DEFAULT_SETTINGS = {
    # --- Main Stage Control ---
    "ENABLE_CLEANUP": True,
    "ENABLE_CHANNEL_SCRAPING": True,
    "ENABLE_STALE_CHANNEL_PRUNING": True,
    "ENABLE_VALIDATOR": True,
    "ENABLE_MANUAL_SUBSCRIPTION_MINING": True,
    "ENABLE_SCRAPED_URL_MINING": True,
    "ENABLE_ARCHIVER": True,

    # --- Logging & Verbosity ---
    "ENABLE_DETAILED_SCRAPING_LOGS": False,

    # --- Scraping & Pruning Parameters ---
    "SCRAPE_PAGE_LIMIT": 50,
    "VALIDATION_PAGE_LIMIT": 5,
    "STALE_CHANNEL_MONTHS": 6,
    
    # --- Time-based Parameters ---
    "CLEANUP_INTERVAL_HOURS": 24,
    "LAST_CLEANUP_TIMESTAMP": "2000-01-01T00:00:00Z",
    
    # --- Network Parameters ---
    "REQUEST_TIMEOUT_SECONDS": 20,

    # --- File & Directory Paths ---
    "DATA_DIRECTORY": "data",
    "OUTPUT_DIRECTORY": "output",
    
    # --- Timezone Configuration ---
    "TIMEZONE_OFFSET_HOURS": 3.5,
}

# --- Global Constants ---
BASE_TELEGRAM_URL = "https://t.me/s/{channel_name}"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# --- Type Hinting Aliases ---
ChannelName = str
MessageId = int
MessageText = str
GenericURL = str
TelegramUsername = str
TelegramProxy = str
ScrapedData = Dict[ChannelName, List[str]]
UpdateCheckpoints = Dict[ChannelName, MessageId]
ExtractedUrls = Dict[ChannelName, List[GenericURL]]
ExtractedUsernames = Dict[ChannelName, List[TelegramUsername]]
ExtractedProxies = Dict[ChannelName, List[TelegramProxy]]
SubscriptionProxies = Dict[GenericURL, List[TelegramProxy]]
ChannelTimestamps = Dict[ChannelName, datetime]

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def load_settings(pref_path: Path) -> dict:
    """Loads settings from preferences.json, creating it with defaults if it doesn't exist."""
    if not pref_path.exists():
        logging.warning(f"'{pref_path}' not found. Creating with default settings.")
        pref_path.parent.mkdir(exist_ok=True)
        with pref_path.open('w', encoding='utf-8') as f:
            json.dump(DEFAULT_SETTINGS, f, indent=4)
        return DEFAULT_SETTINGS
    
    with pref_path.open('r', encoding='utf-8') as f:
        user_settings = json.load(f)
        settings = DEFAULT_SETTINGS.copy()
        settings.update(user_settings)
        return settings

def update_settings(pref_path: Path, key: str, value: str):
    """Updates a specific key in the preferences.json file."""
    settings = load_settings(pref_path)
    settings[key] = value
    with pref_path.open('w', encoding='utf-8') as f:
        json.dump(settings, f, indent=4)
    logging.info(f"Updated setting '{key}' in '{pref_path}'.")

def _load_json_file(file_path: Path, default=None):
    """Safely loads a JSON file."""
    try:
        if file_path.exists():
            with file_path.open('r', encoding='utf-8') as f:
                content = f.read()
                return json.loads(content) if content else default
        return default
    except (json.JSONDecodeError, IOError) as e:
        logging.error(f"Error reading {file_path}: {e}")
        return default

def _save_json_file(file_path: Path, data: Union[Dict, List]) -> None:
    """
    Saves data to a JSON file with pretty printing and sorting.
    Handles lists of strings and lists of dictionaries correctly.
    """
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        data_to_save = data
        if isinstance(data, dict):
            data_to_save = dict(sorted(data.items()))
        elif isinstance(data, list):
            # Only use set for hashable types like strings
            if data and all(isinstance(item, (str, int, float, bool, tuple)) for item in data):
                data_to_save = sorted(list(set(data)))
            # For lists of dicts, we assume the caller has already handled sorting/deduplication
            
        with file_path.open('w', encoding='utf-8') as f:
            json.dump(data_to_save, f, indent=4, ensure_ascii=False)
        logging.info(f"Successfully saved sorted data to {file_path}")
    except IOError as e:
        logging.error(f"Failed to write to file {file_path}: {e}")

# =============================================================================
# STAGE 0: DIRECTORY CLEANER
# =============================================================================
class DirectoryCleaner:
    """Cleans output directories based on a timed interval defined in settings."""
    def __init__(self, settings: dict, pref_path: Path, output_dir: Path):
        self.settings = settings
        self.pref_path = pref_path
        self.output_dir = output_dir

    def run(self):
        """Checks if the cleanup interval has passed and cleans the output directory if so."""
        logging.info("===== STAGE 0: CHECKING CLEANUP STATUS =====")
        last_cleanup_str = self.settings.get("LAST_CLEANUP_TIMESTAMP", "2000-01-01T00:00:00Z")
        last_cleanup_time = datetime.fromisoformat(last_cleanup_str.replace('Z', '+00:00'))
        cleanup_interval = timedelta(hours=self.settings.get("CLEANUP_INTERVAL_HOURS", 24))
        if datetime.now(timezone.utc) > last_cleanup_time + cleanup_interval:
            logging.warning("Cleanup interval has passed. Cleaning output directories...")
            if self.output_dir.exists():
                shutil.rmtree(self.output_dir)
                logging.info(f"Removed old output directory: {self.output_dir}")
            self.output_dir.mkdir(exist_ok=True)
            new_timestamp = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            update_settings(self.pref_path, "LAST_CLEANUP_TIMESTAMP", new_timestamp)
            logging.info("Cleanup complete.")
        else:
            logging.info("Cleanup not required at this time.")

# =============================================================================
# STAGE 1 & 2: SCRAPING & PRUNING LOGIC
# =============================================================================

class TelegramScraper:
    """Handles the scraping of new messages from a list of public Telegram channels."""
    def __init__(self, channels_file: Path, updates_file: Path, settings: dict):
        self.channels_file = channels_file
        self.updates_file = updates_file
        self.settings = settings
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        initial_channels = _load_json_file(self.channels_file, default=[])
        sanitized_channels = self._sanitize_and_sort_channels(initial_channels)
        if sanitized_channels != initial_channels:
            logging.info("Channel list has been sanitized. Updating 'channels.json'.")
            _save_json_file(self.channels_file, sanitized_channels)
        self.channels_to_scrape = sanitized_channels
        self.last_updates: UpdateCheckpoints = _load_json_file(self.updates_file, default={})
        logging.info(f"Scraper initialized for {len(self.channels_to_scrape)} channels.")

    def _sanitize_and_sort_channels(self, channels: List[str]) -> List[str]:
        if not channels: return []
        processed_channels = {ch.lower().strip() for ch in channels if ch and ch.strip()}
        return sorted(list(processed_channels))

    def _get_message_id_from_tag(self, tag: Tag) -> Optional[MessageId]:
        post_id = tag.get('data-post')
        if post_id and (match := re.search(r'/(\d+)$', post_id)):
            return int(match.group(1))
        return None

    def _fetch_channel_html(self, channel: ChannelName, before_id: Optional[MessageId] = None) -> Optional[str]:
        url = BASE_TELEGRAM_URL.format(channel_name=channel)
        params = {'before': before_id} if before_id else {}
        try:
            response = self.session.get(url, params=params, timeout=self.settings["REQUEST_TIMEOUT_SECONDS"])
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch HTML for {channel}: {e}")
            return None

    def _parse_page_messages(self, html: str) -> Tuple[List[Tuple[MessageId, MessageText, Optional[datetime]]], Optional[MessageId], Optional[MessageId]]:
        soup = BeautifulSoup(html, 'html.parser')
        widgets = soup.find_all('div', class_='tgme_widget_message')
        if not widgets: return [], None, None
        messages, ids = [], []
        for widget in widgets:
            if not (msg_id := self._get_message_id_from_tag(widget)): continue
            ids.append(msg_id)
            post_datetime = None
            if time_tag := widget.find('time', class_='time'):
                if dt_str := time_tag.get('datetime'):
                    try:
                        post_datetime = datetime.fromisoformat(dt_str)
                    except ValueError:
                        logging.warning(f"Could not parse datetime: {dt_str}")
            if (text_el := widget.find('div', class_='tgme_widget_message_text')):
                text = unescape(text_el.get_text(separator='\n', strip=True))
                links = [unescape(a['href']) for a in text_el.find_all('a', href=True)]
                if links: text += "\n\n--- Links Found ---\n" + "\n".join(links)
                messages.append((msg_id, text, post_datetime))
        return messages, max(ids, default=None), min(ids, default=None)

    def scrape_channel(self, channel: ChannelName) -> Tuple[List[MessageText], Optional[MessageId], Optional[datetime]]:
        last_known_id = self.last_updates.get(channel, 0)
        all_new_messages: List[Tuple[MessageId, MessageText, Optional[datetime]]] = []
        checkpoint = last_known_id
        latest_timestamp = None
        before_id = None
        for page_num in range(self.settings["SCRAPE_PAGE_LIMIT"]):
            html = self._fetch_channel_html(channel, before_id)
            if not html: break
            page_msgs, latest_id, oldest_id = self._parse_page_messages(html)
            if not page_msgs: break
            if page_num == 0:
                if latest_id: checkpoint = max(checkpoint, latest_id)
                if page_msgs: latest_timestamp = page_msgs[-1][2]
            new_on_page = [msg for msg in page_msgs if msg[0] > last_known_id]
            if self.settings["ENABLE_DETAILED_SCRAPING_LOGS"]:
                for msg_id, _, post_dt_utc in new_on_page:
                    gregorian_time_utc = post_dt_utc.strftime('%Y-%m-%d %H:%M:%S UTC') if post_dt_utc else "N/A"
                    iran_local_time = post_dt_utc + timedelta(hours=self.settings["TIMEZONE_OFFSET_HOURS"]) if post_dt_utc else None
                    jalali_time_irst = jdatetime.datetime.fromgregorian(datetime=iran_local_time).strftime('%Y/%m/%d %H:%M:%S') if iran_local_time else "N/A"
                    tqdm.write(f"  [NEW POST] Channel: {channel} | ID: {msg_id} | Time: {gregorian_time_utc} (Jalali: {jalali_time_irst} IRST)")
            all_new_messages.extend(new_on_page)
            if oldest_id is None or oldest_id <= last_known_id:
                tqdm.write(f"  [INFO] Reached last known message ID for {channel}. Scrape complete.")
                break
            before_id = oldest_id
            if page_num == self.settings["SCRAPE_PAGE_LIMIT"] - 1: logging.warning(f"Reached scrape limit for {channel}.")
            time.sleep(0.5)
        all_new_messages.sort(key=lambda x: x[0])
        return [msg[1] for msg in all_new_messages], checkpoint, latest_timestamp

    def run(self) -> Tuple[ScrapedData, ChannelTimestamps]:
        logging.info("===== STAGE 1: SCRAPING CHANNELS =====")
        all_new_data: ScrapedData = {}
        new_updates: UpdateCheckpoints = self.last_updates.copy()
        channel_timestamps: ChannelTimestamps = {}
        with tqdm(self.channels_to_scrape, desc="Scraping Channels") as pbar:
            for channel in pbar:
                pbar.set_description(f"Scraping: {channel}")
                new_messages, new_checkpoint, latest_ts = self.scrape_channel(channel)
                if new_messages: all_new_data[channel] = new_messages
                if new_checkpoint > new_updates.get(channel, 0):
                    new_updates[channel] = new_checkpoint
                if latest_ts: channel_timestamps[channel] = latest_ts
                time.sleep(1)
        _save_json_file(self.updates_file, new_updates)
        logging.info("Scraping process finished.")
        return all_new_data, channel_timestamps

class StaleChannelManager:
    """Prunes channels that have not been active for a specified duration."""
    def __init__(self, channels_file: Path, invalid_channels_file: Path, settings: dict):
        self.channels_file = channels_file
        self.invalid_channels_file = invalid_channels_file
        self.settings = settings
        self.active_channels = _load_json_file(self.channels_file, default=[])
        self.invalid_channels = _load_json_file(self.invalid_channels_file, default=[])

    def prune(self, channel_timestamps: ChannelTimestamps):
        logging.info("===== STAGE 2: PRUNING STALE CHANNELS =====")
        stale_threshold = datetime.now(timezone.utc) - timedelta(days=self.settings["STALE_CHANNEL_MONTHS"] * 30)
        stale_channels = []
        for channel, last_post_time in channel_timestamps.items():
            if last_post_time and last_post_time < stale_threshold:
                stale_channels.append(channel)
        if not stale_channels:
            logging.info("No stale channels found to prune.")
            return
        logging.warning(f"Found {len(stale_channels)} stale channels to prune: {stale_channels}")
        active_set = set(self.active_channels) - set(stale_channels)
        invalid_set = set(self.invalid_channels) | set(stale_channels)
        _save_json_file(self.channels_file, sorted(list(active_set)))
        _save_json_file(self.invalid_channels_file, sorted(list(invalid_set)))
        logging.info("Stale channel pruning finished.")

# =============================================================================
# STAGE 3: DATA EXTRACTION LOGIC
# =============================================================================
class DataExtractor:
    """Parses scraped messages to extract proxies, URLs, and usernames."""
    URL_REGEX = re.compile(r"""https?://[^\s<>"']+""", re.IGNORECASE)
    USERNAME_REGEX = re.compile(r"""(?:https?://t\.me/|@)(\w{5,32})""", re.VERBOSE | re.IGNORECASE)
    PROXY_REGEX = re.compile(r"""(?:https?://t\.me/|tg://)(?:proxy|socks)\?[^\s<>"']+""", re.VERBOSE | re.IGNORECASE)

    def __init__(self, scraped_data: ScrapedData):
        if not isinstance(scraped_data, dict): raise TypeError("scraped_data must be a dictionary.")
        self.data = scraped_data

    def _extract_from_messages(self, messages: List[str], pattern: re.Pattern) -> Set[str]:
        """Generic helper to apply a regex pattern to a list of messages."""
        return {str(match) for message in messages for match in pattern.findall(message)}

    def process_data(self) -> Tuple[ExtractedProxies, ExtractedUrls, ExtractedUsernames]:
        """Runs the extraction process for all channels in the input data."""
        logging.info("===== STAGE 3: EXTRACTING DATA =====")
        all_urls, all_usernames, all_proxies = {}, {}, {}
        if not self.data: return {}, {}, {}
        for channel, messages in self.data.items():
            found_urls = self._extract_from_messages(messages, self.URL_REGEX)
            found_usernames_raw = self._extract_from_messages(messages, self.USERNAME_REGEX)
            found_proxies = self._extract_from_messages(messages, self.PROXY_REGEX)
            
            generic_urls = found_urls - found_proxies
            found_usernames_clean = {user.lower() for user in found_usernames_raw}
            
            if generic_urls: all_urls[channel] = sorted(list(generic_urls))
            if found_usernames_clean: all_usernames[channel] = sorted(list(found_usernames_clean))
            if found_proxies: all_proxies[channel] = sorted(list(found_proxies))
        logging.info("Extraction process finished.")
        return all_proxies, all_urls, all_usernames

# =============================================================================
# STAGE 4: CHANNEL VALIDATION LOGIC
# =============================================================================
class ChannelValidator:
    """Validates new channels and extracts their proxies."""
    def __init__(self, channels_file: Path, invalid_channels_file: Path, settings: dict):
        self.channels_file = channels_file
        self.invalid_channels_file = invalid_channels_file
        self.settings = settings
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.known_valid_channels = set(_load_json_file(self.channels_file, default=[]))
        self.known_invalid_channels = set(_load_json_file(self.invalid_channels_file, default=[]))

    def _fetch_channel_html(self, channel: ChannelName, before_id: Optional[MessageId] = None) -> Optional[str]:
        url = BASE_TELEGRAM_URL.format(channel_name=channel)
        params = {'before': before_id} if before_id else {}
        try:
            response = self.session.get(url, params=params, timeout=self.settings["REQUEST_TIMEOUT_SECONDS"])
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException:
            logging.warning(f"Could not fetch '{channel}'. It might be private, banned, or non-existent.")
            return None

    def _validate_and_extract_proxies(self, channel: ChannelName) -> List[TelegramProxy]:
        """Performs a shallow scrape and returns any proxies found."""
        all_found_proxies: Set[TelegramProxy] = set()
        before_id = None
        for _ in range(self.settings["VALIDATION_PAGE_LIMIT"]):
            html = self._fetch_channel_html(channel, before_id)
            if not html: break
            clean_html = unescape(html)
            page_proxies = DataExtractor.PROXY_REGEX.findall(clean_html)
            if page_proxies: all_found_proxies.update(page_proxies)
            soup = BeautifulSoup(html, 'html.parser')
            widgets = soup.find_all('div', class_='tgme_widget_message')
            if not widgets: break
            ids = [int(match.group(1)) for w in widgets if (post_id := w.get('data-post')) and (match := re.search(r'/(\d+)$', post_id))]
            if not ids: break
            before_id = min(ids)
            time.sleep(1)
        if all_found_proxies:
            tqdm.write(f"  [SUCCESS] Validation success: Found {len(all_found_proxies)} proxies in '{channel}'.")
        else:
            tqdm.write(f"  [FAILED]  Validation failed: No proxies found in recent posts of '{channel}'.")
        return sorted(list(all_found_proxies))

    def validate_and_update(self, new_usernames: ExtractedUsernames) -> ExtractedProxies:
        """Orchestrates the validation process for a list of newly found usernames."""
        logging.info("===== STAGE 4: VALIDATING NEW CHANNELS =====")
        all_found_users = {user.lower() for user_list in new_usernames.values() for user in user_list}
        truly_new_channels = all_found_users - self.known_valid_channels - self.known_invalid_channels
        if not truly_new_channels:
            logging.info("No new, un-checked channels to validate.")
            return {}
        logging.info(f"Found {len(truly_new_channels)} new channels to validate.")
        newly_validated_proxies: ExtractedProxies = {}
        newly_validated_channels: List[ChannelName] = []
        newly_invalidated_channels: List[ChannelName] = []
        with tqdm(sorted(list(truly_new_channels)), desc="Validating New Channels") as pbar:
            for channel in pbar:
                pbar.set_description(f"Validating: {channel}")
                found_proxies = self._validate_and_extract_proxies(channel)
                if found_proxies:
                    newly_validated_channels.append(channel)
                    newly_validated_proxies[channel] = found_proxies
                else:
                    newly_invalidated_channels.append(channel)
                time.sleep(1)
        if newly_validated_channels:
            logging.info(f"Adding {len(newly_validated_channels)} new valid channels: {newly_validated_channels}")
            self.known_valid_channels.update(newly_validated_channels)
            _save_json_file(self.channels_file, sorted(list(self.known_valid_channels)))
        if newly_invalidated_channels:
            logging.info(f"Adding {len(newly_invalidated_channels)} new invalid channels: {newly_invalidated_channels}")
            self.known_invalid_channels.update(newly_invalidated_channels)
            _save_json_file(self.invalid_channels_file, sorted(list(self.known_invalid_channels)))
        logging.info("Validation process finished.")
        return newly_validated_proxies

# =============================================================================
# STAGE 5: SUBSCRIPTION MINING LOGIC
# =============================================================================

class SubscriptionMiner:
    """Fetches URLs and mines them for Telegram proxies."""
    def __init__(self, subscription_links_file: Path, invalid_links_file: Path, extracted_urls: ExtractedUrls, settings: dict):
        self.subscription_links_file = subscription_links_file
        self.invalid_links_file = invalid_links_file
        self.settings = settings
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        
        urls_to_process = []
        if self.settings.get("ENABLE_MANUAL_SUBSCRIPTION_MINING"):
            manual_subs = _load_json_file(self.subscription_links_file, default=[])
            urls_to_process.extend(manual_subs)
            logging.info(f"Loaded {len(manual_subs)} manual subscription links.")

        if self.settings.get("ENABLE_SCRAPED_URL_MINING"):
            scraped_subs = [url for url_list in extracted_urls.values() for url in url_list]
            urls_to_process.extend(scraped_subs)
            logging.info(f"Loaded {len(scraped_subs)} URLs from scraped posts.")

        self.known_invalid_links = set(_load_json_file(self.invalid_links_file, default=[]))
        combined_urls = set(urls_to_process)
        self.urls_to_mine = {url for url in combined_urls if 't.me/' not in url} - self.known_invalid_links
        logging.info(f"SubscriptionMiner initialized with {len(self.urls_to_mine)} unique URLs to mine (ignoring {len(self.known_invalid_links)} known invalid links).")

    def _fetch_url_content(self, url: GenericURL) -> Optional[str]:
        """Fetches raw content from a URL."""
        try:
            response = self.session.get(url, timeout=self.settings["REQUEST_TIMEOUT_SECONDS"])
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logging.warning(f"Failed to fetch subscription URL {url}: {e}")
            return None
        except Exception as e:
            logging.error(f"An unexpected error occurred while processing URL {url}: {e}")
            return None

    def _parse_subscription_content(self, content: str) -> Set[TelegramProxy]:
        """Parses content, attempting Base64 decode, and extracts proxies."""
        decoded_content = content
        try:
            decoded_content = base64.b64decode(content).decode('utf-8')
        except (ValueError, TypeError, base64.binascii.Error):
            pass
        return set(DataExtractor.PROXY_REGEX.findall(decoded_content))

    def run(self) -> Tuple[SubscriptionProxies, List[GenericURL]]:
        """Executes the subscription mining process."""
        logging.info("===== STAGE 5: MINING SUBSCRIPTION LINKS =====")
        all_subscription_proxies: SubscriptionProxies = {}
        valid_subscription_links: Set[GenericURL] = set()
        newly_invalidated: Set[GenericURL] = set()
        with tqdm(sorted(list(self.urls_to_mine)), desc="Mining Subscriptions") as pbar:
            for url in pbar:
                pbar.set_description(f"Mining: {url[:50]}...")
                content = self._fetch_url_content(url)
                if not content:
                    newly_invalidated.add(url)
                    continue
                found_proxies = self._parse_subscription_content(content)
                if found_proxies:
                    tqdm.write(f"  [SUCCESS] Found {len(found_proxies)} proxies in {url}.")
                    all_subscription_proxies[url] = sorted(list(found_proxies))
                    valid_subscription_links.add(url)
                else:
                    tqdm.write(f"  [FAILED] No proxies found in {url}.")
                    newly_invalidated.add(url)
                time.sleep(0.5)
        if newly_invalidated:
            logging.info(f"Adding {len(newly_invalidated)} new invalid subscription links.")
            self.known_invalid_links.update(newly_invalidated)
            _save_json_file(self.invalid_links_file, sorted(list(self.known_invalid_links)))
        logging.info("Subscription mining process finished.")
        return all_subscription_proxies, sorted(list(valid_subscription_links))

# =============================================================================
# STAGE 6: PROXY ARCHIVER LOGIC
# =============================================================================

class ProxyArchiver:
    """
    Intelligently merges and deduplicates new proxies with an existing archive.
    """
    def __init__(self, channel_archive_path: Path, sub_archive_path: Path, 
                 channel_proxies: ExtractedProxies, sub_proxies: SubscriptionProxies):
        self.channel_archive_path = channel_archive_path
        self.sub_archive_path = sub_archive_path
        self.new_channel_proxies = channel_proxies
        self.new_sub_proxies = sub_proxies

    def _smart_append_and_save(self, archive_path: Path, new_data: Union[ExtractedProxies, SubscriptionProxies]):
        """
        Loads an existing archive, merges new data, deduplicates, and saves.

        Args:
            archive_path (Path): The path to the JSON archive file.
            new_data (Union[ExtractedProxies, SubscriptionProxies]): The new data to merge.
        """
        existing_data = _load_json_file(archive_path, default={})
        
        for source, new_items in new_data.items():
            existing_items = set(existing_data.get(source, []))
            existing_items.update(new_items)
            existing_data[source] = sorted(list(existing_items))
            
        _save_json_file(archive_path, existing_data)

    def run(self):
        """Executes the archival process for all proxy sources."""
        logging.info("===== STAGE 6: ARCHIVING PROXIES =====")
        if self.new_channel_proxies:
            self._smart_append_and_save(self.channel_archive_path, self.new_channel_proxies)
        if self.new_sub_proxies:
            self._smart_append_and_save(self.sub_archive_path, self.new_sub_proxies)
        logging.info("Proxy archival process finished.")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main function to run the full scrape-extract-validate-mine-organize pipeline."""
    logger = setup_logger()
    
    pref_path = Path(DEFAULT_SETTINGS["DATA_DIRECTORY"]) / "preferences.json"
    settings = load_settings(pref_path)

    DATA_DIR = Path(settings["DATA_DIRECTORY"])
    OUTPUT_DIR = Path(settings["OUTPUT_DIRECTORY"])
    
    channels_path = DATA_DIR / "channels.json"
    updates_path = DATA_DIR / "last_updates.json"
    invalid_channels_path = DATA_DIR / "invalid_channels.json"
    subscriptions_path = DATA_DIR / "subscription_links.json"
    invalid_subscriptions_path = DATA_DIR / "invalid_subscription_links.json"
    
    archive_channel_proxies_path = OUTPUT_DIR / "archive_channel_proxies.json"
    archive_subscription_proxies_path = OUTPUT_DIR / "archive_subscription_proxies.json"

    # --- Setup directories and dummy files if they don't exist ---
    DATA_DIR.mkdir(exist_ok=True)
    OUTPUT_DIR.mkdir(exist_ok=True)
    if not channels_path.exists(): _save_json_file(channels_path, ["durov"])
    if not updates_path.exists(): _save_json_file(updates_path, {})
    if not invalid_channels_path.exists(): _save_json_file(invalid_channels_path, [])
    if not subscriptions_path.exists(): _save_json_file(subscriptions_path, [])
    if not invalid_subscriptions_path.exists(): _save_json_file(invalid_subscriptions_path, [])
    
    # --- STAGE 0: CLEANUP ---
    if settings["ENABLE_CLEANUP"]:
        cleaner = DirectoryCleaner(settings=settings, pref_path=pref_path, output_dir=OUTPUT_DIR)
        cleaner.run()
            
    # --- STAGE 1: SCRAPE DATA ---
    scraped_results, channel_timestamps = {}, {}
    if settings["ENABLE_CHANNEL_SCRAPING"]:
        scraper = TelegramScraper(channels_file=channels_path, updates_file=updates_path, settings=settings)
        scraped_results, channel_timestamps = scraper.run()
    else:
        logging.info("===== SKIPPING STAGE 1: CHANNEL SCRAPING (disabled in preferences.json) =====")
    
    # --- STAGE 2: PRUNE STALE CHANNELS ---
    if settings["ENABLE_STALE_CHANNEL_PRUNING"]:
        pruner = StaleChannelManager(channels_file=channels_path, invalid_channels_file=invalid_channels_path, settings=settings)
        pruner.prune(channel_timestamps)
    else:
        logging.info("===== SKIPPING STAGE 2: STALE CHANNEL PRUNING (disabled in preferences.json) =====")
    
    # --- STAGE 3: EXTRACT DATA ---
    extractor = DataExtractor(scraped_results)
    channel_proxies, urls, usernames = extractor.process_data()

    # --- STAGE 4: VALIDATE NEW CHANNELS ---
    if settings["ENABLE_VALIDATOR"] and usernames:
        validator = ChannelValidator(channels_file=channels_path, invalid_channels_file=invalid_channels_path, settings=settings)
        newly_validated_proxies = validator.validate_and_update(usernames)
        for channel, proxies in newly_validated_proxies.items():
            if channel in channel_proxies:
                channel_proxies[channel] = sorted(list(set(channel_proxies[channel] + proxies)))
            else:
                channel_proxies[channel] = proxies
    else:
        logging.info("===== SKIPPING STAGE 4: VALIDATOR (disabled in preferences.json or no new usernames found) =====")
    
    # --- STAGE 5: MINE SUBSCRIPTION LINKS ---
    subscription_proxies = {}
    if settings["ENABLE_MANUAL_SUBSCRIPTION_MINING"] or settings["ENABLE_SCRAPED_URL_MINING"]:
        miner = SubscriptionMiner(subscription_links_file=subscriptions_path, invalid_links_file=invalid_subscriptions_path, extracted_urls=urls, settings=settings)
        subscription_proxies, valid_subs = miner.run()
        if valid_subs:
            _save_json_file(OUTPUT_DIR / "valid_subscription_links.json", valid_subs)
    else:
        logging.info("===== SKIPPING STAGE 5: SUBSCRIPTION MINER (disabled in preferences.json) =====")
        
    # --- STAGE 6: ORGANIZE & ARCHIVE PROXIES ---
    if settings["ENABLE_ORGANIZER"]:
        archiver = ProxyArchiver(
            channel_archive_path=archive_channel_proxies_path,
            sub_archive_path=archive_subscription_proxies_path,
            channel_proxies=channel_proxies,
            sub_proxies=subscription_proxies
        )
        archiver.run()
    else:
        logging.info("===== SKIPPING STAGE 6: ORGANIZER (disabled in preferences.json) =====")
    
    print("\n--- SCRIPT FINISHED ---")
    if settings["ENABLE_ORGANIZER"]:
        print(f"All proxies have been saved and organized into '{OUTPUT_DIR}'.")

if __name__ == "__main__":
    main()
