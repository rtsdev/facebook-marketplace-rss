# Facebook Marketplace RSS Monitor

## Overview

Facebook Marketplace RSS Monitor is a Python application designed to scrape Facebook Marketplace search results, filter ads based on user-defined keywords, and generate an RSS feed for new listings. This allows users to stay updated on items of interest without manually checking the Marketplace.

The application uses Selenium with Firefox to browse and extract ad data, Flask to serve the RSS feed and a web-based configuration editor, APScheduler to periodically check for new ads, and SQLite to store information about seen ads and prevent duplicates.

## Key Features

*   **Automated Ad Monitoring:** Regularly scrapes specified Facebook Marketplace search URLs.
*   **Multi-Level Keyword Filtering:** Filters ads based on their titles using a flexible, multi-level keyword system:
    *   Keywords within the same level are treated with OR logic.
    *   Keywords across different levels are treated with AND logic.
*   **RSS Feed Generation:** Provides an RSS feed (`/rss`) of new and relevant ads, compatible with standard RSS readers.
*   **Web-Based Configuration:** Offers an intuitive web interface (`/edit`) to manage all application settings, including URLs to monitor and keyword filters. Changes are applied dynamically where possible.
*   **Persistent Ad Storage:** Uses an SQLite database to keep track of ads, ensuring users are notified only of new listings.
*   **Old Ad Pruning:** Automatically removes old ad entries from the database (default: ads not seen for 14 days).
*   **Docker Support:** Includes a [`Dockerfile`](Dockerfile:1) and [`docker-compose.yml`](docker-compose.yml:1) for easy containerized deployment.
*   **Logging:** Comprehensive logging with configurable log levels and file output.

## Prerequisites

### Software & Tools
*   **Python 3:** (e.g., Python 3.10+; Docker image uses Python 3.12 from Ubuntu 24.04).
*   **pip:** Python package installer.
*   **Firefox Browser:** Required by Selenium for web scraping. The Docker image installs this automatically.
*   **Docker & Docker Compose:** (Optional, for containerized deployment).
*   **Git:** For cloning the repository.

### Python Libraries
The application relies on several Python libraries. For manual setup, these are typically listed in a `requirements.txt` file. Key libraries include:
*   `Flask`
*   `Selenium`
*   `BeautifulSoup4` (bs4)
*   `APScheduler`
*   `PyRSS2Gen`
*   `requests` (implicitly, via webdriver_manager)
*   `python-dateutil`
*   `tzlocal`
*   `webdriver-manager`
*   `waitress` (for production serving)

## Installation

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/bethekind/facebook-marketplace-rss.git
    cd facebook-marketplace-rss
    ```

### Manual Setup

1.  **Install Python and Pip:** Ensure Python 3 and pip are installed on your system.
2.  **Install Firefox:** Download and install the latest version of Firefox browser.
3.  **Install Python Dependencies:**
    It's recommended to use a virtual environment:
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    pip install -r requirements.txt # Ensure you have a requirements.txt file
    ```
    If `requirements.txt` is not available, you would need to install the libraries listed under "Python Libraries" manually (e.g., `pip install Flask selenium beautifulsoup4 ...`).

### Docker Setup

This is the recommended method for deployment.

#### Building the Docker Image Locally
To build the Docker image yourself:
```bash
docker build -t bethekind/fb-mp-rss:latest .
```

#### Running with Docker Compose (Recommended)
This method uses the [`docker-compose.yml`](docker-compose.yml:1) file.
1.  Ensure Docker and Docker Compose are installed.
2.  Create a `config.json` file in the project root directory (you can copy and modify [`config.sample.json`](config.sample.json:1)). See the [Configuration](#configuration) section for details.
3.  Run the application:
    *   To run, potentially pulling the image `bethekind/fb-mp-rss:latest` from Docker Hub if available, or building locally if not:
        ```bash
        docker-compose up -d
        ```
    *   To force a local build and then run:
        ```bash
        docker-compose up -d --build
        ```
    The application will be accessible at `http://localhost:5000` (or your configured port). The `-d` flag runs it in detached mode.

#### Pulling the Pre-built Image from Docker Hub
If the image is available on Docker Hub, you can pull it directly:
```bash
docker pull bethekind/fb-mp-rss:latest
```

#### Running with `docker run` (Alternative)
1.  Ensure you have a `config.json` file prepared on your host machine (e.g., at `/path/to/your/config.json`).
2.  Run the container using the image (pulled from Docker Hub or built locally):
    ```bash
    docker run --name fb-mp-rss-container -d \
      -v /path/to/your/config.json:/app/config.json \
      -e CONFIG_FILE=/app/config.json \
      -e LOG_LEVEL=INFO \
      -p 5000:5000 \
      bethekind/fb-mp-rss:latest
    ```
    Adjust the port mapping (`-p host_port:container_port`) and volume path as needed.

## Configuration

Configuration is primarily managed through a `config.json` file located in the project's root directory (or as specified by the `CONFIG_FILE` environment variable).

### `config.json` File Overview

Create `config.json` by copying and modifying [`config.sample.json`](config.sample.json:1).

```json
{
    "url_filters": {
        "https://www.facebook.com/marketplace/category/search?query=smart%20tv&exact=false": {
            "level1": ["tv"],
            "level2": ["smart"],
            "level3": ["55\"", "55 inch"]
        },
        "https://www.facebook.com/marketplace/category/search?query=free%20stuff&exact=false": {}
    }
}
```

### Configuration Parameters

*   `url_filters` (Object): A dictionary where each key is a Facebook Marketplace search URL you want to monitor.
    *   The value for each URL is another dictionary defining keyword filter levels (e.g., `"level1"`, `"level2"`).
    *   Each level (e.g., `"level1"`) contains a list of keywords (strings).
        *   **Logic within a level:** Keywords are OR'd (e.g., `["keywordA", "keywordB"]` means match if title contains "keywordA" OR "keywordB").
        *   **Logic between levels:** Levels are AND'd (e.g., `level1` AND `level2` must both be satisfied).
    *   If a URL has an empty object `{}` as its filter value (e.g., `"https://...query=free%20stuff": {}`), all ads from that URL will be included without any keyword filtering on the title.

### Environment Variables

*   `CONFIG_FILE`: Specifies the path to the `config.json` file.
    *   In Docker Compose: Set to `/app/config.json` by default.
    *   If not set, the script defaults to `config.json` in its current working directory.
*   `LOG_LEVEL`: Sets the logging verbosity. Options: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. Default: `INFO`.

### Web-based Configuration Editor (`/edit`)

The application provides a web interface for easier configuration management.

*   **Access URL:** `http://<server_ip>:<server_port>/edit`
    (e.g., `http://localhost:5000/edit` if running with default settings).
*   **Functionality:**
    *   View and modify all settings from `config.json`.
    *   **URL Filters:**
        *   Add new Facebook Marketplace search URLs to monitor.
        *   For each URL, define and manage multi-level keyword filters.
        *   Add or remove filter levels (e.g., "Level 1", "Level 2").
        *   Add or remove keywords within each level.
*   **Saving Changes & Dynamic Reloading:**
    *   When you save the configuration via the UI, the `config.json` file on the server is updated.
    *   A backup of the previous configuration is created (e.g., `config.json.bak`).
    *   The application attempts to dynamically reload the new settings:
        *   **Dynamically Applied:** Changes to URL Filters are typically applied without a full restart. The ad checking job will be rescheduled if the interval changes.

### Manual `config.json` Editing

1.  **Create/Locate `config.json`:**
    *   If it doesn't exist, copy [`config.sample.json`](config.sample.json:1) to `config.json` in the project's root directory.
    *   Modify the values as needed.

2.  **Configuring `url_filters` Manually:**
    *   **Get Marketplace URL:**
        1.  Go to Facebook Marketplace.
        2.  Perform your desired search (e.g., "smart tv").
        3.  Apply any necessary Facebook filters (location, sort order, price range, condition, etc.).
        4.  Copy the complete URL from your browser's address bar. This will be a key in the `url_filters` object.
    *   **Define Keyword Filters:**
        For each URL, create an entry in `url_filters`. The value is an object where keys are `levelX` (e.g., `"level1"`, `"level2"`), and values are lists of keywords.
        Example: To find 55-inch smart TVs:
        ```json
        {
          "https://www.facebook.com/marketplace/category/search?query=smart%20tv&exact=false": {
            "exclude": ["roku"],
            "level1": ["tv", "television"],
            "level2": ["smart", "google tv", "android tv"],
            "level3": ["55\"", "55 inch"]
          }
        }
        ```
        This matches ads whose titles contain:
        ((`tv` OR `television`) AND (`smart` OR `google tv` OR `android tv`) AND (`55"` OR `55 inch`)) AND NOT `roku`.

## Running the Application

### With Docker Compose (Recommended)

1.  Ensure `config.json` is present in the project root.
2.  Start the services:
    ```bash
    docker-compose up -d
    ```
    To rebuild the image if you've made changes to the code or [`Dockerfile`](Dockerfile:1):
    ```bash
    docker-compose up -d --build
    ```

### Manually

1.  Ensure all prerequisites and dependencies from the [Manual Setup](#manual-setup) section are met.
2.  Ensure `config.json` is configured in the project root.
3.  Run the script:
    ```bash
    python3 fb_ad_monitor.py
    ```
    The application will use `waitress` as the WSGI server if installed, otherwise, it falls back to Flask's development server (not recommended for production).

## Usage

Once the application is running:

*   **RSS Feed:** Access the generated RSS feed at:
    `http://<server_ip>:<server_port>/rss`
    (e.g., `http://localhost:5000/rss`)
    Add this URL to your preferred RSS feed reader (e.g., Feedbro, Feedly, Thunderbird). The feed includes ads found/checked recently (typically within the last 7 days).

*   **Configuration Editor:** Manage application settings via the web UI at:
    `http://<server_ip>:<server_port>/edit`
    (e.g., `http://localhost:5000/edit`)

## Pushing the Image to Docker Hub (for Maintainer `bethekind`)

1.  **Log in to Docker Hub:**
    ```bash
    docker login
    ```
    Enter your Docker Hub username (`bethekind`) and password when prompted.

2.  **Build and Tag the Image (if not already done):**
    Ensure your locally built image is tagged correctly. If you built it with a different tag or just have an image ID, retag it:
    ```bash
    # If you built it as 'fb-mp-rss:latest' locally, or you have the image ID:
    # docker tag fb-mp-rss:latest bethekind/fb-mp-rss:latest
    # OR
    # docker tag <image-id> bethekind/fb-mp-rss:latest

    # If you already built it with 'docker build -t bethekind/fb-mp-rss:latest .', this step is done.
    docker build -t bethekind/fb-mp-rss:latest .
    ```

3.  **Push the Image:**
    ```bash
    docker push bethekind/fb-mp-rss:latest
    ```

## Logging

*   The log level can be set using the `LOG_LEVEL` environment variable (e.g., `INFO`, `DEBUG`). Default is `INFO`.
*   Logs are rotated, with a maximum size of 10MB and 2 backup files.

## Database

*   The application uses an SQLite database to store details of ads it has processed.
*   The database schema for the `ad_changes` table is:
    *   `id` (INTEGER, Primary Key, Auto-increment)
    *   `url` (TEXT, Ad's specific URL)
    *   `ad_id` (TEXT, Unique hash of the ad URL)
    *   `title` (TEXT, Ad title)
    *   `price` (TEXT, Ad price)
    *   `first_seen` (TEXT, ISO datetime string in UTC when the ad was first detected)
    *   `last_checked` (TEXT, ISO datetime string in UTC when the ad was last checked/seen)
*   Indexes are created on `ad_id` and `last_checked` for performance.
*   The database is initialized automatically if it doesn't exist when the application starts.
*   Old ads (default: not seen in 14 days) are pruned from the database during each ad check cycle.

## How It Works

1.  **Configuration Loading:** Reads settings from `config.json`.
2.  **Scheduler:** APScheduler runs a job at the configured `refresh_interval`.
3.  **Scraping:**
    *   For each monitored URL, Selenium (with Firefox in headless mode) navigates to the page.
    *   BeautifulSoup parses the HTML content.
    *   Ad details (title, price, link) are extracted.
4.  **Filtering:** Extracted ad titles are checked against the multi-level keyword filters defined for that source URL.
5.  **Database Interaction:**
    *   New, filtered ads are added to the SQLite database.
    *   The `last_checked` timestamp for existing ads is updated.
6.  **RSS Feed Generation:** The `/rss` endpoint queries the database for recent ads (typically last 7 days) and generates an RSS XML feed using PyRSS2Gen.
7.  **Web Server:** Flask serves the RSS feed and the `/edit` configuration UI. Waitress is used as the production WSGI server.

## License

This project is licensed under the BSD 3-Clause License.
Copyright for the original portions of the project belongs to 'regek' (2024).
Copyright for subsequent contributions belongs to 'bethekind' (2025).

Please see the [`LICENSE`](LICENSE:0) file in the root directory of this source tree for the full license text and details on all copyright holders.
If a `NOTICE` file is present, it may contain additional attribution details.
