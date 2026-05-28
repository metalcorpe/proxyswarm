"""Best-effort proxy scraper used as a fallback when no proxy file is supplied.

`scrape_proxies` fans out across a list of public free-proxy endpoints in
parallel and returns a deduplicated, sorted list of `host:port` strings. It is
intentionally lenient: any single source that times out, errors, or returns
garbage is logged and skipped rather than failing the batch, because
free-proxy endpoints are individually unreliable.

The output is *unvalidated* — callers run it through the same routability and
scheme classification the file loader uses (see
`proxyswarm.core._load_proxies`), so this module deliberately knows nothing
about what makes a proxy "good".
"""

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from json import JSONDecodeError

import requests
from loguru import logger

# `type` picks the parser: "text"/"regex" sources are scraped with PROXY_REGEX;
# "geonode" returns JSON and needs structured extraction.
SOURCES: list[dict[str, str]] = [
    {
        "url": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://api.proxyscrape.com/v4/free-proxy-list/get"
        "?request=display_proxies&proxy_format=protocolipport&format=text",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list"
        "/main/proxies/all.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list"
        "/main/proxies/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list"
        "/main/proxies/top-http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list"
        "/main/proxies/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/gfpcom/free-proxy-list/main/proxies/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/gfpcom/free-proxy-list"
        "/main/proxies/socks5.txt",
        "type": "text",
    },
    {"url": "https://spys.me/proxy.txt", "type": "regex"},
    {"url": "https://spys.me/socks.txt", "type": "regex"},
    {"url": "https://www.proxy-list.download/api/v1/get?type=http", "type": "text"},
    {"url": "https://www.proxy-list.download/api/v1/get?type=https", "type": "text"},
    {"url": "https://www.proxy-list.download/api/v1/get?type=socks4", "type": "text"},
    {"url": "https://www.proxy-list.download/api/v1/get?type=socks5", "type": "text"},
    {"url": "https://api.openproxylist.xyz/http.txt", "type": "text"},
    {"url": "https://api.openproxylist.xyz/socks4.txt", "type": "text"},
    {"url": "https://api.openproxylist.xyz/socks5.txt", "type": "text"},
    {"url": "https://rootjazz.com/proxies/proxies.txt", "type": "text"},
    {"url": "http://pubproxy.com/api/proxy?limit=20&format=txt", "type": "text"},
    {
        "url": "https://raw.githubusercontent.com/iplocate/free-proxy-list"
        "/main/all-proxies.txt",
        "type": "text",
    },
    {
        "url": "https://proxylist.geonode.com/api/proxy-list"
        "?limit=500&sort_by=lastChecked&sort_type=desc",
        "type": "geonode",
    },
]

# Optional `scheme://` prefix followed by `ipv4:port`. Spys.me appends junk
# (e.g. `1.2.3.4:8080-US-S`); `findall` ignores the trailing suffix.
PROXY_REGEX = re.compile(
    r"(?:[a-zA-Z0-9]+://)?(?:[0-9]{1,3}\.){3}[0-9]{1,3}:[0-9]{1,5}"
)

_REQUEST_TIMEOUT_SEC = 10


def fetch_source(source: dict[str, str]) -> set[str]:
    """Fetch one source and return its proxies; never raises.

    A failed request or malformed body is logged and yields an empty set so a
    single dead endpoint can't abort the parallel batch.
    """
    url = source["url"]
    stype = source["type"]
    proxies: set[str] = set()

    try:
        resp = requests.get(url, timeout=_REQUEST_TIMEOUT_SEC)
        resp.raise_for_status()

        if stype == "geonode":
            for item in resp.json().get("data", []):
                ip = item.get("ip")
                port = item.get("port")
                if ip and port:
                    # Drop the source's protocol hint and emit bare host:port;
                    # the core loader's `_classify_proxy_scheme` is the single
                    # source of truth for scheme, so every source stays uniform.
                    proxies.add(f"{ip}:{port}")
        else:  # "text" / "regex"
            proxies.update(PROXY_REGEX.findall(resp.text))

        logger.debug("Fetched {} proxies from {}", len(proxies), url)
    except (requests.RequestException, JSONDecodeError) as e:
        logger.warning("Failed to fetch proxies from {}: {}", url, type(e).__name__)

    return proxies


def scrape_proxies() -> list[str]:
    """Scrape every source in `SOURCES` in parallel.

    Returns a deduplicated, sorted list of proxy strings. Sorting keeps the
    on-disk file the caller writes reproducible across runs.
    """
    logger.info("Starting parallel proxy scraping from {} sources...", len(SOURCES))
    all_proxies: set[str] = set()

    with ThreadPoolExecutor(max_workers=min(20, len(SOURCES))) as executor:
        futures = [executor.submit(fetch_source, source) for source in SOURCES]
        for future in as_completed(futures):
            all_proxies.update(future.result())

    logger.success("Scraping complete. Found {} unique proxies.", len(all_proxies))
    return sorted(all_proxies)
