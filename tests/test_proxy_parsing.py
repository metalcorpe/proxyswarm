"""Proxy-host filtering: only globally-routable IP literals (or hostnames) survive.

`_is_bogus_proxy_host` gates which entries `_load_proxies` keeps, so a regression
here silently drops good proxies or admits useless non-routable ones.
"""

import pytest

from proxyswarm import core
from proxyswarm.core import _is_bogus_proxy_host, _load_proxies


@pytest.mark.parametrize(
    "host",
    [
        "8.8.8.8",  # public IPv4
        "1.2.3.4",
        "2606:4700:4700::1111",  # public IPv6
        "proxy.example.com",  # hostname — not an IP literal, let DNS decide
        "",  # empty — `_load_proxies` rejects this separately, never bogus here
    ],
)
def test_routable_or_hostname_is_kept(host):
    assert _is_bogus_proxy_host(host) is False


@pytest.mark.parametrize(
    "host",
    [
        "0.0.0.0",  # unspecified
        "0.1.2.3",  # 0.0.0.0/8 "this network"
        "127.0.0.1",  # loopback
        "10.0.0.1",  # RFC1918 private
        "192.168.1.1",
        "172.16.0.1",
        "169.254.1.1",  # link-local
        "::1",  # IPv6 loopback
    ],
)
def test_non_routable_is_bogus(host):
    assert _is_bogus_proxy_host(host) is True


def test_existing_file_skips_scraping(tmp_path, monkeypatch):
    """A usable proxy file must not trigger the network scrape fallback."""

    def _boom():
        msg = "scrape must not be called when the file has proxies"
        raise AssertionError(msg)

    monkeypatch.setattr(core, "scrape_proxies", _boom)
    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text("http://8.8.8.8:8080\n", encoding="utf-8")

    assert _load_proxies(str(proxy_file)) == ["http://8.8.8.8:8080"]


def test_empty_file_falls_back_to_scrape_then_classifies(tmp_path, monkeypatch):
    """Empty file → scrape → persist → re-read through classification filters.

    The bare `host:port` from the scraper must gain a scheme, the bogus private
    IP must be dropped, and the result must land on disk for restart reuse.
    """
    monkeypatch.setattr(
        core,
        "scrape_proxies",
        lambda: ["8.8.8.8:1080", "1.2.3.4:3128", "192.168.0.1:8080"],
    )
    proxy_file = tmp_path / "proxies.txt"

    result = _load_proxies(str(proxy_file))

    # socks5h for port 1080, http otherwise; the RFC1918 host is filtered out.
    assert set(result) == {"socks5h://8.8.8.8:1080", "http://1.2.3.4:3128"}
    # Scraped proxies were persisted so a restart skips re-scraping.
    assert proxy_file.read_text(encoding="utf-8").splitlines() == [
        "8.8.8.8:1080",
        "1.2.3.4:3128",
        "192.168.0.1:8080",
    ]


def test_empty_scrape_runs_without_proxies(tmp_path, monkeypatch):
    """Missing file + empty scrape degrades to the single-`None` sentinel."""
    monkeypatch.setattr(core, "scrape_proxies", list)
    missing = tmp_path / "does_not_exist.txt"

    assert _load_proxies(str(missing)) == [None]
