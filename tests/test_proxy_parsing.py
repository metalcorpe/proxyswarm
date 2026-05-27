"""Proxy-host filtering: only globally-routable IP literals (or hostnames) survive.

`_is_bogus_proxy_host` gates which entries `_load_proxies` keeps, so a regression
here silently drops good proxies or admits useless non-routable ones.
"""

import pytest

from proxyswarm.core import _is_bogus_proxy_host


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
