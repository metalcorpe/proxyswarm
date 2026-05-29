"""Proxy source endpoints for the fallback scraper.

Kept separate from `scraper.py` so the (large, frequently-edited) source list
can grow without touching the fetching logic. This module is pure data: a
single `SOURCES` list of `{"url", "type"}` dicts that `scraper.fetch_source`
consumes.

`type` picks the parser:
- ``"text"`` / ``"regex"`` — body is scraped with `scraper.PROXY_REGEX`
  (``"regex"`` only flags sources like spys.me that append junk after the
  `host:port`; both are parsed identically).
- ``"geonode"`` — body is JSON and needs structured extraction.

All GitHub endpoints use the canonical ``raw.githubusercontent.com/<owner>/
<repo>/<branch>/<path>`` form; the ``github.com/.../raw/...`` variant only
302-redirects here, and the two string forms would defeat the scraper's
dedup-by-string, so they are normalized at the source.
"""

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
        "url": "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/all.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/top-http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/gfpcom/free-proxy-list/main/proxies/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/gfpcom/free-proxy-list/main/proxies/socks5.txt",
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
        "url": "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/all-proxies.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/proxygenerator1/ProxyGenerator/main/ALL/ALL.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/all_proxies.txt",
        "type": "text",
    },
    {
        "url": "https://proxylist.geonode.com/api/proxy-list?limit=500&sort_by=lastChecked&sort_type=desc",
        "type": "geonode",
    },
    {"url": "https://proxyspace.pro/http.txt", "type": "text"},
    {
        "url": "https://raw.githubusercontent.com/6Kmfi6HP/proxy_files/main/proxies.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/http_proxies.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/https_proxies.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/socks4_proxies.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/socks5_proxies.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/ArteffKod/socks4/main/socks4%20proxy",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/BreakingTechFr/Proxy_Free/main/proxies/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/BreakingTechFr/Proxy_Free/main/proxies/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/BreakingTechFr/Proxy_Free/main/proxies/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/https.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/FifzzSENZE/Master-Proxy/master/proxies/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/FifzzSENZE/Master-Proxy/master/proxies/https.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/FifzzSENZE/Master-Proxy/master/proxies/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/FifzzSENZE/Master-Proxy/master/proxies/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Firmfox/Proxify/main/proxies/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Firmfox/Proxify/main/proxies/https.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Firmfox/Proxify/main/proxies/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Firmfox/Proxify/main/proxies/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/KUTlime/ProxyList/main/ProxyList.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/MrMarble/proxy-list/main/all.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/SevenworksDev/proxy-list/main/proxies/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/SevenworksDev/proxy-list/main/proxies/https.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/SevenworksDev/proxy-list/main/proxies/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/SevenworksDev/proxy-list/main/proxies/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/SevenworksDev/proxy-list/main/proxies/unknown.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Tsprnay/Proxy-lists/master/proxies/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Tsprnay/Proxy-lists/master/proxies/https.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Tsprnay/Proxy-lists/master/proxies/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Tsprnay/Proxy-lists/master/proxies/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/TuanMinPay/live-proxy/master/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/TuanMinPay/live-proxy/master/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/TuanMinPay/live-proxy/master/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Vadim287/free-proxy/main/proxies/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Vadim287/free-proxy/main/proxies/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Vadim287/free-proxy/main/proxies/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Vann-Dev/proxy-list/main/proxies/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Vann-Dev/proxy-list/main/proxies/https.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Vann-Dev/proxy-list/main/proxies/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Vann-Dev/proxy-list/main/proxies/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/VolkanSah/Auto-Proxy-Fetcher/main/proxies.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/XigmaDev/proxy/main/proxies.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/https.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/andigwandi/free-proxy/main/proxy_list.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/berkay-digital/Proxy-Scraper/main/proxies.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/casa-ls/proxy-list/main/http",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/casa-ls/proxy-list/main/socks4",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/casa-ls/proxy-list/main/socks5",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/casals-ar/proxy-list/main/http",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/casals-ar/proxy-list/main/socks4",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/casals-ar/proxy-list/main/socks5",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/chekamarue/proxies/main/https.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/chekamarue/proxies/main/httpss.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/claude89757/free_https_proxies/main/https_proxies.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/claude89757/free_https_proxies/main/isz_https_proxies.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/databay-labs/free-proxy-list/master/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/databay-labs/free-proxy-list/master/https.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/databay-labs/free-proxy-list/master/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/dinoz0rg/proxy-list/main/scraped_proxies/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/dinoz0rg/proxy-list/main/scraped_proxies/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/dinoz0rg/proxy-list/main/scraped_proxies/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/ebrasha/abdal-proxy-hub/main/http-proxy-list-by-EbraSha.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/ebrasha/abdal-proxy-hub/main/https-proxy-list-by-EbraSha.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/ebrasha/abdal-proxy-hub/main/socks4-proxy-list-by-EbraSha.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/ebrasha/abdal-proxy-hub/main/socks5-proxy-list-by-EbraSha.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/elliottophellia/proxylist/master/results/pmix_checked.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/fahimscirex/proxybd/master/proxylist/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/fahimscirex/proxybd/master/proxylist/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/fyvri/fresh-proxy-list/archive/storage/classic/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/fyvri/fresh-proxy-list/archive/storage/classic/https.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/fyvri/fresh-proxy-list/archive/storage/classic/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/fyvri/fresh-proxy-list/archive/storage/classic/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/gitrecon1455/fresh-proxy-list/main/proxylist.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/handeveloper1/Proxy/main/Proxies-Ercin/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/handeveloper1/Proxy/main/Proxies-Ercin/https.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/handeveloper1/Proxy/main/Proxies-Ercin/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/handeveloper1/Proxy/main/Proxies-Ercin/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/hendrikbgr/Free-Proxy-Repo/master/proxy_list.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/im-razvan/proxy_list/main/https.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/im-razvan/proxy_list/main/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/iniridwanul/Hoot/master/anonymous-proxylist/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/iniridwanul/Hoot/master/anonymous-proxylist/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/iniridwanul/Hoot/master/anonymous-proxylist/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/iniridwanul/Hoot/master/proxylist/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/iniridwanul/Hoot/master/proxylist/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/iniridwanul/Hoot/master/proxylist/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/javadbazokar/PROXY-List/main/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/javadbazokar/PROXY-List/main/https.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-https.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/joy-deploy/free-proxy-list/main/data/latest/types/http/proxies.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/joy-deploy/free-proxy-list/main/data/latest/types/socks4/proxies.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/joy-deploy/free-proxy-list/main/data/latest/types/socks5/proxies.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/mmpx12/proxy-list/master/proxies.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/all.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies_anonymous/all.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/murtaja89/public-proxies/main/proxies_all.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/noarche/proxylist-socks5-sock4-exported-updates/main/connect-online.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/noarche/proxylist-socks5-sock4-exported-updates/main/http-online.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/noarche/proxylist-socks5-sock4-exported-updates/main/socks4-online.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/noarche/proxylist-socks5-sock4-exported-updates/main/socks5-online.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/http/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/https/https.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/sock4/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/socks5/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/parserpp/ip_ports/main/proxyinfo.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/https/data.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks4/data.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks5/data.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/r00tee/Proxy-List/main/Https.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/r00tee/Proxy-List/main/Socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/r00tee/Proxy-List/main/Socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS4_RAW.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/saisuiu/Lionkings-Http-Proxys-Proxies/main/free.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/proxies.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/themiralay/Proxy-List-World/master/data.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/https.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/variableninja/proxyscraper/main/proxies/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/variableninja/proxyscraper/main/proxies/socks.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/vmheaven/VMHeaven-Free-Proxy-Updated/main/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/vmheaven/VMHeaven-Free-Proxy-Updated/main/https.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/vmheaven/VMHeaven-Free-Proxy-Updated/main/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/vmheaven/VMHeaven-Free-Proxy-Updated/main/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/yemixzy/proxy-list/main/proxies/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/yemixzy/proxy-list/main/proxies/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/yemixzy/proxy-list/main/proxies/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/yemixzy/proxy-list/main/proxies/unchecked.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/zebbern/Proxy-Scraper/main/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/zebbern/Proxy-Scraper/main/https.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/zebbern/Proxy-Scraper/main/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/zebbern/Proxy-Scraper/main/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/zenjahid/FreeProxy4u/main/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/zenjahid/FreeProxy4u/main/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/zenjahid/FreeProxy4u/main/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/socks5.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/zloi-user/hideip.me/master/connect.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/zloi-user/hideip.me/master/http.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/zloi-user/hideip.me/master/https.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/zloi-user/hideip.me/master/socks4.txt",
        "type": "text",
    },
    {
        "url": "https://raw.githubusercontent.com/zloi-user/hideip.me/master/socks5.txt",
        "type": "text",
    },
]
