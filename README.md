# proxyswarm

A high-performance Python 3.14+ library for self-tuning, free proxy-pool distributed scraping. Contains an efficient fast/slow-lane discovery model that scales to thousands of poor-quality free proxies.

## Concept

Free proxy lists are mostly garbage (timeouts, captive portals, dead hosts) with a small reliable tail. `proxyswarm`’s `ProxyPool` keeps a **fast lane** — a deque of the top-K proxies by score (EWMA latency / EWMA success rate) — and falls back to a bounded **slow lane** scan for discovery. State is persistent, meaning a restart doesn't pay the cold-start cost of rediscovering which proxies work.

## Usage

1. Create a `proxies.txt` with formatted proxies e.g., `http://1.2.3.4:8080`.
2. Inherit from `proxyswarm.UseCase` and implement the abstract methods to define item iteration, how to form requests, how to classify responses, and what to do on success.
3. Hook your usecase up to `proxyswarm.run`.

```python
import argparse
import requests
from proxyswarm import SwarmConfig, FetchOutcome, RequestSpec, UseCase, run

class MyUseCase(UseCase):
    name = "my-usecase"
    
    @classmethod
    def add_arguments(cls, p: argparse.ArgumentParser) -> None:
        pass
        
    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "MyUseCase":
        return cls()
        
    def prepare(self) -> None:
        pass
        
    def session_headers(self) -> dict[str, str]:
        return {}
        
    def iter_items(self):
        yield "item1"
        yield "item2"
        
    def existing_ids(self) -> set[str]:
        return set()
        
    def build_request(self, item_id: str) -> RequestSpec:
        return RequestSpec(url=f"http://example.com/api/{item_id}", method="GET")
        
    def classify(self, response: requests.Response) -> tuple[FetchOutcome, str | None, bytes]:
        body = response.content
        if response.status_code == 200:
            return FetchOutcome.OK, None, body
        return FetchOutcome.PROXY_BAD, None, body
        
    def handle_success(self, item_id: str, body: bytes) -> bool:
        print(f"Success {item_id}")
        return True

if __name__ == "__main__":
    config = SwarmConfig(workers=10)
    use_case = MyUseCase()
    run(use_case, config)
```

Two worked examples ship in `examples/`:

- **`nvd_cve.py`** — a plain rate-limited REST/JSON case: `GET` each CVE record from the NVD API (per-IP rate limit is the reason to spread across proxies), classify by the 200 body, and save the JSON. Good starting point for your own use case.
- **`malware_bazaar.py`** — a full advanced implementation that was used to pull down gigabytes from abuse.ch: `POST` form data, an API that returns HTTP 200 + in-band JSON errors, and AES-encrypted zip extraction.

## Configuration

All configuration is provided via `SwarmConfig`, overriding defaults to suit network IO limitations and proxy traits.
