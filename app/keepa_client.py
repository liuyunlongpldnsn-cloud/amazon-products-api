import os
import requests
from typing import Any, Dict, List, Optional

class KeepaClient:
    """
    Real Keepa client.

    Key location:
      - Environment variable: KEEPA_API_KEY
    """
    def __init__(self, api_key: Optional[str] = None, domain: int = 1, timeout: int = 30):
        self.api_key = api_key or os.getenv("KEEPA_API_KEY")
        if not self.api_key:
            raise RuntimeError("Missing KEEPA_API_KEY env var")
        self.domain = domain
        self.timeout = timeout
        self.base = "https://api.keepa.com"

    def fetch_products(self, asins: List[str], stats: int = 1, buybox: int = 1) -> Dict[str, Any]:
        url = f"{self.base}/product"
        params = {
            "key": self.api_key,
            "domain": self.domain,   # 1 = amazon.com (US)
            "asin": ",".join(asins),
            "stats": stats,
            "buybox": buybox,
        }
        r = requests.get(url, params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()
