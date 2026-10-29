"""
Parse AccsMarket catalog pages (server-rendered HTML, all items on one page).
"""
from typing import Dict, List, Optional
import re
import html as html_lib
from helper.accsmarket_helper import AccsMarketHelper

# "0-10" -> 0, "10+" -> 10, "1k+" -> 1000, "10k+" -> 10000
ORDERS_MIN = {"0-10": 0, "10+": 10, "100+": 100, "1k+": 1000, "10k+": 10000}
REGISTRATION_YEAR_RX = re.compile(r'regist(?:ration date|ered)(?:\s+in)?\s*(?:\d{1,2}[./])*((?:19|20)\d{2})', re.I)


def _badge(chunk: str, name: str):
    m = re.search(r'class="item-badge ' + name + r'"[^>]*>.*?<span>(.*?)</span>', chunk, re.S)
    return html_lib.unescape(m.group(1)).strip() if m else None


def _to_float(value: Optional[str]):
    if value is None:
        return None
    try:
        return float(value.replace("%", "").replace(",", ".").replace(" ", ""))
    except ValueError:
        return None


def parse_catalog_items(raw_html: str) -> List[Dict]:
    """Parse every product block (<div class="soc-body" data-qty data-id data-cost>) of a catalog page."""
    items = []
    for position, chunk in enumerate(re.split(r'<div class="soc-body', raw_html)[1:], start=1):
        attrs = dict(re.findall(r'data-(qty|id|cost)="([^"]*)"', chunk[:300]))
        link = re.search(r'<div class="soc-text">\s*<p>\s*<a href="([^"]+)"[^>]*>(.*?)</a>', chunk, re.S)
        title = html_lib.unescape(re.sub(r"\s+", " ", link.group(2))).strip() if link else None
        orders = _badge(chunk, "orders-badge")
        reg_year = REGISTRATION_YEAR_RX.search(title or "")
        qty = int(attrs["qty"]) if attrs.get("qty", "").isdigit() else None

        items.append({
            "id": attrs.get("id"),
            "position": position,
            "title": title,
            "url": f"{AccsMarketHelper.BASE_URL}{link.group(1)}" if link else None,
            "qty": qty,
            "in_stock": bool(qty),
            "price_from": _to_float(attrs.get("cost")),
            "guarantee": _badge(chunk, "guarantee-badge"),
            "rating": _to_float(_badge(chunk, "star-badge")),
            "reject_rate_pct": _to_float(_badge(chunk, "rejects-badge")),
            "orders": orders,
            "orders_min": ORDERS_MIN.get(orders),
            "is_new": 'item-badge new-badge' in chunk,
            "registration_year": int(reg_year.group(1)) if reg_year else None,
        })
    return items
