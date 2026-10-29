"""
Detect fast-selling "good" AccsMarket items by comparing stock with a per-item baseline.

Baseline (stored in Redis as a hash id -> "qty|epoch_seconds"):
  - first seen / restocked (qty went up)  -> reset baseline, no alert
  - sold = baseline_qty - qty >= min_sold -> alert if the item passes the filter, reset baseline
  - baseline older than the window        -> reset baseline (sales are counted within the window only)
"""
from typing import Dict, List, Tuple


def is_good_item(item: Dict, min_orders: int, max_reject_rate_pct: float) -> bool:
    return (
        (item.get("qty") or 0) > 0
        and item.get("orders_min") is not None and item["orders_min"] >= min_orders
        and item.get("reject_rate_pct") is not None and item["reject_rate_pct"] <= max_reject_rate_pct
    )


def evaluate_sales(
        items: List[Dict],
        baseline: Dict[str, str],
        now: int,
        min_sold: int,
        window_minutes: int,
        min_orders: int,
        max_reject_rate_pct: float,
    ) -> Tuple[List[Dict], Dict[str, str]]:
    """Return (alerts, new_baseline)."""
    alerts = []
    new_baseline = {}

    for item in items:
        item_id, qty = item.get("id"), item.get("qty")
        if not item_id or qty is None:
            continue

        prev = baseline.get(item_id)
        if not prev:
            new_baseline[item_id] = f"{qty}|{now}"
            continue

        base_qty, base_ts = (int(x) for x in prev.split("|"))
        if qty > base_qty:
            # restocked -> start a new baseline
            new_baseline[item_id] = f"{qty}|{now}"
            continue

        sold = base_qty - qty
        if sold >= min_sold:
            if is_good_item(item, min_orders, max_reject_rate_pct):
                alerts.append({**item, "sold": sold, "prev_qty": base_qty, "minutes": (now - base_ts) // 60})
            new_baseline[item_id] = f"{qty}|{now}"
        elif now - base_ts >= window_minutes * 60:
            new_baseline[item_id] = f"{qty}|{now}"
        else:
            new_baseline[item_id] = prev

    alerts.sort(key=lambda a: a["sold"], reverse=True)
    return alerts, new_baseline


def build_alert_messages(alerts: List[Dict], catalog_key: str, max_len: int = 1900) -> List[str]:
    """Bullet-style Discord messages, split so each stays under Discord's 2000-char limit."""
    header = f"🔥 **AccsMarket – hàng tốt đang bán nhanh** ({catalog_key})"
    blocks = []
    for i, a in enumerate(alerts, 1):
        title = a.get("title") or ""
        if len(title) > 140:
            title = title[:140] + "…"
        # masked link on the heading (<...> suppresses Discord's link preview)
        blocks.append("\n".join([
            f"**{i}. [#{a['id']} — ${a['price_from']:g}/pc](<{a['url']}>)**",
            title,
            f"📉 Đã bán **{a['sold']}** ({a['prev_qty']} → {a['qty']}) trong {a['minutes']} phút"
            f" | ⭐ {a['rating']:g} | ❌ lỗi {a['reject_rate_pct']:g}% | 🛒 {a['orders']} đơn | 🛡 {a['guarantee']}",
        ]))

    messages, current = [], header
    for block in blocks:
        if len(current) + len(block) + 2 > max_len:
            messages.append(current)
            current = block
        else:
            current += "\n\n" + block
    messages.append(current)
    return messages
