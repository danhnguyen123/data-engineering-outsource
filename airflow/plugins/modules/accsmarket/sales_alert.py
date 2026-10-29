"""
Detect "good" AccsMarket items worth buying by comparing stock with a per-item baseline.

Baseline (stored in Redis as a hash id -> "qty|epoch_seconds"):
  - first seen                            -> set baseline; alert "new" if the item passes the filter
  - qty went up                           -> reset baseline; alert "restock" if it was sold out (0) and passes the filter
  - sold = baseline_qty - qty >= min_sold -> alert "fast" if the item passes the filter, reset baseline
  - baseline older than the window        -> reset baseline (sales are counted within the window only)
"""
from typing import Dict, List, Tuple

# Alert kinds, in the order they appear in the Discord message
FAST, RESTOCK, NEW = "fast", "restock", "new"
SECTION_TITLES = {
    FAST: "📉 **Đang bán nhanh**",
    RESTOCK: "♻️ **Vừa có hàng lại**",
    NEW: "🆕 **Hàng tốt mới phát hiện**",
}


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
    """Return (alerts, new_baseline). Each alert is the item dict plus a "kind" (fast / restock / new)."""
    alerts = []
    new_baseline = {}

    for item in items:
        item_id, qty = item.get("id"), item.get("qty")
        if not item_id or qty is None:
            continue
        good = is_good_item(item, min_orders, max_reject_rate_pct)

        prev = baseline.get(item_id)
        if not prev:
            new_baseline[item_id] = f"{qty}|{now}"
            if good:
                alerts.append({**item, "kind": NEW})
            continue

        base_qty, base_ts = (int(x) for x in prev.split("|"))
        if qty > base_qty:
            # restocked -> start a new baseline
            new_baseline[item_id] = f"{qty}|{now}"
            if base_qty == 0 and good:
                alerts.append({**item, "kind": RESTOCK})
            continue

        sold = base_qty - qty
        if sold >= min_sold:
            if good:
                alerts.append({**item, "kind": FAST, "sold": sold, "prev_qty": base_qty,
                               "minutes": (now - base_ts) // 60})
            new_baseline[item_id] = f"{qty}|{now}"
        elif now - base_ts >= window_minutes * 60:
            new_baseline[item_id] = f"{qty}|{now}"
        else:
            new_baseline[item_id] = prev

    order = {FAST: 0, RESTOCK: 1, NEW: 2}
    alerts.sort(key=lambda a: (order[a["kind"]], -a.get("sold", 0), -(a.get("qty") or 0)))
    return alerts, new_baseline


def _num(value) -> str:
    return "N/A" if value is None else f"{value:g}"


def build_alert_messages(alerts: List[Dict], catalog_key: str, max_len: int = 1900) -> List[str]:
    """Bullet-style Discord messages grouped by alert kind, split to stay under Discord's 2000-char limit."""
    header = f"🔥 **AccsMarket – cảnh báo hàng tốt** ({catalog_key})"
    blocks, current_kind = [], None
    for i, a in enumerate(alerts, 1):
        title = a.get("title") or ""
        if len(title) > 140:
            title = title[:140] + "…"

        if a["kind"] == FAST:
            stock_line = f"📉 Đã bán **{a['sold']}** ({a['prev_qty']} → {a['qty']}) trong {a['minutes']} phút"
        elif a["kind"] == RESTOCK:
            stock_line = f"📦 Có lại **{a['qty']}** cái"
        else:
            stock_line = f"📦 Còn **{a['qty']}** cái"

        lines = []
        if a["kind"] != current_kind:
            current_kind = a["kind"]
            lines.append(SECTION_TITLES[current_kind])
        lines += [
            # masked link on the heading (<...> suppresses Discord's link preview)
            f"**{i}. [#{a['id']} — ${_num(a['price_from'])}/pc](<{a['url']}>)**",
            title,
            f"{stock_line} | ⭐ {_num(a['rating'])} | ❌ lỗi {_num(a['reject_rate_pct'])}%"
            f" | 🛒 {a['orders']} đơn | 🛡 {a['guarantee']}",
        ]
        blocks.append("\n".join(lines))

    messages, current = [], header
    for block in blocks:
        if len(current) + len(block) + 2 > max_len:
            messages.append(current)
            current = block
        else:
            current += "\n\n" + block
    messages.append(current)
    return messages
