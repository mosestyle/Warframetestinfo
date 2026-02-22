#!/usr/bin/env python3
import json
import os
import re
import time
import urllib.parse
import urllib.request
import urllib.error
from typing import Any, Dict, List, Optional, Tuple

DATA_DIR = "data"
RELICS_OUT = os.path.join(DATA_DIR, "Relics.min.json")
PRICES_OUT = os.path.join(DATA_DIR, "prices.json")

MISSING_TXT = os.path.join(DATA_DIR, "missing_prices.txt")
MISSING_JSON = os.path.join(DATA_DIR, "missing_prices.json")

# -------------------- Relics sources --------------------
# FULL relic rewards (includes vaulted + old relics) from WFCD warframe-drop-data
RELICS_ALL_URL = "https://raw.githubusercontent.com/WFCD/warframe-drop-data/master/data/relics.json"

# Vault flag source (smaller list, but has "vaulted": true/false)
RELICS_VAULT_MAP_URL = (
    "https://raw.githubusercontent.com/WFCD/warframe-relic-data/master/data/Relics.min.json"
)

# -------------------- Warframe.market endpoints --------------------
# Use ONLY statistics endpoint (orders endpoint can 403 from GitHub Actions IPs)
WM_BASE = "https://api.warframe.market/v1"
WM_ITEM_STATS = f"{WM_BASE}/items/{{url_name}}/statistics"

UA = "mosestyle-warframe-relic/2.5 (+github pages actions)"

WM_PLATFORM = "pc"
WM_LANGUAGE = "en"

# Throttling
SLEEP_BETWEEN_WM_CALLS = 0.40  # ~2.5 req/sec
HTTP_TIMEOUT = 60

TIER_ORDER = {"Lith": 0, "Meso": 1, "Neo": 2, "Axi": 3}


def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def http_json(url: str, timeout: int = HTTP_TIMEOUT, attempts: int = 4) -> Any:
    """
    Fetch JSON with retries/backoff.
    Handles transient 429/5xx/connection issues.
    """
    last_err = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": UA,
                    "Accept": "application/json,text/plain,*/*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Connection": "close",
                    # WM headers (harmless for WFCD endpoints)
                    "Platform": WM_PLATFORM,
                    "Language": WM_LANGUAGE,
                    # Helps with some WAF rules (doesn't hurt):
                    "Referer": "https://warframe.market/",
                    "Origin": "https://warframe.market",
                },
            )
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read().decode("utf-8", errors="replace")
                return json.loads(raw)

        except urllib.error.HTTPError as e:
            last_err = e
            if e.code in (429, 500, 502, 503, 504):
                time.sleep(1.5 ** i)
                continue
            raise

        except Exception as e:
            last_err = e
            time.sleep(1.5 ** i)

    if last_err:
        raise last_err
    raise RuntimeError("http_json failed with unknown error")


# -------------------- Relics parsing --------------------

def build_vaulted_map() -> Dict[str, bool]:
    """
    Map "Axi A1" -> vaulted True/False from WFCD warframe-relic-data (limited list).
    """
    try:
        payload = http_json(RELICS_VAULT_MAP_URL)
    except Exception:
        return {}

    m: Dict[str, bool] = {}
    if isinstance(payload, list):
        for r in payload:
            if not isinstance(r, dict):
                continue
            name = r.get("name")
            vaulted = r.get("vaulted")
            if isinstance(name, str) and name.strip() and isinstance(vaulted, bool):
                m[name.strip()] = vaulted
    return m


def build_relics_min() -> List[Dict[str, Any]]:
    """
    Writes data/Relics.min.json in UI-friendly format:
    [
      {"tier":"Axi","name":"A1","vaulted":true,"rewards":[{"item":"...","chance":25.33,"type":"Uncommon"}, ...]},
      ...
    ]

    Source: WFCD warframe-drop-data relics.json (ALL relics).
    Keep ONLY state == "Intact" to avoid duplicates of Exceptional/Flawless/Radiant.
    """
    print("Downloading ALL relics (WFCD warframe-drop-data /data/relics.json)...")
    payload = http_json(RELICS_ALL_URL)

    if not isinstance(payload, dict) or "relics" not in payload or not isinstance(payload["relics"], list):
        raise RuntimeError("Unexpected format for relics.json (expected { relics: [...] }).")

    vault_map = build_vaulted_map()
    if vault_map:
        print(f"Vault map loaded: {len(vault_map)} entries")
    else:
        print("Vault map not available (will default unknown relics to vaulted=True).")

    out: List[Dict[str, Any]] = []
    seen = set()

    for r in payload["relics"]:
        if not isinstance(r, dict):
            continue

        if (r.get("state") or "").strip() != "Intact":
            continue

        tier = (r.get("tier") or "").strip()
        code = (r.get("relicName") or "").strip()
        if not tier or not code:
            continue

        full_name = f"{tier} {code}".strip()
        if full_name in seen:
            continue
        seen.add(full_name)

        rewards = r.get("rewards") or []
        if not isinstance(rewards, list) or not rewards:
            continue

        out_rewards = []
        for rw in rewards:
            if not isinstance(rw, dict):
                continue
            item = (rw.get("itemName") or rw.get("item") or rw.get("name") or "").strip()
            if not item:
                continue

            chance = rw.get("chance")
            try:
                chance = float(chance) if chance is not None else None
            except Exception:
                chance = None

            rtype = (rw.get("rarity") or rw.get("type") or "").strip()
            out_rewards.append({"item": item, "chance": chance, "type": rtype})

        if not out_rewards:
            continue

        vaulted = vault_map.get(full_name, True)

        out.append({"tier": tier, "name": code, "vaulted": vaulted, "rewards": out_rewards})

    def sort_key(x: Dict[str, Any]) -> Tuple[int, str]:
        t = x.get("tier") or ""
        n = x.get("name") or ""
        return (TIER_ORDER.get(t, 99), n)

    out.sort(key=sort_key)

    ensure_data_dir()
    with open(RELICS_OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))

    print(f"Relics written: {len(out)} -> {RELICS_OUT}")

    if len(out) == 0:
        raise RuntimeError("Relics list is empty after parsing. Aborting so we don't publish [].")

    return out


def unique_reward_items(relics_min: List[Dict[str, Any]]) -> List[str]:
    s = set()
    for r in relics_min:
        for rw in r.get("rewards", []):
            it = rw.get("item")
            if it:
                s.add(it)
    return sorted(s)


# -------------------- Warframe.market pricing --------------------

def guess_wm_url_name(item_name: str) -> str:
    """
    Typical WM url_name:
      lowercase + underscores, stripping punctuation.
    """
    s = (item_name or "").strip().lower()
    s = s.replace("&", "and")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


# Manual overrides (keep empty; Option B auto-candidates handles most issues)
WM_URL_OVERRIDES: Dict[str, str] = {
    # You can still pin special cases here if you want:
    # "Kompressa Prime Receiver": "kompressa_prime_reciever",
}


def wm_url_candidates(item_name: str) -> List[str]:
    """
    Generate possible WM url_name candidates.
    This is how we fix cases like:
      receiver -> reciever  (known WM typo on some items)
    """
    base = WM_URL_OVERRIDES.get(item_name) or guess_wm_url_name(item_name)
    cands = [base]

    # Known WM typo: receiver -> reciever
    if base.endswith("_receiver"):
        cands.append(base[:-len("_receiver")] + "_reciever")

    # Also handle receiver in the middle (rare)
    if "_receiver_" in base:
        cands.append(base.replace("_receiver_", "_reciever_"))

    # De-dupe preserving order
    seen = set()
    out = []
    for c in cands:
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out


def _median_from_stats_section(stats_section: Any, window_key: str) -> Optional[int]:
    """
    stats_section = payload['statistics_closed'] or payload['statistics_open']
    window_key: '90days' or '48hours'
    """
    if not isinstance(stats_section, dict):
        return None
    arr = stats_section.get(window_key) or []
    if not isinstance(arr, list) or not arr:
        return None
    last = arr[-1]
    if not isinstance(last, dict):
        return None
    med = last.get("median")
    if med is None:
        return None
    try:
        return int(round(float(med)))
    except Exception:
        return None


def wm_price_from_statistics(url_name: str) -> Optional[int]:
    """
    Primary: statistics_closed 90days median
    Fallback 1: statistics_open 90days median
    Fallback 2: statistics_closed 48hours median
    Fallback 3: statistics_open 48hours median
    """
    url = WM_ITEM_STATS.format(url_name=urllib.parse.quote(url_name))
    try:
        payload = http_json(url)
    except urllib.error.HTTPError as e:
        if e.code in (404, 403):
            return None
        raise

    try:
        p = payload.get("payload", {})
        closed = p.get("statistics_closed")
        open_ = p.get("statistics_open")

        v = _median_from_stats_section(closed, "90days")
        if v is not None:
            return v

        v = _median_from_stats_section(open_, "90days")
        if v is not None:
            return v

        v = _median_from_stats_section(closed, "48hours")
        if v is not None:
            return v

        v = _median_from_stats_section(open_, "48hours")
        if v is not None:
            return v

        return None
    except Exception:
        return None


def build_prices_from_wm_statistics(relics_min: List[Dict[str, Any]]) -> Tuple[Dict[str, int], List[str]]:
    reward_items = unique_reward_items(relics_min)
    print(f"Unique reward items to price: {len(reward_items)}")

    prices: Dict[str, int] = {}
    missing_items: List[str] = []

    for i, item_name in enumerate(reward_items, start=1):
        v: Optional[int] = None

        # Try candidate slugs until one returns a price
        for url_name in wm_url_candidates(item_name):
            try:
                v = wm_price_from_statistics(url_name)
            except urllib.error.HTTPError as e:
                if e.code in (429, 500, 502, 503, 504):
                    # transient — wait a bit and give up on this item for now
                    time.sleep(1.25)
                    v = None
                    break
                raise

            if v is not None:
                break

        if v is None:
            missing_items.append(item_name)
        else:
            prices[item_name] = v

        if i % 25 == 0:
            print(f"  {i}/{len(reward_items)} priced={len(prices)} missing={len(missing_items)}")

        time.sleep(SLEEP_BETWEEN_WM_CALLS)

    print(f"WM pricing done: {len(prices)}/{len(reward_items)} priced. Missing={len(missing_items)}")
    return prices, missing_items


def write_missing_debug(missing_items: List[str]) -> None:
    ensure_data_dir()

    # Text file for quick eyeballing
    missing_sorted = sorted(set(missing_items))
    with open(MISSING_TXT, "w", encoding="utf-8") as f:
        for name in missing_sorted:
            f.write(name + "\n")

    # JSON for future tooling
    with open(MISSING_JSON, "w", encoding="utf-8") as f:
        json.dump(missing_sorted, f, ensure_ascii=False, indent=2)

    print(f"Missing prices written: {len(missing_sorted)} -> {MISSING_TXT} (+ {MISSING_JSON})")


# -------------------- Main --------------------

def main():
    relics_min = build_relics_min()

    prices, missing_items = build_prices_from_wm_statistics(relics_min)

    ensure_data_dir()
    with open(PRICES_OUT, "w", encoding="utf-8") as f:
        json.dump(prices, f, ensure_ascii=False, separators=(",", ":"))
    print(f"Prices written: {len(prices)} -> {PRICES_OUT}")

    write_missing_debug(missing_items)

    # Safety: if something went wrong and we priced almost nothing, fail the workflow
    if len(prices) < 25:
        raise RuntimeError(
            f"Too few prices ({len(prices)}). warframe.market calls may be failing, or endpoint changed."
        )

    print("Done.")


if __name__ == "__main__":
    main()
