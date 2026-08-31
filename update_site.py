#!/usr/bin/env python3
"""
Refresh index.html (the live partner-facing site) with current shipment
progress from archive.org. This is the version that runs in CI (GitHub
Actions) — it has no dependency on any local files, only what's checked
into this repo (this script, index.html, stub_seeds.json).

Run manually with:

    python3 update_site.py

Normally run automatically by .github/workflows/update.yml on a schedule.

It pulls every SEARCHABLE archive.org item in the target collection, groups
by the grouping field (e.g. shiptracking), and additionally recovers "stub"
items that archive.org excludes from search entirely (metadata field
noindex:true — commonly used for items that are received/reserved but not
yet fully processed). Those stubs are invisible to any search query, so this
script finds them a different way: for group codes whose searchable
identifiers follow a detectable "prefix + sequential number" pattern (e.g.
smithfam04, smithfam05, ...), it directly probes archive.org/metadata/<id>
for the full number range (including gaps and a run past the highest known
number) to recover the true total. Group codes with no searchable items at
all (a brand new shipment that hasn't had anything indexed yet) can only be
found this way if you seed one known identifier for them in
stub_seeds.json — see that file for the format.

A shipment counts as "active" if it has had a completion or a newly-added
stub item within ACTIVE_WINDOW_DAYS.

Everything else in the HTML (layout, styling) is left untouched — this
script only ever rewrites the single-line `const SHIPMENTS = {...};` and
`const SNAPSHOT_DATE = "...";` statements.

See the skill's SKILL.md and README.md before adapting the CONFIG block
below to a new partner/collection — in particular, verify the noindex/stub
phenomenon actually exists for the new collection before assuming this whole
pipeline is necessary; some collections may not need it at all (see
"Step 2: check whether stub discovery is even needed" in SKILL.md).
"""

import json
import re
import sys
import time
import urllib.request
import urllib.parse
from collections import defaultdict
from datetime import date, datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================== CONFIG ======================================
# Adjust these for a new partner/collection. Everything below this block is
# domain-generic and should not need to change.

SITE_PATH = "index.html"          # the HTML file this script rewrites
SEEDS_PATH = "newberry_stub_seeds.json"  # not expected to be needed here — see README.md

COLLECTION_QUERY = "collection:newberry AND scanningcenter:indiana"
GROUPING_FIELD = "shiptracking"
COMPLETE_FIELD = "repub_state"
COMPLETE_VALUE = "19"

# Fields pulled from the bulk search index. Must include the identifier,
# GROUPING_FIELD, COMPLETE_FIELD, and any date fields used for recency below.
FIELDS = f"identifier,{GROUPING_FIELD},{COMPLETE_FIELD},republisher_date,publicdate"

ACTIVE_WINDOW_DAYS = 90
# =============================================================================

SCRAPE_URL = "https://archive.org/services/search/v1/scrape"
METADATA_URL = "https://archive.org/metadata/"

ID_PATTERN = re.compile(r"^([a-zA-Z]+?)(\d+)$")


# ---------- Bulk fetch of the searchable index ----------

def fetch_scrape_page(params, retries=4):
    url = SCRAPE_URL + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "partner-site-update/1.0"})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            if attempt == retries - 1:
                raise
            print(f"  (page fetch failed: {e} — retrying, attempt {attempt + 2}/{retries})", flush=True)
            time.sleep(2 * (attempt + 1))


def fetch_all_items():
    print(f"Fetching searchable items from archive.org ({COLLECTION_QUERY})...")
    items = []
    cursor = None
    page = 0
    total = None
    while True:
        page += 1
        params = {"q": COLLECTION_QUERY, "count": "10000", "fields": FIELDS}
        if cursor:
            params["cursor"] = cursor
        data = fetch_scrape_page(params)
        total = data.get("total")
        page_items = data.get("items", [])
        items.extend(page_items)
        print(f"  page {page}: {len(items):,} / {total:,}", flush=True)
        cursor = data.get("cursor")
        if not cursor or not page_items:
            break
        time.sleep(0.2)
    return items


def parse_republisher_date(value):
    if not value:
        return None
    try:
        return datetime.strptime(value[:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def parse_publicdate(value):
    if not value:
        return None
    try:
        return datetime.strptime(value[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


# ---------- Stub discovery (noindex items invisible to search) ----------

def fetch_metadata(identifier, retries=3, timeout=15):
    url = METADATA_URL + urllib.parse.quote(identifier)
    for _ in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "partner-site-update/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            return data if data else None
        except Exception:
            continue
    return "FAIL"


def detect_padding_width(numbers_and_strings):
    smallest = min(numbers_and_strings, key=lambda t: t[0])
    return len(smallest[1])


def format_candidate_id(prefix, n, width):
    if n < 10 ** width:
        return prefix + str(n).zfill(width)
    return prefix + str(n)


def enumerate_full_shipment(prefix, known_numbers, batch_size=15, max_extra_batches=8):
    """
    known_numbers: {number: {"identifier", COMPLETE_FIELD, "publicdate"}} already known.
    Returns the merged full set including any newly-discovered stub items.
    """
    numbers = sorted(known_numbers.keys())
    width = detect_padding_width([(n, known_numbers[n]["identifier"][len(prefix):]) for n in numbers])
    hi = numbers[-1]

    result = dict(known_numbers)
    to_probe = [n for n in range(1, hi + 1) if n not in result]

    def probe(n):
        ident = format_candidate_id(prefix, n, width)
        data = fetch_metadata(ident)
        if data in (None, "FAIL"):
            return (n, None)
        md = data.get("metadata", {})
        return (n, {"identifier": ident, COMPLETE_FIELD: md.get(COMPLETE_FIELD), "publicdate": md.get("publicdate")})

    if to_probe:
        with ThreadPoolExecutor(max_workers=6) as ex:
            futures = [ex.submit(probe, n) for n in to_probe]
            for fut in as_completed(futures):
                n, data = fut.result()
                if data:
                    result[n] = data

    # extend past the highest known number in parallel batches, stop once a whole batch misses
    n = hi + 1
    for _ in range(max_extra_batches):
        batch = list(range(n, n + batch_size))
        hits = 0
        with ThreadPoolExecutor(max_workers=6) as ex:
            futures = [ex.submit(probe, b) for b in batch]
            for fut in as_completed(futures):
                bn, data = fut.result()
                if data:
                    result[bn] = data
                    hits += 1
        n += batch_size
        if hits == 0:
            break

    return result


def find_enumerable_candidates(indexed_by_code):
    """Detect group codes whose identifiers show a clean prefix+number pattern."""
    candidates = {}
    for code, entries in indexed_by_code.items():
        matches = [(e, ID_PATTERN.match(e["identifier"])) for e in entries]
        good = [(e, m) for e, m in matches if m]
        if len(good) < 1 or len(good) < 0.9 * len(entries):
            continue
        prefixes = set(m.group(1).lower() for _, m in good)
        if len(prefixes) != 1:
            continue
        prefix = next(iter(prefixes))
        numbers = {}
        for e, m in good:
            numbers[int(m.group(2))] = {"identifier": e["identifier"], COMPLETE_FIELD: e.get(COMPLETE_FIELD), "publicdate": e.get("publicdate")}
        lo, hi = min(numbers), max(numbers)
        span = hi - lo + 1
        ratio = span / len(numbers)
        # lo<=5 and ratio<=8 are the two safety filters that separate real
        # sequential batches from coincidental digit suffixes (e.g. a
        # periodical volume identifier ending in a year). Validate these
        # against your own collection's data before trusting them blindly —
        # see "Step 3" in SKILL.md.
        if lo <= 5 and ratio <= 8:
            candidates[code] = (prefix, numbers)
    return candidates


def load_seeds():
    try:
        with open(SEEDS_PATH, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


# ---------- Aggregation ----------

def build_shipments_data(items):
    indexed_by_code = defaultdict(list)
    for it in items:
        code = it.get(GROUPING_FIELD)
        if code:
            indexed_by_code[code].append(it)

    candidates = find_enumerable_candidates(indexed_by_code)

    seeds = load_seeds()
    for code, seed in seeds.items():
        if code not in candidates:
            prefix = seed["prefix"]
            seed_id = seed["seed_identifier"]
            data = fetch_metadata(seed_id)
            numbers = {}
            if data and data != "FAIL":
                md = data.get("metadata", {})
                numbers[seed["seed_number"]] = {"identifier": seed_id, COMPLETE_FIELD: md.get(COMPLETE_FIELD), "publicdate": md.get("publicdate")}
            else:
                print(f"  WARNING: seed identifier '{seed_id}' for {code} could not be fetched — skipping stub discovery for this shipment.")
            if numbers:
                candidates[code] = (prefix, numbers)

    print(f"Detected {len(candidates)} group codes eligible for stub discovery (incl. {len(seeds)} seeded).")

    groups = {}

    # Codes with a detectable/seedable pattern: enumerate the true full set.
    for code, (prefix, numbers) in candidates.items():
        print(f"  enumerating {code} (prefix={prefix})...", flush=True)
        full = enumerate_full_shipment(prefix, numbers)
        total = len(full)
        completed = sum(1 for v in full.values() if v.get(COMPLETE_FIELD) == COMPLETE_VALUE)
        last_republish = None
        last_added = None
        for v in full.values():
            pd = parse_publicdate(v.get("publicdate"))
            if pd and (last_added is None or pd > last_added):
                last_added = pd
        # republisher_date is only present in the bulk index fields, not the per-item probe results
        for it in indexed_by_code.get(code, []):
            rd = parse_republisher_date(it.get("republisher_date"))
            if rd and (last_republish is None or rd > last_republish):
                last_republish = rd
        groups[code] = {
            "total": total,
            "completed": completed,
            "last_republish": last_republish,
            "last_added": last_added,
            "discovery": "enumerated",
        }

    # Everything else: indexed-only counts (search-based, may undercount hidden stubs).
    for code, entries in indexed_by_code.items():
        if code in groups:
            continue
        total = len(entries)
        completed = sum(1 for e in entries if e.get(COMPLETE_FIELD) == COMPLETE_VALUE)
        last_republish = None
        last_added = None
        for e in entries:
            rd = parse_republisher_date(e.get("republisher_date"))
            if rd and (last_republish is None or rd > last_republish):
                last_republish = rd
            pd = parse_publicdate(e.get("publicdate"))
            if pd and (last_added is None or pd > last_added):
                last_added = pd
        groups[code] = {
            "total": total,
            "completed": completed,
            "last_republish": last_republish,
            "last_added": last_added,
            "discovery": "indexed-only",
        }

    cutoff = datetime.now(timezone.utc) - timedelta(days=ACTIVE_WINDOW_DAYS)

    active = []
    for code, g in groups.items():
        recent = (g["last_republish"] and g["last_republish"] >= cutoff) or (g["last_added"] and g["last_added"] >= cutoff)
        if recent:
            last_dates = [d for d in (g["last_republish"], g["last_added"]) if d]
            active.append({
                "code": code,
                "total": g["total"],
                "completed": g["completed"],
                "discovery": g["discovery"],
                "last_activity": max(last_dates).strftime("%Y-%m-%d") if last_dates else None,
            })

    active.sort(key=lambda s: (s["completed"] / s["total"] if s["total"] else 0))

    total_items = sum(s["total"] for s in active)
    total_completed = sum(s["completed"] for s in active)

    return {
        "generated_note": f"Snapshot of archive.org metadata for {COLLECTION_QUERY}, grouped by {GROUPING_FIELD}, including stub items recovered via direct identifier discovery where possible",
        "active_window_days": ACTIVE_WINDOW_DAYS,
        "shipment_count": len(active),
        "total_items": total_items,
        "total_completed": total_completed,
        "shipments": active,
    }


def inject(html, data, snapshot_date):
    data_json = json.dumps(data, separators=(",", ":"))

    html, n1 = re.subn(
        r"^const SHIPMENTS = .*;$",
        "const SHIPMENTS = " + data_json.replace("\\", "\\\\") + ";",
        html, count=1, flags=re.M,
    )
    html, n2 = re.subn(
        r'^const SNAPSHOT_DATE = ".*";$',
        f'const SNAPSHOT_DATE = "{snapshot_date}";',
        html, count=1, flags=re.M,
    )

    if not (n1 and n2):
        raise RuntimeError(
            f"Could not find expected markers in {SITE_PATH} "
            f"(SHIPMENTS matched {n1}, SNAPSHOT_DATE matched {n2}). "
            "The file may have been edited in a way that moved/renamed these lines."
        )
    return html


def main():
    try:
        with open(SITE_PATH, encoding="utf-8") as f:
            html = f.read()
    except FileNotFoundError:
        print(f"Could not find {SITE_PATH} in the current directory.")
        sys.exit(1)

    items = fetch_all_items()
    data = build_shipments_data(items)
    snapshot_date = date.today().strftime("%B %-d, %Y")

    required_keys = {"active_window_days", "shipment_count", "total_items", "total_completed", "shipments"}
    missing = required_keys - data.keys()
    if missing:
        print(f"Refusing to write site: built data is missing expected keys: {sorted(missing)}")
        sys.exit(1)

    new_html = inject(html, data, snapshot_date)

    with open(SITE_PATH, "w", encoding="utf-8") as f:
        f.write(new_html)

    print()
    print("Done. Dashboard updated:")
    print(f"  Active shipments (last {ACTIVE_WINDOW_DAYS} days): {data['shipment_count']}")
    print(f"  Items completed / total: {data['total_completed']:,} / {data['total_items']:,}")
    print(f"  Snapshot date: {snapshot_date}")
    for s in data["shipments"]:
        flag = "" if s["discovery"] == "enumerated" else "  (indexed-only, may undercount stubs)"
        print(f"    {s['code']:12s} {s['completed']:4d} / {s['total']:4d}{flag}")
    print()


if __name__ == "__main__":
    main()
