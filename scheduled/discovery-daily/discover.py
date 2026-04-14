"""
discovery-daily/discover.py
Roxton Web — IG Lead Discovery

Flow:
  1. Load existing queue from GitHub (dedup)
  2. WebSearch 35 searches (5 niches x 7 cities) → collect candidate IG usernames
  3. Pre-filter candidates via snippet (website check + positive business signal)
  4. Apify Instagram Scraper → bulk profile pull (followers, bio, externalUrl)
  5. Filter: no website + 50-20k followers + has bio
  6. Score (threshold 3), assign template, resolve DM
  7. Merge into queue, write to GitHub
  8. Print report

Requirements: pip install requests python-dotenv
"""

import os
import sys
import json
import time
import base64
import random
import re
from datetime import datetime, date
from pathlib import Path
from dotenv import load_dotenv
import requests

# ── Config ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / "pipeline" / ".env")

APIFY_TOKEN   = os.getenv("APIFY_TOKEN", "")
GITHUB_TOKEN  = os.getenv("GITHUB_TOKEN", "")
GH_REPO       = "roxtonweb/roxtonweb.github.io"
GH_BRANCH     = "main"
QUEUE_PATH    = "queue/ig-queue.json"
EVENTS_PATH   = "queue/events.jsonl"
TARGET_NEW    = 30        # target per run
MIN_FOLLOWERS = 50
MAX_FOLLOWERS = 20_000
MIN_SCORE     = 3         # was 4 — still quality, just less aggressive

# Local username cache — prevents re-enriching profiles Apify already returned today.
# Stored at pipeline/username_cache.json as a flat list of strings (lowercased, no @).
CACHE_PATH    = ROOT / "pipeline" / "username_cache.json"

GH_HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
}

TODAY = date.today().isoformat()

# ── Niche / City rotations ────────────────────────────────────────────────────
SEARCH_QUERIES = {
    "auto detailing": [
        '"auto detailing" "{city}" site:instagram.com',
        '"mobile detailing" "{city}" site:instagram.com',
        '"car detailing" "{city}" site:instagram.com',
        '"auto detailing" "{city}" "dm" OR "book" site:instagram.com',
        '"mobile auto detailing" "{city}" site:instagram.com',
        '"ceramic coating" "{city}" site:instagram.com',
        '"paint correction" "{city}" site:instagram.com',
    ],
    "pressure washing": [
        '"pressure washing" "{city}" site:instagram.com',
        '"soft washing" "{city}" site:instagram.com',
        '"power washing" "{city}" site:instagram.com',
        '"pressure washing" "{city}" "dm" OR "quote" site:instagram.com',
        '"house washing" "{city}" site:instagram.com',
        '"exterior cleaning" "{city}" site:instagram.com',
    ],
    "lawn care": [
        '"lawn care" "{city}" site:instagram.com',
        '"landscaping" "{city}" site:instagram.com',
        '"lawn mowing" "{city}" site:instagram.com',
        '"lawn maintenance" "{city}" site:instagram.com',
        '"lawn care" "{city}" "dm" OR "quote" site:instagram.com',
        '"grass cutting" "{city}" site:instagram.com',
    ],
    "med spa": [
        '"med spa" "{city}" site:instagram.com',
        '"medspa" "{city}" site:instagram.com',
        '"botox" "{city}" site:instagram.com',
        '"medical spa" "{city}" site:instagram.com',
        '"aesthetic" "{city}" "dm to book" site:instagram.com',
    ],
    "junk removal": [
        '"junk removal" "{city}" site:instagram.com',
        '"hauling" "{city}" site:instagram.com',
        '"junk removal" "{city}" "dm" OR "text" site:instagram.com',
        '"debris removal" "{city}" site:instagram.com',
        '"junk hauling" "{city}" site:instagram.com',
    ],
}

NICHES = ["auto detailing", "pressure washing", "lawn care", "med spa", "junk removal"]

CITIES = [
    "Atlanta GA", "Phoenix AZ", "Dallas TX", "Charlotte NC", "Tampa FL",
    "Denver CO", "Nashville TN", "Austin TX", "San Antonio TX", "Orlando FL",
    "Portland OR", "Minneapolis MN", "St. Louis MO", "Kansas City MO", "Raleigh NC",
    "Las Vegas NV", "Henderson NV", "Scottsdale AZ", "Sacramento CA", "Columbus OH",
    "Jacksonville FL", "Fort Worth TX", "Memphis TN", "Oklahoma City OK", "Louisville KY",
]

NICHE_MAP = {
    "auto detailing":   "auto_detailing",
    "pressure washing": "pressure_washing",
    "lawn care":        "lawn_care",
    "med spa":          "med_spa",
    "junk removal":     "junk_removal",
}

TEMPLATES = {
    "auto_detailing":   ["DETAIL_A", "DETAIL_B", "DETAIL_C", "DETAIL_D", "DETAIL_E", "DETAIL_F"],
    "pressure_washing": ["WASH_A", "WASH_B", "WASH_C", "WASH_D"],
    "lawn_care":        ["LAWN_A", "LAWN_B", "LAWN_C", "LAWN_D"],
    "med_spa":          ["MEDSPA_A", "MEDSPA_B"],
    "junk_removal":     ["JUNK_A", "JUNK_B"],
}

DM_TEMPLATES = {
    # Rules: one paragraph, no line breaks, always "your business" (never actual name),
    # city-only (no state), "build a website" not "mock something up".
    "DETAIL_A": "I noticed your business doesn't have a website, is there a reason for that? I could build one out for free, booking page, packages, everything, and you decide from there if it's worth keeping.",
    "DETAIL_B": "How do people actually book with your business right now, like after seeing your page is it just DM? I could build a free website so that part is way easier, then you can decide from there.",
    "DETAIL_C": "You probably see this happen. People ask about pricing or availability and then just disappear. Usually it's not lost interest, just friction. I could build a free website for your business so they can actually book or request a quote right away, then you can decide if you want to use it.",
    "DETAIL_D": "I saw your work, it's clean. Only thing I noticed is there's nowhere obvious to send people when they're ready to book. I could build a free website for your business and you can decide from there.",
    "DETAIL_E": "Most detailers lose bookings not because someone wasn't interested but because there was nowhere obvious to go after seeing the page. I could build a free website for your business today, booking page, package list, everything, and you can check it out before deciding anything.",
    "DETAIL_F": "Searched for detailers in {city} and your page came up. Good work. Only thing missing is somewhere to send people who are ready to book right now instead of waiting in DM. I could build that out for free, takes about a day, and you decide from there if it's worth keeping.",
    "WASH_A":   "I noticed your business doesn't have a website. If someone wants pricing, where do they go right now? I could build a free website with a quote form and you can decide from there if it's worth using.",
    "WASH_B":   "How do people get pricing from your business right now, like if they're ready after seeing your page? Most people won't wait around in DM. I could build a free website so they can request a quote right away, then you decide from there.",
    "WASH_C":   "Most pressure washing companies in {city} either have no site or one that looks like it was built in 2012. Your business has good photos but nowhere to actually capture the lead. I could build a free website with a quote form and you can see it before committing to anything.",
    "WASH_D":   "Businesses in {city} lose quote requests every week to competitors who have an easy way to capture them online. I could build a free website for your business today, quote form, pricing, mobile-ready, and you decide if it makes sense to keep it.",
    "LAWN_A":   "I saw your business doesn't have a website. If someone finds your page and wants service, what do they do next? I could build a free website so they don't have to wait on DM, then you can decide from there.",
    "LAWN_B":   "How do people usually sign up with your business, is it all DM or do you send them somewhere? I could build a free website with a simple signup flow, then you can decide if it's worth it.",
    "LAWN_C":   "The biggest thing holding back recurring revenue for lawn care businesses is having nowhere for people to actually sign up when they find you. I could build a free website for your business with a recurring service option built in, and you can look at it before deciding anything.",
    "LAWN_D":   "Lawn season is starting and most people book whoever they can find online first. Your business has the work to back it up but nothing to capture those people when they're ready. I could build a free website today and you can decide from there.",
    "MEDSPA_A": "Quick question about your booking flow. If someone wants to book after seeing your page, where do they go? I could build a cleaner website for your business, free, and you can decide from there if it's a better fit.",
    "MEDSPA_B": "Med spas that don't have a clean booking page online lose consultations to ones that do, even if the actual service is worse. I could build a free website for your business today, intake form, clean layout, mobile-ready, and you can decide if it's a better fit than what you have now.",
    "JUNK_A":   "Quick question about same-day jobs. If someone needs something gone today and finds your page, what do they do next? I could build a free website so they can request a pickup right away, then you decide from there.",
    "JUNK_B":   "Same-day jobs go to whoever is easiest to reach online. Your business looks solid but there's nowhere obvious for someone to request a pickup right now without DMing and waiting. I could build a free website with pricing and a request form, and you can check it out before deciding anything.",
}

SERVICE_HINTS = ["full detail", "ceramic coating", "paint correction", "interior detail"]

# ── GitHub helpers ─────────────────────────────────────────────────────────────
def gh_get_file(path):
    r = requests.get(
        f"https://api.github.com/repos/{GH_REPO}/contents/{path}",
        headers=GH_HEADERS, params={"ref": GH_BRANCH}
    )
    if r.status_code == 404:
        return None, None
    r.raise_for_status()
    data = r.json()
    content = base64.b64decode(data["content"]).decode()
    return content, data["sha"]

def gh_put_file(path, content_str, sha, message):
    b64 = base64.b64encode(content_str.encode()).decode()
    payload = {"message": message, "content": b64, "branch": GH_BRANCH}
    if sha:
        payload["sha"] = sha
    r = requests.put(
        f"https://api.github.com/repos/{GH_REPO}/contents/{path}",
        headers=GH_HEADERS, json=payload
    )
    r.raise_for_status()
    return r.json()["commit"]["sha"]

# ── Step 0: Load queue ─────────────────────────────────────────────────────────
def load_queue():
    content, sha = gh_get_file(QUEUE_PATH)
    if content is None:
        print("FAIL: GitHub unreachable or queue missing")
        sys.exit(1)
    queue = json.loads(content)
    # Dedup against ALL statuses — including MANUAL_SENT
    existing_usernames = {e["username"].lstrip("@").lower() for e in queue}
    return queue, sha, existing_usernames

# ── Step 1: WebSearch for candidate usernames ──────────────────────────────────
SKIP_USERNAMES = {"p", "explore", "reel", "reels", "stories", "tv", "accounts"}

WEBSITE_SIGNAL_RE = re.compile(
    r'(?:https?://|www\.)[a-z0-9][-a-z0-9]*\.[a-z]{2,}(?:/[^\s]*)?', re.I
)
SOCIAL_DOMAINS = {"instagram.com", "yelp.com", "facebook.com", "fb.com",
                  "google.com", "linktr.ee", "linkin.bio", "beacons.ai",
                  "taplink.cc", "bio.site", "allmylinks.com", "bit.ly",
                  "amzn.to", "wa.me"}

def snippet_has_website(result):
    """Return True if the search snippet/title shows a real business website."""
    text = " ".join([
        result.get("title", ""),
        result.get("snippet", ""),
        result.get("description", ""),
    ])
    for m in WEBSITE_SIGNAL_RE.finditer(text):
        url = m.group(0).lower()
        if not any(s in url for s in SOCIAL_DOMAINS):
            return True
    return False

BUSINESS_SIGNALS = [
    "dm", "book", "quote", "owner", "llc", "services", "call", "text",
    "client", "hire", "available", "serving", "based", "detailing", "washing",
    "lawn", "spa", "junk", "removal", "hauling", "ceramic", "pressure", "botox",
    "filler", "landscap", "mow", "aesthetic", "mobile", "local",
]

def snippet_looks_like_business(result):
    """Positive signal — snippet/title has at least one business indicator."""
    text = " ".join([
        result.get("title", ""),
        result.get("snippet", ""),
        result.get("description", ""),
    ]).lower()
    return any(s in text for s in BUSINESS_SIGNALS)

def search_candidates(niches_chosen, cities_chosen, existing_usernames):
    """
    Run up to 35 targeted WebSearch calls.
    5 niches x 7 cities, rotating query variants.
    Pre-filters via snippet (negative: has website, positive: business signal).
    Returns list of (username, niche_slug, city_str) tuples.
    """
    from WebSearch import search  # Claude's WebSearch tool — injected at runtime

    candidates = []
    seen = set(existing_usernames)
    skipped_has_website = 0
    skipped_no_signal = 0

    searches = []
    for niche in niches_chosen:
        variants = SEARCH_QUERIES.get(niche, [f'"{niche}" "{{city}}" site:instagram.com'])
        for city_idx, city in enumerate(cities_chosen[:7]):  # 7 cities per niche
            variant = variants[city_idx % len(variants)]
            query = variant.replace("{city}", city)
            searches.append((query, niche, city))

    random.shuffle(searches)
    searches = searches[:35]  # max 35 searches (up from 20)

    for query, niche, city in searches:
        try:
            results = search(query)
            added_this = 0
            for item in results:
                url = item.get("url", "") or item.get("link", "")
                m = re.search(r'instagram\.com/([A-Za-z0-9_.]+)', url)
                if not m:
                    continue
                username = m.group(1).lower()
                if username in SKIP_USERNAMES or username in seen:
                    continue
                # Negative: skip if snippet shows they already have a website
                if snippet_has_website(item):
                    skipped_has_website += 1
                    seen.add(username)
                    continue
                # Positive: only proceed if snippet looks like a real business
                if not snippet_looks_like_business(item):
                    skipped_no_signal += 1
                    seen.add(username)
                    continue
                seen.add(username)
                candidates.append((username, NICHE_MAP[niche], city))
                added_this += 1
            print(f"  [{niche} / {city}] +{added_this} candidates")
        except Exception as e:
            print(f"  WebSearch error ({niche} {city}): {e}")

    print(f"  Total: {len(candidates)} candidates (skipped: {skipped_has_website} had website, {skipped_no_signal} no business signal)")
    return candidates

# ── Step 2: Apify bulk profile enrichment ─────────────────────────────────────
def apify_get_profiles(usernames):
    if not APIFY_TOKEN:
        print("  WARN: No APIFY_TOKEN — skipping profile enrichment")
        return {}

    urls = [f"https://www.instagram.com/{u}/" for u in usernames]
    print(f"  Apify: enriching {len(urls)} profiles...")

    r = requests.post(
        "https://api.apify.com/v2/acts/apify~instagram-scraper/runs",
        params={"token": APIFY_TOKEN},
        json={
            "directUrls": urls,
            "resultsType": "details",
            "resultsLimit": len(urls),
            "proxy": {"useApifyProxy": True},
        },
        timeout=30,
    )
    r.raise_for_status()
    run_id = r.json()["data"]["id"]
    print(f"  Apify run started: {run_id}")

    for attempt in range(40):
        time.sleep(6)  # was 10s — faster polling, same 4-min window
        status_r = requests.get(
            f"https://api.apify.com/v2/actor-runs/{run_id}",
            params={"token": APIFY_TOKEN},
        )
        status = status_r.json()["data"]["status"]
        if status == "SUCCEEDED":
            break
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            print(f"  Apify run failed: {status}")
            return {}
        if attempt % 5 == 0:
            print(f"  Apify status: {status} (attempt {attempt+1}/40)")
    else:
        print("  Apify timed out")
        return {}

    items_r = requests.get(
        f"https://api.apify.com/v2/actor-runs/{run_id}/dataset/items",
        params={"token": APIFY_TOKEN},
    )
    items = items_r.json()
    print(f"  Apify returned {len(items)} profiles")

    profiles = {}
    for item in items:
        uname = item.get("username", "").lower()
        if uname:
            profiles[uname] = item
    return profiles

# ── Steps 3–5: Filter, score, assign template, resolve DM ─────────────────────
def is_real_website(url):
    if not url:
        return False
    blocked = ["facebook.com", "fb.com", "instagram.com", "yelp.com",
               "google.com", "linktr.ee", "linkin.bio", "beacons.ai",
               "taplink.cc", "bio.site", "allmylinks.com"]
    url_lower = url.lower()
    return not any(b in url_lower for b in blocked)

def score_lead(profile, niche_slug):
    score = 0
    followers = profile.get("followersCount", 0) or 0
    bio = profile.get("biography", "") or ""
    website = profile.get("externalUrl", "") or ""

    if MIN_FOLLOWERS <= followers <= 5000:
        score += 3
    elif followers <= MAX_FOLLOWERS:
        score += 1  # 5k-20k: still valuable, just lower signal
    if bio.strip():
        score += 2
    if niche_slug.replace("_", " ") in bio.lower() or any(
        kw in bio.lower() for kw in ["detail", "wash", "lawn", "spa", "junk", "haul", "removal",
                                      "pressure", "botox", "filler", "landscap", "mowing"]
    ):
        score += 2
    if profile.get("postsCount", 0) and profile["postsCount"] > 5:
        score += 1
    if not is_real_website(website):
        score += 2
    return score

def assign_variant(niche_slug, niche_counters):
    variants = TEMPLATES.get(niche_slug, ["DETAIL_A"])
    idx = niche_counters.get(niche_slug, 0) % len(variants)
    niche_counters[niche_slug] = idx + 1
    return variants[idx]

def resolve_dm(variant, business_name, city, service_hint, use_generic):
    """
    Always uses "your business" — never the actual business name in the DM.
    Strips state from city (e.g. "Tampa, FL" → "Tampa").
    """
    city_only = city.split(",")[0].strip()
    template = DM_TEMPLATES.get(variant, "")
    return template.format(
        business_name="your business",
        biz="your business",
        city=city_only,
        service_hint=service_hint,
    )

def _looks_like_username(name, username):
    """Returns True if the 'full name' is just a mangled version of the username."""
    clean_name = name.lower().replace(" ", "").replace("'", "").replace(".", "")
    clean_user = username.lower().replace("_", "").replace(".", "")
    return clean_name == clean_user or len(name.split()) == 1

def make_lead(username, profile, niche_slug, city, score, variant, hint_idx):
    raw_name = profile.get("fullName", "")
    if raw_name and not _looks_like_username(raw_name, username):
        full_name = raw_name
        use_generic = False
    else:
        full_name = username.replace("_", " ").title()
        use_generic = True  # name looks like a username — use "your business" in DM
    service_hint = SERVICE_HINTS[hint_idx % len(SERVICE_HINTS)] if niche_slug == "auto_detailing" else ""
    dm = resolve_dm(variant, full_name, city, service_hint, use_generic)
    # confidence_score: normalized 0-1 quality signal used by send-dms for prioritization
    # Combined with reply_rate_by_variant → final priority = confidence_score * reply_rate
    confidence_score = round(score / 10.0, 2)

    return {
        "username": f"@{username}",
        "business_name": full_name,
        "niche": niche_slug,
        "city": city,
        "followers": profile.get("followersCount", 0) or 0,
        "score": score,
        "confidence_score": confidence_score,
        "variant": variant,
        "service_hint": service_hint,
        "dm": dm,
        "status": "PENDING",
        "added": TODAY,
        "conversation_stage": None,
        "replied": None,
        "reply_text": None,
        "outcome": None,
        "auto_replied": False,
        "site_build_requested": False,
        "mockup_preference": None,
        "preview_url": None,
        "preview_sent_at": None,
        "post_preview_followup_count": 0,
        "tier": None,
        "payment_link": None,
        "payment_link_sent_at": None,
        "paid_at": None,
        "site_live_url": None,
        "revision_requested": False,
        "revision_notes": None,
    }

# ── Main ───────────────────────────────────────────────────────────────────────
def run():
    print(f"=== discovery-daily {TODAY} ===")

    print("\n[0] Loading queue from GitHub...")
    queue, queue_sha, existing_usernames = load_queue()
    print(f"  {len(queue)} existing entries, {len(existing_usernames)} unique usernames")

    # All 5 niches every run, 7 cities rotating daily = 35 searches max
    day_of_year = datetime.now().timetuple().tm_yday
    niches_chosen = NICHES  # all 5 every run
    cities_chosen = [CITIES[i % len(CITIES)] for i in range(day_of_year, day_of_year + 7)]
    print(f"\n[1] Niches: {niches_chosen}")
    print(f"    Cities: {cities_chosen}")

    # Load local username cache (avoids re-enriching usernames Apify already processed)
    try:
        with open(CACHE_PATH) as f:
            local_cache = set(json.load(f))
        print(f"  Username cache: {len(local_cache)} entries")
    except (FileNotFoundError, json.JSONDecodeError):
        local_cache = set()

    existing_usernames |= local_cache  # treat cached usernames as already seen

    print("\n[1] Searching for candidates via WebSearch...")
    try:
        candidates = search_candidates(niches_chosen, cities_chosen, existing_usernames)
    except ImportError:
        print("  WebSearch not available (not running inside Claude). Use mock candidates.")
        candidates = []

    if not candidates:
        print("  No candidates found — stopping.")
        return

    print("\n[2] Enriching profiles via Apify...")
    usernames = [u for u, _, _ in candidates]
    profiles = apify_get_profiles(usernames)

    apify_hit_rate = len(profiles) / len(usernames) * 100 if usernames else 0
    print(f"  Apify hit rate: {apify_hit_rate:.0f}% ({len(profiles)}/{len(usernames)})")

    # Save all enriched usernames to local cache so future runs skip them
    updated_cache = local_cache | set(profiles.keys()) | set(usernames)
    try:
        with open(CACHE_PATH, "w") as f:
            json.dump(sorted(updated_cache), f)
        print(f"  Cache updated: {len(updated_cache)} total entries")
    except Exception as e:
        print(f"  Cache write failed (non-fatal): {e}")

    print("\n[3] Filtering and scoring...")
    new_leads = []
    niche_counters = {}
    hint_idx = 0
    filtered_website = 0
    filtered_followers = 0
    filtered_score = 0
    filtered_no_data = 0
    filtered_private = 0

    for username, niche_slug, city in candidates:
        profile = profiles.get(username, {})

        if not profile:
            filtered_no_data += 1
            continue

        # Filter private accounts — Apify returns isPrivate field
        if profile.get("isPrivate", False):
            filtered_private += 1
            print(f"  SKIP private @{username}")
            continue

        followers = profile.get("followersCount", 0) or 0
        website = profile.get("externalUrl", "") or ""

        if is_real_website(website):
            filtered_website += 1
            continue
        if not (MIN_FOLLOWERS <= followers <= MAX_FOLLOWERS):
            filtered_followers += 1
            continue

        score = score_lead(profile, niche_slug)
        if score < MIN_SCORE:
            filtered_score += 1
            continue

        variant = assign_variant(niche_slug, niche_counters)
        lead = make_lead(username, profile, niche_slug, city, score, variant, hint_idx)
        new_leads.append(lead)
        hint_idx += 1

        print(f"  + @{username} ({niche_slug}, {city}, {followers} followers, score {score}, {variant})")

        if len(new_leads) >= TARGET_NEW:
            break

    print(f"\n  Added: {len(new_leads)}")
    print(f"  Filtered: private={filtered_private}, website={filtered_website}, followers={filtered_followers}, score={filtered_score}, no_data={filtered_no_data}")

    if not new_leads:
        print("No new leads to add.")
        return

    print("\n[6] Writing to GitHub...")
    merged = queue + new_leads
    queue_json = json.dumps(merged, indent=2, ensure_ascii=False)
    commit_sha = gh_put_file(
        QUEUE_PATH, queue_json, queue_sha,
        f"discovery: add {len(new_leads)} leads {TODAY}"
    )
    print(f"  ig-queue.json updated (commit {commit_sha[:8]})")

    events_content, events_sha = gh_get_file(EVENTS_PATH)
    existing_events = events_content or ""
    new_events = "\n".join(
        json.dumps({"ts": f"{TODAY}T02:00:00Z", "event": "LEAD_ADDED",
                    "username": l["username"], "niche": l["niche"],
                    "score": l["score"], "variant": l["variant"]})
        for l in new_leads
    )
    combined_events = (existing_events.rstrip() + "\n" + new_events + "\n").lstrip("\n")
    gh_put_file(EVENTS_PATH, combined_events, events_sha, f"discovery: events {TODAY}")
    print(f"  events.jsonl updated (+{len(new_leads)} lines)")

    niches_added = list({l["niche"] for l in new_leads})
    cities_added = list({l["city"] for l in new_leads})
    pending_total = sum(1 for l in merged if l.get("status") == "PENDING")

    print(f"""
=== discovery-daily report — {TODAY} ===
Added: {len(new_leads)} leads
Niches: {", ".join(niches_added)}
Cities: {", ".join(cities_added)}
Apify hit rate: {apify_hit_rate:.0f}%
Filtered: private={filtered_private}, website={filtered_website}, followers={filtered_followers}, score={filtered_score}, no_data={filtered_no_data}
Queue PENDING total: {pending_total}
""")

if __name__ == "__main__":
    run()
