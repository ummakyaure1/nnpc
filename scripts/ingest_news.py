#!/usr/bin/env python3
"""
News bulletin collector.

Finds recent articles on regulator sites and RSS feeds, has Claude summarise and tag each
one, and proposes:
  - 'news'        one bulletin item per relevant article
  - 'asset_fact'  changes to a title stated in the article (ownership, operator, status...),
                  only when the supporting quote is found word for word in the article
"""
import re
import sys
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree

from bs4 import BeautifulSoup

import pipeline_common as pc

log = pc.log
COLLECTOR = "news"

FACT_TYPES = ["ownership_change", "operator_change", "licence_award", "licence_renewal",
              "licence_conversion", "licence_revocation", "licence_status_change",
              "production_update", "incident", "other"]

ARTICLE_TOOL = {
    "name": "record_article",
    "description": "Record a summary, tags and any explicitly stated facts about upstream titles.",
    "input_schema": {
        "type": "object",
        "properties": {
            "relevant": {"type": "boolean",
                         "description": "True only if the article concerns Nigerian upstream oil and gas."},
            "published_date": {"type": ["string", "null"], "description": "YYYY-MM-DD if stated."},
            "headline": {"type": "string"},
            "summary": {"type": "string", "description": "At most 60 words, neutral, no opinions."},
            "companies": {"type": "array", "items": {"type": "string"}},
            "regulators": {"type": "array", "items": {"type": "string"}},
            "facts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "fact_type": {"type": "string", "enum": FACT_TYPES},
                        "title_number": {"type": "string", "description": "As printed, e.g. 'OML 18'."},
                        "company": {"type": ["string", "null"]},
                        "interest_pct": {"type": ["number", "null"]},
                        "new_status": {"type": ["string", "null"]},
                        "effective_date": {"type": ["string", "null"], "description": "YYYY-MM-DD if stated."},
                        "detail": {"type": "string", "description": "One sentence describing the fact."},
                        "evidence": {"type": "string",
                                     "description": "Exact sentence copied from the article that states this fact."},
                    },
                    "required": ["fact_type", "title_number", "detail", "evidence"],
                },
            },
        },
        "required": ["relevant", "headline", "summary", "companies", "regulators", "facts"],
    },
}

SYSTEM = ("You are an oil and gas news analyst. Use only the article text provided. Never add facts "
          "from your own knowledge. Facts must name a specific licence (OML/OPL/PML/PPL/PEL) and be "
          "stated explicitly in the article; quote the sentence exactly.")


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------
def from_rss(feed: dict) -> list:
    try:
        root = ElementTree.fromstring(pc.fetch(feed["url"], attempts=2).content)
    except Exception as e:
        log.warning(f"RSS feed {feed['name']} unavailable: {e}")
        return []
    items = []
    for item in root.iter():
        tag = item.tag.rsplit("}", 1)[-1]
        if tag not in ("item", "entry"):
            continue
        get = lambda name: next((c for c in item if c.tag.rsplit("}", 1)[-1] == name), None)
        link_el, title_el = get("link"), get("title")
        link = (link_el.get("href") or link_el.text or "").strip() if link_el is not None else ""
        date_el = get("pubDate") or get("published") or get("updated")
        published = None
        if date_el is not None and date_el.text:
            try:
                published = parsedate_to_datetime(date_el.text.strip())
            except (TypeError, ValueError):
                try:
                    published = datetime.fromisoformat(date_el.text.strip().replace("Z", "+00:00"))
                except ValueError:
                    published = None
        if link:
            items.append({"url": link, "title": (title_el.text or "").strip() if title_el is not None else "",
                          "source": feed["name"], "regulator": None, "published": published})
    log.info(f"RSS {feed['name']}: {len(items)} item(s)")
    return items


def from_site(src: dict) -> list:
    pattern = re.compile(src.get("article_url_pattern", "/news"), re.IGNORECASE)
    on_site = pc.same_site(src["start_urls"])
    wanted = lambda u, v: on_site(u) and pattern.search(u) and len(v["text"]) >= 25
    links = pc.crawl_with_fallback(src["start_urls"], lambda u: on_site(u) and bool(pattern.search(u)),
                                   wanted, max_pages=15, depth=1)
    items = [{"url": u, "title": v["text"], "source": src["name"], "regulator": src.get("regulator"),
              "published": None}
             for u, v in links.items() if wanted(u, v) and pc.file_ext(u) in ("", "html", "htm", "php", "aspx")]
    log.info(f"Site {src['name']}: {len(items)} candidate article(s)")
    return items


def page_date(html: str):
    soup = BeautifulSoup(html, "html.parser")
    for attrs in ({"property": "article:published_time"}, {"name": "date"}, {"itemprop": "datePublished"}):
        m = soup.find("meta", attrs=attrs)
        if m and m.get("content"):
            try:
                return datetime.fromisoformat(m["content"][:19]).replace(tzinfo=timezone.utc)
            except ValueError:
                pass
    t = soup.find("time", attrs={"datetime": True})
    if t:
        try:
            return datetime.fromisoformat(t["datetime"][:19]).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------
def process_article(item: dict, cfg: dict, cutoff: datetime):
    resp = pc.fetch(item["url"], attempts=2, timeout=30)
    if "html" not in resp.headers.get("Content-Type", "").lower():
        return None, []
    published = item["published"] or page_date(resp.text)
    if published and published.replace(tzinfo=published.tzinfo or timezone.utc) < cutoff:
        return None, []
    text = pc.html_to_text(resp.text)[:60_000]
    if len(text) < 300:
        return None, []
    lowered = (item["title"] + " " + text).lower()
    if not any(k.lower() in lowered for k in cfg["keywords"]):
        return None, []

    out = pc.call_tool(SYSTEM, [{"type": "text", "text": f"URL: {item['url']}\n\nARTICLE TEXT:\n{text}"}],
                       ARTICLE_TOOL, max_tokens=4000)
    if not out.get("relevant"):
        return None, []

    date_str = out.get("published_date") or (published.date().isoformat() if published else None)
    if date_str:
        try:
            if datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc) < cutoff:
                return None, []
        except ValueError:
            date_str = None
    titles = pc.titles_in(text)
    news = {
        "proposed_data": {
            "published_at": date_str,
            "source_name": item["source"],
            "url": item["url"],
            "headline": out.get("headline") or item["title"],
            "summary": out.get("summary"),
            "tags": {"assets": titles, "companies": out.get("companies") or [],
                     "regulators": sorted({*(out.get("regulators") or []), *([item["regulator"]] if item["regulator"] else [])})},
            "mentions_target_asset": any(t in pc.TARGET_ASSETS for t in titles),
            "source_url": item["url"],
            "extraction_model": pc.ANTHROPIC_MODEL,
        },
        "source_url": item["url"],
        "dedupe_key": pc.make_key("news", item["url"].split("#")[0].rstrip("/")),
    }

    facts = []
    for f in out.get("facts") or []:
        title = pc.normalise_title(f.get("title_number"))
        if not title:
            continue
        if not pc.evidence_found(text, f.get("evidence", "")):
            log.warning(f"  dropped unverified fact about {title}: quote not found in article")
            continue
        facts.append({
            "proposed_data": {
                "title_number": title,
                "is_target_asset": title in pc.TARGET_ASSETS,
                "fact_type": f.get("fact_type"),
                "company": f.get("company"),
                "interest_pct": f.get("interest_pct"),
                "new_status": f.get("new_status"),
                "effective_date": f.get("effective_date"),
                "detail": f.get("detail"),
                "evidence": f.get("evidence"),
                "evidence_verified": True,
                "suggested_grade": "B",
                "source_name": item["source"],
                "published_at": date_str,
                "source_url": item["url"],
            },
            "source_url": item["url"],
            "dedupe_key": pc.make_key("fact", item["url"], title, f.get("fact_type"), f.get("evidence")),
        })
    return news, facts


def main() -> int:
    if not pc.check_env():
        return 1
    cfg = pc.load_config()["news"]
    cutoff = datetime.now(timezone.utc) - timedelta(days=int(cfg.get("lookback_days", 10)))

    candidates = []
    for feed in cfg.get("rss_feeds", []):
        candidates += from_rss(feed)
    for src in cfg.get("sources", []):
        candidates += from_site(src)

    seen, unique = set(), []
    for c in candidates:
        key = c["url"].split("#")[0].rstrip("/")
        if key not in seen:
            seen.add(key)
            unique.append(c)
    unique.sort(key=lambda c: c["published"] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    limit = int(cfg.get("max_articles_per_run", 30))
    log.info(f"{len(unique)} unique candidate article(s); processing up to {limit}")
    for c in unique[:limit]:
        log.info(f"  candidate: [{c['source']}] {c['title'][:90]} | {c['url']}")

    news, facts, failures = [], [], 0
    for item in unique[:limit]:
        try:
            n, f = process_article(item, cfg, cutoff)
        except Exception as e:
            failures += 1
            log.warning(f"Skipped {item['url']}: {e}")
            continue
        if n:
            news.append(n)
            facts += f
            log.info(f"  kept: {n['proposed_data']['headline'][:90]} ({len(f)} fact(s))")

    pc.post_staging("news", news, COLLECTOR)
    pc.post_staging("asset_fact", facts, COLLECTOR)
    pc.write_summary(f"### News\n- Candidates: {len(unique)}\n- Bulletin items proposed: {len(news)}\n"
                     f"- Title facts proposed: {len(facts)}\n- Articles skipped on error: {failures}")
    log.info(f"News done: {len(news)} item(s), {len(facts)} fact(s), {failures} error(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
