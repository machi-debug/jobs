from __future__ import annotations

import argparse
import re
import sys
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from lxml import etree


SITEMAP_URL_DEFAULT = "https://machitech.com/sitemap.xml"
PUBLISHER = "Machitech"
PUBLISHER_URL = "https://machitech.com"
DEFAULT_POSTER_EMAIL = "hr@machitech.com"
DEFAULT_TIMEOUT = 20
DEFAULT_RETRIES = 3
DEFAULT_SLEEP = 0.2

HEADERS = {
    "User-Agent": "MachitechLinkedInFeedBot/1.0 (+https://machitech.com)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ----------------------------
# Cleaning / normalization
# ----------------------------

REMOVE_PHRASES = [
    "share on facebook",
    "share on linkedin",
    "partager sur facebook",
    "partager sur linkedin",
    "click here to apply",
    "cliquer pour appliquer",
    "apply now",
    "postuler",
]

# Lines like "Livermore, Kentucky" / "Victoriaville, Quebec" / "St-Marc-des-Carrières, Quebec"
LOCATION_LINE_RE = re.compile(
    r"^\s*[A-Za-zÀ-ÿ0-9\-\s’'()./]+,\s*(kentucky|quebec|québec|canada|usa|united states|é-u|e-u)\s*$",
    re.IGNORECASE,
)

# Some pages repeat apply CTAs or show multiple location blocks; remove obvious garbage lines.
GARBAGE_LINE_RE = re.compile(
    r"^(?:share|partager)\s+on\s+(?:facebook|linkedin)\s*$|^(?:share|partager)\s+sur\s+(?:facebook|linkedin)\s*$",
    re.IGNORECASE,
)


def clean_description(raw: str) -> str:
    if not raw:
        return ""

    raw = raw.replace("\r\n", "\n").replace("\r", "\n")

    lines = []
    current_section = None

    for line in raw.split("\n"):
        s = line.strip()
        if not s:
            continue

        low = s.lower()

        # Remove junk
        if any(p in low for p in REMOVE_PHRASES):
            continue

        if LOCATION_LINE_RE.match(s):
            continue

        # Normalize section headers
        if s.upper() in ["JOB DESCRIPTION", "DESCRIPTION DU POSTE"]:
            current_section = "description"
            lines.append("\nJOB DESCRIPTION")
            continue

        if s.upper() in ["RESPONSIBILITIES", "RESPONSABILITÉS"]:
            current_section = "responsibilities"
            lines.append("\nRESPONSIBILITIES")
            continue

        if s.upper() in ["REQUIREMENTS", "EXIGENCES"]:
            current_section = "requirements"
            lines.append("\nREQUIREMENTS")
            continue

        # Normalize THIS ROLE section (English + French)
        if "this role is for me" in low or "ce rôle est pour moi" in low:
            current_section = "fit"
            lines.append("\nTHIS JOB IS FOR ME IF...\n")
            continue

        # Add bullets only to responsibilities & requirements
        if current_section in ["responsibilities", "requirements"]:
            lines.append(f"- {s}")
        else:
            lines.append(s)

    text = "\n".join(lines)

    # Clean spacing
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()

    return text


def normalize_jobtype(jobtype: str) -> str:
    jt = (jobtype or "").strip().lower()
    if "full" in jt:
        return "FULL_TIME"
    if "part" in jt:
        return "PART_TIME"
    return (jobtype or "").strip() or "FULL_TIME"


# ----------------------------
# Scraping
# ----------------------------

@dataclass
class JobRecord:
    title: str
    partner_job_id: str
    apply_url: str
    company: str
    description: str
    city: str
    state: str
    country: str
    jobtype: str


def request_with_retries(url: str, timeout: int, retries: int, sleep_s: float, verbose: bool) -> requests.Response:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            if verbose:
                print(f"[warn] fetch failed (attempt {attempt}/{retries}) {url}: {e}", file=sys.stderr)
            if attempt < retries:
                time.sleep(sleep_s * attempt)
    raise RuntimeError(f"Failed to fetch after {retries} attempts: {url}") from last_err


def get_job_urls_from_sitemap(sitemap_url: str, timeout: int, retries: int, sleep_s: float, verbose: bool) -> List[str]:
    r = request_with_retries(sitemap_url, timeout=timeout, retries=retries, sleep_s=sleep_s, verbose=verbose)
    soup = BeautifulSoup(r.content, "xml")

    urls = [loc.text.strip() for loc in soup.find_all("loc") if loc and loc.text]
    # Only job pages (EN) or emplois (FR)
    job_urls = []
    for u in urls:
        if "/jobs/" in u or "/fr/emplois/" in u:
            job_urls.append(u)

    # Remove index/listing pages if present
    exclude = {
        "https://machitech.com/jobs",
        "https://machitech.com/jobs/",
        "https://machitech.com/fr/emplois",
        "https://machitech.com/fr/emplois/",
        "https://machitech.com/emplois",
        "https://machitech.com/emplois/",
    }

    job_urls = [u for u in job_urls if u not in exclude]

    # Deduplicate while preserving order
    seen = set()
    deduped = []
    for u in job_urls:
        if u not in seen:
            seen.add(u)
            deduped.append(u)

    if verbose:
        print(f"[info] sitemap URLs total: {len(urls)}; job URLs: {len(deduped)}", file=sys.stderr)

    return deduped


def infer_location(url: str, page_text: str) -> Tuple[str, str, str]:
    """
    Location rules (customize if needed):
    - English /jobs/ defaults to Livermore, KY, US
    - French /fr/emplois/ defaults to Victoriaville, QC, CA
    - If St-Marc-des-Carrières is detected in content or URL, set city accordingly (QC, CA)
    """
    is_french = "/fr/emplois/" in url or "/emplois/" in url  # safe
    text_low = (page_text or "").lower()
    url_low = url.lower()

    if is_french:
        # Default QC city
        city = "Victoriaville"
        state = "QC"
        country = "CA"

        # Override if St-Marc is referenced
        if "st-marc" in url_low or "st-marc" in text_low or "st marc" in text_low:
            city = "St-Marc-des-Carrières"
        return city, state, country

    # Default US job location
    return "Livermore", "KY", "US"


def extract_title_and_description(html: str) -> Tuple[str, str]:
    soup = BeautifulSoup(html, "html.parser")

    # Title: prefer <h1>
    h1 = soup.find("h1")
    title = h1.get_text(" ", strip=True) if h1 else ""
    if not title:
        # fallback: <title>
        t = soup.find("title")
        title = t.get_text(" ", strip=True) if t else "Machitech Job"

    # Description: try common containers, else fall back to article/body text
    container = soup.select_one(
        ".job-description, .hhs-job-description, .job_posting, .body-container, article, main"
    )
    if container:
        raw_desc = container.get_text("\n", strip=True)
    else:
        raw_desc = soup.get_text("\n", strip=True)

    return title.strip() or "Machitech Job", raw_desc.strip()


def make_partner_job_id(url: str) -> str:
    # Use slug. If trailing slash, drop it.
    path = urlparse(url).path.rstrip("/")
    slug = path.split("/")[-1] if path else "machitech-job"
    return slug or "machitech-job"


def scrape_job(url: str, timeout: int, retries: int, sleep_s: float, verbose: bool) -> JobRecord:
    r = request_with_retries(url, timeout=timeout, retries=retries, sleep_s=sleep_s, verbose=verbose)

    title, raw_desc = extract_title_and_description(r.text)
    desc = clean_description(raw_desc)

    # Location inference uses both URL + page text
    city, state, country = infer_location(url, raw_desc)

    return JobRecord(
        title=title,
        partner_job_id=make_partner_job_id(url),
        apply_url=url,
        company=PUBLISHER,
        description=desc if desc else "See website for details.",
        city=city,
        state=state,
        country=country,
        jobtype=normalize_jobtype("FULL_TIME"),
    )


# ----------------------------
# XML output
# ----------------------------

def build_linkedin_xml(jobs: List[JobRecord], out_path: str, poster_email: str, pretty: bool = True) -> None:
    root = etree.Element("source")

    etree.SubElement(root, "publisher").text = etree.CDATA(PUBLISHER)
    etree.SubElement(root, "publisherurl").text = etree.CDATA(PUBLISHER_URL)

    for j in jobs:
        job_el = etree.SubElement(root, "job")

        etree.SubElement(job_el, "title").text = etree.CDATA(j.title)
        etree.SubElement(job_el, "partnerJobId").text = etree.CDATA(j.partner_job_id)
        etree.SubElement(job_el, "applyUrl").text = etree.CDATA(j.apply_url)
        etree.SubElement(job_el, "company").text = etree.CDATA(j.company)

        etree.SubElement(job_el, "description").text = etree.CDATA(j.description)

        etree.SubElement(job_el, "city").text = etree.CDATA(j.city)
        etree.SubElement(job_el, "state").text = etree.CDATA(j.state)
        etree.SubElement(job_el, "country").text = etree.CDATA(j.country)

        etree.SubElement(job_el, "jobtype").text = etree.CDATA(j.jobtype)
        etree.SubElement(job_el, "posterEmail").text = etree.CDATA(poster_email)

    xml_bytes = etree.tostring(
        root,
        pretty_print=pretty,
        xml_declaration=True,
        encoding="UTF-8",
    )

    with open(out_path, "wb") as f:
        f.write(xml_bytes)


# ----------------------------
# CLI
# ----------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate LinkedIn-friendly XML feed from Machitech job pages.")
    p.add_argument("--sitemap", default=SITEMAP_URL_DEFAULT, help="Sitemap URL (default: Machitech sitemap).")
    p.add_argument("--out", default="linkedin-jobs.xml", help="Output XML filename.")
    p.add_argument("--poster-email", default=DEFAULT_POSTER_EMAIL, help="Poster email to include in each job.")
    p.add_argument("--limit", type=int, default=0, help="Limit number of jobs (0 = no limit).")
    p.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="HTTP timeout seconds.")
    p.add_argument("--retries", type=int, default=DEFAULT_RETRIES, help="HTTP retries.")
    p.add_argument("--sleep", type=float, default=DEFAULT_SLEEP, help="Sleep between requests (seconds).")
    p.add_argument("--no-pretty", action="store_true", help="Disable pretty printing (smaller XML).")
    p.add_argument("--verbose", action="store_true", help="Verbose logging.")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    urls = get_job_urls_from_sitemap(
        sitemap_url=args.sitemap,
        timeout=args.timeout,
        retries=args.retries,
        sleep_s=args.sleep,
        verbose=args.verbose,
    )

    if args.limit and args.limit > 0:
        urls = urls[: args.limit]

    jobs: List[JobRecord] = []
    for i, url in enumerate(urls, start=1):
        if args.verbose:
            print(f"[info] ({i}/{len(urls)}) scraping {url}", file=sys.stderr)
        try:
            jobs.append(scrape_job(url, timeout=args.timeout, retries=args.retries, sleep_s=args.sleep, verbose=args.verbose))
        except Exception as e:
            print(f"[error] skipping {url}: {e}", file=sys.stderr)

    if not jobs:
        print("[error] No jobs scraped; nothing to write.", file=sys.stderr)
        return 2

    build_linkedin_xml(
        jobs=jobs,
        out_path=args.out,
        poster_email=args.poster_email,
        pretty=not args.no_pretty,
    )

    if args.verbose:
        print(f"[info] wrote {len(jobs)} jobs to {args.out}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

