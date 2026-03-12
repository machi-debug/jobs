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
    r"^\s*[A-Za-zÀ-ÿ0-9\-\s''()./]+,\s*(kentucky|quebec|québec|canada|usa|united states|é-u|e-u)\s*$",
    re.IGNORECASE,
)

GARBAGE_LINE_RE = re.compile(
    r"^(?:share|partager)\s+on\s+(?:facebook|linkedin)\s*$|^(?:share|partager)\s+sur\s+(?:facebook|linkedin)\s*$",
    re.IGNORECASE,
)

# Known QC locations — extend this list as needed
QC_LOCATIONS = [
    ("st-marc", "St-Marc-des-Carrières"),
    ("st marc", "St-Marc-des-Carrières"),
    ("victoriaville", "Victoriaville"),
]


def clean_description(raw: str) -> str:
    if not raw:
        return ""

    raw = raw.replace("\r\n", "\n").replace("\r", "\n")

    raw = re.sub(r"</\s*mark\s*>", " ", raw, flags=re.IGNORECASE)
    raw = re.sub(r"<\s*mark[^>]*>", "", raw, flags=re.IGNORECASE)

    lines = []
    current_section = None

    FR_SIGNAL_RE = re.compile(
        r"\b(description\s+du\s+poste|responsabilit[eé]s|exigences|ce\s+r[oô]le\s+est\s+pour\s+moi|si\s+je\s+suis)\b",
        re.IGNORECASE
    )
    is_french = bool(FR_SIGNAL_RE.search(raw))

    HDR_DESC = "DESCRIPTION DU POSTE" if is_french else "JOB DESCRIPTION"
    HDR_RESP = "RESPONSABILITÉS" if is_french else "RESPONSIBILITIES"
    HDR_REQ  = "EXIGENCES" if is_french else "REQUIREMENTS"

    FIT_HDR_FR = "CE RÔLE EST POUR MOI SI JE SUIS..."
    FIT_HDR_EN = "THIS ROLE IS FOR ME IF I AM..."

    FIT_FR_RE = re.compile(r"\bce\s+r[oô]le\s+est\s+pour\s+moi\b", re.IGNORECASE)
    FIT_EN_RE = re.compile(r"\bthis\s+(role|job)\s+is\s+for\s+me\b", re.IGNORECASE)

    FIT_FR_IF_RE = re.compile(r"^\s*si\s+je\s+suis\b", re.IGNORECASE)
    FIT_EN_IF_RE = re.compile(r"^\s*if\s+i\s+am\b", re.IGNORECASE)

    for line in raw.split("\n"):
        s = line.strip()
        if not s:
            continue

        low = s.lower()

        if any(p in low for p in REMOVE_PHRASES):
            continue

        if LOCATION_LINE_RE.match(s):
            continue

        if s.upper() in ["JOB DESCRIPTION", "DESCRIPTION DU POSTE", "DESCRIPTION DE POSTE"]:
            current_section = "description"
            lines.append(f"\n{HDR_DESC}")
            continue

        if s.upper() in ["RESPONSIBILITIES", "RESPONSABILITÉS", "RESPONSABILITES"]:
            current_section = "responsibilities"
            lines.append(f"\n{HDR_RESP}")
            continue

        if s.upper() in ["REQUIREMENTS", "EXIGENCES"]:
            current_section = "requirements"
            lines.append(f"\n{HDR_REQ}")
            continue

        if FIT_FR_RE.search(s):
            current_section = "fit"
            lines.append(f"\n{FIT_HDR_FR}\n")
            continue

        if FIT_EN_RE.search(s):
            current_section = "fit"
            lines.append(f"\n{FIT_HDR_EN}\n")
            continue

        if FIT_FR_IF_RE.match(s) or FIT_EN_IF_RE.match(s):
            current_section = "fit"
            continue

        if current_section in ["responsibilities", "requirements"]:
            lines.append(f"- {s}")
        else:
            lines.append(s)

    text = "\n".join(lines)
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
    job_urls = []
    for u in urls:
        if "/jobs/" in u or "/fr/emplois/" in u:
            job_urls.append(u)

    exclude = {
        "https://machitech.com/jobs",
        "https://machitech.com/jobs/",
        "https://machitech.com/fr/emplois",
        "https://machitech.com/fr/emplois/",
        "https://machitech.com/emplois",
        "https://machitech.com/emplois/",
    }

    job_urls = [u for u in job_urls if u not in exclude]

    seen = set()
    deduped = []
    for u in job_urls:
        if u not in seen:
            seen.add(u)
            deduped.append(u)

    if verbose:
        print(f"[info] sitemap URLs total: {len(urls)}; job URLs: {len(deduped)}", file=sys.stderr)

    return deduped


def infer_locations(url: str, page_text: str) -> List[Tuple[str, str, str]]:
    """
    Returns a list of (city, state, country) tuples.

    - English /jobs/ → always single location: Livermore, KY, US
    - French /fr/emplois/ → detect ALL QC cities mentioned in the page text or URL.
      If multiple QC cities are found, return one tuple per city so the caller
      can create a separate LinkedIn job posting for each location.
      Falls back to Victoriaville, QC, CA if no city is detected.
    """
    is_french = "/fr/emplois/" in url or "/emplois/" in url
    text_low = (page_text or "").lower()
    url_low = url.lower()

    if not is_french:
        return [("Livermore", "KY", "US")]

    # Check all known QC locations against both URL and page text
    found: List[Tuple[str, str, str]] = []
    seen_cities: set = set()

    combined = url_low + " " + text_low

    for keyword, city_name in QC_LOCATIONS:
        if keyword in combined and city_name not in seen_cities:
            found.append((city_name, "QC", "CA"))
            seen_cities.add(city_name)

    # Fallback: default to Victoriaville if nothing matched
    if not found:
        found.append(("Victoriaville", "QC", "CA"))

    return found


def extract_title_and_description(html: str) -> Tuple[str, str]:
    soup = BeautifulSoup(html, "html.parser")

    h1 = soup.find("h1")
    title = h1.get_text(" ", strip=True) if h1 else ""
    if not title:
        t = soup.find("title")
        title = t.get_text(" ", strip=True) if t else "Machitech Job"

    container = soup.select_one(
        ".job-description, .hhs-job-description, .job_posting, .body-container, article, main"
    )
    if container:
        raw_desc = container.get_text("\n", strip=True)
    else:
        raw_desc = soup.get_text("\n", strip=True)

    return title.strip() or "Machitech Job", raw_desc.strip()


def make_partner_job_id(url: str, city: str = "") -> str:
    path = urlparse(url).path.rstrip("/")
    slug = path.split("/")[-1] if path else "machitech-job"
    slug = slug or "machitech-job"

    if city:
        city_slug = city.lower()
        city_slug = re.sub(r"[àáâãäå]", "a", city_slug)
        city_slug = re.sub(r"[èéêë]", "e", city_slug)
        city_slug = re.sub(r"[ìíîï]", "i", city_slug)
        city_slug = re.sub(r"[òóôõö]", "o", city_slug)
        city_slug = re.sub(r"[ùúûü]", "u", city_slug)
        city_slug = re.sub(r"[ç]", "c", city_slug)
        city_slug = re.sub(r"[^a-z0-9]+", "-", city_slug).strip("-")
        full_id = f"{slug}-{city_slug}"
    else:
        full_id = slug

    # LinkedIn limit — truncate to 50 chars, strip any trailing hyphen
    return full_id[:50].rstrip("-")


def scrape_job(url: str, timeout: int, retries: int, sleep_s: float, verbose: bool) -> List[JobRecord]:
    """
    Scrape a single job page and return one JobRecord per detected location.
    If the page mentions two cities (e.g. Victoriaville + St-Marc), two records
    are returned with unique partnerJobIds so LinkedIn creates two separate postings.
    """
    r = request_with_retries(url, timeout=timeout, retries=retries, sleep_s=sleep_s, verbose=verbose)

    title, raw_desc = extract_title_and_description(r.text)
    desc = clean_description(raw_desc)

    locations = infer_locations(url, raw_desc)

    records: List[JobRecord] = []
    for city, state, country in locations:
        records.append(JobRecord(
            title=title,
            partner_job_id=make_partner_job_id(url, city),
            apply_url=url,
            company=PUBLISHER,
            description=desc if desc else "See website for details.",
            city=city,
            state=state,
            country=country,
            jobtype=normalize_jobtype("FULL_TIME"),
        ))

    if verbose and len(records) > 1:
        cities = ", ".join(r.city for r in records)
        print(f"[info] duplicated '{title}' → {len(records)} locations: {cities}", file=sys.stderr)

    return records


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
            # scrape_job now returns a list — one record per location
            records = scrape_job(url, timeout=args.timeout, retries=args.retries, sleep_s=args.sleep, verbose=args.verbose)
            jobs.extend(records)
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
        print(f"[info] wrote {len(jobs)} job entries to {args.out}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

