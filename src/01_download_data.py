"""
Download raw data for the minimum wage border-county project.

Sources:
1. QCEW (BLS) – county-level annual bulk ZIP files (all states, all available years)
   https://www.bls.gov/cew/downloadable-data.htm
   BLS publishes one ZIP per year; each ZIP contains CSVs for every county in the US.
   Years are fixed at 2010–2024: 2010 is chosen because the federal minimum wage last
   changed in 2009, making state-level divergence meaningful only from 2010 onward;
   2024 is the latest year with published annual data (2025 not yet released by BLS).
   State/year filtering for the analysis happens in a later cleaning script.

2. State minimum wage history (FRED, Federal Reserve Bank of St. Louis)
   https://fred.stlouisfed.org
   Each state has a dedicated FRED series (STTMINWG{STATE}), sourced from the U.S.
   Department of Labor Wage and Hour Division. The 5 states with no state-level minimum
   wage law (AL, LA, MS, SC, TN) fall back to the federal series (FEDMINNFRWG).
   All 51 states/DC are combined into a single long-format CSV.

3. IPUMS CPS (Current Population Survey) – March ASEC supplement, 2010–2024
   https://cps.ipums.org
   Used for the secondary heterogeneity analysis (employment effects by age and gender).
   Downloaded programmatically via the IPUMS API (ipumspy) using the credentials stored
   in the project .env file (IPUMS_API_KEY). An extract is submitted requesting the
   March ASEC samples for each year with variables covering state, demographics
   (AGE, SEX), labor force status (EMPSTAT, LABFORCE), hours worked (UHRSWORKT),
   weeks worked (WKSWORK2), and wages (INCWAGE). IPUMS processes the extract in their
   queue (typically minutes to hours) and the script waits and downloads automatically
   once ready. Requires a free IPUMS account at https://ipums.org.

State selection for the border-county analysis is intentionally left to a separate script.
"""

import requests
from pathlib import Path

# Paths
ROOT = Path(__file__).resolve().parents[1]
RAW_QCEW = ROOT / "data" / "raw" / "qcew"
RAW_MIN_WAGE = ROOT / "data" / "raw" / "min_wage"
RAW_CPS = ROOT / "data" / "raw" / "cps"

RAW_STATE_FIPS = ROOT / "data" / "raw" / "state_fips.txt"

RAW_QCEW.mkdir(parents=True, exist_ok=True)
RAW_MIN_WAGE.mkdir(parents=True, exist_ok=True)
RAW_CPS.mkdir(parents=True, exist_ok=True)

CHUNK_SIZE = 1024 * 1024  # 1 MB


def download_file(url: str, dest: Path, overwrite: bool = False) -> bool:
    """Stream-download *url* to *dest*. Returns True on success, False on 404."""
    if dest.exists() and not overwrite:
        print(f"  [skip] {dest.name} already exists")
        return True
    print(f"  [download] {dest.name} ← {url}")
    resp = requests.get(url, stream=True, timeout=120)
    if resp.status_code == 404:
        print(f"  [not found] {url}")
        return False
    resp.raise_for_status()
    with open(dest, "wb") as fh:
        for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
            fh.write(chunk)
    print(f"  [done] {dest.name} ({dest.stat().st_size / 1e6:.1f} MB)")
    return True


def qcew_url(year: int) -> str:
    return (
        f"https://data.bls.gov/cew/data/files/{year}/csv/" f"{year}_annual_by_area.zip"
    )


# 1. QCEW county-level annual bulk files

FIRST_YEAR = (
    2010  # federal minimum wage last changed in 2009; state divergence begins here
)
LAST_YEAR = 2024  # 2025 annual data not yet published by BLS


def download_qcew() -> None:
    print(
        f"\n=== QCEW county annual bulk files (all states, {FIRST_YEAR}–{LAST_YEAR}) ==="
    )
    for year in range(FIRST_YEAR, LAST_YEAR + 1):
        download_file(qcew_url(year), RAW_QCEW / f"{year}_annual_by_area.zip")


# 2. State minimum wage history from FRED
# FRED (Federal Reserve Bank of St. Louis) hosts annual state minimum wage series
# under the pattern STTMINWG{STATE_ABBR} (e.g. STTMINWGIL for Illinois).
# These are freely downloadable as CSV without an API key.
# All 50 states + DC are downloaded and combined into a single long-format CSV.

ALL_STATE_ABBRS = [
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "DC",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
]

FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=STTMINWG{state}"


def download_min_wage() -> None:
    import csv
    import io

    print("\n=== State minimum wage history (FRED, all states) ===")
    dest = RAW_MIN_WAGE / "state_mw_history.csv"

    if dest.exists():
        print(f"  [skip] {dest.name} already exists")
        return

    # States with no state minimum wage default to the federal minimum.
    # FRED series FEDMINNFRWG covers the federal rate.
    federal_rows = {}  # date -> value, populated on first 404
    rows = []  # list of (state, date, min_wage)
    for state in ALL_STATE_ABBRS:
        url = FRED_URL.format(state=state)
        resp = requests.get(url, timeout=30)
        if resp.status_code == 404:
            # No state-level series — fetch federal rate once and use as fallback
            if not federal_rows:
                fed_resp = requests.get(
                    "https://fred.stlouisfed.org/graph/fredgraph.csv?id=FEDMINNFRWG",
                    timeout=30,
                )
                fed_reader = csv.reader(io.StringIO(fed_resp.text))
                next(fed_reader)
                federal_rows = {date: value for date, value in fed_reader}
            for date, value in federal_rows.items():
                rows.append((state, date, value))
            print(
                f"  [federal fallback] {state} (no state minimum wage — using federal rate)"
            )
            continue
        if resp.status_code != 200:
            print(f"  [warn] {state}: HTTP {resp.status_code} — skipping")
            continue
        reader = csv.reader(io.StringIO(resp.text))
        next(reader)  # skip header
        for date, value in reader:
            rows.append((state, date, value))
        print(f"  [done] {state}")

    with open(dest, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["state", "date", "min_wage"])
        writer.writerows(rows)

    print(f"  [saved] {dest.name} ({len(rows)} rows, {len(ALL_STATE_ABBRS)} states)")


# 3. IPUMS CPS (Current Population Survey) — MANUALLY DOWNLOADED
# Used for the secondary heterogeneity analysis (employment effects by age and gender).
# We use the March ASEC supplement (the richest annual labor market data in CPS)
# for 2010–2024, covering all states.
#
# How the extract was obtained:
#   1. Registered for IPUMS CPS at https://uma.pop.umn.edu/cps/registration/new
#   2. Went to https://cps.ipums.org/cps/ and clicked "Get Data"
#   3. Selected samples: March ASEC for each year 2010–2024 (Cross-sectional)
#   4. Selected variables:
#        Preselected : YEAR, MONTH, SERIAL, PERNUM, HWTFINL, CPSID, ASECFLAG,
#                      HFLAG, ASECWTH, CPSIDP, CPSIDV, ASECWT, WTFINL
#        Added       : STATEFIP, AGE, SEX, EMPSTAT, LABFORCE, UHRSWORKT,
#                      WKSWORK2, INCWAGE
#   5. Clicked "View Cart" → "Create Data Extract"
#   6. Set data format to CSV
#   7. Used description: "IDS701 minimum wage border county project - March ASEC 2010-2024"
#   8. Clicked "Submit Extract" — received email when ready
#   9. Downloaded files and saved to: data/raw/cps/
#
# Create a free account at https://ipums.org, register for CPS, and
# repeat steps 2–9 above to obtain the same extract.


# 4. Census Bureau state FIPS table
# Official pipe-delimited file from the Census Bureau.
# Columns: STATE (2-digit FIPS), STUSAB (2-letter abbr), STATE_NAME, STATENS
# Covers all 50 states + DC + territories.
# Used in cleaning notebooks to map state abbreviations → FIPS codes without
# hardcoding the lookup table.

STATE_FIPS_URL = "https://www2.census.gov/geo/docs/reference/state.txt"


def download_state_fips() -> None:
    print("\n=== Census Bureau state FIPS table ===")
    download_file(STATE_FIPS_URL, RAW_STATE_FIPS)


# ── Main ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    download_qcew()
    download_min_wage()
    download_state_fips()
    print("\nAll downloads complete. Note: CPS data must be downloaded manually.")
    print("See instructions in the source code above (section 3) or README.")
