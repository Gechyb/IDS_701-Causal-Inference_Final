# IDS 701 Causal Inference Final Project

## Objective

This project estimates the causal effect of state minimum wage increases on employment in the food services sector (NAICS 722) using a border-county difference-in-differences design. Contiguous county pairs that straddle a state border are used as the comparison group, exploiting variation in state minimum wages while holding local economic conditions approximately constant. The analysis covers 2004 to 2024 and includes an event study to test parallel pre-trends, wage pass-through estimation, and a heterogeneity analysis by age and gender using CPS data.

---

## Repository Structure

```
.
├── src/
│   └── 01_download_data.py     # Downloads raw data from BLS, FRED, and Census
├── notebooks/
│   ├── 02_clean_qcew.ipynb
│   ├── 03_clean_min_wage.ipynb
│   ├── 04_clean_cps.ipynb
│   ├── 05_select_border_pairs.ipynb
│   ├── 06_build_panel.ipynb
│   ├── 07_did_model_setup.ipynb
│   ├── 08_event_study.ipynb
│   ├── 09_cps_heterogeneity.ipynb
│   ├── 10_flowcharts.ipynb
│   └── 11_early_period_2004_2010.ipynb
├── data/
│   ├── raw/                    # Downloaded source files (not tracked in git)
│   ├── intermediate/           # Cleaned single-source parquet files
│   └── processed/              # Final merged analysis panel
└── requirements.txt
```

---

## Notebooks

**02_clean_qcew.ipynb**
Ingests the raw QCEW annual ZIP files downloaded from BLS (2004 to 2024), extracts county-level employment and average weekly wage data for low-wage industries (NAICS 722 food services, 72 accommodation and food services, 44-45 retail, and 721 accommodation), and writes a combined panel to `data/intermediate/qcew_panel.parquet`. The primary industry used in downstream analysis is NAICS 722, which is the standard sector in the minimum wage border-county literature.

**03_clean_min_wage.ipynb**
Processes the state minimum wage CSV downloaded from FRED into a clean annual panel. Keeps January values (standard in the literature), filters to 2003 to 2024, and merges with Census state FIPS codes. Outputs `data/intermediate/min_wage_panel.parquet`.

**04_clean_cps.ipynb**
Cleans the IPUMS CPS March ASEC supplement (manually downloaded, see below) to produce employment rate data by demographic group. Filters to working-age adults (16 to 64) in the labor force, creates age group and gender flags, and saves an individual-level file and a collapsed state-by-year-by-group panel to `data/intermediate/cps_clean.parquet` and `data/intermediate/cps_panel.parquet`.

**05_select_border_pairs.ipynb**
Identifies contiguous cross-state county pairs using the Census Bureau county adjacency file, merges minimum wage history onto each pair, and retains only pairs where the two states had different minimum wages in at least one year between 2004 and 2024. Outputs `data/intermediate/border_pairs.parquet` with 1,132 pairs across 49 states.

**06_build_panel.ipynb**
Merges QCEW employment, minimum wage, and border pair datasets into the analysis-ready panel. Drops invalid state-years (AZ 2003-2006, FL 2003-2005, GA 2024, WY 2024) rather than imputing, filters to border counties, log-transforms outcomes, assigns treatment indicators (higher-wage state = treated), drops BLS-suppressed observations, and saves `data/processed/analysis_panel.parquet` with approximately 157,000 county-by-year-by-industry-by-pair observations.

**07_did_model_setup.ipynb**
Runs the main two-way fixed effects DiD specification with county fixed effects and pair-by-year fixed effects. Estimates the effect of minimum wage on log employment and log average weekly wages for the full nationwide border-county sample, and for a smaller four-border illustrative subsample (NJ/PA, MN/WI, NY/PA, CA/NV). Reports strong wage pass-through and small, statistically imprecise employment effects in NAICS 722, with an additional signed dollar-gap robustness check.

**08_event_study.ipynb**
Tests the parallel pre-trends assumption using an event study. Identifies the first year a minimum wage gap opens within each border pair, constructs relative-time indicators, and runs a TWFE regression with leads and lags. Pre-treatment coefficients near zero support the parallel trends assumption; post-treatment coefficients show the dynamic employment response.

**09_cps_heterogeneity.ipynb**
Estimates minimum wage employment effects separately by age group and gender using state-level CPS data merged with state minimum wages. Young adults aged 20 to 24 show a statistically significant negative effect (β = −0.028, p = 0.046), driven primarily by young men. Teens aged 16 to 19 show a marginally significant positive effect. Older workers and gender breakdowns show near-zero effects. The notebook also checks whether young adults adjust through usual weekly hours and whether the age-group results are driven by very small CPS state-year cells. This analysis covers 2004 to 2024 and uses a state-level panel rather than the border-county design.

**10_flowcharts.ipynb**
Generates two schematic figures illustrating the data pipeline (from raw sources through cleaning to estimation) and the identification strategy (border-county design, parallel trends test, TWFE specification).

**11_early_period_2004_2010.ipynb**
Isolates the 2004 to 2010 sub-period to leverage the federal minimum wage hikes of 2007 to 2009 ($5.15 to $7.25) as an exogenous shock. Compares full-panel, pre-recession, and recession-era estimates to show results are not driven by the Great Recession. Approximately 94% of treatment events in the sample fall within this window.

---

## Replicating Results

### 1. Set Up the Environment

Python 3.10 or later is recommended.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Quick Path: Reproduce Analysis from Included Processed Data

The repository includes the cleaned intermediate files and final analysis panel needed
for the report:

- `data/intermediate/qcew_panel.parquet`
- `data/intermediate/min_wage_panel.parquet`
- `data/intermediate/border_pairs.parquet`
- `data/intermediate/cps_clean.parquet`
- `data/intermediate/cps_panel.parquet`
- `data/processed/analysis_panel.parquet`

To reproduce the tables and figures used in the report, open Jupyter and run the
analysis notebooks from 07 onward:

```bash
jupyter notebook
```

Recommended order:

1. `notebooks/07_did_model_setup.ipynb`
2. `notebooks/08_event_study.ipynb`
3. `notebooks/09_cps_heterogeneity.ipynb`
4. `notebooks/10_flowcharts.ipynb`
5. `notebooks/11_early_period_2004_2010.ipynb`

This path does not require downloading the raw QCEW or CPS files.

### 3. Optional Full Data Rebuild from Raw Sources

Use this path only if you want to rebuild the intermediate and processed data files
from the original sources.

Most raw data is downloaded automatically by running:

```bash
python src/01_download_data.py
```

This script fetches:

- **QCEW county-level annual bulk files (2004 to 2024)** from the Bureau of Labor Statistics. One ZIP file per year is downloaded to `data/raw/qcew/`. Total download is approximately 10 to 15 GB.
  Source: https://www.bls.gov/cew/downloadable-data.htm

- **State minimum wage history** from FRED (Federal Reserve Bank of St. Louis). All 50 states and DC are downloaded as individual CSV series (pattern `STTMINWG{STATE}`) and combined into `data/raw/min_wage/state_mw_history.csv`. States with no state-level minimum wage law use the federal series (`FEDMINNFRWG`).
  Source: https://fred.stlouisfed.org

- **Census Bureau state FIPS table** to `data/raw/state_fips.txt`.
  Source: https://www2.census.gov/geo/docs/reference/state.txt

### 4. Download CPS Data Manually for Full Rebuild

The IPUMS CPS extract must be obtained manually:

1. Register for a free account at https://ipums.org and apply for CPS access.
2. Go to https://cps.ipums.org/cps/ and click "Get Data".
3. Select samples: March ASEC for each year from 2004 to 2024 (cross-sectional).
4. Add the following variables (in addition to the preselected defaults):
   `STATEFIP`, `AGE`, `SEX`, `EMPSTAT`, `LABFORCE`, `UHRSWORKT`, `WKSWORK2`, `INCWAGE`
5. Set the data format to CSV, submit the extract, and wait for the confirmation email.
6. Download the resulting CSV and DDI (codebook) files and save them to `data/raw/cps/`.

### 5. Run the Full Pipeline

After the raw files are in place, run the notebooks in order from 02 through 11.
Each notebook reads from `data/raw/` or `data/intermediate/` and writes outputs
for the next stage. Notebooks 07 through 11 read from
`data/processed/analysis_panel.parquet` produced by notebook 06.

```bash
jupyter notebook
```

Open the `notebooks/` directory and run each notebook top to bottom in sequence.
