# Detecting Potential Opioid Overprescribing (CMS Medicare Claims + Part D PDE)

## Overview
This project builds an end-to-end analytics workflow to **link Medicare medical claims** (pain-related diagnoses) with **Medicare Part D Prescription Drug Event (PDE)** records and generate **provider-level opioid prescribing metrics**. The goal is to surface **potential outlier prescribing behavior** using transparent, rule-based linkage and summary statistics.

At a high level:
- Identify **pain-related diagnosis claims** (e.g., back pain, neck pain, sprain, headache)
- Link each claim to any Part D prescription fills occurring **within a defined time window** after the claim
- Flag which linked fills are **opioids** (via NDC list)
- Aggregate to **provider-level** rates and volume-based metrics to highlight potential outliers

---

## Objective
**Primary question:**  
> “For pain-related claims, which providers have unusually high opioid prescribing rates shortly after a clinical encounter?”

**What this enables:**
- Estimating how often a provider’s pain claims are followed by an opioid fill (within X days)
- Identifying providers with high opioid rates and/or high-volume opioid fills
- Evaluating fill characteristics such as **days supply distribution** (e.g., % of opioid fills ≥ 10 days)

---

## Data Used
This project uses CMS Medicare-style datasets (or equivalent extracts):

1. **Medical Claims (Carrier / Professional claims)**
   - Key fields used: `BENE_ID`, `CLM_ID`, `PRNCPAL_DGNS_CD`, `PRF_PHYSN_NPI`, `CLM_FROM_DT`, `CLM_THRU_DT`

2. **Medicare Part D Prescription Drug Event (PDE)**
   - Key fields used: `PDE_ID`, `BENE_ID`, `SRVC_DT`, `PROD_SRVC_ID (NDC)`, `DAYS_SUPLY_NUM`, `QTY_DSPNSD_NUM`, `PRSCRBR_ID`

3. **ICD Reference Table**
   - Used to map ICD diagnosis codes to descriptions (`icd_code` → `icd_desc_long`)

4. **Opioid NDC Reference**
   - A list/table of opioid NDC codes to flag opioid fills (`ndc`)

---

## Linkage Rule (Core Logic)
**Option A time-window rule (used in this project):**
- For each pain-related claim, link to PDE fills where:
  - Same beneficiary: `BENE_ID` matches
  - Fill date is within:  
    **`SRVC_DT` ∈ [ `CLM_FROM_DT`, `CLM_THRU_DT` + 7 days ]**

This creates a consistent and explainable “claim → prescription” linkage window that can be tuned (0–7, 0–14, etc.).

---

## What the Pipeline Does (Conceptually)
### Data standardization
- Normalize identifiers (remove hyphens, cast to string)
- Convert dates to real date types
- Clean diagnosis codes (remove punctuation/whitespace, uppercase)
- Join ICD reference to attach human-readable diagnosis descriptions

### Cohort creation
- Filter claims down to pain-related conditions using keyword matching on ICD descriptions:
  - back pain, neck pain, sprain, headache/headaches

### Claim ↔ PDE join
- Left join claims to PDE using the time-window rule
- Flag opioid fills via NDC list
- Deduplicate claim rows where needed to avoid inflating matches

### Metrics and outputs
1. **Match-rate validation outputs**
   - Total pain-related claims
   - Claims with ≥ 1 PDE in window
   - Claims with ≥ 1 opioid PDE in window

2. **Claim-level collapsed table** (1 row per claim)
   - `has_any_pde_0_7`
   - `has_opioid_pde_0_7`
   - `num_pde_fills_0_7`
   - `num_opioid_fills_0_7`

3. **Provider-level metrics**
   - Pain claim volume
   - Opioid rate per pain claim
   - Total opioid fills linked to pain claims
   - Simple outlier ranking + optional z-score analysis

4. **Days-supply profile for opioid fills**
   - For each provider: % of opioid fills with `DAYS_SUPLY_NUM` ≥ 3, ≥ 7, ≥ 10 (optional ≥14, ≥30)

---

## Results Achieved (from current run)
From the current dataset + filters:

- **Total pain-related carrier claims:** `25,463`
- **Claims with ≥ 1 PDE match within 0–7 days:** `5,906`
- **Claims with ≥ 1 opioid PDE match (subset of matched claims):** `769`

Then, the project produces:
- A provider ranking by **opioid_rate_per_pain_claim**
- A volume-filtered view of providers to reduce small-sample noise
- A days-supply profile table showing how often providers’ opioid fills exceed thresholds (≥10 days, etc.)

> Note: Some providers show very high opioid rates (even 1.0) at low volumes—this is expected and is why volume filters (e.g., pain_claims ≥ 30) are used for more credible outlier views.

---

## Tech Stack
### Analytics / Transformation
- **PySpark** (Spark DataFrames, joins, aggregations)
- **Python**

### Cloud / Data Platform (design intent)
This workflow aligns cleanly with an AWS lakehouse pattern:
- **S3**: store raw extracts + curated Parquet tables
- **AWS Lambda**: trigger ingestion/processing on file arrival or schedule
- **AWS Glue (Spark ETL)**: scalable transformation + standardization jobs
- **Athena**: interactive SQL analytics over curated Parquet tables

> In this repo, the notebook demonstrates the Spark ETL + analytics logic directly; the same transformations can be moved into Glue jobs and queried via Athena.

---



