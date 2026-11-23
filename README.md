
# Science² — Science of Science

Analysis of the evolution, collaboration, funding, open-access status, and publication costs of research produced in Catalonia.

## Research questions

This project investigates:

- How has Catalan scientific output changed over time?
- How are publications distributed across research fields and institutions?
- Do author productivity patterns follow power-law behaviour?
- How have open-access publishing and article processing charges evolved?
- How are publications connected to funders, publishers, and researcher employment histories?

## Team

Albert Fontbuté · Alba Sentoll · Carlos Jové · Adrià Rojo

## Data sources

| Source | Use |
|---|---|
| [Open Data Portal of Catalonia](https://analisi.transparenciacatalunya.cat/) | Publication records, dates, institutions, DOIs, and ORCID identifiers |
| [OpenAlex](https://openalex.org/) | Work metadata, research fields, publishers, open-access status, APCs, funders, and references |
| [ORCID](https://orcid.org/) | Public researcher employment histories |

The main Catalan publication dataset contains approximately 1.75 million records from 1949–2026. Data licences and retrieval dates should be documented here before publication.

## Repository structure

```text
data/
  external/       Downloaded and enriched datasets
  processed/      Derived datasets, including yearly partitions
notebooks/
  *_load_data.ipynb
  *_enrich_*.ipynb
  *_plots.ipynb
plots/
  adri/           Exported figures
environment.yml   Conda environment
CHECKLIST.md      Reproducibility checklist
```

## Installation

```bash
git clone https://github.com/AdriRed/science-squared-avbd.git
cd science-squared-avbd

conda env create -f environment.yml
conda activate ub-bigdata-viz
jupyter lab
```

Run notebooks from the `notebooks/` directory, since they currently use relative paths such as `../data`.

## Analysis workflow

1. `notebooks/01_load_data.ipynb`  
   Downloads and cleans Catalan publication records, creating DOI and ORCID inputs.

2. `notebooks/02.1_enrich_papers_data.ipynb`  

   > **!! This file doesn't fully download all OpenAlex datasets !!**

   Enriches publications with OpenAlex work metadata, fields, topics, APCs, funders, citations, and publishers.

3. `notebooks/02.2_enrich_authors_data.ipynb`  
   Retrieves public ORCID employment histories with resumable checkpoints.

4. `notebooks/alba_02_plots.ipynb`  
   Creates yearly publication datasets and publication-trend figures.

5. `notebooks/albert_01_power_laws.ipynb`  
   Examines author productivity distributions.

6. `notebooks/albert_02_power_laws.ipynb`  
   Fits productivity distributions by research domain.

7. `notebooks/adri_03_processing.ipynb` and `notebooks/adri_04_plots.ipynb`  
   Analyse APCs, open access, research fields, and publishers.
   
## Credentials

API credentials must be stored in a local `.env` file and must never be committed:

```env
SOCRATA_APP_TOKEN=
SOCRATA_USER_ID=
SOCRATA_USER_PASSWD=
OPENALEX_EMAIL=
OPENALEX_API_KEY=
ORCID_USER_ID=
ORCID_USER_PASSWD=
```

See `.env.example` for the expected variables.

## Reproducibility notes

- The OpenAlex and ORCID crawlers support retries and checkpoints.
- Existing enrichment files are append-only and are not automatically refreshed.
- Delete the relevant Parquet files when a complete rebuild is required.
- Missing APC values represent unknown, unavailable, or non-applicable values; they should not automatically be interpreted as zero.
- Results depend on the retrieval date and on changes to the external APIs.
