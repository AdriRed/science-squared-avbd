I reviewed the current notebooks and README. The README’s analysis sequence points to several **old notebook names**; the current sequence is numbered `01`–`09`. The notebooks cover Catalan publication data, OpenAlex enrichment, ORCID employment data, APC/open-access analysis, publication trends by domain, author productivity, and university collaborations.

A few issues should be addressed before promising that the workflow runs end to end: the ORCID notebook has inconsistent data paths; notebook `06` has a saved `KeyboardInterrupt` and placeholder text; and notebook `05` uses existing Parquet inputs, so its dependencies and outputs should be documented. Also, ORCID IDs and linked employment histories can identify people, so avoid describing all project data as anonymous. The OpenAlex crawler has a separate guide at `docs/openalex-crawler.md`.

Here’s a README draft aligned to the current files:

```md
# Science² — Science of Science

An analysis of research publications associated with Catalonia. The project explores publication trends, research domains, author productivity, open access and article processing charges (APCs), and collaboration between universities.

## Research questions

- How has Catalan research publication output changed over time?
- How are publications distributed across research domains?
- What patterns appear in author publication counts, overall and by domain?
- How have open access and APCs varied over time, by domain, journal, and publisher?
- How frequently do Catalan universities publish in collaboration?

## Team

Albert Fontbuté · Alba Sentoll · Carlos Jové · Adrià Rojo

## Data sources

| Source | Use |
|---|---|
| [Catalonia Open Data Portal](https://analisi.transparenciacatalunya.cat/) | Publication records, including dates, DOIs, institutions, and ORCID identifiers |
| [OpenAlex](https://openalex.org/) | Work, journal, publisher, domain, open-access, APC, and authorship metadata |
| [ORCID Public API](https://info.orcid.org/documentation/) | Public researcher employment information |

The project processes approximately 1.75 million Catalan publication records. Dataset licences, attribution requirements, retrieval dates, and the precise date coverage should be recorded here before redistribution.

## Repository layout

```text
data/
  cache/          Temporary downloads and crawler checkpoints
  external/       Source and enriched datasets
  processed/      Derived datasets and analysis inputs
notebooks/
  lib/            Shared OpenAlex and plotting helpers
  01–09*.ipynb    Data collection, processing, and analysis notebooks
docs/             Crawler documentation
plots/            Exported figures
environment.yml   Conda environment
```

## Setup

```bash
git clone https://github.com/AdriRed/science-squared-avbd.git
cd science-squared-avbd
conda env create -f environment.yml
conda activate ub-bigdata-viz
```

Copy `.env.example` to `.env` and add the credentials required for the APIs you plan to use. Keep `.env` private; never commit API tokens or passwords.

Start JupyterLab from the `notebooks/` directory so the notebooks’ relative paths to `../data` resolve as expected:

```bash
cd notebooks
jupyter lab
```

Some download steps make many API requests, can take a long time, and may be subject to rate limits or quotas. The OpenAlex crawler’s retry and resume behavior is documented in [`docs/openalex-crawler.md`](docs/openalex-crawler.md).

## Notebook workflow

Run the notebooks in numerical order, checking each notebook’s inputs and outputs before proceeding:

1. `01_load_data.ipynb` — downloads and cleans publication records; creates DOI and ORCID inputs.
2. `02.1_enrich_papers_data.ipynb` — runs the resumable OpenAlex crawler.
3. `02.2_enrich_authors_data.ipynb` — retrieves public ORCID employment records.
4. `03_adri_apc_data_processing.ipynb` — joins publication, OpenAlex, journal, publisher, and open-access data for APC analyses.
5. `04_adri_apc_plots.ipynb` — creates open/closed-access, publisher, APC, and alluvial visualizations.
6. `05_alba_plots_domains_topics.ipynb` — examines publication trends by year and research domain/topic.
7. `06_albert_author_publications_data_processing.ipynb` — prepares publication and researcher data for author-productivity analysis.
8. `07_albert_author_publications_per_year_plots.ipynb` — analyzes author publication-count distributions.
9. `08_albert_author_publications_per_domain_plots.ipynb` — compares author productivity distributions across research domains.
10. `09_carlos_university_collaborations_data_processing.ipynb` — builds university co-publication link tables.

**Reproducibility status:** notebook `06` currently contains a saved interruption during output generation. Verify or repair it before treating steps `06`–`08` as a clean run-from-scratch workflow. Some later notebooks also rely on processed Parquet files, so document which notebook creates each required input.

## Data handling and limitations

- Parquet is used for the main tabular datasets; CSV is used for smaller exchange and link tables.
- API results may be incomplete, unavailable, or change over time. Record retrieval dates and retain the source and processing metadata.
- Missing APC values do not necessarily mean zero.
- ORCID identifiers and associated employment information can identify individuals, even when obtained from public profiles. Use only fields needed for the analysis and check institutional data-protection guidance before publishing person-level data.
- Check each source’s terms and licence before redistributing its data. The MIT licence for project code does not automatically apply to third-party datasets.
