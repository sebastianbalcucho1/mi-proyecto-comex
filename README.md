# DIAN Comex Ingestion Pipeline

An automated, robust, and stateful incremental data pipeline designed to ingest historical and current Customs and Foreign Trade (Import) data directly from the official DIAN (Dirección de Impuestos y Aduanas Nacionales de Colombia) platform into a Databricks Unity Catalog Volume (Bronze Layer).

## Project Architecture & Overview
The pipeline operates in two main orchestration steps, ensuring high efficiency, zero data duplication, and optimal computing resource allocation:

* Orchestration & Triggering (GitHub Actions): A weekly cron job triggers a specialized Python scraper inside a headless runner.

* Stateful Incremental Audit (Databricks FS API): Before downloading anything, the script queries the Databricks File System API to build a state cache of already ingested files.

Deterministic Streaming Ingestion (requests): Instead of fighting complex asynchrony in SharePoint dynamic grids, the pipeline targets deterministic, predictable static asset endpoints on the DIAN web server. New/missing files are streamed and sent via REST API to Unity Catalog.

```text
[GitHub Actions Runner] 
       │
       ├── 1. GET /api/2.0/fs/directories/bronze ──> [Databricks Unity Catalog]
       │                                                      │
       │   <─── Returns JSON Cache of Existing Files ─────────┘
       │
       ├── 2. HEAD Check / Download Stream ────────────────> [DIAN Web Servers]
       │
       └── 3. PUT /api/2.0/fs/files/bronze/file.zip ───────> [Databricks Unity Catalog Volume]
```

## Tech Stack
* Orchestrator: GitHub Actions
* Core Language: Python 3.10+
* Key Libraries: requests (for chunked binary streams), urllib3 (SSL resilience), re (deterministic pattern matching).
* Target Platform: Databricks Unity Catalog (Bronze Stage Volume).

## Repository Structure
```text
├── .github/
│   └── workflows/
│       └── dian_pipeline.yml     # GitHub Actions workflow configuration (Cron & Manual)
├── downloads/                    # Temporary container directory for chunked binary stream
├── scraper_dian.py               # Main pipeline ingestion script (Incremental Logic)
└── README.md                     # Documentation 
```



## Core Pipeline Logic: scraper_dian.py
The script avoids full-load computational waste by enforcing a Stateful Ingestion pattern:
* Idempotency: The pipeline can be executed multiple times a day without uploading duplicate binaries or overlapping chunks.
* Network Efficiency: It maps file naming patterns natively used by the DIAN matching the strict structure: MM_Importaciones_YYYY_MonthName.zip (e.g., 12_Importaciones_2024_Diciembre.zip).
* Resilience: Includes a dual validation gate (handling Databricks native HTTP 204 No Content status and handling DIAN SSL certificate issues using unverified contexts securely inside the runner).

## Deployment & Configuration
1. **GitHub Secrets Setup**
To allow the GitHub runner to authenticate safely against your Databricks Workspace Control Plane, you must map the following environment variables under Settings > Secrets and variables > Actions:

| Secret Name | Description | Example |
| :--- | :--- | :--- |
| `DATABRICKS_HOST` | The secure URL endpoint of your Databricks instance. | `https://adb-XXXXXX.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Personal Access Token (PAT) with write access to the target Volume. | `dapiXXXXXXXXXXXXXXXXXXXXXXXX` |

2. **Unity Catalog Destination Target**
The script targets the following internal path infrastructure by default:
VOLUME_PATH = "/Volumes/workspace/comex/comex_stage/bronze"
