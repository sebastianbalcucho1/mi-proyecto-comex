DIAN Comex Ingestion Pipeline 🚀An automated, robust, and stateful incremental data pipeline designed to ingest historical and current Customs and Foreign Trade (Import) data directly from the official DIAN (Dirección de Impuestos y Aduanas Nacionales de Colombia) platform into a Databricks Unity Catalog Volume (Bronze Layer).📌 Project Architecture & OverviewThe pipeline operates in two main orchestration steps, ensuring high efficiency, zero data duplication, and optimal computing resource allocation:Orchestration & Triggering (GitHub Actions): A weekly cron job triggers a specialized Python scraper inside a headless runner.Stateful Incremental Audit (Databricks FS API): Before downloading anything, the script queries the Databricks File System API to build a state cache of already ingested files.Deterministic Streaming Ingestion (requests): Instead of fighting complex asynchrony in SharePoint dynamic grids, the pipeline targets deterministic, predictable static asset endpoints on the DIAN web server. New/missing files are streamed and sent via REST API to Unity Catalog.[GitHub Actions Runner] 
       │
       ├── 1. GET /api/2.0/fs/directories/bronze ──> [Databricks Unity Catalog]
       │                                                      │
       │   <─── Returns JSON Cache of Existing Files ─────────┘
       │
       ├── 2. HEAD Check / Download Stream ────────────────> [DIAN Web Servers]
       │
       └── 3. PUT /api/2.0/fs/files/bronze/file.zip ───────> [Databricks Unity Catalog Volume]
🛠️ Tech StackOrchestrator: GitHub ActionsCore Language: Python 3.10+Key Libraries: requests (for chunked binary streams), urllib3 (SSL resilience), re (deterministic pattern matching).Target Platform: Databricks Unity Catalog (Bronze Stage Volume).📂 Repository Structure├── .github/
│   └── workflows/
│       └── dian_pipeline.yml     # GitHub Actions workflow configuration (Cron & Manual)
├── downloads/                    # Temporary container directory for chunked binary stream
├── scraper_dian.py               # Main pipeline ingestion script (Incremental Logic)
└── README.md                     # Documentation
⚙️ Core Pipeline Logic: scraper_dian.pyThe script avoids full-load computational waste by enforcing a Stateful Ingestion pattern:Idempotency: The pipeline can be executed multiple times a day without uploading duplicate binaries or overlapping chunks.Network Efficiency: It maps file naming patterns natively used by the DIAN matching the strict structure: MM_Importaciones_YYYY_MonthName.zip (e.g., 12_Importaciones_2024_Diciembre.zip).Resilience: Includes a dual validation gate (handling Databricks native HTTP 204 No Content status and handling DIAN SSL certificate issues using unverified contexts securely inside the runner).🚀 Deployment & Configuration1. GitHub Secrets SetupTo allow the GitHub runner to authenticate safely against your Databricks Workspace Control Plane, you must map the following environment variables under Settings > Secrets and variables > Actions:Secret NameDescriptionExampleDATABRICKS_HOSTThe secure URL endpoint of your Databricks instance.https://adb-XXXXXX.cloud.databricks.comDATABRICKS_TOKENPersonal Access Token (PAT) with write access to the target Volume.dapiXXXXXXXXXXXXXXXXXXXXXXXX2. Unity Catalog Destination TargetThe script targets the following internal path infrastructure by default:VOLUME_PATH = "/Volumes/workspace/comex/comex_stage/bronze"
Note: Ensure that the catalog (workspace), schema (comex), and volume (comex_stage) are correctly instantiated before running the pipeline.📈 Monitoring and Execution LogsThanks to the enforcement of the PYTHONUNBUFFERED: 1 environment flag inside the GitHub workflow runner, all status steps, network checks, and API calls are streamed in real time to the Actions log console.Expected Console Output Pattern:🔍 [AUDITORÍA] Consultando estado actual del Unity Catalog Volume...
✅ [AUDITORÍA] Se encontraron 288 archivos ya existentes en Databricks.

📚 [LOG] Iniciando Extracción Incremental Inteligente (Rango: 2001 - 2026)...

📂 [PROCESANDO] Evaluando histórico para el Año: 2026
   ⏭️ [SALTADO] '04_Importaciones_2026_Abril.zip' ya existe en la Capa Bronze. Omitiendo descarga.
   ⏭️ [SALTADO] '03_Importaciones_2026_Marzo.zip' ya existe en la Capa Bronze. Omitiendo descarga.

📂 [PROCESANDO] Evaluando histórico para el Año: 2024
   🆕 [NUEVO DATOS] Detectado archivo faltante: '12_Importaciones_2024_Diciembre.zip'. Descargando...
   ✅ [DESCARGADO] Guardado temporal local (264.31 MB)
   🚀 [API Databricks] Transfiriendo binario a Unity Catalog Volume...
   🔥 [ÉXITO] 12_Importaciones_2024_Diciembre.zip inyectado correctamente en la Capa Bronze.
🔮 Next Steps: Downstream Processing in Databricks (Silver Layer)Once the files successfully land in the Bronze Volume as raw .zip binaries, you can initialize a notebook in Databricks using PySpark to untar/unzip them on the fly and parse them into Delta Tables:# Spark validation snippet
archivos_bronze = dbutils.fs.ls("dbfs:/Volumes/workspace/comex/comex_stage/bronze/")

for f in archivos_bronze:
    print(f"📦 Delta Source Candidate: {f.name} | Size: {round(f.size / (1024*1024), 2)} MB")
Maintainer: Data Engineering Team | Behavioral Economics & Analytics Subdirection.
