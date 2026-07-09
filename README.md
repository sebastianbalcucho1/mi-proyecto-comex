# Pipeline de Ingesta de DIAN Comex

Un pipeline de datos incremental, automatizado, robusto y con estado, diseñado para ingerir datos históricos y actuales de Aduanas y Comercio Exterior (Importación) directamente desde la plataforma oficial de DIAN (Dirección de Impuestos y Aduanas Nacionales de Colombia) a un Volumen de Catálogo Databricks Unity (Capa Bronce).

## Arquitectura y Descripción General del Proyecto
El pipeline opera en dos pasos principales de orquestación, lo que garantiza una alta eficiencia, cero duplicación de datos y una asignación óptima de recursos informáticos:

* Orquestación y Activación (GitHub Actions): Una tarea programada semanal (cron job) activa un raspador Python especializado dentro de un ejecutor sin interfaz gráfica.

* Auditoría Incremental con Estado (API del Sistema de Archivos de Databricks): Antes de descargar cualquier archivo, el script consulta la API del Sistema de Archivos de Databricks para crear una caché de estado de los archivos ya ingeridos.

Ingesta de datos en tiempo real (solicitudes): En lugar de lidiar con la compleja asincronía de las cuadrículas dinámicas de SharePoint, la canalización se dirige a puntos finales de recursos estáticos, deterministas y predecibles en el servidor web DIAN. Los archivos nuevos o faltantes se transmiten y envían a través de la API REST al catálogo de Unity.

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

## Tecnologías utilizadas
* Orquestador: GitHub Actions
* Lenguaje principal: Python 3.10+
* Bibliotecas clave: requests (para flujos binarios fragmentados), urllib3 (resiliencia SSL), re (coincidencia de patrones determinista).
* Plataforma de destino: Catálogo Databricks Unity (Volumen de la Etapa Bronce).

## Estructura del repositorio
```text
├── .github/
│   └── workflows/
│       └── dian_pipeline.yml     # Configuración del flujo de trabajo de GitHub Actions (Cron y manual)
├── downloads/                    # Directorio contenedor temporal para el flujo binario fragmentado
├── scraper_dian.py               # Script principal de ingesta del pipeline (lógica incremental)
└── README.md                     # Documentación
```

## Lógica principal del pipeline: scraper_dian.py
El script evita el desperdicio computacional por carga completa al aplicar un patrón de ingesta con estado:
* Idempotencia: El pipeline se puede ejecutar varias veces al día sin cargar binarios duplicados ni fragmentos superpuestos.
* Eficiencia de red: Mapea los patrones de nombres de archivo utilizados de forma nativa por DIAN, que coinciden con la estructura estricta: MM_Importaciones_AAAA_NombreDelMes.zip (p. ej., 12_Importaciones_2024_Diciembre.zip).

* Resiliencia: Incluye una doble puerta de validación (que gestiona el estado HTTP 204 Sin contenido nativo de Databricks y los problemas con el certificado SSL de DIAN mediante contextos no verificados de forma segura dentro del ejecutor).

## Implementación y configuración
1. **Configuración de secretos de GitHub**
Para permitir que el ejecutor de GitHub se autentique de forma segura con el plano de control de su espacio de trabajo de Databricks, debe asignar las siguientes variables de entorno en Configuración > Secretos y variables > Acciones:

| Nombre Secreto | Descripción | Ejemplo |
| :--- | :--- | :--- |
| `DATABRICKS_HOST` | El punto final URL seguro de su instancia de Databricks. | `https://adb-XXXXXX.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Token de acceso personal (PAT) con permisos de escritura para el volumen de destino. | `dapiXXXXXXXXXXXXXXXXXXXXXXXX` |

2. **Destino del catálogo de Unity**
El script utiliza la siguiente ruta interna de forma predeterminada:
VOLUME_PATH = "/Volumes/workspace/comex/comex_stage/bronze"
