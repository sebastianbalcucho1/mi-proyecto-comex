import os
import requests
import urllib3
import datetime

# Desactivar advertencias de certificados SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")
VOLUME_PATH = "/Volumes/workspace/comex/comex_stage/bronze"

URL_BASE_DIAN = "https://www.dian.gov.co/dian/cifras/Basesestadisticasimportaciones"
OUTPUT_DIR = "./downloads"

MESES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}

def obtener_archivos_existentes_databricks():
    """Consulta la API de Databricks para listar los archivos ya almacenados en la Capa Bronze"""
    print("🔍 [AUDITORÍA] Consultando estado actual del Unity Catalog Volume...", flush=True)
    host_limpio = DATABRICKS_HOST.rstrip("/")
    # Endpoint oficial de Databricks para listar contenido de directorios en el FS
    url_api = f"{host_limpio}/api/2.0/fs/directories{VOLUME_PATH}"
    
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url_api, headers=headers, timeout=20)
        
        # Si la carpeta tiene archivos, la API devuelve 200 y una lista bajo la llave 'contents'
        if response.status_code == 200:
            datos = response.json()
            archivos_existentes = set()
            for elemento in datos.get("contents", []):
                # Extraemos solo el nombre del archivo de la ruta completa
                path_completo = elemento.get("path", "")
                nombre_archivo = path_completo.split("/")[-1]
                if nombre_archivo:
                    archivos_existentes.add(nombre_archivo)
            
            print(f"✅ [AUDITORÍA] Se encontraron {len(archivos_existentes)} archivos ya existentes en Databricks.", flush=True)
            return archivos_existentes
            
        elif response.status_code == 404:
            print("ℹ️ [AUDITORÍA] El directorio destino no existe aún (Primera corrida). Procesando full load.", flush=True)
            return set()
        else:
            print(f"⚠️ [WARN] No se pudo auditar Databricks (Código {response.status_code}). Se procederá en modo preventivo.", flush=True)
            return set()
    except Exception as e:
        print(f"🚨 [ERROR] Falló la conexión de auditoría con Databricks: {str(e)}. Continuando sin caché...", flush=True)
        return set()

def ejecutar_pipeline_incremental():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 1. FACHADA DE ESTADO: Obtener lo que ya procesamos en corridas anteriores
    archivos_en_lake = obtener_archivos_existentes_databricks()
    
    anio_actual = datetime.datetime.now().year
    anios_a_procesar = list(range(2001, anio_actual + 1))
    
    print(f"\n📚 [LOG] Iniciando Extracción Incremental Inteligente (Rango: 2001 - {anio_actual})...", flush=True)
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    # Recorremos de la actualidad hacia atrás
    for anio in reversed(anios_a_procesar):
        print(f"\n📂 [PROCESANDO] Evaluando histórico para el Año: {anio}", flush=True)
        
        for num_mes in reversed(range(1, 13)):
            nombre_mes = MESES[num_mes]
            str_mes = str(num_mes).zfill(2)
            
            nombre_archivo = f"{str_mes}_Importaciones_{anio}_{nombre_mes}.zip"
            url_descarga = f"{URL_BASE_DIAN}/{nombre_archivo}"
            ruta_local = os.path.join(OUTPUT_DIR, nombre_archivo)
            
            # 2. CONTROL INCREMENTAL: Si el archivo ya existe en Databricks, lo ignoramos de inmediato
            if nombre_archivo in archivos_en_lake:
                print(f"   ⏭️ [SALTADO] '{nombre_archivo}' ya existe en la Capa Bronze. Omitiendo descarga.", flush=True)
                continue
                
            try:
                # Si no existe, validamos si la DIAN ya lo publicó
                r_check = requests.head(url_descarga, verify=False, headers=headers, timeout=15)
                
                if r_check.status_code == 200:
                    print(f"   🆕 [NUEVO DATOS] Detectado archivo faltante: '{nombre_archivo}'. Descargando...", flush=True)
                    
                    with requests.get(url_descarga, verify=False, headers=headers, stream=True, timeout=30) as r:
                        r.raise_for_status()
                        with open(ruta_local, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=65536):
                                if chunk:
                                    f.write(chunk)
                                    
                    peso_mb = round(os.path.getsize(ruta_local) / (1024 * 1024), 2)
                    print(f"   ✅ [DESCARGADO] Guardado temporal local ({peso_mb} MB)", flush=True)
                    
                    # ==============================================================================
                    # INYECCIÓN DIRECTA A DATABRICKS VIA API REST
                    # ==============================================================================
                    print(f"   🚀 [API Databricks] Transfiriendo binario a Unity Catalog Volume...", flush=True)
                    host_limpio = DATABRICKS_HOST.rstrip("/")
                    url_api = f"{host_limpio}/api/2.0/fs/files{VOLUME_PATH}/{nombre_archivo}"
                    
                    headers_db = {
                        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
                        "Content-Type": "application/octet-stream"
                    }
                    
                    with open(ruta_local, "rb") as f_bin:
                        archivo_binario = f_bin.read()
                        
                    response = requests.put(url_api, headers=headers_db, data=archivo_binario)
                    
                    # Agregamos el 204 nativo de Databricks para marcar éxito real
                    if response.status_code in [200, 201, 204]:
                        print(f"   🔥 [ÉXITO] {nombre_archivo} inyectado correctamente en la Capa Bronze.", flush=True)
                    else:
                        print(f"   ⚠️ [ALERTA] API Databricks devolvió código HTTP {response.status_code} | Detalle: {response.text}", flush=True)
                        
                    os.remove(ruta_local)
                    
                elif r_check.status_code == 404:
                    continue
                else:
                    print(f"   ⚠️ Respuesta HTTP inesperada ({r_check.status_code}) para {nombre_archivo}", flush=True)
                    
            except Exception as e:
                print(f"   ❌ Falla procesando {nombre_archivo}: {str(e)}", flush=True)
                continue

    print("\n🏁 [FIN] Pipeline de extracción incremental finalizado con éxito.", flush=True)

if __name__ == "__main__":
    ejecutar_pipeline_incremental()
