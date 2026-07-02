import os
import requests
import urllib3
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Desactivar advertencias de certificados SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")
VOLUME_PATH = "/Volumes/workspace/comex/comex_stage/bronze"

URL_BASE_DIAN = "https://www.dian.gov.co/dian/cifras/Basesestadisticasimportaciones"
OUTPUT_DIR = "./downloads"

# Ajusta este número según la velocidad que quieras. 4 o 5 hilos en paralelo suele ser el punto dulce 
# para no saturar los límites de tasa (rate limits) de la API de Databricks o de la DIAN.
MAX_WORKERS = 4 

MESES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}

def obtener_archivos_existentes_databricks():
    print("🔍 [AUDITORÍA] Consultando estado actual del Unity Catalog Volume...", flush=True)
    host_limpio = DATABRICKS_HOST.rstrip("/")
    url_api = f"{host_limpio}/api/2.0/fs/directories{VOLUME_PATH}"
    
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url_api, headers=headers, timeout=20)
        if response.status_code == 200:
            datos = response.json()
            archivos_existentes = set()
            for elemento in datos.get("contents", []):
                path_completo = elemento.get("path", "")
                nombre_archivo = path_completo.split("/")[-1]
                if nombre_archivo:
                    archivos_existentes.add(nombre_archivo)
            print(f"✅ [AUDITORÍA] Se encontraron {len(archivos_existentes)} archivos ya existentes en Databricks.", flush=True)
            return archivos_existentes
        elif response.status_code == 404:
            return set()
        else:
            return set()
    except Exception as e:
        print(f"🚨 [ERROR] Falló la conexión de auditoría: {str(e)}", flush=True)
        return set()

def procesar_un_mes(anio, num_mes, archivos_en_lake, headers):
    """Función independiente para procesar un único archivo mensual (será ejecutada por un hilo)"""
    nombre_mes = MESES[num_mes]
    str_mes = str(num_mes).zfill(2)
    nombre_archivo = f"{str_mes}_Importaciones_{anio}_{nombre_mes}.zip"
    url_descarga = f"{URL_BASE_DIAN}/{nombre_archivo}"
    ruta_local = os.path.join(OUTPUT_DIR, f"{anio}_{nombre_archivo}") # Nombre único por hilo
    
    if nombre_archivo in archivos_en_lake:
        return f"   ⏭️ [SALTADO] '{nombre_archivo}' ya existe en la Capa Bronze."
        
    try:
        r_check = requests.head(url_descarga, verify=False, headers=headers, timeout=15)
        
        if r_check.status_code == 200:
            print(f"   🆕 [DESCARGANDO EN PARALELO] -> '{nombre_archivo}'...", flush=True)
            
            with requests.get(url_descarga, verify=False, headers=headers, stream=True, timeout=45) as r:
                r.raise_for_status()
                with open(ruta_local, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        if chunk:
                            f.write(chunk)
                            
            peso_mb = round(os.path.getsize(ruta_local) / (1024 * 1024), 2)
            
            # Transferencia a Databricks Volume
            host_limpio = DATABRICKS_HOST.rstrip("/")
            url_api = f"{host_limpio}/api/2.0/fs/files{VOLUME_PATH}/{nombre_archivo}"
            
            headers_db = {
                "Authorization": f"Bearer {DATABRICKS_TOKEN}",
                "Content-Type": "application/octet-stream"
            }
            
            with open(ruta_local, "rb") as f_bin:
                archivo_binario = f_bin.read()
                
            response = requests.put(url_api, headers=headers_db, data=archivo_binario)
            
            # Limpieza inmediata del archivo temporal local
            if os.path.exists(ruta_local):
                os.remove(ruta_local)
                
            if response.status_code in [200, 201, 204]:
                return f"   🔥 [ÉXITO] {nombre_archivo} inyectado correctamente ({peso_mb} MB)."
            else:
                return f"   ⚠️ [ALERTA] API Databricks devolvió código HTTP {response.status_code} para {nombre_archivo}"
                
        elif r_check.status_code == 404:
            return None # El mes no existe en la DIAN, se ignora en silencio
        else:
            return f"   ⚠️ Respuesta HTTP inesperada ({r_check.status_code}) para {nombre_archivo}"
            
    except Exception as e:
        if os.path.exists(ruta_local):
            os.remove(ruta_local)
        return f"   ❌ Falla crítica procesando {nombre_archivo}: {str(e)}"

def ejecutar_pipeline_concurrente():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    archivos_en_lake = obtener_archivos_existentes_databricks()
    anio_actual = datetime.datetime.now().year
    
    # Construimos la lista de todas las tareas (combinaciones de año y mes)
    tareas = []
    for anio in range(2001, anio_actual + 1):
        for num_mes in range(1, 13):
            tareas.append((anio, num_mes))
            
    # Invertimos el orden para priorizar los datos más recientes
    tareas.reverse()
    
    print(f"\n⚡ [LOG] Iniciando Extracción Concurrente con {MAX_WORKERS} hilos activos...", flush=True)
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    # Orquestador del grupo de hilos concurrentes
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Enviamos todas las tareas al pool
        futures = {
            executor.submit(procesar_un_mes, anio, num_mes, archivos_en_lake, headers): (anio, num_mes)
            for anio, num_mes in tareas
        }
        
        # Conforme cada hilo va terminando, imprimimos su resultado en los logs en tiempo real
        for future in as_completed(futures):
            resultado = future.result()
            if resultado:
                print(resultado, flush=True)

    print("\n🏁 [FIN] Pipeline de extracción masiva en paralelo finalizado con éxito.", flush=True)

if __name__ == "__main__":
    ejecutar_pipeline_concurrente()
