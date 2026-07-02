import os
import requests
import urllib3
import datetime

# Desactivar advertencias de certificados SSL (muy común en la infraestructura DIAN)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")
VOLUME_PATH = "/Volumes/workspace/comex/comex_stage/bronze"

URL_BASE_DIAN = "https://www.dian.gov.co/dian/cifras/Basesestadisticasimportaciones"
OUTPUT_DIR = "./downloads"

# Diccionario semántico para mapear el número de mes con el nombre exacto de la URL de la DIAN
MESES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}

def ejecutar_pipeline_estatico():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Rango histórico dinámico desde 2001 hasta el año actual en curso
    anio_actual = datetime.datetime.now().year
    anios_a_procesar = list(range(2001, anio_actual + 1))
    
    print(f"📚 [LOG] Iniciando Extracción Industrial (Rango: 2001 - {anio_actual})...", flush=True)
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    # Recorremos el histórico al revés (de la actualidad hacia atrás) para priorizar los datos más nuevos
    for anio in reversed(anios_a_procesar):
        print(f"\n📂 [PROCESANDO] Generando consultas para el Año: {anio}", flush=True)
        
        for num_mes in reversed(range(1, 13)):
            nombre_mes = MESES[num_mes]
            str_mes = str(num_mes).zfill(2) # Convierte 5 en '05' o 12 en '12'
            
            # Formato exacto extraído de tu captura de pantalla
            nombre_archivo = f"{str_mes}_Importaciones_{anio}_{nombre_mes}.zip"
            url_descarga = f"{URL_BASE_DIAN}/{nombre_archivo}"
            ruta_local = os.path.join(OUTPUT_DIR, nombre_archivo)
            
            try:
                # Hacemos una petición ligera HEAD para validar si el archivo existe antes de descargarlo
                r_check = requests.head(url_descarga, verify=False, headers=headers, timeout=15)
                
                if r_check.status_code == 200:
                    print(f"   ⬇️ [DETECTADO] Archivo mensual real: '{nombre_archivo}'. Descargando por stream...", flush=True)
                    
                    # Descarga por bloques para cuidar la memoria del contenedor
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
                    
                    if response.status_code in [200, 201]:
                        print(f"   🔥 [ÉXITO] {nombre_archivo} inyectado correctamente en Capa Bronze.", flush=True)
                    else:
                        print(f"   ⚠️ [ALERTA] API Databricks devolvió código HTTP {response.status_code}", flush=True)
                        
                    os.remove(ruta_local)
                    
                elif r_check.status_code == 404:
                    # Es totalmente normal para meses futuros del año corriente
                    continue
                else:
                    print(f"   ⚠️ Respuesta HTTP inesperada ({r_check.status_code}) para {nombre_archivo}", flush=True)
                    
            except Exception as e:
                print(f"   ❌ Falla procesando {nombre_archivo}: {str(e)}", flush=True)
                continue

    print("\n🏁 [FIN] Pipeline de extracción histórica estática finalizado con éxito.", flush=True)

if __name__ == "__main__":
    ejecutar_pipeline_estatico()
