import os
import requests
import urllib3
import datetime

# Desactivar alertas de certificados SSL antiguos
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")
VOLUME_PATH = "/Volumes/workspace/comex/comex_stage/bronze"

URL_BASE_DIAN = "https://www.dian.gov.co/dian/cifras/BasesEstadisticas/Importaciones"
OUTPUT_DIR = "./downloads"

def descargar_e_inyectar_historico_dinamico():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # CONTROL DE TIEMPO DINÁMICO (Cero Hardcoding)
    anio_actual = datetime.datetime.now().year
    anios_a_procesar = list(range(2001, anio_actual + 1))
    
    print(f"📚 Iniciando Pipeline Resiliente de Importaciones (Rango: 2001 - {anio_actual})...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    for anio in anios_a_procesar:
        nombre_archivo = f"Importaciones_{anio}.zip"
        url_descarga = f"{URL_BASE_DIAN}/Importaciones_{anio}.zip"
        ruta_local = os.path.join(OUTPUT_DIR, nombre_archivo)
        
        print(f"\n⏳ [{anio}] Conectando con el endpoint...")
        
        try:
            with requests.get(url_descarga, verify=False, headers=headers, stream=True, timeout=30) as r:
                if r.status_code == 200:
                    with open(ruta_local, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=65536):
                            if chunk:
                                f.write(chunk)
                    
                    peso_mb = round(os.path.getsize(ruta_local) / (1024 * 1024), 2)
                    print(f"✅ Descargado: {nombre_archivo} ({peso_mb} MB)")
                    
                    # Inyección mediante la API de Databricks
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
                        print(f"🚀 {nombre_archivo} indexado con éxito en el Volumen.")
                    else:
                        print(f"⚠️ Error API Databricks para {anio}: Status {response.status_code}")
                    
                    os.remove(ruta_local)
                    
                elif r.status_code == 404:
                    print(f"ℹ️ El año {anio} no reporta base de datos consolidada (404 Not Found). Saltando.")
                else:
                    print(f"❌ Error HTTP {r.status_code} en año {anio}")
                    
        except Exception as e:
            print(f"🚨 Excepción en año {anio}: {str(e)}")
            continue

    print("\n🏁 Pipeline de extracción finalizado con éxito.")

if __name__ == "__main__":
    descargar_e_inyectar_historico_dinamico()
