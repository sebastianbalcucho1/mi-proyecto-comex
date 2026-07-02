import os
import time
import requests
from playwright.sync_api import sync_playwright

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")
VOLUME_PATH = "/Volumes/workspace/comex/comex_stage/bronze"

URL_DIAN = "https://www.dian.gov.co/dian/cifras/Paginas/Bases-Estadisticas-de-Comercio-Exterior-Importaciones-y-Exportaciones.aspx"
OUTPUT_DIR = "./downloads"

def ejecutar_pipeline_interactivo():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    with sync_playwright() as p:
        print("🤖 Levantando Chromium Headless...")
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        page = context.new_page()
        
        print("🌐 Navegando a la suite de Comercio Exterior de la DIAN...")
        page.goto(URL_DIAN, timeout=90000)
        page.wait_for_load_state("networkidle")
        
        # PASO 1: Clic en el acordeón principal "Bases Estadísticas de Importaciones"
        print("➡️ Pasos de navegación: Abriendo sección principal de Importaciones...")
        seccion_importaciones = page.locator("text=Bases Estadísticas de Importaciones").first
        seccion_importaciones.click()
        time.sleep(2)
        
        # PASO 2: Identificar los bloques de años disponibles en el DOM
        # Buscamos elementos que comiencen con la palabra 'Año'
        bloques_anios = page.locator("//span[contains(text(), 'Año') or contains(text(), 'Año')] | //a[contains(text(), 'Año')]").all()
        print(f"📊 Se detectaron {len(bloques_anios)} bloques anuales en la grilla.")
        
        # Iteramos de forma estructurada por cada año visible
        for i in range(len(bloques_anios)):
            try:
                nodo_anio = bloques_anios[i]
                texto_anio = nodo_anio.inner_text().strip()
                print(f"\n📂 Expandiendo nodo: {texto_anio}")
                
                # Clic en el año para desplegar los archivos mensuales (PASO 2 de tu flujo)
                nodo_anio.click()
                time.sleep(3) # Espera obligatoria para que SharePoint cargue los elementos hijos
                
                # PASO 3: Capturar los enlaces de los archivos que se acaban de desplegar debajo
                # Filtramos los elementos <a> que tengan palabras clave de importaciones o meses y que pertenezcan a la vista activa
                enlaces_mensuales = page.locator("a").all()
                
                for el in enlaces_mensuales:
                    texto_mes = el.inner_text().strip()
                    href = el.get_attribute("href")
                    
                    # Si el texto coincide con la estructura de tu captura (ej: 12_Importaciones_2024_Diciembre)
                    if texto_mes and "importaciones" in texto_mes.lower() and ("sharepoint" in str(href).lower() or href == "" or "#" in str(href)):
                        print(f"   ⬇️ Archivo mensual detectado: '{texto_mes}'. Iniciando descarga...")
                        
                        # Capturamos el evento de descarga nativo de SharePoint al hacer clic (PASO 4 de tu flujo)
                        with page.expect_download(timeout=60000) as download_info:
                            el.click()
                        
                        download = download_info.value
                        nombre_archivo_final = f"{texto_mes.replace(' ', '_')}.zip"
                        ruta_local = os.path.join(OUTPUT_DIR, nombre_archivo_final)
                        
                        # Guardamos el archivo en el contenedor de GitHub
                        download.save_as(ruta_local)
                        peso_mb = round(os.path.getsize(ruta_local) / (1024 * 1024), 2)
                        print(f"   ✅ Archivo en GitHub: {nombre_archivo_final} ({peso_mb} MB)")
                        
                        # ==============================================================================
                        # INYECCIÓN INMEDIATA A DATABRICKS VIA API REST
                        # ==============================================================================
                        print(f"   🚀 Transfiriendo a Unity Catalog Volume...")
                        host_limpio = DATABRICKS_HOST.rstrip("/")
                        url_api = f"{host_limpio}/api/2.0/fs/files{VOLUME_PATH}/{nombre_archivo_final}"
                        
                        headers_db = {
                            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
                            "Content-Type": "application/octet-stream"
                        }
                        
                        with open(ruta_local, "rb") as f_bin:
                            archivo_binario = f_bin.read()
                            
                        response = requests.put(url_api, headers=headers_db, data=archivo_binario)
                        
                        if response.status_code in [200, 201]:
                            print(f"   🔥 Inyección Completa en Bronze: {nombre_archivo_final}")
                        else:
                            print(f"   ⚠️ Error en API Databricks (Status {response.status_code})")
                            
                        # Limpieza de disco local inmediato
                        os.remove(ruta_local)
                
                # Volvemos a hacer clic en el año para colapsarlo y mantener limpio el DOM antes del siguiente
                nodo_anio.click()
                time.sleep(1)
                
            except Exception as e:
                print(f"🚨 No se pudo procesar un elemento de la grilla debido a: {str(e)}")
                continue
                
        browser.close()
        print("\n🏁 Pipeline de extracción interactiva finalizado con éxito.")

if __name__ == "__main__":
    ejecutar_pipeline_interactivo()
