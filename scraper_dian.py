import os
import time
import requests
import urllib3
from playwright.sync_api import sync_playwright

# Desactivar advertencias de certificados SSL inseguros
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuración del entorno e inyección a Databricks
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")
VOLUME_PATH = "/Volumes/workspace/comex/comex_stage/bronze"

URL_DIAN = "https://www.dian.gov.co/dian/cifras/Paginas/Bases-Estadisticas-de-Comercio-Exterior-Importaciones-y-Exportaciones.aspx"
OUTPUT_DIR = "./downloads"

def ejecutar_pipeline_interactivo():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    with sync_playwright() as p:
        print("🤖 [LOG] Levantando Chromium Headless en GitHub...", flush=True)
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        page = context.new_page()
        
        print("🌐 [LOG] Conectando a la URL oficial de la DIAN...", flush=True)
        page.goto(URL_DIAN, timeout=90000)
        
        print("⏳ [LOG] Esperando la carga base del DOM estructural...", flush=True)
        page.wait_for_load_state("domcontentloaded")
        time.sleep(5)  # Pausa técnica de estabilización para SharePoint
        
        print("➡️ [LOG] Pasos de navegación: Abriendo acordeón de 'Bases Estadísticas de Importaciones'...", flush=True)
        seccion_importaciones = page.locator("text=Bases Estadísticas de Importaciones").first
        seccion_importaciones.click()
        time.sleep(3)
        
        print("🔍 [LOG] Escaneando la grilla web buscando elementos de tipo 'Año'...", flush=True)
        # Selector híbrido para capturar los nodos de la tabla dinámica
        bloques_todos = page.locator(".ms-commentall-title, .ms-gb, a, span").all()
        bloques_anios = [b for b in bloques_todos if "año" in str(b.inner_text()).lower()]
        
        print(f"📊 [LOG] Se detectaron {len(bloques_anios)} nodos de años potenciales.", flush=True)
        
        for i in range(len(bloques_anios)):
            try:
                nodo_anio = bloques_anios[i]
                texto_anio = nodo_anio.inner_text().strip()
                
                print(f"\n📂 [PROCESANDO] Expandiendo el nodo dinámico: {texto_anio}", flush=True)
                nodo_anio.click()
                time.sleep(4)  # Espera para que SharePoint renderice los archivos mensuales hijos
                
                # Capturamos todas las etiquetas de enlace generadas abajo
                enlaces_candidatos = page.locator("a").all()
                
                for el in enlaces_candidatos:
                    texto_mes = el.inner_text().strip()
                    href = el.get_attribute("href")
                    
                    # Filtro semántico basado en el patrón real: '12_Importaciones_2024_Diciembre'
                    if texto_mes and "importaciones" in texto_mes.lower():
                        print(f"   ⬇️ [DETECTADO] Archivo mensual: '{texto_mes}'. Disparando descarga...", flush=True)
                        
                        # Capturar el flujo de bytes directamente mediante el evento del navegador
                        with page.expect_download(timeout=60000) as download_info:
                            el.click()
                        
                        download = download_info.value
                        nombre_archivo_final = f"{texto_mes.replace(' ', '_')}.zip"
                        ruta_local = os.path.join(OUTPUT_DIR, nombre_archivo_final)
                        
                        # Persistir temporalmente en el contenedor de GitHub
                        download.save_as(ruta_local)
                        peso_mb = round(os.path.getsize(ruta_local) / (1024 * 1024), 2)
                        print(f"   ✅ [DESCARGADO] Archivo en GitHub: {nombre_archivo_final} ({peso_mb} MB)", flush=True)
                        
                        # ==============================================================================
                        # INYECCIÓN A LA API DE ARCHIVOS DE DATABRICKS
                        # ==============================================================================
                        print(f"   🚀 [API] Transfiriendo binario a Unity Catalog Volume...", flush=True)
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
                            print(f"   🔥 [ÉXITO] {nombre_archivo_final} inyectado correctamente en Capa Bronze.", flush=True)
                        else:
                            print(f"   ⚠️ [ALERTA] API Databricks devolvió código HTTP {response.status_code} | Detalle: {response.text}", flush=True)
                            
                        # Limpieza del disco local de GitHub para el siguiente mes
                        os.remove(ruta_local)
                        
                # Colapsamos el año para limpiar la vista y el DOM antes del siguiente ciclo
                nodo_anio.click()
                time.sleep(1)
                
            except Exception as e:
                print(f"🚨 [ERROR] Falla en iteración de elemento de la grilla: {str(e)}", flush=True)
                continue
                
        browser.close()
        print("\n🏁 [FIN] Pipeline de extracción interactiva finalizado con éxito.", flush=True)

if __name__ == "__main__":
    ejecutar_pipeline_interactivo()
