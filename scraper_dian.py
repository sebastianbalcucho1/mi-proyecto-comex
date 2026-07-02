import os
import time
import requests
import urllib3
from playwright.sync_api import sync_playwright

# Desactivar advertencias de certificados SSL inseguros
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")
VOLUME_PATH = "/Volumes/workspace/comex/comex_stage/bronze"

URL_DIAN = "https://www.dian.gov.co/dian/cifras/Paginas/Bases-Estadisticas-de-Comercio-Exterior-Importaciones-y-Exportaciones.aspx"
OUTPUT_DIR = "./downloads"

def ejecutar_pipeline_interactivo():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    with sync_playwright() as p:
        print("🤖 [LOG] Levantando Chromium Headless en GitHub...", flush=True)
        browser = p.chromium.launch(headless=True, args=["--disable-gpu", "--no-sandbox"])
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800}
        )
        page = context.new_page()
        
        print("🌐 [LOG] Conectando a la URL oficial de la DIAN...", flush=True)
        page.goto(URL_DIAN, timeout=90000)
        
        print("⏳ [LOG] Esperando la carga estructural del DOM...", flush=True)
        page.wait_for_load_state("domcontentloaded")
        time.sleep(5)
        
        print("➡️ [LOG] Pasos de navegación: Abriendo acordeón de 'Bases Estadísticas de Importaciones'...", flush=True)
        seccion_importaciones = page.locator("text=Bases Estadísticas de Importaciones").first
        seccion_importaciones.click()
        
        print("⏳ [LOG] Esperando a que SharePoint renderice la grilla interna de años...", flush=True)
        try:
            page.wait_for_selector(".ms-gb", timeout=15000)
            print("✅ [LOG] Grilla de SharePoint detectada.", flush=True)
        except Exception:
            print("⚠️ [WARN] No se detectó la clase .ms-gb, continuando con estrategia adaptativa...", flush=True)
        
        time.sleep(3)
        
        print("🔍 [LOG] Escaneando la grilla web buscando nodos de tipo 'Año'...", flush=True)
        # Apuntamos específicamente a las filas agrupadoras de SharePoint (.ms-gb) que contienen los años
        bloques_todos = page.locator(".ms-gb").all()
        
        bloques_anios = []
        textos_vistos = set()
        
        for b in bloques_todos:
            try:
                if b.is_visible():
                    txt = b.inner_text()
                    if txt and "año" in txt.lower():
                        txt_clean = txt.strip().replace("\n", " ")
                        if txt_clean not in textos_vistos:
                            bloques_anios.append(b)
                            textos_vistos.add(txt_clean)
            except Exception:
                continue
        
        print(f"📊 [LOG] Se detectaron {len(bloques_anios)} nodos de años únicos y visibles: {list(textos_vistos)}", flush=True)
        
        # Iteramos sobre los años encontrados
        for i in range(len(bloques_anios)):
            try:
                nodo_anio = bloques_anios[i]
                if not nodo_anio.is_visible():
                    continue
                    
                texto_anio = nodo_anio.inner_text().strip().replace("\n", " ")
                
                print(f"\n📂 [PROCESANDO] Expandiendo el nodo dinámico: '{texto_anio}'", flush=True)
                nodo_anio.click()
                time.sleep(5) # Espera un poco más larga para el despliegue asíncrono de los meses
                
                # Buscamos SOLO los enlaces que aparezcan en la grilla y que realmente tengan textos de meses/archivos
                enlaces_candidatos = page.locator("a").all()
                archivos_en_nodo = 0
                
                for el in enlaces_candidatos:
                    try:
                        if not el.is_visible():
                            continue
                            
                        texto_mes = el.inner_text().strip()
                        href = el.get_attribute("href") or ""
                        
                        # FILTRO ESTRICTO: Evitamos el botón del acordeón principal y seleccionamos nombres con patrones de archivos de importaciones
                        if texto_mes and "importa" in texto_mes.lower() and texto_mes != "Bases Estadísticas de Importaciones":
                            archivos_en_nodo += 1
                            print(f"   ⬇️ [DETECTADO] Archivo mensual real: '{texto_mes}'. Disparando descarga...", flush=True)
                            
                            with page.expect_download(timeout=60000) as download_info:
                                el.click()
                            
                            download = download_info.value
                            nombre_archivo_final = f"{texto_mes.replace(' ', '_')}.zip"
                            ruta_local = os.path.join(OUTPUT_DIR, nombre_archivo_final)
                            
                            download.save_as(ruta_local)
                            peso_mb = round(os.path.getsize(ruta_local) / (1024 * 1024), 2)
                            print(f"   ✅ [DESCARGADO] Archivo en GitHub: {nombre_archivo_final} ({peso_mb} MB)", flush=True)
                            
                            # Inyección a la API de Databricks
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
                                print(f"   ⚠️ [ALERTA] API Databricks devolvió código {response.status_code}", flush=True)
                                
                            os.remove(ruta_local)
                    except Exception as e_enlace:
                        continue
                
                if archivos_en_nodo == 0:
                    print(f"   ℹ️ El nodo '{texto_anio}' no expuso sub-enlaces de importaciones en este ciclo.", flush=True)
                
                # Volvemos a hacer clic para colapsar y mantener limpio el DOM visual
                nodo_anio.click()
                time.sleep(2)
                
            except Exception as e:
                print(f"🚨 [ERROR] Falla en iteración del año: {str(e)}", flush=True)
                continue
                
        browser.close()
        print("\n🏁 [FIN] Pipeline de extracción interactiva finalizado con éxito.", flush=True)

if __name__ == "__main__":
    ejecutar_pipeline_interactivo()
