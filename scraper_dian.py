import os
import requests
from playwright.sync_api import sync_playwright

# Captura de credenciales seguras desde el entorno de GitHub
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")
VOLUME_PATH = "/Volumes/workspace/comex/comex_stage/bronze"

URL_DIAN = "https://www.dian.gov.co/dian/cifras/Paginas/Bases-Estadisticas-de-Comercio-Exterior-Importaciones-y-Exportaciones.aspx"
OUTPUT_DIR = "./downloads"

def extraer_y_subir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    with sync_playwright() as p:
        print("🤖 Iniciando navegador Chromium en entorno seguro de GitHub...")
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
        page = context.new_page()
        
        print("🌐 Navegando a la URL oficial de la DIAN...")
        page.goto(URL_DIAN, timeout=60000)
        
        print("⏳ Esperando renderizado de la grilla dinámica de SharePoint...")
        # Espera explícita a que aparezca la grilla que viste en tu captura
        page.wait_for_selector("text=Año", timeout=30000)
        
        # Mapeamos los enlaces generados en la sesión interactiva
        enlaces = page.query_selector_all("a")
        url_descarga = None
        nombre_archivo = None
        target_href = None
        
        print("🔍 Buscando el archivo más reciente (Año corriente)...")
        for el in enlaces:
            href = el.get_attribute("href")
            texto = el.inner_text().strip()
            
            if href and (".zip" in href.lower() or "sharepoint" in href.lower()):
                # Filtramos por año y palabras clave de importaciones/exportaciones
                if any(k in texto.lower() or k in href.lower() for k in ["2026", "2025", "importa", "exporta"]):
                    url_descarga = href if href.startswith("http") else f"https://www.dian.gov.co{href}"
                    nombre_archivo = f"{texto.replace(' ', '_').replace(':', '')}.zip"
                    target_href = href
                    break
        
        if not url_descarga:
            print("⚠️ No se detectaron archivos bajo los filtros iniciales. Abortando.")
            return

        print(f"🎯 Archivo objetivo identificado: {nombre_archivo}")
        
        # Simulación del evento clic para forzar la descarga con cookies de sesión de SharePoint
        with page.expect_download(timeout=60000) as download_info:
            page.locator(f"a[href='{target_href}']").first.click()
        
        download = download_info.value
        ruta_local = os.path.join(OUTPUT_DIR, nombre_archivo)
        download.save_as(ruta_local)
        print(f"✅ Archivo descargado en el contenedor local: {ruta_local}")
        browser.close()
        
        # ==============================================================================
        # INYECCIÓN MEDIANTE API REST DE DATABRICKS (Unity Catalog API v2.0)
        # ==============================================================================
        print("🚀 Iniciando transferencia directa a la Capa Bronze de Databricks...")
        
        # Limpieza de la URL del host para evitar diagonales dobles
        host_limpio = DATABRICKS_HOST.rstrip("/")
        url_api = f"{host_limpio}/api/2.0/fs/files{VOLUME_PATH}/{nombre_archivo}"
        
        headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/octet-stream"
        }
        
        with open(ruta_local, "rb") as f:
            archivo_binario = f.read()
            
        # Petición PUT nativa para escribir el binario directo en el volumen
        response = requests.put(url_api, headers=headers, data=archivo_binario)
        
        if response.status_code in [200, 201]:
            print("🔥 ¡Fase de Extracción Completada! El archivo está disponible en Databricks.")
        else:
            raise Exception(f"Falla en API Databricks. Código: {response.status_code} | Detalle: {response.text}")

if __name__ == "__main__":
    extraer_y_subir()
