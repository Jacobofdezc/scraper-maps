# pip install playwright pandas
# playwright install chromium

from playwright.sync_api import sync_playwright
import pandas as pd
import time
import json
import os

def scrape_google_maps(query: str, max_results: int = 50) -> list[dict]:
    
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            locale="es-ES",
            # Guardar las cookies entre ejecuciones para no tener que aceptar siempre
            storage_state="cookies.json" if os.path.exists("cookies.json") else None
        )
        page = context.new_page()

        url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
        page.goto(url, wait_until="networkidle")
        time.sleep(2)

        # ── Aceptar términos automáticamente ──
        aceptar_terminos(page)

        # ── Guardar cookies para próximas ejecuciones ──
        context.storage_state(path="cookies.json")
        print("  ✓ Cookies guardadas para futuras ejecuciones")

        # ... resto del código igual

        # Scroll para cargar más resultados
        print(f"Cargando resultados para: {query}")
        scroll_panel = page.locator('div[role="feed"]')

        previous_count = 0
        stale_scrolls = 0

        while len(results) < max_results and stale_scrolls < 5:
            # Obtener todos los negocios visibles
            listings = page.locator('a[href*="google.com/maps/place"]').all()
            current_count = len(listings)

            if current_count == previous_count:
                stale_scrolls += 1
            else:
                stale_scrolls = 0
                previous_count = current_count

            print(f"  → {current_count} negocios encontrados...")

            # Hacer scroll en el panel lateral
            scroll_panel.evaluate("el => el.scrollBy(0, 1000)")
            time.sleep(1.5)

        # Procesar cada negocio
        listings = page.locator('a[href*="google.com/maps/place"]').all()
        print(f"\nExtrayendo datos de {min(len(listings), max_results)} negocios...")

        for i, listing in enumerate(listings[:max_results]):
            try:
                listing.click()
                time.sleep(2)

                data = extract_business_data(page)
                if data:
                    results.append(data)
                    print(f"  [{i+1}] {data.get('nombre', 'Sin nombre')}")

            except Exception as e:
                print(f"  Error en negocio {i+1}: {e}")
                continue

        browser.close()

    return results

def aceptar_terminos(page):
    """Acepta automáticamente los términos y cookies de Google Maps."""
    
    try:
        # Espera hasta 10 segundos a que aparezca algún botón de aceptar
        page.wait_for_selector('button', timeout=10000)
        
        # Lista de textos posibles del botón (Google los cambia según idioma y versión)
        textos_aceptar = [
            "Aceptar todo",
            "Aceptar todos",
            "Accept all",
            "Agree",
            "I agree",
            "Aceptar",
            "Accept",
            "Rechazar todo",  # A veces hay que hacer clic en rechazar para cerrar
        ]
        
        for texto in textos_aceptar:
            boton = page.get_by_role("button", name=texto)
            if boton.is_visible():
                boton.click()
                print(f"  ✓ Banner aceptado automáticamente ({texto})")
                time.sleep(1)
                return True
        
        print("  → No se encontró banner de términos, continuando...")
        return False
        
    except Exception as e:
        print(f"  → Sin banner de términos: {e}")
        return False

def extract_business_data(page) -> dict:
    """Extrae todos los datos del negocio actualmente abierto."""

    data = {}

    def safe_text(selector: str, attribute: str = None) -> str:
        try:
            el = page.locator(selector).first
            if attribute:
                return el.get_attribute(attribute) or ""
            return el.inner_text() or ""
        except:
            return ""

    # Nombre
    data["nombre"] = safe_text('h1.DUwDvf')

    # Categoría
    data["categoria"] = safe_text('button.DkEaL')

    # Dirección
    data["direccion"] = safe_text('[data-item-id="address"] .Io6YTe')

    # Teléfono
    data["telefono"] = safe_text('[data-item-id*="phone:tel"] .Io6YTe')

    # Sitio web
    data["web"] = safe_text('[data-item-id="authority"] .Io6YTe')

    # Rating (puntuación)
    data["rating"] = safe_text('div.F7nice span[aria-hidden="true"]')

    # Número de reseñas
    reseñas_raw = safe_text('div.F7nice span[aria-label*="reseñas"]', "aria-label")
    data["num_reseñas"] = reseñas_raw.replace(" reseñas", "").replace(",", "").strip()

    # Horario (si está abierto / horas)
    data["estado_horario"] = safe_text('div[jsaction*="openhours"] .ZDu9vd span span')

    # URL de Google Maps
    data["url_maps"] = page.url

    # Coordenadas (extraídas de la URL)
    try:
        url = page.url
        if "@" in url:
            coords = url.split("@")[1].split(",")
            data["latitud"] = coords[0]
            data["longitud"] = coords[1]
    except:
        data["latitud"] = ""
        data["longitud"] = ""

    return data if data.get("nombre") else None


def exportar_resultados(data: list[dict], nombre_archivo: str = "negocios"):
    """Exporta los resultados a CSV y JSON, con filtro de negocios sin web."""

    if not data:
        print("No hay datos para exportar.")
        return

    df = pd.DataFrame(data)

    # Columna que marca si tiene web o no
    df["tiene_web"] = df["web"].apply(lambda x: "No" if str(x).strip() == "" else "Sí")

    # CSV con todos los negocios
    df.to_csv(f"{nombre_archivo}.csv", index=False, encoding="utf-8-sig")
    print(f"\n✓ CSV completo guardado: {nombre_archivo}.csv ({len(df)} negocios)")

    # CSV solo con los que NO tienen web (tus clientes potenciales)
    sin_web = df[df["tiene_web"] == "No"]
    sin_web.to_csv(f"{nombre_archivo}_sin_web.csv", index=False, encoding="utf-8-sig")
    print(f"✓ CSV sin web guardado: {nombre_archivo}_sin_web.csv ({len(sin_web)} negocios)")

    # JSON
    with open(f"{nombre_archivo}.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✓ JSON guardado: {nombre_archivo}.json")

    # Resumen en terminal
    print(f"\n{'='*40}")
    print(f"  Total negocios encontrados : {len(df)}")
    print(f"  Con página web             : {len(df[df['tiene_web'] == 'Sí'])}")
    print(f"  Sin página web (potencial) : {len(sin_web)}")
    print(f"{'='*40}")

    # Vista previa de los que no tienen web
    if not sin_web.empty:
        cols = ["nombre", "categoria", "telefono", "direccion"]
        cols_disponibles = [c for c in cols if c in sin_web.columns]
        print("\nNegocios sin web (primeros 5):")
        print(sin_web[cols_disponibles].head(5).to_string(index=False))

# ─── EJECUCIÓN ────────────────────────────────────────────────────────────────

# ─── EJECUCIÓN ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    
    # Lista todas las búsquedas que quieras aquí
    BUSQUEDAS = [
        "clínicas dentales en Madrid",
        "clínicas de medicina estética premium",
        "centros de fisioterapia madrid",
        "centros de estética avanzada con aparatología",
        "centros de depilación láser en Madrid",
        "clínicas veterinarias urbanas",
    ]
    
    MAX_RESULTADOS_POR_BUSQUEDA = 20
    NOMBRE_ARCHIVO = "todos_los_negocios_Madrid"

    todos_los_resultados = []

    for busqueda in BUSQUEDAS:
        print(f"\n{'='*50}")
        print(f"Buscando: {busqueda}")
        print(f"{'='*50}")
        
        negocios = scrape_google_maps(busqueda, MAX_RESULTADOS_POR_BUSQUEDA)
        
        # Añade la columna "busqueda" para saber de dónde viene cada resultado
        for negocio in negocios:
            negocio["busqueda"] = busqueda
        
        todos_los_resultados.extend(negocios)
        print(f"✓ {len(negocios)} negocios añadidos (total acumulado: {len(todos_los_resultados)})")
        
        # Pausa entre búsquedas para no levantar sospechas
        print("Esperando 5 segundos antes de la siguiente búsqueda...")
        time.sleep(5)

    # Exportar todo junto
    exportar_resultados(todos_los_resultados, nombre_archivo=NOMBRE_ARCHIVO)