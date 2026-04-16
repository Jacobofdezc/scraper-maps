from playwright.async_api import async_playwright
import pandas as pd
import asyncio
import json
import os
import sys
import subprocess
import tempfile


# ─── ACEPTAR TÉRMINOS ─────────────────────────────────────────────────────────

async def aceptar_terminos_async(page):
    try:
        await page.wait_for_selector('button', timeout=10000)
        textos_aceptar = [
            "Aceptar todo", "Aceptar todos", "Accept all",
            "Agree", "I agree", "Aceptar", "Accept",
        ]
        for texto in textos_aceptar:
            boton = page.get_by_role("button", name=texto)
            if await boton.is_visible():
                await boton.click()
                await asyncio.sleep(1)
                return True
        return False
    except:
        return False


# ─── EXTRACCIÓN DE DATOS ──────────────────────────────────────────────────────

async def extract_business_data_async(page) -> dict:
    data = {}

    async def safe_text(selector: str, attribute: str = None) -> str:
        try:
            el = page.locator(selector).first
            if attribute:
                return await el.get_attribute(attribute) or ""
            return await el.inner_text() or ""
        except:
            return ""

    data["nombre"]         = await safe_text('h1.DUwDvf')
    data["categoria"]      = await safe_text('button.DkEaL')
    data["direccion"]      = await safe_text('[data-item-id="address"] .Io6YTe')
    data["telefono"]       = await safe_text('[data-item-id*="phone:tel"] .Io6YTe')
    data["web"]            = await safe_text('[data-item-id="authority"] .Io6YTe')
    data["rating"]         = await safe_text('div.F7nice span[aria-hidden="true"]')
    data["num_reseñas"]    = (await safe_text('div.F7nice span[aria-label*="reseñas"]', "aria-label")).replace(" reseñas", "").strip()
    data["estado_horario"] = await safe_text('div[jsaction*="openhours"] .ZDu9vd span span')
    data["url_maps"]       = page.url

    try:
        url = page.url
        if "@" in url:
            coords = url.split("@")[1].split(",")
            data["latitud"]  = coords[0]
            data["longitud"] = coords[1]
    except:
        data["latitud"]  = ""
        data["longitud"] = ""

    return data if data.get("nombre") else None


# ─── SCRAPER PRINCIPAL (ASYNC) ────────────────────────────────────────────────

async def scrape_google_maps_async(query: str, max_results: int = 50) -> list[dict]:
    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            locale="es-ES",
            storage_state="cookies.json" if os.path.exists("cookies.json") else None
        )
        page = await context.new_page()

        url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
        await page.goto(url, wait_until="networkidle")
        await asyncio.sleep(2)

        await aceptar_terminos_async(page)
        await context.storage_state(path="cookies.json")

        print(f"Cargando resultados para: {query}")
        scroll_panel = page.locator('div[role="feed"]')

        previous_count = 0
        stale_scrolls = 0

        while len(results) < max_results and stale_scrolls < 5:
            listings = await page.locator('a[href*="google.com/maps/place"]').all()
            current_count = len(listings)

            if current_count == previous_count:
                stale_scrolls += 1
            else:
                stale_scrolls = 0
                previous_count = current_count

            print(f"  → {current_count} negocios encontrados...")
            await scroll_panel.evaluate("el => el.scrollBy(0, 1000)")
            await asyncio.sleep(1.5)

        listings = await page.locator('a[href*="google.com/maps/place"]').all()
        print(f"\nExtrayendo datos de {min(len(listings), max_results)} negocios...")

        for i, listing in enumerate(listings[:max_results]):
            try:
                await listing.click()
                await asyncio.sleep(2)
                data = await extract_business_data_async(page)
                if data:
                    results.append(data)
                    print(f"  [{i+1}] {data.get('nombre', 'Sin nombre')}")
            except Exception as e:
                print(f"  Error en negocio {i+1}: {e}")
                continue

        await browser.close()

    return results


# ─── SCRAPER COMPATIBLE CON STREAMLIT EN WINDOWS ──────────────────────────────

def scrape_google_maps(query: str, max_results: int = 50) -> list[dict]:
    """Lanza el scraper en un proceso Python completamente separado.
    Soluciona el conflicto entre Playwright y Streamlit en Windows."""

    script = f"""
import asyncio
import json
import os
import sys

async def main():
    from playwright.async_api import async_playwright
    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(locale="es-ES")
        page = await context.new_page()

        url = "https://www.google.com/maps/search/{query.replace(' ', '+')}"
        await page.goto(url, wait_until="networkidle")
        await asyncio.sleep(2)

        try:
            for texto in ["Aceptar todo", "Aceptar todos", "Accept all", "Aceptar"]:
                boton = page.get_by_role("button", name=texto)
                if await boton.is_visible():
                    await boton.click()
                    await asyncio.sleep(1)
                    break
        except:
            pass

        scroll_panel = page.locator('div[role="feed"]')
        previous_count = 0
        stale_scrolls = 0

        while len(results) < {max_results} and stale_scrolls < 5:
            listings = await page.locator('a[href*="google.com/maps/place"]').all()
            current_count = len(listings)
            if current_count == previous_count:
                stale_scrolls += 1
            else:
                stale_scrolls = 0
                previous_count = current_count
            await scroll_panel.evaluate("el => el.scrollBy(0, 1000)")
            await asyncio.sleep(1.5)

        listings = await page.locator('a[href*="google.com/maps/place"]').all()

        for listing in listings[:{max_results}]:
            try:
                await listing.click()
                await asyncio.sleep(2)

                async def safe(sel, attr=None):
                    try:
                        el = page.locator(sel).first
                        if attr:
                            return await el.get_attribute(attr) or ""
                        return await el.inner_text() or ""
                    except:
                        return ""

                data = {{
                    "nombre":         await safe("h1.DUwDvf"),
                    "categoria":      await safe("button.DkEaL"),
                    "direccion":      await safe('[data-item-id="address"] .Io6YTe'),
                    "telefono":       await safe('[data-item-id*="phone:tel"] .Io6YTe'),
                    "web":            await safe('[data-item-id="authority"] .Io6YTe'),
                    "rating":         await safe('div.F7nice span[aria-hidden="true"]'),
                    "num_reseñas":    (await safe('div.F7nice span[aria-label*="reseñas"]', "aria-label")).replace(" reseñas", "").strip(),
                    "estado_horario": await safe('div[jsaction*="openhours"] .ZDu9vd span span'),
                    "url_maps":       page.url,
                }}

                try:
                    if "@" in page.url:
                        coords = page.url.split("@")[1].split(",")
                        data["latitud"]  = coords[0]
                        data["longitud"] = coords[1]
                except:
                    data["latitud"]  = ""
                    data["longitud"] = ""

                if data.get("nombre"):
                    results.append(data)

            except:
                continue

        await browser.close()

    print(json.dumps(results, ensure_ascii=False))

asyncio.run(main())
"""

    with tempfile.NamedTemporaryFile(mode='w', suffix='.py',
                                     delete=False, encoding='utf-8') as f:
        f.write(script)
        tmp_path = f.name

    try:
        result = subprocess.run(
            [sys.executable, tmp_path],
            capture_output=True,
            text=True,
            encoding='utf-8',
            timeout=300
        )

        if result.returncode != 0:
            print(f"Error en subproceso:\n{result.stderr}")
            return []

        output = result.stdout.strip()
        if output:
            return json.loads(output)
        return []

    except subprocess.TimeoutExpired:
        print("Timeout: el scraper tardó demasiado")
        return []
    except Exception as e:
        print(f"Error inesperado: {e}")
        return []
    finally:
        os.unlink(tmp_path)


# ─── EXPORTAR RESULTADOS ──────────────────────────────────────────────────────

def exportar_resultados(data: list[dict], nombre_archivo: str = "negocios"):
    if not data:
        print("No hay datos para exportar.")
        return

    df = pd.DataFrame(data)
    df["tiene_web"] = df["web"].apply(lambda x: "No" if str(x).strip() == "" else "Sí")

    df.to_csv(f"{nombre_archivo}.csv", index=False, encoding="utf-8-sig")
    print(f"\n✓ CSV completo: {nombre_archivo}.csv ({len(df)} negocios)")

    sin_web = df[df["tiene_web"] == "No"]
    sin_web.to_csv(f"{nombre_archivo}_sin_web.csv", index=False, encoding="utf-8-sig")
    print(f"✓ CSV sin web:  {nombre_archivo}_sin_web.csv ({len(sin_web)} negocios)")

    with open(f"{nombre_archivo}.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✓ JSON:         {nombre_archivo}.json")

    print(f"\n{'='*40}")
    print(f"  Total negocios      : {len(df)}")
    print(f"  Con web             : {len(df[df['tiene_web'] == 'Sí'])}")
    print(f"  Sin web (potencial) : {len(sin_web)}")
    print(f"{'='*40}")


# ─── EJECUCIÓN DIRECTA ────────────────────────────────────────────────────────

if __name__ == "__main__":

    BUSQUEDAS = [
        "restaurantes japoneses Madrid",
        "peluquerías Barcelona centro",
    ]

    MAX_RESULTADOS_POR_BUSQUEDA = 20
    NOMBRE_ARCHIVO = "todos_los_negocios"

    todos_los_resultados = []

    for busqueda in BUSQUEDAS:
        print(f"\n{'='*50}")
        print(f"Buscando: {busqueda}")
        print(f"{'='*50}")

        negocios = scrape_google_maps(busqueda, MAX_RESULTADOS_POR_BUSQUEDA)

        for negocio in negocios:
            negocio["busqueda"] = busqueda

        todos_los_resultados.extend(negocios)
        print(f"✓ {len(negocios)} negocios añadidos (total: {len(todos_los_resultados)})")

    exportar_resultados(todos_los_resultados, NOMBRE_ARCHIVO)