from playwright.async_api import async_playwright
import pandas as pd
import asyncio
import json
import os
import sys
import subprocess
import tempfile


# ─── SELECTORES (actualizados y con fallbacks) ────────────────────────────────

SELECTORES = {
    "nombre":         ["h1.DUwDvf", "h1[class*='fontHeadlineLarge']", "h1"],
    "categoria":      ["button.DkEaL", "button[jsaction*='category']", "[class*='fontBodyMedium'] button"],
    "direccion":      ['[data-item-id="address"] .Io6YTe', '[data-item-id="address"]', '[aria-label*="dirección"] .Io6YTe'],
    "telefono":       ['[data-item-id*="phone:tel"] .Io6YTe', '[data-tooltip*="teléfono"] .Io6YTe', '[aria-label*="teléfono"]'],
    "web":            ['[data-item-id="authority"] .Io6YTe', '[data-item-id="authority"]', '[aria-label*="sitio web"] .Io6YTe'],
    "rating":         ['div.F7nice span[aria-hidden="true"]', 'span[aria-hidden="true"]'],
    "num_reseñas":    ['div.F7nice span[aria-label*="reseña"]', 'span[aria-label*="reseña"]'],
    "horario":        ['div[jsaction*="openhours"] .ZDu9vd span span', '[aria-label*="hora"] span'],
}


def scrape_google_maps(query: str, max_results: int = 50) -> list[dict]:
    """
    Lanza el scraper en subproceso separado para compatibilidad con Streamlit.
    Usa stderr para logs y stdout solo para JSON, evitando mezcla de salidas.
    """

    script = f"""
import asyncio, json, sys
from playwright.async_api import async_playwright

SELECTORES = {json.dumps(SELECTORES)}

async def safe(page, selectors, attr=None):
    if isinstance(selectors, str):
        selectors = [selectors]
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if await el.count() == 0:
                continue
            if attr:
                val = await el.get_attribute(attr)
            else:
                val = await el.inner_text()
            if val and val.strip():
                return val.strip()
        except:
            continue
    return ""

async def main():
    results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        context = await browser.new_context(locale="es-ES", viewport={{"width": 1280, "height": 800}})
        page = await context.new_page()

        url = "https://www.google.com/maps/search/{query.replace(chr(39), '').replace(' ', '+')}"
        print(f"Abriendo: {{url}}", file=sys.stderr)
        
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3)
        except Exception as e:
            print(f"Error cargando página: {{e}}", file=sys.stderr)
            print("[]")
            return

        # Aceptar cookies
        for texto in ["Aceptar todo", "Accept all", "Aceptar", "Agree"]:
            try:
                boton = page.get_by_role("button", name=texto, exact=True)
                if await boton.is_visible():
                    await boton.click()
                    await asyncio.sleep(2)
                    print(f"Cookies aceptadas con: {{texto}}", file=sys.stderr)
                    break
            except:
                pass

        # Esperar el panel de resultados
        try:
            await page.wait_for_selector('div[role="feed"]', timeout=15000)
        except:
            print("No se encontró el panel de resultados (div[role=feed])", file=sys.stderr)
            # Captura de pantalla para debug (solo en entorno de desarrollo)
            # await page.screenshot(path="/tmp/debug.png")

        # Scroll para cargar resultados
        print("Scrolling para cargar resultados...", file=sys.stderr)
        stale = 0
        prev = 0
        while stale < 5:
            try:
                listings = await page.locator('a[href*="/maps/place/"]').all()
                cur = len(listings)
                print(f"  → {{cur}} negocios visibles", file=sys.stderr)
                if cur >= {max_results}:
                    break
                if cur == prev:
                    stale += 1
                else:
                    stale = 0
                    prev = cur
                await page.locator('div[role="feed"]').evaluate("el => el.scrollBy(0, 1500)")
                await asyncio.sleep(2)
            except Exception as e:
                print(f"Error en scroll: {{e}}", file=sys.stderr)
                stale += 1

        listings = await page.locator('a[href*="/maps/place/"]').all()
        total = min(len(listings), {max_results})
        print(f"Extrayendo {{total}} negocios...", file=sys.stderr)

        for i, listing in enumerate(listings[:total]):
            try:
                await listing.click()
                # Esperar a que cargue el panel de detalle
                await page.wait_for_selector("h1", timeout=8000)
                await asyncio.sleep(1.5)

                nombre = await safe(page, SELECTORES["nombre"])
                if not nombre:
                    print(f"  [{{i+1}}] Sin nombre, saltando", file=sys.stderr)
                    continue

                num_reseñas_raw = await safe(page, SELECTORES["num_reseñas"], attr="aria-label")
                num_reseñas = num_reseñas_raw.replace(" reseñas", "").replace(",", "").strip()

                data = {{
                    "nombre":         nombre,
                    "categoria":      await safe(page, SELECTORES["categoria"]),
                    "direccion":      await safe(page, SELECTORES["direccion"]),
                    "telefono":       await safe(page, SELECTORES["telefono"]),
                    "web":            await safe(page, SELECTORES["web"]),
                    "rating":         await safe(page, SELECTORES["rating"]),
                    "num_reseñas":    num_reseñas,
                    "estado_horario": await safe(page, SELECTORES["horario"]),
                    "url_maps":       page.url,
                    "latitud":        "",
                    "longitud":       "",
                }}

                try:
                    if "@" in page.url:
                        coords = page.url.split("@")[1].split(",")
                        data["latitud"]  = coords[0]
                        data["longitud"] = coords[1]
                except:
                    pass

                results.append(data)
                print(f"  [{{i+1}}/{{total}}] {{nombre}}", file=sys.stderr)

            except Exception as e:
                print(f"  Error negocio {{i+1}}: {{e}}", file=sys.stderr)
                continue

        await browser.close()

    # Solo JSON en stdout — nada más
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

        # Mostrar logs del subproceso (aparecen en la consola donde corre Streamlit)
        if result.stderr:
            print("=== LOG SCRAPER ===")
            print(result.stderr)
            print("==================")

        if result.returncode != 0:
            print(f"El subproceso terminó con error (código {result.returncode})")
            return []

        # stdout debe contener SOLO el JSON
        output = result.stdout.strip()
        if not output:
            print("El scraper no devolvió ningún dato (stdout vacío)")
            return []

        # Tomar solo la última línea que sea JSON válido (por si hay prints inesperados)
        for line in reversed(output.splitlines()):
            line = line.strip()
            if line.startswith("["):
                return json.loads(line)

        print(f"No se encontró JSON válido en la salida:\n{output[:500]}")
        return []

    except subprocess.TimeoutExpired:
        print("Timeout: el scraper tardó más de 5 minutos")
        return []
    except json.JSONDecodeError as e:
        print(f"Error parseando JSON: {e}\nSalida recibida:\n{result.stdout[:500]}")
        return []
    except Exception as e:
        print(f"Error inesperado: {e}")
        return []
    finally:
        try:
            os.unlink(tmp_path)
        except:
            pass


# ─── EXPORTAR ─────────────────────────────────────────────────────────────────

def exportar_resultados(data: list[dict], nombre_archivo: str = "negocios"):
    if not data:
        print("No hay datos para exportar.")
        return
    df = pd.DataFrame(data)
    df["tiene_web"] = df["web"].apply(lambda x: "No" if str(x).strip() == "" else "Sí")
    df.to_csv(f"{nombre_archivo}.csv", index=False, encoding="utf-8-sig")
    sin_web = df[df["tiene_web"] == "No"]
    sin_web.to_csv(f"{nombre_archivo}_sin_web.csv", index=False, encoding="utf-8-sig")
    print(f"✓ {len(df)} negocios exportados ({len(sin_web)} sin web)")


if __name__ == "__main__":
    resultados = scrape_google_maps("restaurantes Madrid centro", max_results=10)
    print(f"\nResultados: {len(resultados)}")
    exportar_resultados(resultados, "test")