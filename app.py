import streamlit as st
import pandas as pd
import os
from scraper import scrape_google_maps  # importa tu scraper existente

st.set_page_config(page_title="Scraper Google Maps", page_icon="📍", layout="wide")

st.title("📍 Scraper de Google Maps")
st.markdown("Encuentra negocios sin página web y conviértelos en clientes.")

# ── Panel de búsqueda ──────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Configuración")
    
    busquedas_input = st.text_area(
        "Búsquedas (una por línea)",
        placeholder="restaurantes Madrid centro\npeluquerías Barcelona\nfontaneros Valencia",
        height=150
    )
    
    max_resultados = st.slider("Máximo de resultados por búsqueda", 5, 50, 20)
    nombre_archivo = st.text_input("Nombre del archivo CSV", value="resultados")
    ejecutar = st.button("🚀 Iniciar búsqueda", use_container_width=True)

# ── Ejecución ──────────────────────────────────────────────────────────────────
if ejecutar and busquedas_input.strip():
    busquedas = [b.strip() for b in busquedas_input.strip().split("\n") if b.strip()]
    todos = []

    progress = st.progress(0)
    status = st.empty()

    for i, busqueda in enumerate(busquedas):
        status.info(f"Buscando: {busqueda}...")
        negocios = scrape_google_maps(busqueda, max_resultados)
        
        if not negocios:
            status.warning(f"⚠️ Sin resultados para: '{busqueda}'. Revisa la consola para ver los logs.")
            continue
       
        for negocio in negocios:
            negocio["busqueda"] = busqueda
        
        todos.extend(negocios)
        progress.progress((i + 1) / len(busquedas))

    status.success(f"✓ Búsqueda completada. {len(todos)} negocios encontrados.")

    if todos:
        df = pd.DataFrame(todos)
        df["tiene_web"] = df["web"].apply(lambda x: "No" if str(x).strip() == "" else "Sí")
        st.session_state["df"] = df
        st.session_state["nombre_archivo"] = nombre_archivo

# ── Resultados ─────────────────────────────────────────────────────────────────
if "df" in st.session_state:
    df = st.session_state["df"]
    nombre = st.session_state["nombre_archivo"]

    # Métricas resumen
    col1, col2, col3 = st.columns(3)
    col1.metric("Total negocios", len(df))
    col2.metric("Con web", len(df[df["tiene_web"] == "Sí"]))
    col3.metric("Sin web (potencial)", len(df[df["tiene_web"] == "No"]))

    # Filtros
    st.subheader("Resultados")
    filtro = st.radio("Mostrar", ["Todos", "Solo sin web", "Solo con web"], horizontal=True)
    
    if filtro == "Solo sin web":
        df_mostrar = df[df["tiene_web"] == "No"]
    elif filtro == "Solo con web":
        df_mostrar = df[df["tiene_web"] == "Sí"]
    else:
        df_mostrar = df

    st.dataframe(df_mostrar, use_container_width=True)

    # Botones de descarga
    col1, col2 = st.columns(2)
    
    with col1:
        csv_todos = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button(
            "⬇️ Descargar todos (CSV)",
            csv_todos,
            file_name=f"{nombre}.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    with col2:
        sin_web = df[df["tiene_web"] == "No"]
        csv_sin_web = sin_web.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button(
            "⬇️ Descargar sin web (CSV)",
            csv_sin_web,
            file_name=f"{nombre}_sin_web.csv",
            mime="text/csv",
            use_container_width=True
        )

elif not ejecutar:
    st.info("👈 Configura tu búsqueda en el panel izquierdo y pulsa 'Iniciar búsqueda'.")