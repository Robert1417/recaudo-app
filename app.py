import streamlit as st
import pandas as pd

st.title("📊 Calculadora de Recaudo — Versión 1")

st.markdown("""
Cargue su base **`cartera_asignada_filtrada`** y luego escriba una **referencia** para ver la información asociada.
""")

# --- 1. Carga del archivo ---
uploaded_file = st.file_uploader("📂 Cargar base (CSV o Excel)", type=["csv", "xlsx"])

if uploaded_file:
    try:
        # Detección automática del tipo de archivo
        if uploaded_file.name.lower().endswith(".csv"):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file, engine="openpyxl")

        st.success("✅ Base cargada correctamente")

        # Mostrar primeras filas
        st.dataframe(df.head())

        # --- 2. Buscar referencia ---
        referencia = st.text_input("🔎 Ingrese la referencia para buscar")

        if referencia:
            # Filtramos coincidencias exactas
            resultados = df[df["Referencia"].astype(str) == str(referencia)]

            if not resultados.empty:
                st.subheader("📋 Resultados encontrados")
                st.dataframe(resultados)

                st.markdown("### 💡 Valores clave del primer registro encontrado")
                fila = resultados.iloc[0]

                # Mostrar valores numéricos con formato
                st.write(f"**🏦 Banco:** {fila['Banco']}")
                st.write(f"**💰 Deuda Resuelve:** {fila['Deuda Resuelve']:,}")
                st.write(f"**📆 Apartado Mensual:** {fila['Apartado Mensual']:,}")
                st.write(f"**🎯 Comisión Mensual:** {fila['Comisión Mensual']:,}")

            else:
                st.warning("⚠️ No se encontró esa referencia en la base.")
    except Exception as e:
        st.error(f"❌ Ocurrió un error al leer el archivo: {e}")
        st.stop()
