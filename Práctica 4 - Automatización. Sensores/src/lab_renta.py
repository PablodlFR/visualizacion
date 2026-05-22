from dagster import asset, asset_check, AssetCheckResult, MetadataValue, Output
from pathlib import Path
import pandas as pd
from plotnine import *
import requests
import re
import subprocess
import geopandas as gpd

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "outputs"

# ======================
# CARGA DE DATOS
# ======================
@asset
def cargar_distribucion_renta():
    return pd.read_csv(DATA_DIR / "distribucion-renta-canarias.csv")

@asset
def cargar_codislas():
    return pd.read_csv(DATA_DIR / "codislas.csv", encoding="latin1", sep=";")


@asset
def cargar_nivel_estudios():
    return pd.read_csv(DATA_DIR / "nivelestudios.csv")

@asset
def cargar_municipios_geojson():
    geo = gpd.read_file(DATA_DIR / "Municipios-2024.json")
    geo["geocode"] = geo["geocode"].astype(str).str.zfill(5)
    return geo

# ======================
# LIMPIEZA
# ======================
@asset
def limpiar_datos(cargar_distribucion_renta):
    df = cargar_distribucion_renta.copy()
    df.columns = df.columns.str.strip()
    df["TIME_PERIOD_CODE"] = pd.to_numeric(df["TIME_PERIOD_CODE"], errors="coerce")
    df["OBS_VALUE"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
    df["TERRITORIO_CODE"] = df["TERRITORIO_CODE"].astype(str).str.strip()
    df = df.dropna(subset=["TIME_PERIOD_CODE", "OBS_VALUE", "MEDIDAS#es", "TERRITORIO_CODE"])
    return df


@asset
def limpiar_codislas(cargar_codislas):
    cod = cargar_codislas.copy()
    cod.columns = cod.columns.str.strip()
    cod["CPRO"] = cod["CPRO"].astype(str).str.strip().str.zfill(2)
    cod["CMUN"] = cod["CMUN"].astype(str).str.strip().str.zfill(3)
    cod["INE5"] = cod["CPRO"] + cod["CMUN"]
    cod["NOMBRE"] = cod["NOMBRE"].astype(str).str.strip()
    cod["ISLA"] = cod["ISLA"].astype(str).str.strip()
    return cod[["INE5", "ISLA", "NOMBRE"]].drop_duplicates()


@asset
def limpiar_nivel_estudios(cargar_nivel_estudios):
    df = cargar_nivel_estudios.copy()
    df.columns = df.columns.str.strip()
    df["INE5"] = df["Municipios de 500 habitantes o más"].astype(str).str[:5]
    df["MUNICIPIO"] = df["Municipios de 500 habitantes o más"].astype(str).str[6:].str.strip()
    df["Total"] = pd.to_numeric(df["Total"], errors="coerce")
    df = df.dropna(subset=["INE5", "Total"])
    return df


# ======================
# UNIONES
# ======================
@asset
def unir_renta_codislas(limpiar_datos, limpiar_codislas):
    renta = limpiar_datos.copy()
    renta = renta[renta["TERRITORIO_CODE"].str.fullmatch(r"\d{5}", na=False)].copy()
    renta["INE5"] = renta["TERRITORIO_CODE"].str.zfill(5)
    df = renta.merge(limpiar_codislas, on="INE5", how="left")
    return df.rename(columns={"NOMBRE": "MUNICIPIO"})

@asset
def unir_estudios_codislas(limpiar_nivel_estudios, limpiar_codislas):
    df = limpiar_nivel_estudios.copy()
    df = df.merge(limpiar_codislas, on="INE5", how="left")
    return df

@asset
def renta_mapa_municipios(unir_renta_codislas):
    df = unir_renta_codislas.copy()
    df = df[
        (df["TIME_PERIOD_CODE"] == 2023) &
        (df["MEDIDAS#es"] == "Sueldos y salarios")
    ].copy()
    return df

@asset
def unir_geojson_rentas(cargar_municipios_geojson, renta_mapa_municipios):
    geo = cargar_municipios_geojson.copy()
    renta = renta_mapa_municipios.copy()
    return geo.merge(renta, left_on="geocode", right_on="INE5", how="left")

# ======================
# VISUALIZACIONES MANUALES
# ======================
@asset
def grafico_lineas(limpiar_datos):
    df = limpiar_datos.copy()
    df = df[df["TERRITORIO#es"] == "Canarias"]
    df = df[df["MEDIDAS#es"].isin(["Sueldos y salarios", "Pensiones", "Prestaciones por desempleo"])]

    g = (
        ggplot(df, aes("TIME_PERIOD_CODE", "OBS_VALUE", color="MEDIDAS#es", group="MEDIDAS#es"))
        + geom_line(size=1.2)
        + geom_point(size=2)
        + labs(
            title="Evolución de las principales fuentes de renta en Canarias (2015–2023)",
            x="Año",
            y="Porcentaje (%)",
            color="Tipo de renta",
        )
        + theme(figure_size=(10, 5))
    )

    out = OUTPUT_DIR / "grafico_renta_lineas.png"
    g.save(str(out), dpi=150)
    return str(out)


@asset
def grafico_barras_agrupadas(limpiar_datos):
    df = limpiar_datos.copy()
    df = df[df["TERRITORIO#es"] == "Canarias"]

    g = (
        ggplot(df, aes("factor(TIME_PERIOD_CODE)", "OBS_VALUE", fill="MEDIDAS#es"))
        + geom_col(position="dodge")
        + labs(
            title="Distribución de la renta en Canarias",
            x="Año",
            y="Valor (%)",
            fill="Tipo de renta",
        )
        + theme(figure_size=(10, 5))
    )

    out = OUTPUT_DIR / "grafico_renta_barras.png"
    g.save(str(out), dpi=150)
    return str(out)


@asset
def grafico_lanzarote_heatmap(unir_renta_codislas):
    df = unir_renta_codislas.copy()
    df = df[(df["ISLA"] == "Lanzarote") & (df["MEDIDAS#es"] == "Sueldos y salarios")]

    g = (
        ggplot(df, aes("MUNICIPIO", "factor(TIME_PERIOD_CODE)", fill="OBS_VALUE"))
        + geom_tile()
        + labs(
            title="Sueldos y salarios en Lanzarote",
            x="Municipio",
            y="Año",
            fill="% renta",
        )
        + theme(figure_size=(11, 6))
        + theme(axis_text_x=element_text(rotation=35, ha="right"))
    )

    out = OUTPUT_DIR / "grafico_lanzarote_heatmap.png"
    g.save(str(out), dpi=150)
    return str(out)


@asset
def grafico_lanzarote_scatter_educacion_vs_desempleo(unir_renta_codislas, unir_estudios_codislas):
    edu = unir_estudios_codislas.copy()
    edu = edu[(edu["ISLA"] == "Lanzarote") & (edu["Periodo"] == "1-ene-23")].copy()

    edu["Total"] = pd.to_numeric(edu["Total"], errors="coerce")

    primaria = (
        edu[edu["Nivel de estudios en curso"].astype(str).str.strip() == "Educación primaria e inferior"]
        .groupby("INE5", as_index=False)["Total"]
        .sum()
        .rename(columns={"Total": "PRIMARIA_N"})
    )

    renta = unir_renta_codislas.copy()
    renta = renta[(renta["ISLA"] == "Lanzarote") & (renta["TIME_PERIOD_CODE"] == 2023)].copy()

    desempleo = (
        renta[renta["MEDIDAS#es"] == "Prestaciones por desempleo"][["INE5", "MUNICIPIO", "OBS_VALUE"]]
        .rename(columns={"OBS_VALUE": "DESEMPLEO_PCT"})
        .drop_duplicates()
    )

    sueldos = (
        renta[renta["MEDIDAS#es"] == "Sueldos y salarios"][["INE5", "OBS_VALUE"]]
        .rename(columns={"OBS_VALUE": "SUELDOS_PCT"})
        .drop_duplicates()
    )

    df = desempleo.merge(sueldos, on="INE5", how="left").merge(primaria, on="INE5", how="left")
    df["PRIMARIA_N"] = df["PRIMARIA_N"].fillna(0)

    g = (
        ggplot(df, aes("DESEMPLEO_PCT", "SUELDOS_PCT"))
        + geom_point(aes(size="PRIMARIA_N"), alpha=0.7)
        + geom_text(aes(label="MUNICIPIO"), va="bottom", size=8)
        + labs(
            title="Lanzarote: desempleo vs sueldos (2023)",
            x="% Prestaciones por desempleo",
            y="% Sueldos y salarios",
            size="Nº primaria e inferior",
        )
        + theme(figure_size=(10, 6))
    )

    out = OUTPUT_DIR / "grafico_lanzarote_scatter.png"
    g.save(str(out), dpi=150)
    return str(out)

@asset
def mapa_rentas_municipios(unir_geojson_rentas):
    geo = unir_geojson_rentas.copy()

    ax = geo.plot(
        column="OBS_VALUE",
        legend=True,
        figsize=(10, 8),
        missing_kwds={"color": "lightgrey", "label": "Sin datos"}
    )

    ax.set_title("Sueldos y salarios por municipio en Canarias (2023)")
    ax.set_axis_off()

    # Cambiar título de la barra de color
    cbar = ax.get_figure().axes[-1]
    cbar.set_ylabel("Porcentaje (%)")

    out = OUTPUT_DIR / "mapa_rentas_municipios.png"
    ax.figure.savefig(out, dpi=150, bbox_inches="tight")
    return str(out)

# ======================
# GENERACIÓN AUTOMÁTICA CON IA
# ======================
@asset
def template_ia(limpiar_datos):
    columnas = ", ".join(limpiar_datos.columns)

    template_tecnico = """
def generar_plot(df):
    # El código debe seguir esta estructura:
    # plot = (ggplot(df, aes(...)) + geom_...)
    # return plot
"""

    system_content = (
        "Eres un experto en la gramática de gráficos y Plotnine. "
        "Tu tarea es traducir descripciones en lenguaje natural a código Python ejecutable. "
        f"Usa siempre este template: {template_tecnico}. "
        "Devuelve exclusivamente código Python. "
        "La función debe llamarse generar_plot(df). "
        "Debe devolver obligatoriamente el objeto plot con return plot. "
        "No añadas explicaciones. "
        "No uses elementos avanzados de theme que puedan no ser compatibles con plotnine."
    )

    descripcion_grafico = f"""
    - Dataset: limpiar_datos
    - Columnas disponibles: {columnas}
    - Filtrar solo las filas donde TERRITORIO#es sea 'Canarias'.
    - Filtrar solo las medidas 'Sueldos y salarios', 'Pensiones' y 'Prestaciones por desempleo'.
    - Estéticas:
        * Variable 'TIME_PERIOD_CODE' mapeada al eje X.
        * Variable 'OBS_VALUE' mapeada al eje Y.
        * Variable 'MEDIDAS#es' mapeada al color y al grupo.
    - Geometría:
        * Usar geom_line y geom_point.
    - Etiquetas:
        * Título: 'Evolución de las principales fuentes de renta en Canarias'
        * Eje X: 'Año'
        * Eje Y: 'Porcentaje (%)'
        * Leyenda: 'Tipo de renta'
    - Diseño:
        * Usar un gráfico claro y sencillo.
        * Incluir theme(figure_size=(10, 5)).
    """

    user_content = f"Basándote en esta descripción, completa el template:\n{descripcion_grafico}"

    return {
        "model": "ollama/llama3.1:8b",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content}
        ],
        "temperature": 0.1,
        "stream": False
    }


@asset
def codigo_generado_ia(context, template_ia):
    url = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer sk-1234"
    }

    try:
        response = requests.post(url, json=template_ia, headers=headers, timeout=60)
        response.raise_for_status()

        res_json = response.json()
        codigo_raw = res_json["choices"][0]["message"]["content"]

        match = re.search(r"```python\s*(.*?)\s*```", codigo_raw, re.DOTALL)
        if match:
            codigo_final = match.group(1)
        else:
            codigo_final = codigo_raw

        codigo_final = codigo_final.strip()

        archivo_codigo = OUTPUT_DIR / "codigo_ia_generado.py"
        with open(archivo_codigo, "w", encoding="utf-8") as f:
            f.write(codigo_final)

        return Output(
            value=codigo_final,
            metadata={
                "codigo_completo": MetadataValue.md(f"```python\n{codigo_final}\n```"),
                "archivo_codigo": MetadataValue.path(str(archivo_codigo))
            }
        )

    except Exception as e:
        context.log.error(f"Error en la petición a la IA: {e}")
        raise e


@asset
def visualizacion_ia(context, codigo_generado_ia, limpiar_datos):
    import plotnine

    df = limpiar_datos.copy()

    entorno_ejecucion = globals().copy()
    entorno_ejecucion["plotnine"] = plotnine
    entorno_ejecucion.update({
        k: v for k, v in plotnine.__dict__.items() if not k.startswith("_")
    })
    entorno_ejecucion["pd"] = pd

    try:
        exec(codigo_generado_ia, entorno_ejecucion)

        grafico = entorno_ejecucion["generar_plot"](df)

        ruta_archivo = OUTPUT_DIR / "visualizacion_ia.png"
        grafico.save(str(ruta_archivo), width=10, height=6, dpi=100)

        # Subida automática a GitHub
        subprocess.run(["git", "add", str(ruta_archivo)], check=False)
        subprocess.run(["git", "commit", "-m", "Actualización automática del gráfico IA"], check=False)
        subprocess.run(["git", "push", "origin", "main"], check=False)

        return Output(
            value=str(ruta_archivo),
            metadata={
                "ruta": MetadataValue.path(str(ruta_archivo)),
                "mensaje": MetadataValue.text("Gráfico generado automáticamente con IA y guardado correctamente.")
            }
        )

    except Exception as e:
        context.log.error(f"Error al renderizar el gráfico: {e}")
        raise e


# ======================
# CHECKS
# ======================
@asset_check(asset=limpiar_datos)
def check_nulos_criticos(limpiar_datos):
    columnas = ["TIME_PERIOD_CODE", "OBS_VALUE", "MEDIDAS#es"]
    nulos = limpiar_datos[columnas].isnull().sum().sum()

    return AssetCheckResult(
        passed=bool(nulos == 0),
        metadata={
            "total_nulos": MetadataValue.int(int(nulos)),
            "descripcion": MetadataValue.text("Comprueba que no haya nulos en columnas clave.")
        }
    )


@asset_check(asset=limpiar_codislas)
def check_estandarizacion(limpiar_codislas):
    invalidos = (~limpiar_codislas["INE5"].astype(str).str.fullmatch(r"\d{5}", na=False)).sum()

    return AssetCheckResult(
        passed=bool(invalidos == 0),
        metadata={
            "codigos_invalidos": MetadataValue.int(int(invalidos)),
            "descripcion": MetadataValue.text("Comprueba que los códigos INE5 tengan 5 dígitos.")
        }
    )


@asset_check(asset=limpiar_datos)
def check_cardinalidad(limpiar_datos):
    df = limpiar_datos.copy()
    df = df[df["TERRITORIO#es"] == "Canarias"]
    categorias = df["MEDIDAS#es"].dropna().nunique()

    return AssetCheckResult(
        passed=bool(categorias <= 6),
        metadata={
            "n_categorias": MetadataValue.int(int(categorias)),
            "descripcion": MetadataValue.text("Comprueba que no haya demasiadas categorías.")
        }
    )


@asset_check(asset=limpiar_datos)
def check_continuidad(limpiar_datos):
    df = limpiar_datos.copy()
    df = df[df["TERRITORIO#es"] == "Canarias"]
    anios = sorted(df["TIME_PERIOD_CODE"].dropna().unique())

    return AssetCheckResult(
        passed=bool(len(anios) >= 3),
        metadata={
            "n_anios": MetadataValue.int(int(len(anios))),
            "descripcion": MetadataValue.text("Comprueba que la serie temporal tenga varios años.")
        }
    )


@asset_check(asset=limpiar_datos)
def check_eje_cero(limpiar_datos):
    df = limpiar_datos.copy()
    df = df[df["TERRITORIO#es"] == "Canarias"]
    minimo = df["OBS_VALUE"].min()

    return AssetCheckResult(
        passed=bool(minimo >= 0),
        metadata={
            "valor_minimo": MetadataValue.float(float(minimo)),
            "descripcion": MetadataValue.text("Comprueba que el eje Y pueda partir de cero.")
        }
    )