from dagster import asset
from pathlib import Path
import pandas as pd
from plotnine import *

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
    df = df.dropna(subset=["TIME_PERIOD_CODE", "OBS_VALUE", "MEDIDAS#es"])
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


# ======================
# VISUALIZACIONES
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
    # nº de personas en "Educación primaria e inferior" (Lanzarote)
    edu = unir_estudios_codislas.copy()
    edu = edu[(edu["ISLA"] == "Lanzarote") & (edu["Periodo"] == "1-ene-23")].copy()

    edu["Total"] = pd.to_numeric(edu["Total"], errors="coerce")

    primaria = (
        edu[edu["Nivel de estudios en curso"].astype(str).str.strip() == "Educación primaria e inferior"]
        .groupby("INE5", as_index=False)["Total"]
        .sum()
        .rename(columns={"Total": "PRIMARIA_N"})
    )

    # Renta 2023 (Lanzarote): X desempleo, Y sueldos
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