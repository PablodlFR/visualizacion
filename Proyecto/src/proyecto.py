from dagster import asset, asset_check, AssetCheckResult, MetadataValue, Output
from pathlib import Path
import pandas as pd
from plotnine import *
import requests
import re
import subprocess
import geopandas as gpd
import matplotlib.pyplot as plt

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "outputs"

# =========================
# CARGA DE DATOS
# =========================

@asset
def cargar_renta():
    return pd.read_csv(DATA_DIR / "rentamedia.csv")


@asset
def cargar_ocupacion():
    return pd.read_csv(DATA_DIR / "ocupacion.csv")


@asset
def cargar_actividad():
    return pd.read_csv(DATA_DIR / "actividad.csv")


@asset
def cargar_geojson_2024():
    return gpd.read_file(DATA_DIR / "secciones_20240101_tenerife.json")


# =========================
# LIMPIEZA DE DATOS
# =========================

@asset
def limpiar_renta(cargar_renta):
    df = cargar_renta.copy()

    df["año"] = df["año"].astype(int)
    df["OBS_VALUE"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
    df["municipio"] = df["municipio"].astype(str).str.strip()

    df = df.dropna(subset=["OBS_VALUE", "TERRITORIO_CODE", "MEDIDAS_CODE"])

    return df


@asset
def limpiar_ocupacion(cargar_ocupacion):
    df = cargar_ocupacion.copy()

    df["año"] = df["año"].astype(int)
    df["num_casos"] = pd.to_numeric(df["num_casos"], errors="coerce")
    df["municipio"] = df["municipio"].astype(str).str.strip()

    df = df.dropna(subset=["num_casos", "geocode", "ocupacion"])

    return df


@asset
def limpiar_actividad(cargar_actividad):
    df = cargar_actividad.copy()

    df["Periodo"] = df["Periodo"].astype(int)
    df["num_casos"] = pd.to_numeric(df["num_casos"], errors="coerce")
    df["municipio"] = df["municipio"].astype(str).str.strip()

    df = df.dropna(subset=["num_casos", "geocode", "Actividad económica"])

    return df


# =========================
# UNIONES Y PREPARACIÓN
# =========================

@asset
def unir_renta_ocupacion(limpiar_renta, limpiar_ocupacion):
    renta = limpiar_renta.copy()
    ocupacion = limpiar_ocupacion.copy()

    renta["clave_seccion"] = renta["TERRITORIO_CODE"].astype(str).str.split("_", n=1).str[1]
    ocupacion["clave_seccion"] = ocupacion["geocode"].astype(str).str.split("_", n=1).str[1]

    return renta.merge(
        ocupacion,
        on=["año", "clave_seccion"],
        suffixes=("_renta", "_ocupacion")
    )


@asset
def unir_renta_actividad(limpiar_renta, limpiar_actividad):
    renta = limpiar_renta.copy()
    actividad = limpiar_actividad.copy()

    renta["clave_seccion"] = renta["TERRITORIO_CODE"].astype(str).str.split("_", n=1).str[1]
    actividad["clave_seccion"] = actividad["geocode"].astype(str).str.split("_", n=1).str[1]

    return renta.merge(
        actividad,
        left_on=["año", "clave_seccion"],
        right_on=["Periodo", "clave_seccion"],
        suffixes=("_renta", "_actividad")
    )


@asset
def unir_renta_mapa(limpiar_renta, cargar_geojson_2024):
    renta = limpiar_renta.copy()
    mapa = cargar_geojson_2024.copy()

    renta = renta[
        (renta["año"] == 2023) &
        (renta["MEDIDAS_CODE"] == "RENTA_BRUTA_MEDIA_HOGAR")
    ]

    gdf = mapa.merge(
        renta,
        left_on="geocode",
        right_on="TERRITORIO_CODE",
        how="left"
    )

    return gdf


@asset
def unir_ocupacion_mapa(limpiar_ocupacion, cargar_geojson_2024):
    ocupacion = limpiar_ocupacion.copy()
    mapa = cargar_geojson_2024.copy()

    ocupacion = ocupacion[ocupacion["año"] == 2023]

    ocupacion["clave_seccion"] = ocupacion["geocode"].astype(str).str.split("_", n=1).str[1]
    mapa["clave_seccion"] = mapa["geocode"].astype(str).str.split("_", n=1).str[1]

    ocupacion["es_cualificada"] = ocupacion["ocupacion"].str.contains(
        "Directores/gerentes",
        case=False,
        na=False
    )

    ocupacion["casos_cualificados"] = ocupacion["num_casos"].where(
        ocupacion["es_cualificada"],
        0
    )

    df = (
        ocupacion.groupby("clave_seccion", as_index=False)
        .agg(
            total_ocupacion=("num_casos", "sum"),
            ocupacion_cualificada=("casos_cualificados", "sum")
        )
    )

    df["porcentaje_cualificada"] = (
        df["ocupacion_cualificada"] / df["total_ocupacion"] * 100
    )

    gdf = mapa.merge(df, on="clave_seccion", how="left")

    return gdf


@asset
def unir_servicios_mapa(limpiar_actividad, cargar_geojson_2024):
    actividad = limpiar_actividad.copy()
    mapa = cargar_geojson_2024.copy()

    actividad = actividad[actividad["Periodo"] == 2023]

    actividad["clave_seccion"] = actividad["geocode"].astype(str).str.split("_", n=1).str[1]
    mapa["clave_seccion"] = mapa["geocode"].astype(str).str.split("_", n=1).str[1]

    total = (
        actividad.groupby("clave_seccion", as_index=False)["num_casos"]
        .sum()
        .rename(columns={"num_casos": "total"})
    )

    servicios = actividad[actividad["Actividad económica"] == "Servicios"]

    servicios = (
        servicios.groupby("clave_seccion", as_index=False)["num_casos"]
        .sum()
        .rename(columns={"num_casos": "servicios"})
    )

    df = total.merge(servicios, on="clave_seccion", how="left")
    df["servicios"] = df["servicios"].fillna(0)
    df["porcentaje_servicios"] = df["servicios"] / df["total"] * 100

    gdf = mapa.merge(df, on="clave_seccion", how="left")

    return gdf


# =========================
# VISUALIZACIONES
# =========================

@asset
def grafico_renta_media(limpiar_renta):
    df = limpiar_renta.copy()

    df = df[df["MEDIDAS_CODE"] == "RENTA_BRUTA_MEDIA_HOGAR"]

    df_group = df.groupby("año", as_index=False)["OBS_VALUE"].mean()

    p = (
        ggplot(df_group, aes(x="factor(año)", y="OBS_VALUE"))
        + geom_col(fill="#4C72B0")
        + labs(
            title="Renta bruta media por hogar por año",
            x="Año",
            y="Renta (€)"
        )
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    p.save(OUTPUT_DIR / "renta_media.png", width=6, height=4, dpi=300)

    return df_group


@asset
def grafico_renta_vs_ocupacion(unir_renta_ocupacion):
    df = unir_renta_ocupacion.copy()

    df = df[
        (df["MEDIDAS_CODE"] == "RENTA_BRUTA_MEDIA_HOGAR") &
        (df["año"] == 2023)
    ]

    df["es_cualificada"] = df["ocupacion"].str.contains(
        "Directores/gerentes",
        case=False,
        na=False
    )

    df["casos_cualificados"] = df["num_casos"].where(
        df["es_cualificada"],
        0
    )

    df_group = (
        df.groupby(["clave_seccion", "OBS_VALUE"], as_index=False)
        .agg(
            total_ocupacion=("num_casos", "sum"),
            ocupacion_cualificada=("casos_cualificados", "sum")
        )
    )

    df_group["porcentaje_cualificada"] = (
        df_group["ocupacion_cualificada"] / df_group["total_ocupacion"] * 100
    )

    p = (
        ggplot(df_group, aes(x="porcentaje_cualificada", y="OBS_VALUE"))
        + geom_point(alpha=0.6, color="#4C72B0")
        + labs(
            title="Relación entre la renta y la ocupación cualificada por sección (2023)",
            x="Ocupación cualificada (%)",
            y="Renta bruta media por hogar (€)"
        )
        + theme(plot_title=element_text(size=11))
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    p.save(OUTPUT_DIR / "renta_vs_ocupacion.png", width=6, height=4, dpi=300)

    return df_group


@asset
def grafico_actividad_renta(unir_renta_actividad):
    df = unir_renta_actividad.copy()

    df = df[
        (df["MEDIDAS_CODE"] == "RENTA_BRUTA_MEDIA_HOGAR") &
        (df["año"] == 2023)
    ]

    total = (
        df.groupby("clave_seccion", as_index=False)["num_casos"]
        .sum()
        .rename(columns={"num_casos": "total"})
    )

    sector = (
        df.groupby(["clave_seccion", "Actividad económica"], as_index=False)["num_casos"]
        .sum()
    )

    df_prop = sector.merge(total, on="clave_seccion")
    df_prop["porcentaje"] = df_prop["num_casos"] / df_prop["total"] * 100

    renta = df[["clave_seccion", "OBS_VALUE"]].drop_duplicates()

    df_final = df_prop.merge(renta, on="clave_seccion")

    df_final["nivel_renta"] = pd.qcut(
        df_final["OBS_VALUE"],
        q=3,
        labels=["Baja", "Media", "Alta"]
    )

    df_group = (
        df_final.groupby(["nivel_renta", "Actividad económica"], as_index=False)["porcentaje"]
        .mean()
    )

    df_group = df_group[df_group["Actividad económica"] != "No consta"]

    colores = {
        "Servicios": "#4C72B0",
        "Industria": "#55A868",
        "Construcción": "#C44E52",
        "Agricultura, ganadería y pesca": "#E5AE38"
    }

    p = (
        ggplot(df_group, aes(x="nivel_renta", y="porcentaje", fill="Actividad económica"))
        + geom_col(position="stack")
        + scale_fill_manual(values=colores)
        + labs(
            title="Composición de la actividad económica según nivel de renta (2023)",
            x="Nivel de renta",
            y="Porcentaje (%)"
        )
        + theme(plot_title=element_text(size=11))
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    p.save(OUTPUT_DIR / "actividad_renta.png", width=7, height=4, dpi=300)

    return df_group


@asset
def mapa_renta_2023(unir_renta_mapa):
    gdf = unir_renta_mapa.copy()

    fig, ax = plt.subplots(figsize=(9, 8))

    gdf.plot(
        column="OBS_VALUE",
        cmap="Blues",
        legend=True,
        edgecolor="white",
        linewidth=0.1,
        ax=ax,
        vmin=gdf["OBS_VALUE"].quantile(0.05),
        vmax=gdf["OBS_VALUE"].quantile(0.95)
    )

    ax.set_title("Renta bruta media por hogar por sección (2023)")
    ax.axis("off")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUTPUT_DIR / "mapa_renta_2023.png", dpi=300, bbox_inches="tight")
    plt.close()

    return gdf


@asset
def mapa_ocupacion_cualificada(unir_ocupacion_mapa):
    gdf = unir_ocupacion_mapa.copy()

    fig, ax = plt.subplots(figsize=(9, 8))

    gdf.plot(
        column="porcentaje_cualificada",
        cmap="Greens",
        legend=True,
        edgecolor="white",
        linewidth=0.1,
        ax=ax,
        vmin=gdf["porcentaje_cualificada"].quantile(0.05),
        vmax=gdf["porcentaje_cualificada"].quantile(0.95)
    )

    ax.set_title("Ocupación cualificada por sección (2023)")
    ax.axis("off")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    plt.savefig(
        OUTPUT_DIR / "mapa_ocupacion_cualificada_2023.png",
        dpi=300,
        bbox_inches="tight"
    )
    plt.close()

    return gdf


@asset
def mapa_servicios(unir_servicios_mapa):
    gdf = unir_servicios_mapa.copy()

    fig, ax = plt.subplots(figsize=(9, 8))

    gdf.plot(
        column="porcentaje_servicios",
        cmap="Purples",
        legend=True,
        edgecolor="white",
        linewidth=0.1,
        ax=ax,
        vmin=gdf["porcentaje_servicios"].quantile(0.05),
        vmax=gdf["porcentaje_servicios"].quantile(0.95)
    )

    ax.set_title("Porcentaje de actividad en servicios por sección (2023)")
    ax.axis("off")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    plt.savefig(
        OUTPUT_DIR / "mapa_servicios_2023.png",
        dpi=300,
        bbox_inches="tight"
    )
    plt.close()

    return gdf

# ======================
# CHECKS
# ======================

# Comprueba que el dataset de renta contiene las columnas necesarias para el análisis.
@asset_check(asset=cargar_renta)
def check_columnas_renta(cargar_renta):
    columnas_esperadas = {
        "año",
        "MEDIDAS_CODE",
        "MEDIDAS#es",
        "TERRITORIO_CODE",
        "OBS_VALUE"
    }

    columnas_faltantes = columnas_esperadas - set(cargar_renta.columns)

    return AssetCheckResult(
        passed=len(columnas_faltantes) == 0,
        metadata={
            "columnas_faltantes": ", ".join(columnas_faltantes),
            "n_columnas": len(cargar_renta.columns)
        }
    )

# Comprueba que el dataset de ocupación contiene las columnas necesarias para calcular la ocupación cualificada.
@asset_check(asset=cargar_ocupacion)
def check_columnas_ocupacion(cargar_ocupacion):
    columnas_esperadas = {
        "ocupacion",
        "año",
        "num_casos",
        "geocode"
    }

    columnas_faltantes = columnas_esperadas - set(cargar_ocupacion.columns)

    return AssetCheckResult(
        passed=len(columnas_faltantes) == 0,
        metadata={
            "columnas_faltantes": ", ".join(columnas_faltantes),
            "n_columnas": len(cargar_ocupacion.columns)
        }
    )

# Comprueba que el dataset de actividad contiene las columnas necesarias para analizar los sectores económicos.
@asset_check(asset=cargar_actividad)
def check_columnas_actividad(cargar_actividad):
    columnas_esperadas = {
        "Actividad económica",
        "Periodo",
        "num_casos",
        "geocode"
    }

    columnas_faltantes = columnas_esperadas - set(cargar_actividad.columns)

    return AssetCheckResult(
        passed=len(columnas_faltantes) == 0,
        metadata={
            "columnas_faltantes": ", ".join(columnas_faltantes),
            "n_columnas": len(cargar_actividad.columns)
        }
    )

# Comprueba que la variable principal de renta no tenga valores nulos ni negativos tras la limpieza.
@asset_check(asset=limpiar_renta)
def check_renta_valida(limpiar_renta):
    df = limpiar_renta.copy()

    n_nulos = df["OBS_VALUE"].isna().sum()
    n_negativos = (df["OBS_VALUE"] < 0).sum()

    return AssetCheckResult(
        passed=bool(n_nulos == 0 and n_negativos == 0),
        metadata={
            "valores_nulos": int(n_nulos),
            "valores_negativos": int(n_negativos)
        }
    )


# Comprueba que el número de casos de ocupación no tenga valores nulos ni negativos tras la limpieza, ya que representa personas.
@asset_check(asset=limpiar_ocupacion)
def check_ocupacion_valida(limpiar_ocupacion):
    df = limpiar_ocupacion.copy()

    n_nulos = df["num_casos"].isna().sum()
    n_negativos = (df["num_casos"] < 0).sum()

    return AssetCheckResult(
        passed=bool(n_nulos == 0 and n_negativos == 0),
        metadata={
            "valores_nulos": int(n_nulos),
            "valores_negativos": int(n_negativos)
        }
    )

# Comprueba que el número de casos de actividad económica no tenga valores nulos ni negativos tras la limpieza, ya que representa personas.
@asset_check(asset=limpiar_actividad)
def check_actividad_valida(limpiar_actividad):
    df = limpiar_actividad.copy()

    n_nulos = df["num_casos"].isna().sum()
    n_negativos = (df["num_casos"] < 0).sum()

    return AssetCheckResult(
        passed=bool(n_nulos == 0 and n_negativos == 0),
        metadata={
            "valores_nulos": int(n_nulos),
            "valores_negativos": int(n_negativos)
        }
    )

# Comprueba que la unión entre renta y ocupación no queda vacía.
@asset_check(asset=unir_renta_ocupacion)
def check_union_renta_ocupacion_no_vacia(unir_renta_ocupacion):
    df = unir_renta_ocupacion.copy()

    return AssetCheckResult(
        passed=len(df) > 0,
        metadata={
            "filas_union": len(df)
        }
    )

# Comprueba que la unión entre renta y actividad económica no queda vacía.
@asset_check(asset=unir_renta_actividad)
def check_union_renta_actividad_no_vacia(unir_renta_actividad):
    df = unir_renta_actividad.copy()

    return AssetCheckResult(
        passed=len(df) > 0,
        metadata={
            "filas_union": len(df)
        }
    )

# Comprueba que el mapa de renta tiene datos asociados en la mayoría de secciones.
@asset_check(asset=unir_renta_mapa)
def check_mapa_renta_con_datos(unir_renta_mapa):
    gdf = unir_renta_mapa.copy()

    total_secciones = len(gdf)
    secciones_con_datos = gdf["OBS_VALUE"].notna().sum()
    porcentaje_con_datos = secciones_con_datos / total_secciones * 100

    return AssetCheckResult(
        passed=bool(porcentaje_con_datos >= 95),
        metadata={
            "total_secciones": int(total_secciones),
            "secciones_con_datos": int(secciones_con_datos),
            "porcentaje_con_datos": float(porcentaje_con_datos)
        }
    )


# Comprueba que el porcentaje de ocupación cualificada está dentro del rango lógico 0-100.
@asset_check(asset=unir_ocupacion_mapa)
def check_porcentaje_ocupacion_cualificada(unir_ocupacion_mapa):
    gdf = unir_ocupacion_mapa.copy()

    valores_validos = gdf["porcentaje_cualificada"].between(0, 100)
    n_invalidos = (~valores_validos & gdf["porcentaje_cualificada"].notna()).sum()

    return AssetCheckResult(
        passed=bool(n_invalidos == 0),
        metadata={
            "valores_invalidos": int(n_invalidos),
            "minimo": float(gdf["porcentaje_cualificada"].min()),
            "maximo": float(gdf["porcentaje_cualificada"].max())
        }
    )

# Comprueba que el porcentaje de actividad en servicios está dentro del rango lógico 0-100.
@asset_check(asset=unir_servicios_mapa)
def check_porcentaje_servicios(unir_servicios_mapa):
    gdf = unir_servicios_mapa.copy()

    valores_validos = gdf["porcentaje_servicios"].between(0, 100)
    n_invalidos = (~valores_validos & gdf["porcentaje_servicios"].notna()).sum()

    return AssetCheckResult(
        passed=bool(n_invalidos == 0),
        metadata={
            "valores_invalidos": int(n_invalidos),
            "minimo": float(gdf["porcentaje_servicios"].min()),
            "maximo": float(gdf["porcentaje_servicios"].max())
        }
    )