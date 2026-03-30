import pandas as pd
from dagster import asset

@asset
def poblacion_test():
    # Creamos un pequeño dataframe manual para no depender de archivos externos aún
    data = {
        "isla": ["Tenerife", "Gran Canaria", "Lanzarote", "Fuerteventura"],
        "habitantes": [931646, 855521, 156112, 119732]
    }
    return pd.DataFrame(data)

@asset(deps=[poblacion_test])
def total_canarias(poblacion_test):
    # Una métrica simple: la suma total
    total = poblacion_test['habitantes'].sum()
    print(total)
    return poblacion_test