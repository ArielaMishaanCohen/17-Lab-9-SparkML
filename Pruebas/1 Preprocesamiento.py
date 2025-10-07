import pandas as pd
import openpyxl 
import numpy as np
import os
from pathlib import Path
import unicodedata
from pyspark.sql.functions import desc
from pyspark.sql.functions import count, countDistinct
from pyspark.sql.functions import col

# Funciones
def limpiar_nombre_columna(col, seen):
    col_clean = ''.join(
        c for c in unicodedata.normalize('NFD', str(col))
        if unicodedata.category(c) != 'Mn'
    )
    col_clean = (
        col_clean.strip()
        .lower()
        .replace(" ", "_")
        .replace(".", "_")
        .replace("-", "_")
    )

    if col_clean in seen:
        seen[col_clean] += 1
        col_clean = f"{col_clean}_{seen[col_clean]}"
    else:
        seen[col_clean] = 0
    return col_clean

def unir_excels_y_guardar_tabla(carpeta, nombre_tabla):
    folder_path = Path(carpeta)
    all_data = []

    for file in sorted(os.listdir(folder_path)):
        if file.endswith(".xlsx"):
            year = int(''.join(filter(str.isdigit, file)))
            df = pd.read_excel(folder_path / file)
            df["año"] = year
            all_data.append(df)

    df_all = pd.concat(all_data, ignore_index=True)

    seen = {}
    df_all.columns = [limpiar_nombre_columna(c, seen) for c in df_all.columns]
    df_all = df_all.loc[:, ~df_all.columns.duplicated()]

    for col in ["año", "mes_ocu", "hora_ocu", "edad_per"]:
        if col in df_all.columns:
            df_all[col] = pd.to_numeric(df_all[col], errors="coerce").astype("Int64")

    for col in df_all.columns:
        if df_all[col].dtype == "object":
            df_all[col] = df_all[col].astype(str)

    df_spark = spark.createDataFrame(df_all)
    df_spark.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(nombre_tabla)

    print(f"Guardada tabla '{nombre_tabla}' con {df_spark.count()} filas")
    df_spark.printSchema()


# CARGA DE DATOS

dic_fallecidos = pd.read_excel('ine/Diccionario Fallecidos y Lesionados.xlsx', header=1)
dic_hechos = pd.read_excel('ine/Diccionario Hechos.xlsx', header=1)
dic_vehiculos = pd.read_excel('ine/Diccionario Vehículos.xlsx', header= 1)

# Fallecidos
fallecidos15 = pd.read_excel('ine/Fallecidos/fallecidos2015.xlsx')
fallecidos16 = pd.read_excel('ine/Fallecidos/fallecidos2016.xlsx')
fallecidos17 = pd.read_excel('ine/Fallecidos/fallecidos2017.xlsx')
fallecidos18 = pd.read_excel('ine/Fallecidos/fallecidos2018.xlsx')
fallecidos19 = pd.read_excel('ine/Fallecidos/fallecidos2019.xlsx')
fallecidos20 = pd.read_excel('ine/Fallecidos/fallecidos2020.xlsx')
fallecidos21 = pd.read_excel('ine/Fallecidos/fallecidos2021.xlsx')
fallecidos22 = pd.read_excel('ine/Fallecidos/fallecidos2022.xlsx')
fallecidos23 = pd.read_excel('ine/Fallecidos/fallecidos2023.xlsx')

lista_fallecidos = [fallecidos15, fallecidos16, fallecidos17, fallecidos18, fallecidos19, fallecidos20, fallecidos21, fallecidos22, fallecidos23]

# Hechos
hechos15 = pd.read_excel('ine/Hechos/hechos2015.xlsx')
hechos16 = pd.read_excel('ine/Hechos/hechos2016.xlsx')
hechos17 = pd.read_excel('ine/Hechos/hechos2017.xlsx')
hechos18 = pd.read_excel('ine/Hechos/hechos2018.xlsx')
hechos19 = pd.read_excel('ine/Hechos/hechos2019.xlsx')
hechos20 = pd.read_excel('ine/Hechos/hechos2020.xlsx')
hechos21 = pd.read_excel('ine/Hechos/hechos2021.xlsx')
hechos22 = pd.read_excel('ine/Hechos/hechos2022.xlsx')
hechos23 = pd.read_excel('ine/Hechos/hechos2023.xlsx')

lista_hechos = [hechos15, hechos16, hechos17, hechos18, hechos19, hechos20, hechos21, hechos22, hechos23]

# Hechos
vehiculos15 = pd.read_excel('ine/Vehiculos/vehiculos2015.xlsx')
vehiculos16 = pd.read_excel('ine/Vehiculos/vehiculos2016.xlsx')
vehiculos17 = pd.read_excel('ine/Vehiculos/vehiculos2017.xlsx')
vehiculos18 = pd.read_excel('ine/Vehiculos/vehiculos2018.xlsx')
vehiculos19 = pd.read_excel('ine/Vehiculos/vehiculos2019.xlsx')
vehiculos20 = pd.read_excel('ine/Vehiculos/vehiculos2020.xlsx')
vehiculos21 = pd.read_excel('ine/Vehiculos/vehiculos2021.xlsx')
vehiculos22 = pd.read_excel('ine/Vehiculos/vehiculos2022.xlsx')
vehiculos23 = pd.read_excel('ine/Vehiculos/vehiculos2023.xlsx')

lista_vehiculos = [vehiculos15, vehiculos16, vehiculos17, vehiculos18, vehiculos19, vehiculos20, vehiculos21, vehiculos22, vehiculos23]

# Cambio de datos para dfs 2022 y 2023

dic_fallecidos.columns = dic_fallecidos.columns.str.strip()

diccionario_fallecidos = {}
variable_actual = None

for _, row in dic_fallecidos.iterrows():
    if pd.notna(row["Variable"]):  
        variable_actual = row["Variable"]
        diccionario_fallecidos[variable_actual] = {}
    
    # Intentar convertir a número
    codigo = pd.to_numeric(row["Código"], errors="coerce")
    valor = row["Valor"]

    if variable_actual and pd.notna(codigo) and pd.notna(valor):
        diccionario_fallecidos[variable_actual][int(codigo)] = str(valor)
