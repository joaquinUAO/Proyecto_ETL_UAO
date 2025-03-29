import yaml
import psycopg2
from psycopg2 import sql
import sqlalchemy
from sqlalchemy import create_engine, text
import pandas as pd
import kaggle
import numpy as np
import logging

#rutaDatasetKaggle = "fedesoriano/electric-power-consumption"
#file = "powerconsumption.csv"

def load_config(file_path="config.yaml"):
    with open(file_path, "r") as file:
        return yaml.safe_load(file)
    
def conectar_bd(db_config):
    # Load credentials
    db_user = db_config["user"]
    db_password = db_config["password"]
    db_host = db_config["host"]
    db_port = db_config["port"]
    db_name = db_config["name"]
    # DB connection
    conn = psycopg2.connect(
        dbname="postgres",
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    conn.autocommit = True
    print("Conexión exitosa")

    # Creamos la base de datos para el ejercicio
    db_name = "project1_db"
    existe = 0
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
            print(f"Base de datos '{db_name}' creada exitosamente.")
    except psycopg2.errors.DuplicateDatabase:
        print(f"La base de datos '{db_name}' ya existe.")
        existe = 1
    finally:
        conn.close()

    engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

    return conn, engine, existe

def crear_tabla(conn, engine):
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS project1_db (
                id SERIAL PRIMARY KEY,
                datetime TIMESTAMP,
                temperature FLOAT,
                humidity FLOAT,
                wind_speed FLOAT,
                general_diffuse_flows FLOAT,
                diffuse_flows FLOAT,
                power_consumption_zone1 FLOAT,
                power_consumption_zone2 FLOAT,
                power_consumption_zone3 FLOAT,
                fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
        conn.commit()

def renombrar_dataset(file_csv):
    rename_columnas = {
        "Datetime": "datetime",
        "Temperature": "temperature",
        "Humidity": "humidity",
        "WindSpeed": "wind_speed",
        "GeneralDiffuseFlows": "general_diffuse_flows",
        "DiffuseFlows": "diffuse_flows",
        "PowerConsumption_Zone1": "power_consumption_zone1",
        "PowerConsumption_Zone2": "power_consumption_zone2",
        "PowerConsumption_Zone3": "power_consumption_zone3"
    }
    file_csv.rename(columns=rename_columnas, inplace=True)
    return file_csv

def cargar_a_staging(conn, engine, file_csv):
    file_csv.to_sql("project1_db", engine, if_exists="replace", index=False)
    print("Datos cargados en la base de datos exitosamente.")
    # Leer datos de la tabla staging para validar
    with engine.connect() as conn:
        data_staging = pd.read_sql("SELECT * FROM project1_db", conn)
    return data_staging

def extraer_a_staging():
    # Estraemos el dataset directamente desde kaggle
    kaggle.api.dataset_download_files("fedesoriano/electric-power-consumption", unzip=True)
    file_csv = pd.read_csv("powerconsumption.csv", sep=",")
    logging.info("Archivo descargado desde kaggle")
    # Configuramos los parámetros de conexión a la base de datos
    config = load_config()
    db_config = config["database"]
    logging.info(config["database"])
    # Realizamos la conexión a la base de datos validando su existencia
    conn, engine, existe = conectar_bd(db_config)        
    crear_tabla(conn, engine)        

    # Como el nombre de las columnas en el dataframe difiere del nombre de las columnas en db, renombramos las columnas del dataframe para facilitar el cargue de datos desde python
    file_csv_rename = renombrar_dataset(file_csv)

    with engine.connect() as conn:
        data_transform = pd.read_sql("SELECT * FROM project1_db", conn)

    # Validar si la tabla está vacía
    etl_total = 0
    if data_transform.empty:
        etl_total = 1
        # Subimos los datos a la base de datos usando una función de pandas
        data_staging = cargar_a_staging(conn, engine, file_csv_rename)
        print("Dataset almacenado en área de staging existosamente")
    else:
        # Filtrar solo los registros que NO están en la base de datos
        new_data = file_csv_rename[~file_csv_rename['datetime'].isin(data_transform['datetime'])]
        # Si hay datos nuevos, los insertamos
        if not new_data.empty:
            data_staging = cargar_a_staging(conn, engine, file_csv_rename)
            print("Dataset almacenado en área de staging existosamente")
            etl_total = 1
        else:
            print("No hay datos nuevos. El proceso de ETL no se ejecuta.")
            etl_total = 0

    return conn, engine, etl_total

def obtener_temporada_marruecos(fecha):
    año = fecha.year    
    # Definir fechas de estaciones en Marruecos
    primavera = pd.Timestamp(f"{año}-03-21")
    verano = pd.Timestamp(f"{año}-06-21")
    otoño = pd.Timestamp(f"{año}-09-21")
    invierno = pd.Timestamp(f"{año}-12-21")

    # Asignar temporada según la fecha
    if primavera <= fecha < verano:
        return 'Primavera'
    elif verano <= fecha < otoño:
        return 'Verano'
    elif otoño <= fecha < invierno:
        return 'Otoño'
    else:
        return 'Invierno'

def transforms(conn, engine):
    with engine.connect() as conn:
        data_transform = pd.read_sql("SELECT * FROM project1_db", conn)
    print("Dataset cargado desde el área de staging")
        #################### Transformacion 1:
    ### - Cambio de formato de la columna fecha de objeto a fecha, así como la generación de nuevas columnas relacionadas con la fecha    
    data_transform['datetime'] = pd.to_datetime(data_transform['datetime'])
    ### - Generación de nuevas columnas derivadas de la fecha
    data_transform['season'] = data_transform['datetime'].apply(obtener_temporada_marruecos)
    data_transform['year'] = data_transform['datetime'].dt.year
    data_transform['month'] = data_transform['datetime'].dt.month
    data_transform['day'] = data_transform['datetime'].dt.day
    data_transform['hour'] = data_transform['datetime'].dt.hour
    data_transform['minute'] = data_transform['datetime'].dt.minute
    data_transform['day_name'] = data_transform['datetime'].dt.day_name()
    ### - Cambio de formato de fecha a consecutivo numérico
    data_transform["datetime"] = data_transform["datetime"].dt.strftime("%Y%m%d%H%M").astype(int)

    #################### Transformacion 2:
    ### - Generación de nueva columna con potencial eólico y eliminación de la columna wind_speed
    air_density = 1.225  # kg/m³ (a nivel del mar)
    radius = 20  # Radio de las aspas en metros
    swept_area = np.pi * radius**2  # Área barrida en m²
    power_coefficient = 0.4  # Coeficiente de potencia típico
    data_transform["wind_power_kwatts"] = 0.5 * air_density * swept_area * ((data_transform["wind_speed"]) ** 3) * power_coefficient/1000
    data_transform = data_transform.drop(['wind_speed'], axis=1)

    #################### Transformacion 3:
    ### - Generación de nueva columna con potencial solar y eliminación de las columnas general_diffuse_flows y diffuse_flows
    w1 = 0.6  # Peso para la radiación directa
    w2 = 0.4  # Peso para la radiación difusa
    panel_efficiency = 0.20  # 18% eficiencia típica
    panel_area = 2  # m² (ejemplo con un sistema pequeño)
    data_transform["solar_power_kwatts"] = panel_efficiency * panel_area * ((w1 * (data_transform["general_diffuse_flows"]*2)) + (w2 * data_transform["diffuse_flows"]*2))/100
    data_transform = data_transform.drop(['general_diffuse_flows','diffuse_flows'], axis=1)

    #################### Transformacion 4:
    ### - Generación de nueva columna con potencia de consumo total
    data_transform['power_consumption_total'] = data_transform[['power_consumption_zone1', 'power_consumption_zone2', 'power_consumption_zone3']].sum(axis=1)
    ### - Generación de nuevas columnas de conversióna energía de cada variable de potencia
    data_transform['energy_consumption_zone1'] = data_transform['power_consumption_zone1']/6
    data_transform['energy_consumption_zone2'] = data_transform['power_consumption_zone2']/6
    data_transform['energy_consumption_zone3'] = data_transform['power_consumption_zone3']/6
    data_transform['energy_consumption_total'] = data_transform['power_consumption_total']/6

    print("Transformaciones realizadas exitosamente!")
    return data_transform

# Consulta SQL para crear las tablas
def crear_tablas_dimensional(conn, engine):
    with engine.connect() as conn:
        conn.execute(text("""
            -- Tabla de Dimensión: Tiempo
            CREATE TABLE IF NOT EXISTS dim_time (
                time_key SERIAL PRIMARY KEY,
                datetime TIMESTAMP UNIQUE NOT NULL,
                year INT,
                month INT,
                day INT,
                hour INT,
                minute INT,
                day_name VARCHAR(10),
                season VARCHAR(10)
            );

            -- Tabla de Dimensión: Clima
            CREATE TABLE IF NOT EXISTS dim_weather (
                weather_key SERIAL PRIMARY KEY,
                temperature FLOAT,
                humidity FLOAT
            );

            -- Tabla de Dimensión: Fuentes de Energía
            CREATE TABLE IF NOT EXISTS dim_power_source (
                power_source_key SERIAL PRIMARY KEY,
                wind_power_kwatts FLOAT,
                solar_power_kwatts FLOAT
            );

            -- Tabla de Hechos: Consumo de Energía
            CREATE TABLE IF NOT EXISTS fact_energy_consumption (
                id SERIAL PRIMARY KEY,
                time_key INT REFERENCES dim_time(time_key),
                weather_key INT REFERENCES dim_weather(weather_key),
                power_source_key INT REFERENCES dim_power_source(power_source_key),
                power_consumption_zone1 FLOAT,
                power_consumption_zone2 FLOAT,
                power_consumption_zone3 FLOAT,
                power_consumption_total FLOAT,
                energy_consumption_zone1 FLOAT,
                energy_consumption_zone2 FLOAT,
                energy_consumption_zone3 FLOAT,
                energy_consumption_total FLOAT
            );
        """))
        conn.commit()

def cargar_a_dimensional(conn, engine, data_transform):
    data_transform['datetime'] = pd.to_datetime(data_transform['datetime'], format='%Y%m%d%H%M')  # Convertir formato
    dim_time = data_transform[['datetime', 'year', 'month', 'day', 'hour', 'minute', 'day_name', 'season']].drop_duplicates()
    dim_time.to_sql('dim_time', engine, if_exists='append', index=False)

    dim_weather = data_transform[['temperature', 'humidity']].drop_duplicates()
    dim_weather.to_sql('dim_weather', engine, if_exists='append', index=False)

    dim_power_source = data_transform[['wind_power_kwatts', 'solar_power_kwatts']].drop_duplicates()
    dim_power_source.to_sql('dim_power_source', engine, if_exists='append', index=False)

    # Obtener las claves primarias de cada dimensión
    with engine.connect() as conn:
        dim_time_df = pd.read_sql("SELECT * FROM dim_time", conn)
        dim_weather_df = pd.read_sql("SELECT * FROM dim_weather", conn)
        dim_power_source_df = pd.read_sql("SELECT * FROM dim_power_source", conn)

    # Hacer join para asignar las claves a cada fila del DataFrame original
    df_fact = data_transform.merge(dim_time_df, on=['datetime', 'year', 'month', 'day', 'hour', 'minute', 'day_name', 'season'])
    len(df_fact)
    df_fact = df_fact.merge(dim_weather_df, on=['temperature', 'humidity'])
    len(df_fact)
    df_fact = df_fact.merge(dim_power_source_df, on=['wind_power_kwatts', 'solar_power_kwatts'])
    len(df_fact)

    # Mantener solo columnas relevantes para la tabla de hechos
    df_fact = df_fact[['time_key', 'weather_key', 'power_source_key',
                    'power_consumption_zone1', 'power_consumption_zone2', 'power_consumption_zone3',
                    'power_consumption_total', 'energy_consumption_zone1', 'energy_consumption_zone2',
                    'energy_consumption_zone3', 'energy_consumption_total']]

    # Insertar en fact_energy_consumption
    df_fact.to_sql('fact_energy_consumption', engine, if_exists='append', index=False, dtype={"time_key": sqlalchemy.INTEGER})

def load(conn, engine,data_transform):
    print("Hola Tilín 2")
    crear_tablas_dimensional(conn, engine)
    # Subimos los datos a la base de datos usando una función de pandas
    cargar_a_dimensional(conn, engine, data_transform)
    print("Dataset cargado en bd modelo dimensional")
    with engine.connect() as conn:
        dim_time = pd.read_sql("SELECT * FROM dim_time", conn)
    with engine.connect() as conn:
        dim_weather = pd.read_sql("SELECT * FROM dim_weather", conn)
    with engine.connect() as conn:
        dim_power_source = pd.read_sql("SELECT * FROM dim_power_source", conn)
    with engine.connect() as conn:
        fact_energy_consumption = pd.read_sql("SELECT * FROM fact_energy_consumption", conn)
    return dim_time, dim_weather, dim_power_source, fact_energy_consumption

def etl_full():
    conn, engine, etl_total = extraer_a_staging()
    if etl_total:
        data_transform= transforms(conn, engine)
        dim_time, dim_weather, dim_power_source, fact_energy_consumption = load(conn, engine,data_transform)
        print(dim_time.head())
        print(dim_weather.head())
        print(dim_power_source.head())
        print(fact_energy_consumption.head())
    else:
        data_transform = pd.DataFrame()
        print("ETL No ejecutado porque no hay datos nuevos")

    return data_transform

def borrar_tablas():
    try:
        # Conectar a PostgreSQL
        config = load_config()
        db_config = config["database"]
        # Realizamos la conexión a la base de datos validando su existencia
            # Load credentials
        db_user = db_config["user"]
        db_password = db_config["password"]
        db_host = db_config["host"]
        db_port = db_config["port"]
        db_name = db_config["name"]
        # DB connection
        conn = psycopg2.connect(
            dbname="postgres",
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        conn.autocommit = True
        print("Conexión exitosa")
        cur = conn.cursor()

        if conn.closed == 0:  # Verifica si la conexión sigue abierta
            tablas = ["fact_energy_consumption", "dim_time", "dim_weather", "dim_power_source"]

            for tabla in tablas:
                cur.execute(f"DROP TABLE IF EXISTS {tabla} CASCADE;")
            
            conn.commit()
            print("✅ Tablas eliminadas correctamente.")

    except psycopg2.Error as e:
        print(f"❌ Error eliminando tablas: {e}")

    finally:
        if conn and not conn.closed:  # Solo cierra si la conexión sigue activa
            cur.close()
            conn.close()
            print("🔒 Conexión cerrada correctamente.")

#### Prueba de la función de manera local, descomentar si se quiere probar desde este mismo archivo
#file_input = extraer_a_staging()
#print("Dataset descargado directamente:")
#print(file_input)