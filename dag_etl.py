from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yaml
import psycopg2
from psycopg2 import sql
import sqlalchemy
from sqlalchemy import create_engine, text
import pandas as pd
import kaggle
import numpy as np
import logging
import json


# Cargar configuración desde YAML
def load_config(file_path="config.yaml"):
    with open(file_path, "r") as file:
        return yaml.safe_load(file)

# Conexión a la base de datos
def conectar_bd(db_config):
    # Load credentials
    db_user = db_config["user"]
    db_password = db_config["password"]
    db_host = db_config["host"]
    db_port = db_config["port"]
    db_name = db_config["name"]
    # DB connection
    logging.info("Conectando a base de datos") 
    conn = psycopg2.connect(
        dbname="postgres",
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    conn.autocommit = True
    logging.info("Conexión a base de datos exitosa")   

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

def cargar_a_staging(engine, file_csv):
    # Convertir DataFrame a lista de tuplas
    records_list = [
        tuple(map(lambda x: x.item() if isinstance(x, np.generic) else x, row)) 
        for row in file_csv.to_records(index=False)
    ]

    # Obtener conexión a PostgreSQL
    conn = engine.raw_connection()
    cur = conn.cursor()

    # Definir la consulta de inserción
    insert_query = """
    INSERT INTO project1_db (datetime, temperature, humidity, wind_speed, 
                                general_diffuse_flows, diffuse_flows, 
                                power_consumption_zone1, power_consumption_zone2, 
                                power_consumption_zone3)
    VALUES %s
    """
    
    # Ejecutar inserción en lotes
    psycopg2.extras.execute_values(cur, insert_query, records_list)
    conn.commit()

    # Cerrar conexión
    cur.close()
    conn.close()

def cargar_a_wather(engine, file_csv):
    # Convertir DataFrame a lista de tuplas
    records_list = [
        tuple(map(lambda x: x.item() if isinstance(x, np.generic) else x, row)) 
        for row in file_csv.to_records(index=False)
    ]

    # Obtener conexión a PostgreSQL
    conn = engine.raw_connection()
    cur = conn.cursor()

    # Definir la consulta de inserción
    insert_query = """
    INSERT INTO dim_weather (temperature, humidity)
    VALUES %s
    """
    
    # Ejecutar inserción en lotes
    psycopg2.extras.execute_values(cur, insert_query, records_list)
    conn.commit()

    # Cerrar conexión
    cur.close()
    conn.close()

def cargar_a_time(engine, file_csv):
    # Convertir DataFrame a lista de tuplas
    records_list = [
        tuple(map(lambda x: x.item() if isinstance(x, np.generic) else x, row)) 
        for row in file_csv.to_records(index=False)
    ]

    # Obtener conexión a PostgreSQL
    conn = engine.raw_connection()
    cur = conn.cursor()

    # Definir la consulta de inserción
    insert_query = """
    INSERT INTO dim_time (datetime, year, month, day, hour, minute, day_name, season)
    VALUES %s
    """
    
    # Ejecutar inserción en lotes
    psycopg2.extras.execute_values(cur, insert_query, records_list)
    conn.commit()

    # Cerrar conexión
    cur.close()
    conn.close()

def cargar_a_power_source(engine, file_csv):
    # Convertir DataFrame a lista de tuplas
    records_list = [
        tuple(map(lambda x: x.item() if isinstance(x, np.generic) else x, row)) 
        for row in file_csv.to_records(index=False)
    ]

    # Obtener conexión a PostgreSQL
    conn = engine.raw_connection()
    cur = conn.cursor()

    # Definir la consulta de inserción
    insert_query = """
    INSERT INTO dim_power_source (wind_power_kwatts, solar_power_kwatts)
    VALUES %s
    """
    
    # Ejecutar inserción en lotes
    psycopg2.extras.execute_values(cur, insert_query, records_list)
    conn.commit()

    # Cerrar conexión
    cur.close()
    conn.close()

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

# Extraer datos desde Kaggle y cargar a staging
def extraer_a_staging():
    kaggle.api.dataset_download_files("fedesoriano/electric-power-consumption", unzip=True)
    file_csv = pd.read_csv("powerconsumption.csv", sep=",")

    config = load_config()
    db_config = config["database"]
    logging.info("Cargamos datos del yaml")   
    # Realizamos la conexión a la base de datos validando su existencia
    conn, engine, existe = conectar_bd(db_config)
    logging.info("Conectado a la base de datos para staging")       
    crear_tabla(conn, engine)
    logging.info("Tabla staging creada")         

    # Como el nombre de las columnas en el dataframe difiere del nombre de las columnas en db, renombramos las columnas del dataframe para facilitar el cargue de datos desde python
    file_csv_rename = renombrar_dataset(file_csv)

    # Subimos los datos a la base de datos usando una función de pandas
    logging.info("Inicio de carga a staging...") 
    conn, engine, existe = conectar_bd(db_config)
    cargar_a_staging(engine, file_csv_rename)
    logging.info("Dataset almacenado en área de staging existosamente")

# Transformaciones
def transforms():
    config = load_config()
    db_config = config["database"]
    # Realizamos la conexión a la base de datos validando su existencia
    conn, engine, existe = conectar_bd(db_config) 
    logging.info("Cargando datos desde el area de staging para transformaciones...")
    
    with engine.connect() as conn:
        result = conn.execute("SELECT * FROM project1_db")
        rows = result.fetchall()  # Obtener todos los registros
        columns = result.keys()   # Obtener los nombres de las columnas

    # Convertir a DataFrame
    data_transform = pd.DataFrame(rows, columns=columns)

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

    data_transform.to_csv("dataset_transformado.csv", index=False)

    print("Transformaciones realizadas exitosamente!")

# Cargar datos al modelo dimensional
def load():
    config = load_config()
    db_config = config["database"]
    # Realizamos la conexión a la base de datos validando su existencia
    conn, engine, existe = conectar_bd(db_config)
    logging.info("Creando tablas dimensionales y de hechos")
    # Crear tablas
    crear_tablas_dimensional(conn, engine)
    logging.info("Tablas creadas exitosamente!")

    data_transform = pd.read_csv("dataset_transformado.csv")  # Convertir JSON a DataFrame

    data_transform['datetime'] = pd.to_datetime(data_transform['datetime'], format='%Y%m%d%H%M')  # Convertir formato
    dim_time = data_transform[['datetime', 'year', 'month', 'day', 'hour', 'minute', 'day_name', 'season']].drop_duplicates()
    cargar_a_time(engine, dim_time)
    logging.info("Almacenada dimensión tiempo")

    dim_weather = data_transform[['temperature', 'humidity']].drop_duplicates()
    cargar_a_wather(engine, dim_weather)
    logging.info("Almacenada dimensión ambientales")

    dim_power_source = data_transform[['wind_power_kwatts', 'solar_power_kwatts']].drop_duplicates()
    cargar_a_power_source(engine, dim_power_source)
    logging.info("Almacenada dimensión fuentes de energía")

    # Obtener claves primarias
    # Obtener las claves primarias de cada dimensión
    with engine.connect() as conn:
        result = conn.execute("SELECT * FROM dim_time")
        rows = result.fetchall()  # Obtener todos los registros
        columns = result.keys()   # Obtener los nombres de las columnas
        dim_time_df = pd.DataFrame(rows, columns=columns)
        result = conn.execute("SELECT * FROM dim_weather")
        rows = result.fetchall()  # Obtener todos los registros
        columns = result.keys()   # Obtener los nombres de las columnas
        dim_weather_df = pd.DataFrame(rows, columns=columns)
        result = conn.execute("SELECT * FROM dim_power_source")
        rows = result.fetchall()  # Obtener todos los registros
        columns = result.keys()   # Obtener los nombres de las columnas
        dim_power_source_df = pd.DataFrame(rows, columns=columns)

    logging.info("Extrayendo keys")

    # Hacer join para asignar las claves a cada fila del DataFrame original
    df_fact = data_transform.merge(dim_time_df, on=['datetime', 'year', 'month', 'day', 'hour', 'minute', 'day_name', 'season'])
    df_fact = df_fact.merge(dim_weather_df, on=['temperature', 'humidity'])
    df_fact = df_fact.merge(dim_power_source_df, on=['wind_power_kwatts', 'solar_power_kwatts'])
    logging.info("Extrayendo keys")

    # Mantener solo columnas relevantes para la tabla de hechos
    df_fact = df_fact[['time_key', 'weather_key', 'power_source_key',
                    'power_consumption_zone1', 'power_consumption_zone2', 'power_consumption_zone3',
                    'power_consumption_total', 'energy_consumption_zone1', 'energy_consumption_zone2',
                    'energy_consumption_zone3', 'energy_consumption_total']]

    # Insertar en fact_energy_consumption
    df_fact.to_sql('fact_energy_consumption', engine, if_exists='append', index=False, dtype={"time_key": sqlalchemy.INTEGER})

# Definir DAG en Airflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 24),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_energy_consumption',
    default_args=default_args,
    description='ETL de consumo energético desde Kaggle a PostgreSQL',
    schedule_interval=timedelta(days=1),
)

task_extract = PythonOperator(task_id='extract', python_callable=extraer_a_staging, dag=dag)
task_transform = PythonOperator(task_id='transform', python_callable=transforms, dag=dag)
task_load = PythonOperator(task_id='load', python_callable=load, dag=dag)

task_extract >> task_transform >> task_load