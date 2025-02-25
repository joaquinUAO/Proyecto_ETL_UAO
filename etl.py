import yaml
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, text
import pandas as pd
import kaggle

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
    db_name = "proyect1_db"
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
            CREATE TABLE IF NOT EXISTS tabla_etl (
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
    file_csv.to_sql("proyect1_db", engine, if_exists="replace", index=False)
    print("Datos cargados en la base de datos exitosamente.")
    # Leer datos de la tabla staging para validar
    with engine.connect() as conn:
        data_staging = pd.read_sql("SELECT * FROM proyect1_db", conn)
    return data_staging

def extraer_a_staging():
    file_csv = pd.read_csv("powerconsumption.csv", sep=",")

    config = load_config()
    db_config = config["database"]
    conn, engine, existe = conectar_bd(db_config)        
    
    if existe == 0:
        # Creamos la tabla con las columnas correspondientes
        crear_tabla(conn, engine)        

    # Como el nombre de las columnas en el dataframe difiere del nombre de las columnas en db, renombramos las columnas del dataframe para facilitar el cargue de datos desde python
    file_csv_rename = renombrar_dataset(file_csv)

    # Subimos los datos a la base de datos usando una función de pandas
    data_staging = cargar_a_staging(conn, engine, file_csv_rename)
    print("Dataset almacenado en área de staging:")
    print(data_staging)

    return file_csv

#file_input = extraer_a_staging()
#print("Dataset descargado directamente:")
#print(file_input)