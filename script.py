#Importamos las librerias necesarias para que corra el codigo

import yfinance as yf
import requests
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd
from getpass import getpass

# CREANDO LA CONEXION

password = getpass("Introduce tu contraseña: ")
user = "juanchi_martinezc_coderhouse"
host = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
port = "5439"
database_name = "data-engineer-database"

connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database_name}"
conn = create_engine(connection_string)

# Insertamos la informacion de los Tickers a Redshift

tickers = ['AAPL','MSFT','AMZN','NVDA','GOOGL','META','TSLA','UNH','JPM']

# Diccionario para almacenar los datos
ticker_data = {}

for ticker in tickers:
    # Descargar datos de Yahoo Finance
    data = yf.download(ticker, period="1y")
    data.reset_index(inplace=True)
    data.drop(columns=['Close'], inplace=True)

    # Almacena los datos en el diccionario usando el ticker como clave
    ticker_data[ticker] = data
    
# Ahora puedes acceder a los datos individualmente usando los tickers como claves
ticker_data['AAPL'].head()  # Para acceder a los datos de AAPL

# Iterar a través de los datos de los tickers y cargarlos en Redshift
for ticker, data in ticker_data.items():
    table_name = f'hist_{ticker}'
    data.to_sql(table_name, conn, index=False, if_exists='append')
