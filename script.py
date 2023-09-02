#Importamos las librerias necesarias para que corra el codigo

import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine
import os
import dotenv

# cargamos las variables de entorno
dotenv.load_dotenv()  

# leemos las varaibles de entorno
user = os.getenv('user')
password = os.getenv('password')
host = os.getenv('host')
port = os.getenv('port')
database_name = os.getenv('database_name')

connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database_name}"
conn = create_engine(connection_string)

# Insertamos la informacion de los Tickers a Redshift

tickers = ['AAPL','MSFT','AMZN','NVDA','GOOGL','META','TSLA','UNH','JPM','CSCO']

# Diccionario para almacenar los datos
ticker_data = {}

for ticker in tickers:
    # Descargar datos de Yahoo Finance
    data = yf.download(ticker, period="1y")
    data.reset_index(inplace=True)
    data.drop(columns=['Close'], inplace=True)
    data['Ticker'] = ticker

    # Almacena los datos en el diccionario usando el ticker como clave
    ticker_data[ticker] = data
    
# Concatenar los datos en una sola tabla
all_data = pd.concat(list(ticker_data.values()), ignore_index=True)

# Imprimir los primeros registros para verificar
print(all_data)

# Iterar a través de los datos de los tickers y cargarlos en Redshift
all_data.to_sql('TICKERS', conn, index=False, if_exists='append')
