#Automação para montitoramento e comparação de carteiras de investimentos.

import yfinance as yf
from pandas_datareader import data as pdr
import requests
import sqlite3
from datetime import datetime
import time

#Carteira realizada manualmente.
LCarteira = {'TOTS3.SA/SANB11.SA': '2023-01-30', 
            'ITUB4.SA/JBSS3.SA': '2023-01-30',
            'HYPE3.SA/BBDC3.SA': '2023-01-30', 
            'GGBR4.SA/BRFS3.SA': '2023-01-30',
            'SLCE3.SA/BPAN4.SA': '2023-01-30'}

#Conexão com o database.
def connect_db():
    con = sqlite3.connect('path')
    cur = con.cursor()
    return con, cur
            
#Função responsável por inserir os lucros do dia de cada carteira.
def insert_lucro_monitora(carteira, lucro):
    con, cur = connect_db()
    data = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    cur.execute('INSERT INTO Monitoramento VALUES (?, ?, ?);', (carteira, data, lucro))
    con.commit()
    con.close()

#Função responsavel por ranquear a melhor carteira do dia.
def select_rank():
    con, cur = connect_db()
    data = '2023-01-30' #datetime.today().strftime('%Y-%m-%d')
    dataframe = cur.execute("SELECT par, date from rank WHERE DATE = '%s'" % data)
    return dataframe, con

#Função responsavel por identificar carteira de terceiros ou carteira gerada pelo rank.
def lucro_carteira(tipo):
    if (tipo == 'Terceiro'):
        carteira = LCarteira
    elif (tipo == 'Auto'):
        carteira, con = select_rank()
        
        
    lucro = 0
    for row in carteira:
        if(tipo == 'Terceiro'):
            long, short = row.split('/')
            x = yf.Ticker(long)
            hist_long_atual = x.history(period="1y")
            y = yf.Ticker(short)
            hist_short_atual = x.history(period="1y")
            lucro = lucro + (((hist_long_atual.loc[carteira[row]]['Close']/hist_long_atual.iloc[-1]['Close'])-1)*100)
            lucro = lucro + int(((((hist_short_atual.loc[carteira[row]]['Close']/hist_short_atual.iloc[-1]['Close'])-1)*100)*-1))
        elif (tipo == 'Auto'):
            long, short = row[0].split('/')
            x = yf.Ticker(long)
            hist_long_atual = x.history(period="1y")
            y = yf.Ticker(short)
            hist_short_atual = x.history(period="1y")
            lucro = lucro + (((hist_long_atual.loc[row[1]]['Close']/hist_long_atual.iloc[-1]['Close'])-1)*100)
            lucro = lucro + int(((((hist_short_atual.loc[row[1]]['Close']/hist_short_atual.iloc[-1]['Close'])-1)*100)*-1))
    if (tipo == 'Auto'):
        con.close()
    return lucro

def send_telegram(terceiroCarteira, autoCarteira):

    insert_lucro_monitora('Terceiro', terceiroCarteira)
    insert_lucro_monitora('Auto', autoCarteira)
    
    print('Persistido na tabela com sucesso!')
    #Função responsável por informar o lucro das carteiras via telegram.        
    TOKEN = "{TOKEN}"
    chat_id = "{CHAT}"
    message = 'Lucro da Carteira Automatizada: '+ str(autoCarteira)+ '%'
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
    print(requests.get(url).json()) # this sends the message

    message = 'Lucro da Carteira de Terceiros: '+str(terceiroCarteira)+ '%'
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
    print(requests.get(url).json()) # this sends the message

send_telegram(round(lucro_carteira('Terceiro'), 2), round(lucro_carteira('Auto'), 2))
