import yfinance as yf
import pandas as pd
import time
from ta.trend import SMAIndicator
import time
from threading import Thread
from datetime import datetime
import sqlite3

#Função responsável por conectar no database.
def connect_db():
    con = sqlite3.connect('path')
    cur = con.cursor()
    return con, cur
#Download de preços dos ativos via API Yahoo finance.
def dados():
    dic_dados = {}
    data = ['RRRP3.SA','ALSO3.SA','ALPA4.SA','ABEV3.SA','ARZZ3.SA','ASAI3.SA','AZUL4.SA','B3SA3.SA','BPAN4.SA','BBSE3.SA'
    ,'BBDC3.SA','BBDC4.SA','BRAP4.SA','BBAS3.SA','BRKM5.SA','BRFS3.SA','BPAC11.SA','CRFB3.SA','CCRO3.SA','CMIG4.SA','CIEL3.SA'
    ,'COGN3.SA','CPLE6.SA','CSAN3.SA','CPFE3.SA','CMIN3.SA','CVCB3.SA','CYRE3.SA','DXCO3.SA','ECOR3.SA','ELET3.SA','ELET6.SA'
    ,'EMBR3.SA','ENBR3.SA','ENGI11.SA','ENEV3.SA','EGIE3.SA','EQTL3.SA','EZTC3.SA','FLRY3.SA','GGBR4.SA','GOAU4.SA','GOLL4.SA'
    ,'NTCO3.SA','SOMA3.SA','HAPV3.SA','HYPE3.SA','IGTI11.SA','ITSA4.SA','ITUB4.SA','JBSS3.SA','KLBN11.SA','RENT3.SA','LWSA3.SA'
    ,'LREN3.SA','MGLU3.SA','MRFG3.SA','CASH3.SA','BEEF3.SA','MRVE3.SA','MULT3.SA','PCAR3.SA','PETR3.SA','PETR4.SA','PRIO3.SA'
    ,'PETZ3.SA','QUAL3.SA','RADL3.SA','RAIZ4.SA','RDOR3.SA','RAIL3.SA','SBSP3.SA','SANB11.SA','SMTO3.SA','CSNA3.SA','SLCE3.SA'
    ,'SUZB3.SA','TAEE11.SA','VIVT3.SA','TIMS3.SA','TOTS3.SA','UGPA3.SA','USIM5.SA','VALE3.SA','VIIA3.SA','VBBR3.SA','WEGE3.SA'
    ,'YDUQ3.SA']
    
    for row in data:
        x = yf.Ticker(row)
        his = x.history(period="2y")
        his = his.drop(columns=['Dividends', 'Stock Splits'])
        his.reset_index(drop=False)
        dic_dados[row]= his
    return dic_dados
    
#Função responsável por persistir os preços do ativo na tabela.
def persistir_sql(values):
    con, cur = connect_db()
    data = datetime.today().strftime('%Y-%m-%d')
    cur.execute('INSERT INTO LongShort VALUES (?, ?);', (str(values).replace('[','').replace(']',''),str(data)))
    con.commit()
    con.close()

#Função responsável por aplicar filtro para identificar os ativos que serão long e short.
def filtros(dic_dados):
    fLongs = []
    fShort = []
    rPares = {}

    sma_ratios_filtro = []

    #Gerando as medias.
    for row in dic_dados:
        sma_9 = SMAIndicator(dic_dados[row]['Close'], window=9)
        sma_21 = SMAIndicator(dic_dados[row]['Close'], window=21)

        dic_dados[row]['sma_9'] = sma_9.sma_indicator()
        dic_dados[row]['sma_21'] = sma_21.sma_indicator()

        if dic_dados[row]['sma_9'].iloc[-1] > dic_dados[row]['sma_21'].iloc[-1]:
            fLongs.append(row)
        if dic_dados[row]['sma_21'].iloc[-1] > dic_dados[row]['sma_9'].iloc[-1]:
            fShort.append(row)

    print('Ativos que passaram no filtro de Long: ', fLongs, '\n')
    print('Ativos que passaram no filtro de Short: ', fShort)

    y = 0
    #For para filtro dos melhores pares combinatória.
    for row in fLongs:
        for x in fShort:
            rPares[row+'/'+x] = dic_dados[row]['Close']/ dic_dados[x]['Close']
            rPares[row+'/'+x] = rPares[row+'/'+x].dropna()
            
            sma_9 = SMAIndicator(rPares[row+'/'+x].astype(float), window=9)
            sma_21 = SMAIndicator(rPares[row+'/'+x].astype(float), window=21)

            rPares[row+'/'+x]['sma_9'] = sma_9.sma_indicator()
            rPares[row+'/'+x]['sma_21'] = sma_21.sma_indicator()
            
            sma_ratios_filtro.append(row+'/'+x)
            persistir_sql(row+'/'+x)
            y = y + 1
            print(y)
    print('Pares que passaram no filtro: ', sma_ratios_filtro)

#Main
inicio = time.time()
dados_raw = dados()
thread = Thread(target=filtros(dados_raw))
thread.start()
final = time.time()
print(final - inicio, ' segundos')
