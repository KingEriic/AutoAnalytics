#Script de ranqueamento do melhor par para long e short.

import sqlite3
from datetime import datetime

#Conexão com o database
def connect_db():
    con = sqlite3.connect('path')
    cur = con.cursor()
    return con, cur
#Função responsável por selecionar os pares.
def select_longshort_sql():
    con, cur = connect_db()
    data = '2023-01-19'
    dataframe = cur.execute("SELECT pares from LongShort WHERE DATE = '%s'" % data)
    con.commit()
    return dataframe, con
    
#Função responsável por persistir os pares ranqueados
def persistir_rank_sql(long, short):
    con, cur = connect_db()
    data = datetime.today().strftime('%Y-%m-%d')
    for x in range(1, 6):
        ativo_long = long[x][1]
        qtd_long = long[x][0]
        ativo_short = short[x][1]
        qtd_short = short[x][0]
        par = (ativo_long + '/' + ativo_short)
        cur.execute('INSERT INTO Rank VALUES (?, ?, ?, ?);', (par ,str(data), qtd_long, qtd_short))
        con.commit()
    con.close()

def main():
    #Select na tabela LongShort
    dataframe, con = select_longshort_sql()

    list_long = []
    list_short = []

    for row in dataframe: 
        long, short = str(row).split('/')
        list_long.append(long.replace('(', '').replace("'", ''))
        list_short.append(short.replace(',)', '').replace("'", ''))

    list_verif_long = []
    list_verif_short = []

    list_count_long = {}
    list_count_short = {}

    for row in list_long:
        if row in list_verif_long:
            pass
        else:
            freq = list_long.count(row)
            list_verif_long.append(row)
            list_count_long[row] = freq

    for row in list_short:
        if row in list_verif_short:
            pass
        else:
            freq = list_short.count(row)
            list_verif_short.append(row)
            list_count_short[row] = freq
    con.close()

    return list_count_long, list_count_short

list_count_long, list_count_short = main()

value_key_pairs = ((value, key) for (key,value) in list_count_long.items())
sorted_value_key_pairs_long = sorted(value_key_pairs, reverse=True)

value_key_pairs_short = ((value, key) for (key,value) in list_count_short.items())
sorted_value_key_pairs_short = sorted(value_key_pairs_short, reverse=True)

persistir_rank_sql(sorted_value_key_pairs_long, sorted_value_key_pairs_short)
print('Pares inseridos com sucesso!')

