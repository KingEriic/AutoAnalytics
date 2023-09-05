import sqlite3

con = sqlite3.connect('path')
cur = con.cursor()
cur.execute("DROP TABLE IF EXISTS Rank")
cur.execute("DROP TABLE IF EXISTS LongShort")

sql ='''CREATE TABLE Rank(
   Par,
   Date,
   Qtd_Long,
   Qtd_Short
)'''
cur.execute(sql)
con.commit()
con.close()

sql ='''CREATE TABLE LongShort(
   pares,
   date
)'''
cur.execute(sql)
con.commit()
con.close()

sql ='''CREATE TABLE Monitoramento(
   Carteira,
   Date,
   Lucro
)'''
cur.execute(sql)
con.commit()
con.close()


#drop
# cur.execute("delete from Monitoramento ")
# con.commit()
# con.close()