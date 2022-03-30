"""
Desafio 2
Escreva uma classe utilizando a linguagem python que faça a conexão com o banco de dados Postgres,
utilizando a biblioteca Pandas:
a) Criar uma tabela;
b) Inserir uma linha contendo uma coluna indexada, uma coluna texto, uma coluna numérica,
uma coluna boolena e uma coluna datetime;
c) Faça o versionamento no github ou gitlab;
d) Crie uma branch no repositório chamado texte;
e) Nesta branch no repositório.
"""
import pandas as pd
import psycopg2


class ConectaTabelaDB:  # classe para conectar com Postgres e manipular tabelas

    # Conecta com o banco de dados:
    def __init__(self):
        self.con = psycopg2.connect(host='localhost',
                                    database='desafiogt2',
                                    user='postgres',
                                    password='1234')
        self.cur = self.con.cursor()

    # Método para criar tabelas:
    def criatabela(self, tab):
        sql = f'CREATE TABLE {tab} (' \
              f'id serial NOT NULL,' \
              f'texto varchar(50),' \
              f'numero int,' \
              f'opcao bit,' \
              f'data date,' \
              f'PRIMARY KEY (id));'
        self.cur.execute(sql)
        self.con.commit()
        self.con.close()

    # Método para apagar tabelas:
    def apagatabela(self, tab):
        sql = f'DROP TABLE IF EXISTS {tab};'
        self.cur.execute(sql)
        self.con.commit()
        self.con.close()

    # Método para inserir dados:
    def inseredados(self, tab, text, num, op, dat):
        sql = f"INSERT INTO {tab} (texto, numero, opcao, data) VALUES " \
              f"('{text}', '{num}', '{op}', '{dat}');"
        self.cur.execute(sql)
        self.con.commit()
        self.con.close()

    # Método para ler dados da tabela com Pandas:
    def le_tabela(self, a):
        self.cur = self.con.cursor()
        self.cur.execute(f'SELECT * FROM {a};')
        recset = self.cur.fetchall()
        registros = []
        for rec in recset:
            registros.append(rec)
        self.con.close()
        df_bd = pd.DataFrame(registros, columns=['id', 'texto', 'numero', 'opcao', 'data'])
        print(df_bd)


tabela = 'desafiogt2'

texto = 'Palhoça-SC'
numero = '999333777'
opcao = '1'
data = '2011-07-09'

db = ConectaTabelaDB()
# db.criatabela(tabela)
# db.apagatabela(tabela)
# db.inseredados(tabela, texto, numero, opcao, data)
# db.le_tabela(tabela)
