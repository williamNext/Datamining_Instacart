# o programa minera produtos comprados das 18 as 22 horas da sexta-feira
# dos departamentos de alcool, bebidas em geral, hortifruti, snacks e congelados.
#
# Os dados das tabelas csv foram colocados no banco e filtrados por query,
# para fins de melhor desempenho o resultado das querys foi armazenado em um arquivo utilizando a biblioteca numpy,
# para não haver necessidade de consulta do banco toda vez que rodar o programa.
#
# foi adotado com regra o threshold minimo de 10 casos para a definição dos padrões.
# A confiança mínima das regras de associação é de 80%.

import sqlite3
import numpy as np

import pyfpgrowth

#CRIA A CONEXÃO COM O BANCO DE DADOS
def create_connection(db_file):

    conn = None
    try:
        conn = sqlite3.connect(db_file)

    except sqlite3.Error as e:
        print(e)

    return conn


# FAZ A QUERY DE SELEÇÃO DOS PRODUTOS DENTRO DOS PADRES ESTABELECIDOS NO OBJETIVO
def select_all_tasks(conn):

    cur = conn.cursor()
    cur.execute("select order_products.order_id, order_products.product_id from order_products "
                "join orders on order_products.order_id = orders.order_id "
                "join products on order_products.product_id = products.product_id "
                "join departments on products.department_id = departments.department_id "
                "where ( departments.department_id = 1 "
                "or departments.department_id = 5 "
                "or departments.department_id = 7 "
                "or departments.department_id = 19 "
                "or departments.department_id = 4 )"
                "and orders.order_hour_of_day >= 18 "
                "and orders.order_hour_of_day <= 22 "
                "and orders.order_dow = 6;")

    rows = cur.fetchall()
    return rows;


# TRATA OS DADOS PARA A ETRADA NO ALGORITMO DO FP-GROWTH
def tratar_produtos(rows):
    dataset = {}

    for product in rows:
        try:
            dataset[str(product[0])].append(str(product[1]))
        except:
            dataset[str(product[0])] = []
            dataset[str(product[0])].append(str(product[1]))


    dataset_tratado = []
    for key, transaction in dataset.items():
        dataset_tratado.append(transaction)

    return dataset_tratado


# FUNÇÃO QUE TRANSFORMA A LISTA DE IDS DE PRODUTOS NOS SEUS RESPECTIVOS NOMES ARA MELHOR VISUALIZAÇÃO
def select_nome_prod(con, product_id):
    select = "select product_name from products where product_id = ?"

    cur = con.cursor()
    cur.execute(select, (product_id,))
    return cur.fetchone();

if __name__ == '__main__':
    con = create_connection('dataset')

    #PARTE COMENTADA RODOU SOMENTE NA PRIMEIRA EXECUÇÃO,
    # NÃO HA NECESSIDADE DE SER EXECUTADA NOVAMENTE POIS ESTÁ SALVA NO ARQUIVO

    # rows = select_all_tasks(con)
    # dataset = tratar_produtos(rows)
    # np.save('dataset_tratado', dataset, allow_pickle=True)

    #    FAZ O LOAD DOS DADOS SALVOS NO ARQUIVO
    dataset = np.load('dataset_tratado.npy', allow_pickle=True)




    # ENCONTRA PADRÕES QUE OCORRERAM NO MINIMO 10 VEZES
    patterns = pyfpgrowth.find_frequent_patterns(dataset, 10)

    #GERA AS REGRAS COM MAIS DE 80% DE CONFIANÇA
    rules = pyfpgrowth.generate_association_rules(patterns, 0.8)


    # EXIBE ASSOCIAÇÕES COM ID PRODUTO - SEM BANCO // RESULTADO DO SELECT INICIAL CARREGADO PELO NUMPY
    for rule in rules:
        print(rule)


    # EXIBE ASSOCIAÇÕES COM NOME DO PRODUTO - PRECISA DO BANCO (1.5 GB)
    # LINK: https://drive.google.com/file/d/1U8bQ-2QxinHPy9pkbTqkGKraoIPFlKph/view?usp=sharing
    # array_rules_names = []
    # for rule in rules:
    #     temp = []
    #
    #     for product_id in rule:
    #         temp.append(select_nome_prod(con, product_id))
    #
    #     array_rules_names.append(temp)
    #
    # for rule in array_rules_names:
    #     print(rule)

