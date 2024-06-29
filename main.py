import pandas as pd
import mysql.connector
import datetime
import sys
import requests
import json
import os

url_api_dolar = "https://economia.awesomeapi.com.br/last/USD-BRL"
api_dolar = requests.get(url_api_dolar)
dolar_data = api_dolar.json()

cotacao_dolar = float(dolar_data["USDBRL"]["ask"])
custo_api_call = 0.0054


api_url = "https://api.dataforseo.com/v3/business_data/google/my_business_info/live"

auth_key = os.environ.get("DATAFORSEO_API_KEY")

if auth_key is None:
    print("API_KEY environment variable is not set.")
else:
    print("Using API key:", auth_key)


headers = {"Authorization": f"Basic {auth_key}", "Content-Type": "application/json"}


def gen_stamp():
    current_time = datetime.datetime.now()
    date_str = f"{current_time.day:02}{current_time.month:02}{current_time.year%100}"
    time_str = f"{current_time.hour:02}{current_time.minute:02}{current_time.second:03}"
    return f"{date_str}{time_str}"


db_user = input("User DB: ")
db_passwd = input("Passwd DB: ")
db_host = input("Host: ")
db_name = input("DB: ")

stamp = gen_stamp()
print(f"Timestamp: {stamp}")

uf = input("Digite a Unidade Federativa desejada (obrigatório): ")
if not uf:
    print("O preenchimento da UF é obrigatório!")
    sys.exit()
municipio = input(
    "Digite o municipio desejado (deixe em branco se não quiser filtrar por município): "
)
cnae = input(
    "Digite o CNAE principal desejado (deixe em branco se quiser buscar por todos os CNAEs): "
)
sn_mei = input("Deseja excluir MEI? (S ou N): ")
sn_nome_fantasia = input("Deseja excluir empresas sem Nome Fantasia? (S ou N): ")
limite = input(
    "Digite quantas linhas deseja buscar (deixe em branco para não limitar a busca): "
)

cnpj = mysql.connector.connect(
    user=db_user, password=db_passwd, host=db_host, database=db_name
)

cursor = cnpj.cursor()

query = f"""
SELECT *
FROM empresas
INNER JOIN estabelecimentos ON estabelecimentos.cnpj_base = empresas.cnpj_base
INNER JOIN municipio ON municipio.codigo_municipio = estabelecimentos.municipio
INNER JOIN natureza_juridica ON natureza_juridica.codigo_natureza_juridica = empresas.natureza_juridica
INNER JOIN simples on simples.cnpj_base = empresas.cnpj_base
WHERE estabelecimentos.situacao_cadstral = 2
AND estabelecimentos.uf = '{uf}'
"""

if municipio:
    query += f"""
AND LOWER(municipio.municipio) = LOWER('{municipio}')
"""

if cnae:
    query += f"""
AND estabelecimentos.cnae_principal = '{cnae}'
    """
if sn_mei == "S":
    query += f"""
AND simples.opcao_mei <> 'S'
"""
if sn_nome_fantasia == "S":
    query += f"""
AND estabelecimentos.nome_fantasia <> ''
"""

if limite:
    query += f"""
LIMIT {limite};
"""

print(
    f"""
UF: {uf}
Município: {municipio}
CNAE: {cnae}
MEI: {sn_mei}
Apenas nome fantasia? {sn_nome_fantasia}
"""
)

confirma = input("Confirma a query? (S ou N): ")

if confirma == "N":
    print("Fechando programa.")
    sys.exit()
elif confirma == "S":
    print("Realizando query...")
else:
    print("Fechando programa.")
    sys.exit()

cursor.execute(query)
rows = cursor.fetchall()

print("Estruturando DataFrame...")
cnpj_df = pd.DataFrame(rows, columns=[column[0] for column in cursor.description])

norva_ordem_colunas = [
    "cnpj_base",
    "razao_social",
    "nome_fantasia",
    "identificador_matriz_filial",
    "situacao_cadstral",
    "motivo_situacao_cadastral",
    "natureza_juridica",
    "codigo_natureza_juridica",
    "qualificacao_responsavel",
    "capital_social",
    "porte",
    "ente_federativo",
    "tipo_logradouro",
    "logradouro",
    "numero",
    "complemento",
    "bairro",
    "cep",
    "nome_da_cidade_no_exterior",
    "pais",
    "codigo_municipio",
    "municipio",
    "uf",
    "data_inicio_atividade",
    "situuacao_especial",
    "data_situacao_especial",
    "ddd_1",
    "telefone_1",
    "ddd_2",
    "telefone_2",
    "ddd_fax",
    "fax",
    "email",
    "opcao_simples",
    "data_entrada_simples",
    "data_exclusao_simples",
    "opcao_mei",
    "data_entrada_mei",
    "data_exclusao_mei",
    "cnae_principal",
    "cnae_secundaria",
]

cnpj_df = cnpj_df[norva_ordem_colunas]
cnpj_df.columns = [
    f"{col}_{i}" if cnpj_df.columns.duplicated().any() else col
    for i, col in enumerate(cnpj_df.columns)
]
print("Removendo colunas duplicadas...")
columns_to_drop = [
    "cnpj_base_1",
    "cnpj_base_2",
    "situacao_cadstral_6",
    "motivo_situacao_cadastral_7",
    "natureza_juridica_8",
    "codigo_natureza_juridica_10",
    "qualificacao_responsavel_11",
    "porte_13",
    "ente_federativo_14",
    "nome_da_cidade_no_exterior_21",
    "pais_22",
    "situuacao_especial_28",
    "data_situacao_especial_29",
    "data_entrada_simples_38",
    "data_exclusao_simples_39",
    "data_entrada_mei_41",
    "data_exclusao_mei_42",
    "codigo_municipio_23",
    "municipio_24",
]
cnpj_df = cnpj_df.drop(columns=columns_to_drop)

contagem_linhas = cnpj_df.shape[0]
custo_busca_USD = round(contagem_linhas * custo_api_call, 2)
custo_busca_BRL = round(custo_busca_USD * cotacao_dolar, 2)
print(f"Estabelecimentos encontrados: {contagem_linhas}")
print(f"Custo estimado: USD {custo_busca_USD} | BRL {custo_busca_BRL}")
valida_info = input("Deseja buscar informações? (S ou N): ")
if valida_info == "S":
    tem_gmb = []
    numeros = []
    sites = []
    categorias = []
    descricoes = []
    titulos = []
    enderecos = []
    for index, row in cnpj_df.iterrows():
        keyword = row["nome_fantasia_4"]
        print("buscando para: " + keyword)
        payload_dict = {
            "keyword": keyword,
            "location_code": 1031967,
            "language_code": "pt-BR",
        }
        response = requests.post(api_url, headers=headers, json=[payload_dict])

        if response.status_code == 200:
            data = response.json()
            task_status_code = data["tasks"][0]["status_code"]
            if task_status_code == 40102:
                tem_gmb.append("N")
                print("Sem resultado de busca")
                numeros.append("Sem resultado de busca")
                sites.append("Sem resultado de busca")
                categorias.append("Sem resultado de busca")
                descricoes.append("Sem resultado de busca")
                titulos.append("Sem resultados de busca")
                enderecos.append("Sem resultados de busca")
            if task_status_code == 20000:
                numero = data["tasks"][0]["result"][0]["items"][0]["phone"]
                site = data["tasks"][0]["result"][0]["items"][0]["url"]
                categoria = data["tasks"][0]["result"][0]["items"][0]["category"]
                descricao = data["tasks"][0]["result"][0]["items"][0]["description"]
                titulo = data["tasks"][0]["result"][0]["items"][0]["title"]
                endereco = data["tasks"][0]["result"][0]["items"][0]["address"]
                print(f"Titulo encontrado: {titulo}")
                print(f"Categoria encontrada: {categoria}")
                print(f"Descrição encontrada: {descricao}")
                print(f"Endereço encontrado: {endereco}")
                print(f"Numero encontrado: {numero}")
                print(f"Site encontrado: {site}")
                tem_gmb.append("S")
                numeros.append(numero)
                sites.append(site)
                categorias.append(categoria)
                descricoes.append(descricao)
                titulos.append(titulo)
                enderecos.append(endereco)
        else:
            print(f"Error: HTTP status code {response.status_code}")
            tem_gmb.append("not found")
            numeros.append("not found")
            sites.append("not found")
            categorias.append("not found")
            descricoes.append("not found")
            titulos.append("not found")
            enderecos.append("not found")

    cnpj_df["tem_gmb"] = tem_gmb
    cnpj_df["titulos_gmb"] = titulos
    cnpj_df["categoria_gmb"] = categorias
    cnpj_df["descricao_gmb"] = descricoes
    cnpj_df["endereco_gmb"] = enderecos
    cnpj_df["numero_valido_gmb"] = numeros
    cnpj_df["site_gmb"] = sites

print("Salvando planila em XLSX...")

if not municipio:
    municipio = "todos"

if not cnae:
    cnae = "todos"

if not limite:
    limite = "completo"

cnpj_df.to_excel(f"output/{uf}_{municipio}_{cnae}_{limite}_{stamp}.xlsx", index=True)

cnpj.close()
