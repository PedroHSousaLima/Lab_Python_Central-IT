import requests
import json
import urllib3
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
import os

data_e_hora_atuais = datetime.now()
mes = data_e_hora_atuais.strftime('%m')


urllib3.disable_warnings()

mes = "Fevereiro"
ano = "2021"

caminho = "C:\\Dados_Citsmart\\Consulta\\Encerrado\\Estruturantes\\\P2\\"

#datafim = str(datetime.now() - timedelta(days= 7))[0:10] + " 00:00:00"
#datafim1 = str(datetime.now() - timedelta(days= 0))[0:10] + " 23:59:59"


datafim = "2021-02-16 00:00:00"
datafim1 = "2021-02-28 23:59:59"

path = f'C:\\Dados_Citsmart\\Consulta\\Encerrado\\Estruturantes\\\P2\\'
#path2 = f'C:\\Dados_Citsmart\\Consolidado\\Encerrado\\2020\\Novembro\\'
path2 = f'C:\\Users\\Administrator\\OneDrive - CENTRAL IT TECNOLOGIA DA INFORMAÇÃO LTDA\\Repositorio_Date\\DADOS\\CISTMART\\CSV\\CONSOLIDADO\\{ano}\\{mes}\\'


Inicio = datetime.now()


def autenticar(link,login,senha):
    try:
        r = requests.post(f'{link}',
                      verify=False,
        data=json.dumps({'userName':f'{login}','password':f'{senha}',
                     'platform':'Telefonia', 'platform':'Telefonia'}),
        headers={'Accept': 'application/json', 'Content-Type': 'application/json'})
        response = json.loads(r.text)
        sessionID = response['sessionID']
        return sessionID
    except:
        pass


def convert_data(data):
    try:
        datetime.utcfromtimestamp(float(str(data)[:10])) - timedelta(hours=3)
    except:
        return data
    else:
        return datetime.utcfromtimestamp(float(str(data)[:10])) - timedelta(hours=3)



def obter_dados(nomeTabela,queryName,session,link,parametros='',datas=''):
    try:
        resposta = requests.post(link,
                                         verify=False,
                                         data=json.dumps({"sessionID": str(session), "queryName": queryName,
                                                          "parameters": parametros}),
                                         headers={'Accept': 'application/json', 'Content-Type': 'application/json'})

        # print(resposta.text)
        resposta_json = resposta.json()
        resposta_df = pd.DataFrame(resposta_json['result'])
        # resposta_df.to_csv(f'{caminho}{nomeTabela}.csv', sep=';',
        #                                 encoding='utf-8-sig', index=False)
        # print(resposta_df)
        data_arquivo = str(datetime.now())[0:10]
        resposta_df.columns = [x.lower() for x in resposta_df.columns]
        # print(resposta_df.columns)
        for data in datas:
            try:
                resposta_df.loc[:, data] = resposta_df.loc[:, data].map(lambda x: convert_data(x))
            except:
                pass


        resposta_df.reset_index(inplace=True,drop=True)
        resposta_df.to_csv(f'{caminho}{nomeTabela}.csv', sep=';',
       # resposta_df.to_csv(f'{caminho}{nomeTabela}-{data_arquivo}.csv', sep=';',
                                       encoding='utf-8-sig', index=False)
    except Exception as ex:
        print(ex)
        pass
    print(f'{nomeTabela}')
    return resposta_df



CREDENCIAIS = {


     "ESTRUTURANTES-IP1-16-30": {

        "link_acesso": 'https://18.230.28.166/citsmart/services/login',
        "link_dados": 'https://18.230.28.166/citsmart/services/data/query',
        "login": 'citsmart.local\webservice_estruturantes',
        "senha": '!@webservice@!'},


     "ESTRUTURANTES-IP2-16-30": {

        "link_acesso": 'https://18.229.253.136/citsmart/services/login',
        "link_dados": 'https://18.229.253.136/citsmart/services/data/query',
        "login": 'citsmart.local\webservice_estruturantes',
        "senha": '!@webservice@!'},


}


contratos = list(CREDENCIAIS)



for contrato in contratos:
    link_acesso = CREDENCIAIS[contrato]['link_acesso']
    link_dados = CREDENCIAIS[contrato]['link_dados']
    login = CREDENCIAIS[contrato]['login']
    senha = CREDENCIAIS[contrato]['senha']
    sessionID = autenticar(link_acesso, login, senha)
    print(contrato)

    try:
        solicitacao = obter_dados(nomeTabela=f'solicitacao_{contrato}', queryName='SOLICITACAO', session=sessionID,
                                      link=link_dados, parametros={"datafim": datafim, "datafim1": datafim1},
                                      datas=["data hora captura","data hora fim","data hora limite",
                                             "data hora solicitacao","duracao ate a captura a","duracao ate a captura b"])

    except:
        pass


Fim = datetime.now()
print(f"{Fim - Inicio}")



Inicio2 = datetime.now()

import os



def status(x):
    if x == 1:
        return "Em andamento"
    elif x == 2:
        return "Suspensa"
    elif x == 3:
        return "Cancelada"
    elif x == 4:
        return "Resolvida"
    elif x == 5:
        return "Reaberta"
    elif x == 6:
        return "Fechada"
    else:
        "Não identificado"


def sla(a, b):
    try:
        a = int(a)
        b = int(b)
    except:
        return '0h0m'
    else:
        return f'{str(a)}h{str(b)}m'


def atraso(a, b):
    try:
        a = int(a)
        b = int(b)
    except:
        return '00:00hs'
    else:
        return f'{str(a)}:{str(b)}Hs'


def date_to_datetime(x):
    try:
        return pd.datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
    except:
        pass


def datetime_to_date(x):
    try:
        return pd.datetime.strftime(x, "%Y-%m-%d %H:%M:%S")
    except:
        pass

def nota(x):
    if x == 66:
        return "1"
    elif x == 67:
        return "2"
    elif x == 117:
        return "3"
    elif x == 116:
        return "6"
    elif x == 68:
        return "4"
    elif x == 69:
        return "5"
    elif x == 70:
        return "7"
    elif x == 71:
        return "8"
    elif x == 72:
        return "9"
    elif x == 73:
        return "10"
    elif x == 126:
        return "1"
    elif x == 125:
        return "2"
    elif x == 167:
        return "3"
    elif x == 124:
        return "4"
    elif x == 166:
        return "5"
    elif x == 123:
        return "6"
    elif x == 122:
        return "7"
    elif x == 121:
        return "8"
    elif x == 120:
        return "9"
    elif x == 119:
        return "10"
    else:
        return "Sem avaliação"


files = []
# r=root, d=directories, f = files
for r, d, f in os.walk(path):
    for file in f:
        if '.csv' in file:
            files.append(file)

conjunto = set()
for x in files:
    conjunto.add(x.split("_")[1].split(".")[0])

# print(conjunto)

arquivos = {}
for arquivoNome in files:
    # print(arquivoNome)
    try:
        arquivos[arquivoNome.rstrip('.csv')] = pd.read_csv(f'{path}{arquivoNome}', sep=';', low_memory=False)
    except:
        pass

for z in conjunto:
    try:
        nome = z
        reg = arquivos[f'solicitacao_{nome}']
    except Exception as ex:
        print(ex)
        pass
    else:
        reg.situacao = reg.situacao.map(lambda x: status(x))
        for x in reg.index:
            reg.loc[x, 'SLA'] = sla(reg.loc[x, 'prazohh'], reg.loc[x, 'prazomm'])

        for z in reg.index:
            reg.loc[z, 'tempo de atraso'] = atraso(reg.loc[z, 'tempoatrasohh'], reg.loc[z, 'tempoatrasomm'])

        for x in reg[~reg.reclassificado.isnull()]['reclassificado'].index:
            reg.loc[x, 'reclassificado'] = 1

        reg.loc[:, 'reaberto'] = reg.loc[:, 'reaberto'].map(lambda x: 1 if x != 0 else np.nan)

        for x in reg[~reg['base de conhecimento'].isnull()]['base de conhecimento'].index:
            reg.loc[x, 'base de conhecimento'] = 1

        reg.loc[:, 'nota satisfacao'] = reg.loc[:, 'nota satisfacao'].map(lambda x: nota(x))


        for x in reg.index:
            try:
                reg.loc[x, 'duracao ate a captura'] = date_to_datetime(
                    reg.loc[x, 'duracao ate a captura a']) - date_to_datetime(reg.loc[x, 'duracao ate a captura b'])
            except:
                pass
        reg.drop_duplicates(inplace=True)

        reg = reg[
                ['solicitacao', 'tipo grupo', 'tipo atividade', 'tipo demanda servico', 'situacao', 'solicitante', 'contrato', 'data hora solicitacao', 'SLA',
             'data hora limite', 'data hora fim', 'tempo de atraso', 'nomeservico', 'grupo', 'tecnico', 'origem',
             'reclassificado',
             'reaberto', 'base de conhecimento', 'criador', 'data hora captura', 'duracao ate a captura',
             'primeira captura',
             'nota satisfacao', 'slaatrasado', 'pf_criador', 'pf_tecnico', 'catalogo', 'id grupo executor', 'id grupo criador','observacoescriador','observacoestecnico']]

        reg.to_csv(f'{path2}consolidado_{nome} - {mes} - {ano}.csv', sep=';', encoding='utf-8-sig', index=False)
      

Fim2 = datetime.now()
print(f"{Fim2 - Inicio2}")

