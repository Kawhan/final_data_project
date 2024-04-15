import zipfile
from pathlib import Path

import pandas as pd

DATA_DIR = Path().resolve().parent / 'dados'
TSE_DATA_DIR = DATA_DIR / 'tse'


def read_tse_csv_from_zip(zip_file, csv_file):
    zf = zipfile.ZipFile(zip_file)
    with zf.open(csv_file) as csv_f:
        return pd.read_csv(csv_f, encoding='latin1', delimiter=';', decimal=',')


candidatos = (
    read_tse_csv_from_zip(TSE_DATA_DIR / 'consulta_cand_2020.zip',
                          'consulta_cand_2020_PB.csv')
    .query("DS_CARGO == 'PREFEITO'")
    .set_index('SQ_CANDIDATO')
)


municipios = (
    pd.read_csv(DATA_DIR/'municipios_brasileiros_tse.csv')
    .query("uf == 'PB'")
    .rename(columns={'codigo_tse': 'SG_UE', 'nome_municipio': 'NOME_MUNICIPIO'})
)
municipios

past_dataset = (
    pd.read_csv(DATA_DIR/'consulta_cand_2016_PB.csv',
                delimiter=';', encoding='latin1')
    .query("DS_CARGO == 'PREFEITO'")
    .set_index('SG_UE')
    .rename(columns={'SG_PARTIDO': 'SG_PAST_PARTIDO'})
)
past_dataset

df = (
    candidatos
    # Junção usando o número do candidato
    .join(municipios.set_index('SG_UE'), on='SG_UE')
    .join(past_dataset, rsuffix='_past', on=['SG_UE'])
    .filter(['ANO_ELEICAO', 'SG_UF', 'SG_UE', 'NOME_MUNICIPIO', 'DS_CARGO', 'NR_CANDIDATO', 'NM_URNA_CANDIDATO',
             'SG_PARTIDO', 'SG_PAST_PARTIDO', 'DT_NASCIMENTO', 'NR_IDADE_DATA_POSSE', 'DS_GENERO',
             'DS_GRAU_INSTRUCAO', 'DS_ESTADO_CIVIL', 'DS_COR_RACA', 'DS_OCUPACAO',
             'DS_SIT_TOT_TURNO', 'NM_CANDIDATO'
             ])
)
df
df.to_csv(DATA_DIR / 'eleicao_2020_with_2016_pb_prefeito.csv')
