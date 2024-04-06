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


votacao = (
    read_tse_csv_from_zip(TSE_DATA_DIR / 'votacao_secao_2020_PB.zip',
                          'votacao_secao_2020_PB.csv')
    .query("DS_CARGO == 'Prefeito'")
    .groupby(['SG_UE', 'NR_VOTAVEL'])
    .agg({'QT_VOTOS': 'sum'})
    .rename(columns={'NR_VOTAVEL': 'NR_CANDIDATO'})
)

receitas = (
    read_tse_csv_from_zip(
        TSE_DATA_DIR / 'prestacao_de_contas_eleitorais_candidatos_2020.zip',
        'receitas_candidatos_2020_PB.csv')
    .groupby(['SQ_CANDIDATO'])
    .agg({'VR_RECEITA': 'sum'})
)
receitas

bens = (
    read_tse_csv_from_zip(TSE_DATA_DIR / 'bem_candidato_2020.zip',
                          'bem_candidato_2020_PB.csv')
    .groupby(['SQ_CANDIDATO'])
    .agg({'VR_BEM_CANDIDATO': 'sum'})
)
bens

df = (
    candidatos
    # Junção usando o número do candidato
    .join(votacao, on=['SG_UE', 'NR_CANDIDATO'])
    .join(receitas)  # Junção usando o SQ_CANDIDATO
    .join(bens)
    .filter(['ANO_ELEICAO', 'SG_UF', 'DS_CARGO', 'NR_CANDIDATO', 'NM_URNA_CANDIDATO',
             'SG_PARTIDO', 'DT_NASCIMENTO', 'NR_IDADE_DATA_POSSE', 'DS_GENERO',
             'DS_GRAU_INSTRUCAO', 'DS_ESTADO_CIVIL', 'DS_COR_RACA', 'DS_OCUPACAO',
             'DS_SIT_TOT_TURNO', 'QT_VOTOS', 'VR_RECEITA', 'VR_BEM_CANDIDATO', 'VR_DESPESA_MAX_CAMPANHA', 'NM_CANDIDATO'
             ])
)
df
df.to_csv(DATA_DIR / 'eleicao_2020_pb_prefeito.csv')
