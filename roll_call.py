# -*- coding: utf-8 -*-
"""
Created on Wed Apr 12 08:11:16 2023

@author: felipe
"""
import pandas as pd
import numpy as np
import statsmodels.formula.api as smf
import itertools
from stargazer.stargazer import Stargazer
import statsmodels.api as sm
from itertools import product
from haversine import haversine, Unit
import re
from datetime import datetime
import sqlite3
import vaex
import modin.pandas as pd
import dask.dataframe as dd
import dask_ml.preprocessing as dpp
!pip install haversine

# read in Excel sheets
#roll_call_all = pd.read_excel("Roll_calls_types_55_all.xlsx", sheet_name="Propositions_types_all")
#roll_call_amendments = pd.read_excel("Roll_calls_types_55_all.xlsx", sheet_name="Amendments_all")
#roll_call_unexpected = pd.read_excel("Roll_calls_types_55_all.xlsx", sheet_name="UnexpectedMotions_all")
roll_call_votes_55 = pd.read_excel("votacoesVotos-55.xlsx", sheet_name="Part4_1")
roll_call_votes_56 = pd.read_excel("votacoesVotos-56.xlsx", sheet_name="votacoesVotos-56")
deputados_55 = pd.read_excel("C:/Users/felip/OneDrive/Desktop/Economia/PhD/Thesis/Code/deputados_55.xlsx", sheet_name='Deputados')
deputados_56 = pd.read_excel("C:/Users/felip/OneDrive/Desktop/Economia/PhD/Thesis/Code/deputados_56.xlsx", sheet_name='Deputados')
#lottery = pd.read_excel("Lottery_55_legis.xlsx", sheet_name='Append1')
#comm = pd.read_excel('orgaosDeputados-L55.xlsx', sheet_name="Sheet2")
roll_calls_55 = pd.read_excel("votacoes-2015.xlsx", sheet_name="votacoes_voto")
roll_calls_56 = pd.read_excel("votacoes-2019.xlsx", sheet_name="Sheet3")

orientacoes_55 = pd.read_excel("orientacoes55.xlsx", sheet_name="Sheet1")
orientacoes_56 = pd.read_excel("orientacoes56.xlsx", sheet_name="completa")

coalizoes = pd.read_excel("coalizoes.xlsx", sheet_name="Coalizao")
neighbors = pd.read_excel("deputados_55.xlsx", sheet_name='Sheet1')

prop_tipo = pd.read_excel("C:/Users/felip/OneDrive/Desktop/Economia/PhD/Thesis/Code/votacoesVotos-55.xlsx", sheet_name="Planilha1")

# merge data frames based on idVotacao
#roll_call_all_votes = pd.merge(roll_call_all, roll_call_votes, on="idVotacao")
#roll_call_amendments_votes = pd.merge(roll_call_amendments, roll_call_votes, on="idVotacao")
#roll_call_unexpected_votes = pd.merge(roll_call_unexpected, roll_call_votes, on="idVotacao")

# subset data frames based on year and month of the vote
#roll_call_all_2015_votes = roll_call_all_votes[roll_call_all_votes["ano_votacao_y"] == 2015]
#roll_call_amendments_2015_votes = roll_call_amendments_votes[roll_call_amendments_votes["ano_votacao"] == 2015]
#roll_call_unexpected_2015_votes = roll_call_unexpected_votes[roll_call_unexpected_votes["ano_votacao"] == 2015]

#roll_call_years = [2016, 2017, 2018]
#roll_call_years_votes = roll_call_all_votes[roll_call_all_votes["ano_y"].isin(roll_call_years)]

### Completing the data (by imputing absence votes)


deputados = pl.concat([pairwise_df_55, pairwise_df_56])






roll_call_votes_55 = pd.read_excel("votacoesVotos-55.xlsx", sheet_name="Part4")

roll_call_votes_56 = pd.read_excel("votacoesVotos-56.xlsx", sheet_name="Part4_1")


roll_call_votes = roll_call_votes_55
roll_call_votes = roll_call_votes_56


# Set the threshold for minimum frequency
threshold = 20

# Count the frequency of each unique value in the "deputado_nome" column
value_counts = roll_call_votes['deputado_nome'].value_counts()

# Get the names of observations with frequency below the threshold
below_threshold = value_counts[value_counts < threshold].index

# Filter the dataset to exclude observations with names below the threshold
filtered_roll_call_votes = roll_call_votes[~roll_call_votes['deputado_nome'].isin(below_threshold)]

# Print the filtered dataset
print(filtered_roll_call_votes)

print(below_threshold)





df = roll_call_votes

df = filtered_roll_call_votes




deputados = deputados_56
roll_calls = roll_calls_56
orientacoes = orientacoes_55
roll_call_votes = filtered_roll_call_votes

roll_call_votes['unique_values'] = roll_call_votes['idVotacao'].drop_duplicates()

deputados.dropna(subset=['age'], inplace=True)


deputados['Birth'] = pd.to_datetime(deputados['Birth'])

# Calculate age based on current date
current_date = datetime(2024, 3, 4)
deputados['age'] = current_date.year - deputados['Birth'].dt.year
deputados['age'] = deputados['age'].astype(int)

# Display DataFrame with age column
print(deputados)

deputados.to_csv('deputados_56.csv', index=False)


#rollcalls = pd.DataFrame({'id': roll_call_all['unique_values']}).dropna()

# Create a new DataFrame with all possible combinations
df = pd.DataFrame(list(product(roll_calls['id'], deputados['ideCadastro'])), columns=['idVotacao', 'deputado_id'])

# Merge with original data

#print(df['idVotacao'].dtype)
#print(roll_call_all['unique_values'].dtype)

#print(orientacoes["concatenated"].dtype)
#print(df["concatenated"].dtype)
roll_call_votes_f = roll_call_votes.dropna(subset=['unique_values'])

#df['legislator_id'] = df['legislator_id'].astype('int32')
#deputados['ideCadastro'] = deputados['ideCadastro'].astype(int)
#roll_call_all_f['idVotacaoOG'] = roll_call_all['idVotacaoOG'].astype('int32')

df.head()
df = df.merge(deputados, left_on='deputado_id', right_on='ideCadastro')
#df = df.merge(roll_call_all_f, left_on='idVotacao', right_on='idVotacaoOG')

#export_csv = roll_call_all_votes.to_excel ('roll_call_all_votes.xlsx', index = True, header=True, encoding='utf-8-sig', engine='xlsxwriter' ) #Don't forget to add '.csv' at the end of the path

#df['Partido'] = df['deputado_id'].map(deputados.set_index('ideCadastro')['Partido'])

df['concatenated'] = df['idVotacao'].astype(str) + '_' + df['Partido'].astype(str)
df['concatenated2'] = df['idVotacao'].astype(str) + '_' + df['deputado_id'].astype(str)
#df["idVotacaoOG"] = df['idVotacao'].str.replace('-', '', regex=True)

orientacoes.drop_duplicates(subset='concatenated', keep='first', inplace=True)
deputados.drop_duplicates(subset='ideCadastro', keep='first', inplace=True)


roll_call_votes['concatenated'] = roll_call_votes['idVotacao'].astype(str) + '_' + roll_call_votes['deputado_id'].astype(str)


df['orientacao'] = df['concatenated'].map(orientacoes.set_index('concatenated')['orientacao'])
df['voto'] = df['concatenated2'].map(roll_call_votes.set_index('concatenated')['voto'])
df['propTipo'] = df['idVotacao'].map(roll_call_votes_f.set_index('idVotacao')['proposicao_codTipo'])

df = df[~(df['orientacao'] == 5)]
df = df[~(df['voto'] == 4)]

df["orientacao_f"] = df["orientacao"].fillna(6)
df['voto'] = df['voto'].fillna(6)
grouped = df.groupby('voto').describe()
print(grouped)
grouped = df.groupby('orientacao_f').describe()
print(grouped)

df['DistCapital'] = df['UFEleito'].map(capitals.set_index('Estado')['distance_to_brasilia_km'])
df['lnDistCapital'] = df['UFEleito'].map(capitals.set_index('Estado')['lnDistCapital'])


df['show'] = df['voto'].apply(lambda x: 1 if x == 6 else 0)



pairwise_df_subset['show'] = pairwise_df_subset['voto'].apply(lambda x: 1 if x == 6 else 0)



pairwise_df_subset['numLegislatura'] = pairwise_df_subset['numLegislatura'].fillna(56)

pairwise_df_subset['Estado'] = pairwise_df_subset['Estado'].fillna(pairwise_df_subset['UFEleito'])


#unique_names = df['Partido'].unique()

#print(unique_names)



deputados["Coalizao_1"] = deputados['Partido'].map(coalizoes.set_index('Partido')['Coalizao_1'])







# check if there are any missing values in col1
#if df['DistCapital'].isna().any():
#    print("There are missing values in col1")
#else:
#    print("There are no missing values in col1")
#
#missing_values_df = df[df["DistCapital"].isna()]


df_notna = df[df['orientacao'].notna()]
df_notna  = df_notna [~(df_notna ['orientacao'] == 5)]
df_notna  = df_notna [~(df_notna ['voto'] == 4)]


df_56 = df


subset_df = df_notna

# Calculate NC variable
subset_df['NC'] = np.where(subset_df['orientacao'] == subset_df['voto'], 0, 1)


#### Building Voting guidance structure


orientacoes = pd.read_excel("votacoesOrientacoes-2019.xlsx", sheet_name="bancadas")


# define a regular expression pattern to match party acronyms
party_pattern = r'(?<!^)(?=[A-Z][a-z])'

# iterate over the rows of the DataFrame
for index, row in orientacoes.iterrows():
    # extract the token from the current row
    token = row['siglaBancada']
    
    # split the token into separate party acronyms using the regular expression pattern
    parties = re.split(party_pattern, token)
    
    # do something with the list of party acronyms (e.g., print them)
    print(parties)
    
orientacoes['siglaBancada'] = orientacoes['siglaBancada'].str.findall(r'[A-Z][a-z]*')

# Explode the list to create new rows
orientacoes = orientacoes.explode('siglaBancada')

# Sort the dataframe by idVotacao and reset the index
orientacoes = orientacoes.sort_values('idVotacao').reset_index(drop=True)    

export_csv = subset_df.to_excel ('subset_df_NC.xlsx', index = True, header=True, encoding='utf-8-sig', engine='xlsxwriter' ) #Don't forget to add '.csv' at the end of the path

orientacoes = pd.read_excel("orientacoes.xlsx", sheet_name="Sheet1")


deputados


unique_values = pairwise_df['Andar'].unique()

print(unique_values)


######## GEOGRAPHICAL DATA


# Geographic coordinates of Brasilia
brasilia_lat = -15.793889
brasilia_lon = -47.882778

# Load data of state capitals and their coordinates
cities = pd.read_csv('https://raw.githubusercontent.com/kelvins/Municipios-Brasileiros/main/csv/municipios.csv',
                       usecols=['codigo_ibge', 'nome', 'latitude', 'longitude', 'capital'],
                       dtype={'codigo_ibge': str})

capitals = cities[cities['capital'] == 1]  # select only state capitals

# Calculate the distances between each state capital and Brasilia
capitals['distance_to_brasilia_km'] = capitals.apply(lambda x: haversine((x['latitude'], x['longitude']), 
                                                                          (brasilia_lat, brasilia_lon), 
                                                                          unit=Unit.KILOMETERS), axis=1)
# Display the results
print(capitals[['nome', 'distance_to_brasilia_km']])



capitais = ['Rio Branco', 'Maceió', 'Macapá', 'Manaus', 'Salvador', 'Fortaleza', 'Brasília', 'Vitória', 'Goiânia', 'São Luís', 'Cuiabá', 'Campo Grande', 'Belo Horizonte', 'Belém', 'João Pessoa', 'Curitiba', 'Recife', 'Teresina', 'Rio de Janeiro', 'Natal', 'Porto Alegre', 'Porto Velho', 'Boa Vista', 'Florianópolis', 'São Paulo', 'Aracaju', 'Palmas']
#ibge_codes = ['1200401', '2704302', '1600303', '1302603', '2927408', '2304400', '5300108', '3205309', '5208707', '2111300', '5103403', '5002704', '3106200', '1501402', '2507507', '4106902', '2611606', '2211001', '3304557', '2408102', '4314902', '1100205', '1400100', '4205407', '3550308', '1721000']

#selected = cities[cities['codigo_ibge'].isin(ibge_codes)]

# Calculate the distances between each state capital and Brasilia
#selected['distance_to_brasilia_km'] = selected.apply(lambda x: haversine((x['latitude'], x['longitude']), 
#                                                                          (brasilia_lat, brasilia_lon), 


siglas = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']

df_capitais = pd.DataFrame({'capital': capitais, 'UF': siglas})
print(df_capitais)

capitals['Estado'] = capitals['nome'].map(df_capitais.set_index('capital')['UF'])


# Display the results
print(capitals[['nome', 'distance_to_brasilia_km']])

# Calculate the log values of "PropOtherParty" and add them as a new column "logPOP"
capitals["lnDistCapital"] = np.log(capitals["distance_to_brasilia_km"])


##### OFFICE DATA (COMBINATION AND PERMUTATION)

offices = pd.read_excel("deputados_55.xlsx", sheet_name="offices")

office_numbers = offices['gabinete'].drop_duplicates()

pairwise_matrix = pd.DataFrame(0, index=office_numbers, columns=office_numbers)

# Loop over all pairs of office numbers and update the pairwise matrix by setting the corresponding elements to 1
for i in range(len(office_numbers)):
    for j in range(i+1, len(office_numbers)):
        pairwise_matrix.iloc[i, j] = 1
        pairwise_matrix.iloc[j, i] = 1

print(pairwise_matrix)

'''
# create a sample DataFrame
#df = pd.DataFrame({'idVotacao': [1, 1, 1, 2, 2, 2],
#                   'deputado_id': [1, 2, 3, 1, 2, 3],
#                   'voto': ['yes', 'no', 'yes', 'yes', 'yes', 'no']})

# create a list of unique idVotacao values
id_votacao_list = roll_call_amendments_2015_votes['idVotacao'].unique()

# create an empty DataFrame to store the pairwise data
pairwise_df = pd.DataFrame(columns=['deputado1', 'deputado2', 'Convergence'])

# iterate over each unique idVotacao value
for id_votacao in id_votacao_list:
    # create a sub-DataFrame for the current idVotacao value
    sub_df = roll_call_amendments_2015_votes[roll_call_amendments_2015_votes['idVotacao'] == id_votacao]
    
    # get a list of unique deputado_id values in the sub-DataFrame
    deputado_id_list = sub_df['deputado_id'].unique()
    
    # iterate over each unique pair of deputado_id values in the sub-DataFrame
    for i, deputado1 in enumerate(deputado_id_list):
        for j in range(i+1, len(deputado_id_list)):
            deputado2 = deputado_id_list[j]
            
            # check if the two deputados are not the same
            if deputado1 != deputado2:
                # get the voto for each deputado in the current idVotacao
                voto1 = sub_df[sub_df['deputado_id'] == deputado1]['voto'].iloc[0]
                voto2 = sub_df[sub_df['deputado_id'] == deputado2]['voto'].iloc[0]
                
                # check if the two deputados had the same voto
                if voto1 == voto2:
                    # add a row to the pairwise DataFrame with the two deputados and the convergence value
                    pairwise_df = pairwise_df.append({'deputado1': deputado1,
                                                      'deputado2': deputado2,
                                                      'Convergence': 1},
                                                     ignore_index=True)
                else:
                # add entry to pairwise_df with Convergence = 0
                    pairwise_df = pairwise_df.append({
                        'deputado1': deputado1,
                        'deputado2': deputado2,
                        'Convergence': 0
                    }, ignore_index=True)    

# group by deputado1 and deputado2, then sum the convergence values
pairwise_df = pairwise_df.groupby(['deputado1', 'deputado2']).sum().reset_index()


# print the pairwise DataFrame
print(pairwise_df)

'''


pairwise_df = pairwise_matrix

# Create a new column that contains the sorted tuple of each pair
pairwise_df['SortedPair'] = pairwise_df[['deputado1', 'deputado2']].apply(lambda x: tuple(sorted(x)), axis=1)

# Check for duplicates in the SortedPair column
duplicates = pairwise_df[pairwise_df.duplicated('SortedPair')]

if duplicates.empty:
    print("There are no repeated pairs in the pairwise matrix.")
else:
    print("The following pairs are repeated:")
    print(duplicates)
   


##### REBEL RATE


deputados = deputados_56

# Assume your data frame is called "deputados"
# and has columns "andar" and "partido"

# Create a new column to hold the proportion of deputados
# with the same partido in the same andar
deputados['PropSameCoal'] = 0.0

# Iterate over each row in the data frame
for i, row in deputados.iterrows():
    # Get the andar value for the current row
    andar = row['Andar']
    # Get the partido value for the current row
    partido = row['Coalizao']
    # Find all other rows with the same andar value
    same_andar = deputados[deputados['Andar'] == andar]
    # Calculate the proportion of rows with the same partido
    prop_same_party = sum(same_andar['Coalizao'] == partido) / len(same_andar)
    # Update the value of the PropSameParty column for the current row
    deputados.at[i, 'PropSameCoal'] = prop_same_party


deputados['PropOtherCoal'] = 1 - deputados['PropSameCoal'] 
    




'''

pairwise_df = pd.read_excel("pairwise_df_votes_2015.xlsx", sheet_name="Sheet1")

pairwise_df = pairwise_df[['deputado1','deputado2','Convergence']]

print(pairwise_df)


# pivot the pairwise data frame to get an adjacency matrix
adjacency_matrix = pairwise_df.pivot(index='deputado1', columns='deputado2', values='Convergence')

# fill in any missing values with 0
adjacency_matrix.fillna(0, inplace=True)

# display the adjacency matrix
print(adjacency_matrix)


export_csv = adjacency_matrix.to_excel ('adjacency_matrix.xlsx', index = True, header=True, encoding='utf-8-sig', engine='xlsxwriter' ) #Don't forget to add '.csv' at the end of the path

'''


############################################


subset_df = df_notna

# Calculate NC variable
subset_df['NC'] = np.where(subset_df['orientacao'] == subset_df['voto'], 0, 1)

# Create a dictionary of idVotacao -> set of deputado_id
id_votacao_dict = subset_df.groupby('idVotacao')['deputado_id'].apply(set).to_dict()

# Initialize adjacency matrix with zeros
adj_matrix = pd.DataFrame(0, index=roll_call_unexpected_2015_votes['deputado_id'].unique(), columns=roll_call_unexpected_2015_votes['deputado_id'].unique())

# Iterate over idVotacao and deputado_id pairs to populate the adjacency matrix
for id_votacao, deputado_set in id_votacao_dict.items():
    for deputado_id in deputado_set:
        # Get the subset of rows for the current id_votacao and deputado_id
        deputado_df = subset_df[(subset_df['idVotacao'] == id_votacao) & (subset_df['deputado_id'] == deputado_id)]
        # Skip if no matching rows are found
        if deputado_df.empty:
            continue
        # Get the value of NC for the pivotal deputado_id
        pivotal_NC = deputado_df.iloc[0]['NC']
        # Iterate over other deputado_ids in the same idVotacao and update adjacency matrix
        for other_deputado_id in deputado_set:
            if other_deputado_id != deputado_id:
                other_df = subset_df[(subset_df['idVotacao'] == id_votacao) & (subset_df['deputado_id'] == other_deputado_id)]
                # Skip if no matching rows are found
                if other_df.empty:
                    continue
                # Check if IC condition is met
                if pivotal_NC == 1 and other_df.iloc[0]['orientacao'] == deputado_df.iloc[0]['voto']:
                    adj_matrix.at[deputado_id, other_deputado_id] += 1

# Calculate percentage of times the IC condition was met for each deputado_id pair
total_votes = roll_call_unexpected_2015_votes.groupby('idVotacao')['deputado_id'].nunique().sum()
adj_matrix = (adj_matrix / total_votes)












import pandas as pd
import numpy as np

# read in the data
df = pd.read_csv('votes.csv')

# subset the data where 'orientacao' is not null
subset_df = df[df['orientacao'].notnull()]

# calculate the 'NC' column for each group
nc = subset_df.groupby(['idVotacao', 'deputado_id']) \
              .apply(lambda x: np.where(x['orientacao'] == x['voto'], 0, 1)) \
              .reset_index(drop=True)

# assign the 'NC' column to the original dataframe
df.loc[subset_df.index, 'NC'] = nc.values


# Calculate NC variable
subset_df['NC'] = np.where(subset_df['orientacao'] == subset_df['voto'], 0, 1)

df = subset_df

# calculate the 'IC' column for each group
ic = df.groupby(['idVotacao', 'deputado_id']) \
       .apply(lambda x: np.where((x['NC'] == 1) & (x['orientacao'] == x['voto']), 1, 0)) \
       .reset_index(drop=True)

# create the adjacency matrix
adj_matrix = ic.groupby(['deputado_id', 'deputado_id']) \
               .mean() \
               .unstack() \
               .fillna(0)

# set the diagonal to zero
np.fill_diagonal(adj_matrix.values, 0)

# rename the columns and index
adj_matrix.columns = adj_matrix.columns.droplevel()
adj_matrix.index.name = None

# display the adjacency matrix
print(adj_matrix)






########################


df = roll_call_votes_55

df1 = df.groupby(['deputado_id','voto'])['idVotacao']

sampled_df = df.sample(n=500, random_state=42)



# Assuming your original dataset is stored in a pandas DataFrame called 'df'
# with columns 'idVotacao', 'deputado_id', and 'voto'

# Group the DataFrame by 'idVotacao'
grouped = df.groupby('idVotacao')

pairwise_data = []

# Iterate over each group
for group_name, group_data in grouped:
    # Get unique 'deputado_id' values for the current 'idVotacao'
    deputado_ids = group_data['deputado_id'].unique()
    
    # Generate pairs of 'deputado_id' values
    pairs = [(id1, id2) for id1 in deputado_ids for id2 in deputado_ids if id1 != id2]
    
    # Check if the pairs have the same 'voto' and create the pairwise data
    for pair in pairs:
        id1, id2 = pair
        voto1 = group_data.loc[group_data['deputado_id'] == id1, 'voto'].iloc[0]
        voto2 = group_data.loc[group_data['deputado_id'] == id2, 'voto'].iloc[0]
        
        pairwise_data.append((id1, id2, group_name, int(voto1 == voto2)))

# Create a new DataFrame with pairwise data
pairwise_df = pd.DataFrame(pairwise_data, columns=['deputado_id_1', 'deputado_id_2', 'idVotacao', 'convergence'])

# Print the pairwise DataFrame
print(pairwise_df)





import pandas as pd
from itertools import combinations
import cupy as cp

import pandas as pd
import cupy as cp
from itertools import combinations

# Convert pandas DataFrame to CuPy DataFrame
selected_columns = ['idVotacao', 'voto', 'deputado_id']
df_selected = df.loc[:, selected_columns]
df_selected = df_selected.applymap(lambda x: x.replace('-', '') if isinstance(x, str) else x)
df_selected = df_selected.astype('float64')
cp_df = cp.asarray(df_selected)


# Group the DataFrame by 'idVotacao'

# Group the CuPy array by 'idVotacao'
grouped = {}

for idVotacao, group_data in zip(cp.unique(cp_df[:, 0]), cp.split(cp_df, cp.bincount(cp_df[:, 0]))[1:]):
    grouped[idVotacao] = group_data



grouped = pd.DataFrame(cp.asnumpy(cp_df)).groupby('idVotacao')

pairwise_data = []

# Iterate over each group
for group_name, group_data in grouped:
    # Get unique 'deputado_id' values for the current 'idVotacao'
    deputado_ids = cp.unique(group_data['deputado_id'])
    
    # Generate pairs of 'deputado_id' values
    for id1, id2 in combinations(deputado_ids, 2):
        voto1 = group_data[group_data['deputado_id'] == id1]['voto'][0]
        voto2 = group_data[group_data['deputado_id'] == id2]['voto'][0]
        
        pairwise_data.append((id1, id2, group_name, int(voto1 == voto2)))

# Convert the pairwise data to a CuPy array
pairwise_data_cp = cp.asarray(pairwise_data)

# Create a new DataFrame with pairwise data
pairwise_df = pd.DataFrame(cp.asnumpy(pairwise_data_cp), columns=['deputado_id_1', 'deputado_id_2', 'idVotacao', 'convergence'])

# Print the pairwise DataFrame
print(pairwise_df)


df = df_selected

import pandas as pd
from itertools import combinations

# Group the DataFrame by 'idVotacao'
grouped = df.groupby('idVotacao')

pairwise_data = []

# Iterate over each group
for group_name, group_data in grouped:
    # Get unique 'deputado_id' values for the current 'idVotacao'
    deputado_ids = group_data['deputado_id'].unique()
    
    # Generate pairs of 'deputado_id' values
    for id1, id2 in combinations(deputado_ids, 2):
        voto1 = group_data.loc[group_data['deputado_id'] == id1, 'voto'].iloc[0]
        voto2 = group_data.loc[group_data['deputado_id'] == id2, 'voto'].iloc[0]
        
        pairwise_data.append((id1, id2, group_name, int(voto1 == voto2)))

# Create a new DataFrame with pairwise data
pairwise_df = pd.DataFrame(pairwise_data, columns=['deputado1', 'deputado2', 'idVotacao', 'convergence'])

# Print the pairwise DataFrame
print(pairwise_df)















sampled_df = df.sample(n=81, random_state=42)


df = sampled_df


### CREATING ADJACENCY MATRIX


df = filtered_roll_call_votes

df.replace("nan", pd.NA, inplace=True)


# assume your original dataset is stored in a pandas DataFrame called "df"
# with columns "idVotacao", "deputado_id", and "voto"

# first, create a pivot table to group by both idVotacao and deputado_id
# and count the number of times each combination of voto occurs
table = pd.pivot_table(df, values='voto', index='idVotacao',
                       columns='deputado_id', aggfunc=lambda x: x.tolist())
table = table.applymap(lambda x: pd.to_numeric(str(x).strip("[]")))

# fill any missing values with zeros
table = table.fillna(6)

df = pd.DataFrame(table)
#original_df = df

#sampled_df = original_df.sample(n=10, random_state=42)
deputado_ids = df.columns

# Create an empty list to store the pairwise matrix data
pairwise_matrix = []

# Iterate over each row (idVotacaoOG)
for idVotacao, row in df.iterrows():
    # Generate pairs of individuals within the group
    for i, individual1 in enumerate(deputado_ids):
        for j, individual2 in enumerate(deputado_ids[i+1:], start=i+1):
            # Check if the individuals have the same value in the original matrix
            convergence = int(row[individual1] == row[individual2])
            
            pairwise_matrix.append([idVotacao, individual1, individual2, convergence])

# Convert the pairwise matrix to a pandas DataFrame
pairwise_df = pd.DataFrame(pairwise_matrix, columns=['idVotacao', 'deputado1', 'deputado2', 'Convergence'])

# Print the pairwise DataFrame
print(pairwise_df.head)


unique_values = pairwise_df['idVotacao'].unique().tolist()

# Print the list of unique values
print(len(unique_values))

column_mapping = {
    'idVotacao': 'idVotacao',
    'deputado_1': 'deputado1',
    'deputado_2': 'deputado2',
    'Convergence': 'Convergence'
}

# Rename the columns using the column_mapping dictionary
pairwise_df.columns = [column_mapping.get(col, col) for col in pairwise_df.columns]

# Print the updated DataFrame with the new column names
print(pairwise_df)















# Convert the 'voto' column to numeric, coercing errors to NaN
df['voto_numeric'] = pd.to_numeric(df['voto'], errors='coerce')

# Check for any NaN values in the resulting numeric column
nan_positions = df[df['voto_numeric'].isna()].index

# Print the positions of NaN values
print("Positions of NaN values:", nan_positions)




# assume your original dataset is stored in a pandas DataFrame called "df"
# with columns "idVotacao", "deputado_id", and "voto"

# first, create a pivot table to group by both idVotacao and deputado_id
# and count the number of times each combination of voto occurs
table = pd.pivot_table(df, values='voto', index='idVotacao',
                       columns='deputado_id', aggfunc=lambda x: x.tolist())


table = table.applymap(lambda x: pd.to_numeric(str(x).strip("[]")))

# fill any missing values with zeros
table = table.fillna(6)

# create an empty adjacency matrix with rows and columns
# corresponding to the unique deputado_id values
deputados = sorted(df['deputado_id'].unique())
adj_matrix = pd.DataFrame(np.zeros((len(deputados), len(deputados))),
                          index=deputados, columns=deputados)

# fill the adjacency matrix with pairwise similarities
for i, deputado1 in enumerate(deputados):
    for j, deputado2 in enumerate(deputados):
        if i != j:
            # pairwise similarity is the sum of the times they voted the same across all idVotacao
            similarity = sum(table.loc[:, deputado1] == table.loc[:, deputado2]) / table.shape[0]
            adj_matrix.iloc[i, j] = similarity

# print the resulting adjacency matrix
print(adj_matrix)


#export_csv = adj_matrix.to_excel ('adjacency_matrix12.xlsx', index = True, header=True, encoding='utf-8-sig', engine='xlsxwriter' ) #Don't forget to add '.csv' at the end of the path



### CREATING PAIRWISE MATRIX


# assume your adjacency matrix is stored in a pandas DataFrame called "adj_matrix"
# with rows and columns corresponding to the unique deputado_id values

# create a list of all unique pairs of deputado_id values
deputados = sorted(adj_matrix.index)
deputados_pairs = list(itertools.combinations(deputados, 2))

# create an empty pairwise matrix with columns "deputado_id1", "deputado_id2", and "Convergence"
pairwise_matrix = pd.DataFrame(columns=["deputado1", "deputado2", "Convergence"])

# fill the pairwise matrix with deputado_id pairs and corresponding convergence values
for pair in deputados_pairs:
    convergence = adj_matrix.loc[pair[0], pair[1]]
    pairwise_matrix = pairwise_matrix.append({"deputado1": pair[0], "deputado2": pair[1], "Convergence": convergence}, ignore_index=True)

# drop any duplicate pairs (e.g., (1,2) and (2,1))
pairwise_matrix = pairwise_matrix.drop_duplicates(subset=["deputado1", "deputado2"])

# print the resulting pairwise matrix
print(pairwise_matrix)


neighbors['concatGabinete'] = neighbors['office_1'].astype(str).str.replace('\.0', '') + "_" + neighbors['office_2'].astype(str).str.replace('\.0', '')

pairwise_matrix['concatGabinete'] = pairwise_matrix['Gabinete1'].astype(str).str.replace('\.0', '') + "_" + pairwise_matrix['Gabinete2'].astype(str).str.replace('\.0', '')


pairwise_df['Neighbor'] = pairwise_df['concatGabinete'].map(neighbors.set_index('concatGabinete').reset_index(drop=False)['Neighbor'])
is_column_only_nan = pairwise_df['Neighbor'].isna().all()

# Print the resultcolumn_type = df['column_name'].dtype
print(column_type)
print(is_column_only_nan)






# Check for discrepancies in the 'concatGabinete' column
mismatched_values = pairwise_matrix[~pairwise_matrix['concatGabinete'].isin(neighbors['concatGabinete'])]['concatGabinete']
print(mismatched_values)

export_csv = pairwise_df_subset.to_excel ('pairwise_matrix.xlsx', index = True, header=True, encoding='utf-8-sig', engine='xlsxwriter' ) #Don't forget to add '.csv' at the end of the path

pairwise_matrix = pd.read_excel("pairwise_matrix.xlsx", sheet_name="Sheet1")


pairwise_df = pairwise_matrix
print(pairwise_df_55.columns)

#pairwise_df = pd.read_excel('pairwise_matrix.xlsx', sheet_name="Sheet1")


deputados = pd.read_excel("deputados_55.xlsx", sheet_name='Deputados')
pairwise_df = pairwise_df_subset

# Assuming pairwise_agreements and deputados are DataFrames and deputado1 is a column in pairwise_agreements

pairwise_df['Nome1'] = pairwise_df['deputado1'].map(deputados.set_index('ideCadastro')['Nome'])

pairwise_df['Nome2'] = pairwise_df['deputado2'].map(deputados.set_index('ideCadastro')['Nome'])

pairwise_df['Gabinete1'] = pairwise_df['deputado1'].map(deputados.set_index('ideCadastro')['Gabinete'])

pairwise_df['Gabinete2'] = pairwise_df['deputado2'].map(deputados.set_index('ideCadastro')['Gabinete'])

pairwise_df['Anexo1'] = pairwise_df['deputado1'].map(deputados.set_index('ideCadastro')['Anexo'])
    
pairwise_df['Anexo2'] = pairwise_df['deputado2'].map(deputados.set_index('ideCadastro')['Anexo'])

pairwise_df['Andar1'] = pairwise_df['deputado1'].map(deputados.set_index('ideCadastro')['Andar'])

pairwise_df['Andar2'] = pairwise_df['deputado2'].map(deputados.set_index('ideCadastro')['Andar'])

pairwise_df['Estado1'] = pairwise_df['deputado1'].map(deputados.set_index('ideCadastro')['Estado'])

pairwise_df['Estado2'] = pairwise_df['deputado2'].map(deputados.set_index('ideCadastro')['Estado'])

pairwise_df['Partido1'] = pairwise_df['deputado1'].map(deputados.set_index('ideCadastro')['Partido'])

pairwise_df['Partido2'] = pairwise_df['deputado2'].map(deputados.set_index('ideCadastro')['Partido'])

pairwise_df['BLS1'] = pairwise_df['deputado1'].map(deputados.set_index('ideCadastro')['BLS'])

pairwise_df['BLS2'] = pairwise_df['deputado2'].map(deputados.set_index('ideCadastro')['BLS'])

pairwise_df['sexo1'] = pairwise_df['deputado1'].map(deputados.set_index('ideCadastro')['sexo'])

pairwise_df['sexo2'] = pairwise_df['deputado1'].map(deputados.set_index('ideCadastro')['sexo'])

pairwise_df['age1'] = pairwise_df['deputado1'].map(deputados.set_index('ideCadastro')['age'])

pairwise_df['age2'] = pairwise_df['deputado2'].map(deputados.set_index('ideCadastro')['age'])

pairwise_df['Legislatura'] = 56

pairwise_df['votacao_tipo'] = pairwise_df['idVotacao'].map(roll_calls.set_index('id')['votacao_tipo'])

pairwise_df['consenso'] = pairwise_df['idVotacao'].map(roll_calls.set_index('id')['abs(consenso)'])
pairwise_df['prop_sim'] = pairwise_df['idVotacao'].map(roll_calls.set_index('id')['prop_sim'])



pairwise_df['Lottery1'] = pairwise_df['deputado1'].map(deputados.set_index('ideCadastro')['Loteria'])

#pairwise_df['Lottery1'] = pairwise_df['deputado1'].map(lottery.set_index('deputado_id')['Lottery'])

pairwise_df['Lottery2'] = pairwise_df['deputado2'].map(deputados.set_index('ideCadastro')['Loteria'])

#pairwise_df['Lottery2'] = pairwise_df['deputado2'].map(lottery.set_index('deputado_id')['Lottery'])

pairwise_df['Lottery1'] = pairwise_df['Lottery1'].apply(lambda x: 0 if pd.isnull(x) else x)

pairwise_df['Lottery2'] = pairwise_df['Lottery2'].apply(lambda x: 0 if pd.isnull(x) else x)

pairwise_df['Lottery'] = np.where(pairwise_df['Lottery1'] + pairwise_df['Lottery2'] > 0, 1, 0)

pairwise_df['Ideology'] = abs(pairwise_df['BLS1'] - pairwise_df['BLS2'])

pairwise_df['age_dif'] = abs(pairwise_df['age1'] - pairwise_df['age2'])

pairwise_df['sexo'] = np.where(pairwise_df['sexo1'] == pairwise_df['sexo2'], 1, 0)

pairwise_df['Estado'] = np.where(pairwise_df['Estado1'] == pairwise_df['Estado2'], 1, 0)

pairwise_df['Partido'] = np.where(pairwise_df['Partido1'] == pairwise_df['Partido2'], 1, 0)

pairwise_df['PartidoNO'] = np.where(pairwise_df['Partido'] == 1, 0, 1)

pairwise_df['Senior'] = np.where(pairwise_df['Lottery1'] + pairwise_df['Lottery2'] == 1, 0, 1)


pairwise_df.dropna(subset=['Gabinete1','Gabinete2'], inplace=True)

# Drop entire columns from the DataFrame
pairwise_df.drop(columns=['Estado1', 'Estado2', 'Partido1', 'Partido2', 'Coalizao1', 'Coalizao2', 'age1', 'age2','sexo1', 'sexo2', 'Lottery1', 'Lottery2'], inplace=True)
pairwise_df.drop(columns=['Gabinete1','Gabinete2','Lottery','BLS1','BLS2'], inplace=True)

print(pairwise_df.columns)
print(pairwise_df_55_2.columns)

print(pairwise_df.head)
pairwise_df['Gabinete1'] = pairwise_df['Gabinete1'].astype(int)
pairwise_df['Gabinete2'] = pairwise_df['Gabinete2'].astype(int)
pairwise_df['Neighbor'] = pairwise_df['Neighbor'].astype(int)
pairwise_df = pairwise_df.dropna(subset=['Neighbor'])


print(pairwise_df['concatGabinete'])
pairwise_df['concatGabinete'] = pairwise_df['Gabinete1'].astype(str).str.replace('\.0', '') + pairwise_df['Gabinete2'].astype(str).str.replace('\.0', '')

pairwise_df['concatGabinete'] = pairwise_df['Gabinete1'].astype(str) + '_' + pairwise_df['Gabinete2'].astype(str)
neighbors = neighbors.drop_duplicates(subset='concatGabinete', keep='first')

# Check index type
print(pairwise_df['concatGabinete'].dtype)
print(offices['concatGabinete'].dtype)
neighbors.index = neighbors.index.astype(str)


import itertools

# Get unique values from the 'Gabinete1' column
unique_values = pairwise_df['Gabinete1'].unique()
unique_values = list(range(1,1001))
# Generate pairs of unique values
pairs = list(itertools.combinations(unique_values, 2))

# Create a new DataFrame 'offices' with pairs as the 'concatGabinete' column
offices = pd.DataFrame({'concatGabinete': ['{}_{}'.format(pair[0], pair[1]) for pair in pairs]})

# Map values from 'neighbors' DataFrame into 'offices' DataFrame
offices['Neighbor'] = offices['concatGabinete'].map(neighbors.set_index('concatGabinete')['Neighbor'])
offices['Neighbor'] = offices['Neighbor'].apply(lambda x: 0 if pd.isnull(x) else x)

offices['Neighbor'] = offices['Neighbor'].astype(int)

unique_values = pairwise_df['votacao_tipo'].unique()
print(unique_values)

pairwise_df['Neighbor'] = pairwise_df['concatGabinete'].map(offices.set_index('concatGabinete')['Neighbor'])

# Filter DataFrame to select rows where 'Neighbor' column contains NaN values
nan_rows = pairwise_df[pairwise_df['Neighbor'].isna()]

# Get the values from the 'concatGabinete' column for these rows
corresponding_concatGabinete_values = nan_rows['Neighbor']

print(corresponding_concatGabinete_values)


# Get unique values from the 'Gabinete1' column
unique_values = pairwise_df_56['Gabinete1'].unique()
unique_values = np.append(unique_values, 613)
unique_values = list(range(1,1001))


# Generate pairs of unique values
pairs = list(itertools.combinations(unique_values, 2))

# Create pairs in both forms
all_pairs = []
for pair in pairs:
    pair_1 = '{}_{}'.format(pair[0], pair[1])
    pair_2 = '{}_{}'.format(pair[1], pair[0])
    all_pairs.append(pair_1)
    all_pairs.append(pair_2)

# Create a new DataFrame 'offices' with pairs as the 'concatGabinete' column
offices = pd.DataFrame({'concatGabinete': all_pairs})
neighbors.drop_duplicates(subset='concatGabinete', keep='first', inplace=True)

# Map values from 'neighbors' DataFrame into 'offices' DataFrame
offices['Neighbor'] = offices['concatGabinete'].map(neighbors.set_index('concatGabinete')['Neighbor'])



pairwise_df.dropna(subset=['Neighbor'], inplace=True)












# Map corresponding values from neighbors DataFrame to pairwise_df based on 'concatGabinete'
pairwise_df['Neighbor'] = pairwise_df['concatGabinete'].map(offices['Neighbor'])
neighbor_value = pairwise_df.loc[pairwise_df['concatGabinete'] == '201_202', 'Neighbor'].values[0]
# Check if any rows match the condition
if '201_202' in pairwise_df['concatGabinete'].values:
    # Find the row where 'concatGabinete' is equal to '201_202' and get the corresponding value in the 'Neighbor' column
    neighbor_value = pairwise_df.loc[pairwise_df['concatGabinete'] == '201_202', 'Neighbor'].values[0]
else:
    # Handle case where '201_202' does not exist in 'concatGabinete' column
    neighbor_value = None  # or any other appropriate handling

# Map corresponding values from neighbors DataFrame to pairwise_df based on concatGabinete
pairwise_df['Neighbor'] = pairwise_df['concatGabinete'].map(neighbors['Neighbor'])
print(pairwise_df['Neighbor'])
print(pairwise_df.head())

pairwise_df["Neighbor"]= pairwise_df["concatGabinete"].map(neighbors_dict)


pairwise_df['Neighbor'] = pairwise_df['concatGabinete'].map(neighbors.set_index('concatGabinete')['Neighbor'])
mismatched_values = pairwise_df[~pairwise_df['concatGabinete'].isin(neighbors['concatGabinete'])]['concatGabinete']
print(mismatched_values)


pairwise_df['Andar'] = np.where(pairwise_df['Andar1'] == pairwise_df['Andar2'], 1, 0)

pairwise_df['Anexo'] = np.where(pairwise_df['Anexo1'] == pairwise_df['Anexo2'], 1, 0)

pairwise_df = df

pairwise_df_subset_2 = pairwise_df[pairwise_df['Lottery'] == 1].copy()
pairwise_df_subset56 = pairwise_df_subset


export_csv = pairwise_df_subset56.to_excel ('pairwise_df_subset56.xlsx', index = True, header=True, encoding='utf-8-sig', engine='xlsxwriter' ) #Don't forget to add '.csv' at the end of the path

pairwise_df_subset = pairwise_matrix

## Descriptive Statistics (input data)

average = pairwise_df_subset['Convergence'].mean()
print(average)






### CREATING INDIVIDUAL DATA

pairwise_df = pairwise_df_subset56
pairwise_df_subset56 = pairwise_df

pairwise_df['Coalizao1'] = pairwise_df['deputado1'].map(deputados.set_index('ideCadastro')['Coalizao'])
pairwise_df['Coalizao2'] = pairwise_df['deputado2'].map(deputados.set_index('ideCadastro')['Coalizao'])
pairwise_df['Coalizao'] = np.where(pairwise_df['Coalizao1'] == pairwise_df['Coalizao2'], 1, 0)
pairwise_df['CoalizaoNO'] = np.where(pairwise_df['Coalizao'] == 1, 0, 1)



pairwise_df['PropOtherParty'] = pairwise_df['deputado_id'].map(deputados.set_index('ideCadastro')['PropOtherParty'])
pairwise_df['PropOtherCoal'] = pairwise_df['deputado_id'].map(deputados.set_index('ideCadastro')['PropOtherCoal'])

pairwise_df['Lottery'] = pairwise_df['deputado_id'].map(deputados.set_index('ideCadastro')['Loteria'])
pairwise_df['Lottery'] = pairwise_df['Lottery'].apply(lambda x: 0 if pd.isnull(x) else x)

pairwise_df['BLS'] = pairwise_df['deputado_id'].map(deputados.set_index('ideCadastro')['BLS'])


pairwise_df_subset_2 = pairwise_df[pairwise_df['Lottery'] == 1].copy()


pairwise_df_subset_56 = pairwise_df_subset
pairwise_df_subset56['numLegislatura'] = '56'

pairwise_df_subset= pd.concat([pairwise_df_subset_1, pairwise_df_subset_2])
pairwise_df_subset = df_notna
import matplotlib.pyplot as plt

# Replace "NC" and "PropOtherParty" with the actual column names in your dataset

other_party_values = pairwise_df_subset["PropOtherCoal"]
print(pairwise_df.columns)
# Calculate descriptive statistics for the NC column
nc_values = pairwise_df["sexo"]
nc_stats = pd.DataFrame(nc_values.describe())
print(nc_stats)
# Create a boxplot of the NC values
plt.boxplot(nc_values)

# Set plot title and labels
plt.title("Distribution of NC Values")
plt.ylabel("NC Values")

# Show the plot
plt.show()

# Show descriptive statistics for the NC column
print("Descriptive statistics for NC column:")




import seaborn as sns

# Replace "PropOtherParty" with the actual column name in your dataset
other_party_values = pairwise_df_subset["normPOP"]

# Create a kernel density plot of the PropOtherParty values
sns.kdeplot(other_party_values)

# Set plot title and labels
plt.title("Distribution of normPOP Values")
plt.xlabel("logPOP Values")
plt.ylabel("Density")

# Show the plot
plt.show()

# Replace "PropOtherParty" with the actual column name in your dataset
other_party_values = pairwise_df["convergence"]

# Get descriptive statistics for the PropOtherParty column
other_party_stats = other_party_values.describe()

# Print the statistics
print("Descriptive statistics for logPOP column:")
print(other_party_stats)


import seaborn as sns
import matplotlib.pyplot as plt

# Replace "pairwise_df_subset" with the actual name of your dataframe
sns.set(style="whitegrid")
fig, axs = plt.subplots(ncols=2, figsize=(12, 6))

# Plot the distribution of "PropOtherParty"
sns.histplot(data=pairwise_df_subset, x="PropOtherCoal", kde=True, ax=axs[0])
axs[0].set(xlabel="PropOtherCoal", ylabel="Count")
axs[0].set_title("Distribution of PropOtherParty")

# Plot the scatter plot between "PropOtherParty" and "NC"
sns.scatterplot(data=pairwise_df_subset, x="PropOtherParty", y="NC", ax=axs[1])
axs[1].set(xlabel="PropOtherParty", ylabel="NC")
axs[1].set_title("Relationship between PropOtherParty and NC")

# Display the plot
plt.show()

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Replace "pairwise_df_subset" with the actual name of your dataframe
df = pairwise_df_subset.copy()

# Normalize the "PropOtherParty" column
prop_min = df["PropOtherParty"].min()
prop_range = df["PropOtherParty"].max() - prop_min
df["PropOtherParty_norm"] = (df["PropOtherParty"] - prop_min) / prop_range

sns.set(style="whitegrid")
fig, axs = plt.subplots(ncols=2, figsize=(12, 6))

# Plot the distribution of "PropOtherParty_norm"
sns.histplot(data=df, x="PropOtherParty_norm", kde=True, ax=axs[0])
axs[0].set(xlabel="PropOtherParty (normalized)", ylabel="Count")
axs[0].set_title("Distribution of PropOtherParty (normalized)")

# Plot the scatter plot between "PropOtherParty_norm" and "NC"
sns.scatterplot(data=df, x="PropOtherParty_norm", y="NC", ax=axs[1])
axs[1].set(xlabel="PropOtherParty (normalized)", ylabel="NC")
axs[1].set_title("Relationship between PropOtherParty and NC")

# Display the plot
plt.show()


import numpy as np

# Calculate normalized values of "PropOtherParty" and add them as a new column "normPOP"
pairwise_df_subset["normPOP"] = (pairwise_df_subset["PropOtherParty"] - pairwise_df_subset["PropOtherParty"].mean()) / pairwise_df_subset["PropOtherParty"].std()

# Calculate the log values of "PropOtherParty" and add them as a new column "logPOP"
pairwise_df_subset["logPOP"] = np.log(pairwise_df_subset["PropOtherParty"])


average_NC = deputados["PropOtherCoal"].mean()
print("The average value of NC is:", average_NC)





pairwise_df_subset = pairwise_df_subset[~(pairwise_df_subset['voto'] == 2)]


column_stats = pairwise_df_subset['voto'].describe()

# Print the statistics
print(column_stats)
value_counts = pairwise_df_subset['Neighbor'].value_counts()

# Calculate descriptive statistics for the value counts
value_stats = value_counts.describe()

# Print the value counts and value statistics
print(value_counts)
print(value_stats)

pairwise_df_55 = pairwise_df
print(pairwise_df_55.columns)
print(pairwise_df_56.columns)
# Reorder the columns of df2 to match the order of columns in df1
pairwise_df_55 = pairwise_df_55[pairwise_df_56.columns]

# Concatenate the two DataFrames along the columns axis
pairwise_df = pd.concat([pairwise_df_55, pairwise_df_56], axis=1)

pairwise_df_subset_f = pairwise_df_subset.dropna(subset=['dist'])


pairwise_df_subset = pairwise_df_subset_f




import modin.pandas as mpd


pairwise_df_55 = mpd.DataFrame(pairwise_df_55_pandas)



import vaex
import sqlite3

# Establish a connection to the SQLite database
conn = sqlite3.connect('C:/Users/felip/Desktop/Economia/PhD/Thesis/Code/pairwise_56_agree1.db')

# Define the SQL query
query = "SELECT * FROM pairwise_56_agree1"

# Read the SQL query into a Pandas DataFrame
import pandas as pd
pairwise_df_56_pandas = pd.read_sql(query, conn)

# Convert the Pandas DataFrame to a Vaex DataFrame
pairwise_df_55 = vaex.from_pandas(pairwise_df_55_pandas)

# Display the first few rows of the Vaex DataFrame
print(pairwise_df_55.head())


# Establish a connection to the SQLite database
conn_str = 'sqlite://C:/Users/felip/Desktop/Economia/PhD/Thesis/Code/pairwise_55_agree1.db'

# Read data from SQL database into a Modin DataFrame using the parallel implementation
query = "SELECT * FROM pairwise_56_agree1"
modin_df = mpd.read_sql_table(query, conn_str)

import modin.pandas as mpd
import ray
ray.init()
from modin.engines import RayEngine
from modin.experimental.engines import OmnisciOnRayEngine
from sqlalchemy import create_engine

del pairwise_df
import modin.pandas as pd

# Establish a connection to the SQL database
# Replace 'sqlite:///your_database.db' with the connection string for your database
engine = create_engine('sqlite://C:/Users/felip/Desktop/Economia/PhD/Thesis/Code/pairwise_56_agree1.db')

# Read data from SQL database into a Modin DataFrame using the parallel implementation
query = "SELECT deputado1, deputado2, convergence, Neighbor, Legislatura, votacao_tipo, age_dif, sexo, Estado, Partido, PartidoNO, Coalizao, CoalizaoNO, Andar, Senior FROM pairwise_56_agree1"

# Define the URI with the correct absolute path to the SQLite database file
uri = 'sqlite:///C:/Users/felip/OneDrive/Desktop/Economia/PhD/Thesis/Code/pairwise_56_agree1.db'

# Read data from the SQLite database into a Polars DataFrame
pairwise_df_56 = pl.read_database_uri(query=query, uri=uri)

pairwise_df_55.columns
selected_columns = ['deputado1','deputado2','convergence', 'Neighbor', 'Legislatura', 'votacao_tipo','age_dif','sexo','Estado','Partido','PartidoNO','Coalizao','CoalizaoNO','Andar']
pairwise_df_55 = pairwise_df_55.select(['deputado1','deputado2','convergence', 'Neighbor', 'Legislatura', 'votacao_tipo','age_dif','sexo','Estado','Partido','PartidoNO','Coalizao','CoalizaoNO','Andar'])


pairwise_df['votacao_cod'] = pairwise_df['votacao_tipo'].map(prop_tipo.set_index('proposicao_siglaTipo')['proposicao_codTipo'])
pairwise_df['votacao_cod'] = pairwise_df['votacao_cod'].astype(np.int32)
# Filter DataFrame to select rows where 'Neighbor' column contains NaN values
nan_rows = pairwise_df[pairwise_df['Legislatura'].isna()]
# Get the values from the 'concatGabinete' column for these rows
corresponding_concatGabinete_values = nan_rows['votacao_cod']

print(corresponding_concatGabinete_values)

pairwise_df.dropna(subset=['votacao_cod'], inplace=True)

# Cast the 'convergence' column to int32 and create a new DataFrame with the modified column
pairwise_df_55['votacao_tipo'] = pairwise_df_55.to_dummies(columns=['votacao_tipo'])
pairwise_df_56 = pairwise_df_56.select(pl.col('deputado1').cast(pl.Int32),
                                 pl.col('deputado2').cast(pl.Int32),
                                 pl.col('convergence').cast(pl.Int32),
                                 pl.col('Neighbor').cast(pl.Int32),
                                 pl.col('Legislatura').cast(pl.Int32),
                                 pl.col('votacao_tipo').cast(pl.Utf8),
                                 pl.col('age_dif').cast(pl.Int32),
                                 pl.col('sexo').cast(pl.Int32),
                                 pl.col('Estado').cast(pl.Int32),
                                 pl.col('Partido').cast(pl.Int32),
                                 pl.col('PartidoNO').cast(pl.Int32),
                                 pl.col('Coalizao').cast(pl.Int32),
                                 pl.col('CoalizaoNO').cast(pl.Int32),
                                 pl.col('Andar').cast(pl.Int32),
                                 pl.col('Senior').cast(pl.Int32))

print(pairwise_df.head)
pairwise_df = pairwise_df.to_pandas()

!pip install adbc-driver-sqlite pyarrow
!pip install -U pyfixest
!pip install pyfixest

print(pairwise_df_55.head)

import pyfixest as pf
from pyfixest.estimation import feols
from pyfixest.summarize import summary

fixest = pf.Fixest(data = pairwise_df)
print(pairwise_df.columns)
# OLS Estimation: estimate multiple models at once
fit = feols("convergence ~ Neighbor + Partido + Estado | csw0(votacao_tipo, Legislatura )", data = pairwise_df, vcov = {'deputado1':'group_id'})
fit = feols("convergence ~ Neighbor + Partido + Estado ", data = pairwise_df, vcov = {'deputado1':'group_id'})
fit = feols("convergence ~ Neighbor", data = pairwise_df)

print(fit.summary)
fit.tidy()

del pairwise_df
!pip install polars
import polars as pl


polars_df = pl.from_pandas(pairwise_df)
del pairwise_df
# Display the Polars DataFrame
print(polars_df)

## OLS

from fixedeffect.fe import fixedeffect, did, getfe

pairwise_df_subset = pd.read_excel("pairwise_df_subset.xlsx", sheet_name="Sheet2")


pairwise_df_subset ['NC'] = np.where(pairwise_df_subset ['orientacao'] == pairwise_df_subset ['voto'], 0, 1)


from linearmodels.panel import PanelOLS
mod = PanelOLS.from_formula("convergence ~ Neighbor + Coalizao + Partido + EntityEffects + TimeEffects",
                            data=pairwise_df.set_index(["votacao_tipo", "Legislatura"]))



mod = PanelOLS.from_formula("lwage ~ expersq+union+married+hours+EntityEffects+TimeEffects",
                            data=data.set_index(["nr", "year"]))

result = mod.fit(cov_type='clustered', cluster_entity=True, cluster_time=True)
print(result.summary)

print(pairwise_df.columns)



# Model 1: Floor
model1 = smf.ols('convergence ~ Neighbor + C(Legislatura) + C(votacao_cod)', data=pairwise_df).fit(cov_type='cluster', cov_kwds={'groups': pairwise_df['deputado1']})
model1 = smf.ols('convergence ~ Neighbor + Coalizao + age_dif + Estado + Senior + C(Legislatura) + C(votacao_cod)', data=pairwise_df).fit(cov_type='cluster', cov_kwds={'groups': pairwise_df['deputado1']})
model1 = smf.ols('convergence ~ Neighbor + Partido + Estado + Senior + C(Legislatura) + C(votacao_cod)', data=pairwise_df).fit(cov_type='cluster', cov_kwds={'groups': pairwise_df['deputado1']})

model1 = smf.ols('convergence ~ Coalizao + Estado + Senior + age_dif + Neighbor:Coalizao + Neighbor:CoalizaoNO + C(votacao_cod) + C(Legislatura)', data=pairwise_df).fit(cov_type='cluster', cov_kwds={'groups': pairwise_df['deputado1']})
model1 = smf.ols('convergence ~ Neighbor + Coalizao + Partido', data=pairwise_df_55).fit(cov_type='cluster', cov_kwds={'groups': pairwise_df['deputado1']})
model1 = smf.ols('convergence ~ Neighbor + Partido + sexo + age_dif + C(votacao_tipo) + C(Legislatura)', data=pairwise_df).fit(cov_type='cluster', cov_kwds={'groups': pairwise_df['deputado1']})

cov_type='cluster', cov_kwds={'groups': df['id']})

model1 = smf.ols('NC ~ PropOtherCoal + lnDistCapital  + C(propTipo) + C(numLegislatura) + C(Andar) + C(Coalizao)', data=pairwise_df_subset).fit(cov_type='cluster', cov_kwds={'groups': pairwise_df_subset['deputado_id']})
model1_c = model1.get_robustcov_results(cov_type='cluster', groups=pairwise_df_subset['deputado_id'])

print(model1.summary())
print(model1_c.summary())
stargazer = Stargazer([model1])
print(stargazer.render_latex())

# Breusch-Pagan test for heteroskedasticity
_, pvalue_bp, _, _ = sm.stats.diagnostic.het_breuschpagan(model1.resid, model1.model.exog)
print(f"Breusch-Pagan test p-value: {pvalue_bp}")

# White test for heteroskedasticity
_, pvalue_white, _, _ = sm.stats.diagnostic.het_white(model1.resid, model1.model.exog)
print(f"White test p-value: {pvalue_white}")

robust = model1.fit(cov_type='HC3')
print(robust.summary())

# Select a subset of columns to include in the correlation matrix
cols = ['Andar', 'Partido', 'col3']

# Calculate the correlation matrix for the selected columns
corr_matrix = df[cols].corr()

# Print the correlation matrix
print(corr_matrix)




model1_t = smf.ols('Convergence ~ Andar + Partido + Estado + C(Lottery) + C(Partido)', data=pairwise_df_subset).fit()
model1_t_c = model1_t.get_robustcov_results(cov_type='cluster', groups=pairwise_df_subset['deputado1'])

print(model1_t.summary())
print(model1_t_c.summary())

model1_h = smf.ols('Convergence ~ Estado + Partido + Neighbor:Partido + Neighbor:PartidoNO + C(Legislatura) + C(Strata)', data=pairwise_df_subset).fit(cov_type='cluster', cov_kwds={'groups': pairwise_df_subset['deputado1']})
model1_h_c = model1_h.get_robustcov_results(cov_type='cluster', groups=pairwise_df_subset['deputado1'])

print(model1_h.summary())
print(model1_h_c.summary())



stargazer = Stargazer([model1_h])
print(stargazer.render_latex())

# Model 2: Building

model2 = smf.ols('Convergence ~ Anexo', data=pairwise_df_subset).fit()
model2_c = model2.get_robustcov_results(cov_type='cluster', groups=pairwise_df_subset['deputado1'])

print(model2.summary())
print(model2_c.summary())

model2_t = smf.ols('Convergence ~ Anexo + Partido + Estado', data=pairwise_df_subset).fit()
model2_t_c = model2_t.get_robustcov_results(cov_type='cluster', groups=pairwise_df_subset['deputado1'])

print(model2_t.summary())
print(model2_t_c.summary())


# Rename the 'A' column to 'NewColumn'
pairwise_df_subset = pairwise_df_subset.rename(columns={'1_dist': 'distInv'})


'''
import pandas as pd
from itertools import combinations

# Read the data into a pandas dataframe
df = pd.read_csv('voting_data.csv')

# Get unique roll call IDs
unique_ids = roll_call_amendments_votes['idVotacao'].unique()

# Create an empty list to store the pairwise data
pairwise_data = []

# Loop over the unique roll call IDs
for roll_call_id in unique_ids:

    # Get the subset of the data for this roll call
    roll_call_data = roll_call_amendments_votes[roll_call_amendments_votes['idVotacao'] == roll_call_id]

    # Get all possible pairs of deputados for this roll call
    pairs = combinations(roll_call_data['deputado_id'].unique(), 2)

    # Loop over the pairs
    for pair in pairs:
        deputado1, deputado2 = pair

        # Get the subset of the data for this pair of deputados
        pair_data = roll_call_data[(roll_call_data['deputado_id'] == deputado1) | (roll_call_data['deputado_id'] == deputado2)]

        # Check if the deputados had the same vote or not
        convergence = pair_data['voto'].nunique() == 1

        # Calculate the percentage of times they had the same vote
        num_same = pair_data.groupby('voto')['deputado_id'].nunique().min()
        num_total = pair_data['deputado_id'].nunique()
        percentage_same = num_same / num_total

        # Add the data to the pairwise_data list
        pairwise_data.append((deputado1, deputado2, convergence, percentage_same))

# Convert the pairwise_data list to a pandas dataframe
pairwise_df = pd.DataFrame(pairwise_data, columns=['deputado1', 'deputado2', 'convergence', 'percentage_same'])

# Print the first few rows of the dataframe
print(pairwise_df.head())

'''

pairwise_df_subset.rename(columns = {'deputado_id_1':'deputado1', 'deputado_id_2':'deputado2'}, inplace = True) 

pairwise_df_55_2 = pairwise_df
pairwise_df = pairwise_df_55

pairwise_df = pl.concat([pairwise_df_55, pairwise_df_56])
del pairwise_df_55
del pairwise_df_56
print(pairwise_df.head)
print(pairwise_df.columns)

pairwise_df_56 = pairwise_df_56[pairwise_df_55.columns]

print(pairwise_df_55.columns)
print(pairwise_df_56.columns)

pairwise_df_55 = pairwise_df_55[pairwise_df_55['prop_sim'] <= 0.90]
# Define the list of values to filter
values_to_filter = ['PEC', 'PDC', 'PL', 'PLP', 'REQ', 'PRC', 'MPV','VETO']

# Filter rows where 'votacao_tipo' column contains any of the values in the list
pairwise_df_55 = pairwise_df_55[pairwise_df_55['votacao_tipo'].isin(values_to_filter)]

# Step 1: Connect to SQLite database
conn = sqlite3.connect('pairwise_df_agree1.db')

# Step 2: Create a cursor object
cursor = conn.cursor()

# Step 3: Create a table schema
table_schema = """
CREATE TABLE IF NOT EXISTS pairwise_55 (
    deputado1 INTEGER,
    deputado2 INTEGER,
    idVotacao INTEGER,
    convergence INTEGER,
    Lottery INTEGER,
    Gabinete1 INTEGER,
    Gabinete2 INTEGER,
    Andar1 INTEGER,
    Andar2 INTEGER,
    BLS1 INTEGER,
    BLS2 INTEGER,
    Legislatura INTEGER,
    votacao_tipo TEXT,
    consenso INTEGER,
    prop_sim INTEGER,
    Ideology INTEGER,
    age_dif INTEGER,
    sexo INTEGER,
    Estado INTEGER,
    Partido INTEGER,
    PartidoNO INTEGER,
    Coalizao INTEGER,
    CoalizaoNO INTEGER,
    Senior INTEGER,    
    concatGabinete TEXT,
    Neighbor INTEGER,
    Andar INTEGER    
);
"""

# Step 3: Create a table schema
table_schema = """
CREATE TABLE IF NOT EXISTS pairwise_55 (
    deputado1 INTEGER,
    deputado2 INTEGER,
    idVotacao INTEGER,
    convergence INTEGER,
    Lottery1 INTEGER,
    Lottery2 INTEGER,
    Lottery INTEGER
);
"""

# Step 4: Execute the table schema
cursor.execute(table_schema)



# Step 5: Import data from a Pandas DataFrame
# Assuming you have a DataFrame named df with your data
pairwise_df.to_sql('pairwise_df_agree1', conn, if_exists='replace', index=False)

# Step 6: Perform SQL queries
# Example: Select all rows from the table
query = "SELECT * FROM pairwise_55_agree1"
pairwise_df_55 = pd.read_sql(query, conn)

# Step 7: Close cursor and connection
cursor.close()
conn.close()


pairwise_df_55 = pd.read_sql_table('your_table_name', 'sqlite:///your_database_file.db')


