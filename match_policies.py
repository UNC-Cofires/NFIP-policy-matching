import numpy as np
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import os

### *** HELPER FUNCTIONS *** ###

def temporal_matching(df,buffer_days=0):
    """
    This function will attempt to match NFIP policy records expiring on a given date with those that go
    into effect on the same date (+/- some number of buffer days), with the goal of identifying policy 
    renewals that are likely to have ocurred at the same property. 
    
    This function should be applied to dataframes that have already been grouped or filtered based on 
    time-invariant property characteristics to narrow down the number of options. 
    """
    
    match_df = pd.DataFrame(index=df.index)
    match_df['is_distinct'] = ~df[['policyEffectiveDate','policyTerminationDate']].duplicated(keep=False)
    match_df['num_match'] = np.zeros(len(df),dtype=int)
    match_df['match_id'] = pd.NA
        
    for i in range(len(df)):
        
        termination_date = df['policyTerminationDate'].iloc[i]
        m = (np.abs((df['policyEffectiveDate'] - termination_date).dt.days) <= buffer_days)
        
        num_match = np.sum(m)
        match_df.iloc[i,1] = num_match
        
        if num_match == 1:
            match_df.iloc[i,2] = df[m].index.values[0]
        
    return match_df

### *** INITIAL SETUP *** ###

pwd = os.getcwd()

# Get code of state to run
state_idx = int(os.environ['SLURM_ARRAY_TASK_ID'])
state_abbreviations = np.loadtxt('state_abbreviations.txt',dtype=str)
state = state_abbreviations[state_idx]

# Create output folder
outfolder = os.path.join(pwd,f'match_info/{state}')
if not os.path.exists(outfolder):
    os.makedirs(outfolder,exist_ok=True)

# Specify path to OpenFEMA data
openfema_dir = '/proj/characklab/projects/kieranf/OpenFEMA'

# Specify property-specific columns that will be used for matching
time_invariant_columns = ['latitude',
                          'longitude',
                          'censusBlockGroupFips',
                          'ratedFloodZone',
                          'reportedZipCode',
                          'originalNBDate',
                          'originalConstructionDate',
                          'numberOfFloorsInInsuredBuilding']

usecols = ['id','propertyState','policyEffectiveDate','policyTerminationDate'] + time_invariant_columns

policies_path = os.path.join(openfema_dir,'FimaNfipPolicies.parquet')
policies = pd.read_parquet(policies_path,engine='pyarrow',columns=usecols,filters=[('propertyState','=',state)])

# Filter out pre-2009 policy data (doesn't reflect full policy base in force)
cutoff_date = '2010-01-01'

m = (policies['policyEffectiveDate']>=cutoff_date)
policies = policies[m].reset_index(drop=True).set_index('id')
policies.sort_values(by='policyEffectiveDate',inplace=True)

original_policy_ids = policies.index.to_list()

# Filter out policies that are missing data on the attributes we'll use for matching

incomplete_data_mask = policies[time_invariant_columns].isna().any(axis=1)
incomplete_data_ids = policies[incomplete_data_mask].index.to_list()

policies = policies[~incomplete_data_mask]

# Group policies based on time-invariant characteristics, then attempt to identify policy renewals
match_info = policies.groupby(time_invariant_columns,group_keys=False).apply(temporal_matching)

# Save to file
outname = os.path.join(outfolder,f'{state}_match_info.parquet')
match_info.to_parquet(outname)

# Join info to policy data
policies = policies.join(match_info,on='id',how='left')

## Represent policy renewals through time as directed graph

# When building directed graph, leave out edges that are part of one-to-many matches
multiple_match_mask = (~match_info['is_distinct'])|(match_info['num_match'] > 1)
match_info = match_info[~multiple_match_mask]

# Get list of edges/nodes
nodelist = match_info.index.to_numpy()
edgelist = match_info['match_id'].reset_index().dropna().to_numpy()

# Initialize networkx directed graph object 
G = nx.DiGraph()
G.add_nodes_from(nodelist)
G.add_edges_from(edgelist)

# Get root nodes (i.e., first observation of policyholder in dataset) 
root_nodes = [node for node in G.nodes if G.in_degree(node) == 0]

# Create a dataframe describing policyholder "stints" (e.g., periods of continuous insurance coverage)

stint_id_list = []
stint_policy_list = []

for i,node in enumerate(root_nodes):
    stint_id = state + '_' + str(i)
    member_policies = [node]+list(nx.descendants(G,node))
    stint_id_list += [stint_id]*len(member_policies)
    stint_policy_list += member_policies
    
stint_info = pd.DataFrame(data={'stint_id':stint_id_list,'id':stint_policy_list}).set_index('id')
stint_info = stint_info.join(policies[['num_match','match_id']],on='id',how='left')
policies = policies.join(stint_info[['stint_id']],on='id',how='left')

# Make NA values consistent 
stint_info.loc[stint_info['match_id'].isna(),'match_id'] = pd.NA

# Save to file
outname = os.path.join(outfolder,f'{state}_stint_info.parquet')
stint_info.to_parquet(outname)

# Get list of policy ids that aren't members of a stint
unmatched_policy_ids = policies[policies['stint_id'].isna()].index.to_list()
matched_policy_ids = policies[~policies['stint_id'].isna()].index.to_list()

# Create dataframe that we can use to track what happened to each policy in the original dataset
# (e.g., discarded due to missing data, matched, unmatched, etc.)

selection_flow = pd.DataFrame({'id':original_policy_ids,'outcome':pd.NA})
selection_flow.loc[selection_flow['id'].isin(incomplete_data_ids),'outcome'] = 'Excluded - Missing data'
selection_flow.loc[selection_flow['id'].isin(unmatched_policy_ids),'outcome'] = 'Excluded - Multiple matches'
selection_flow.loc[selection_flow['id'].isin(matched_policy_ids),'outcome'] = 'Included'

outname = os.path.join(outfolder,f'{state}_selection_flow.parquet')
selection_flow.to_parquet(outname)