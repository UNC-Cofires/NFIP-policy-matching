import numpy as np
import pandas as pd
import os

# Get current working directory 
pwd = os.getcwd()

state_abbreviations = np.loadtxt('state_abbreviations.txt',dtype=str)

# Read in info on identified stints and selection flow
stint_filepaths = [os.path.join(pwd,f'match_info/{state}/{state}_stint_info.parquet') for state in state_abbreviations]
policy_selection_filepaths = [os.path.join(pwd,f'match_info/{state}/{state}_selection_flow.parquet') for state in state_abbreviations]
stint_info = pd.concat([pd.read_parquet(filepath) for filepath in stint_filepaths]).reset_index()
stint_info = stint_info.convert_dtypes(dtype_backend='pyarrow')
policy_selection_flow = pd.concat([pd.read_parquet(filepath) for filepath in policy_selection_filepaths]).reset_index(drop=True)

# Read in data on matched claims / policies and selection flow
claim_match_filepaths = [os.path.join(pwd,f'match_info/{state}/{state}_claim_match_info.parquet') for state in state_abbreviations]
claim_selection_filepaths = [os.path.join(pwd,f'match_info/{state}/{state}_claim_selection_flow.parquet') for state in state_abbreviations]
claim_match_info = pd.concat([pd.read_parquet(filepath) for filepath in claim_match_filepaths]).reset_index(drop=True)
claim_selection_flow = pd.concat([pd.read_parquet(filepath) for filepath in claim_selection_filepaths]).reset_index(drop=True)

# Read in OpenFEMA policy & claim data
openfema_dir = '/proj/characklab/projects/kieranf/OpenFEMA'

policies_path = os.path.join(openfema_dir,'FimaNfipPolicies.parquet')
policies = pd.read_parquet(policies_path,engine='pyarrow')

claims_path = os.path.join(openfema_dir,'FimaNfipClaims.parquet')
claims = pd.read_parquet(claims_path,engine='pyarrow')

# Get claim and policy info from time period of interest
claims = claims[claims['id'].isin(claim_selection_flow['id'])]
policies = policies[policies['id'].isin(policy_selection_flow['id'])]

# Attach matching outcome
claims = pd.merge(claims,claim_selection_flow,on='id',how='left').rename(columns={'outcome':'matching_outcome'})
policies = pd.merge(policies,policy_selection_flow,on='id',how='left').rename(columns={'outcome':'matching_outcome'})

# Attach policy id to claims
claims = pd.merge(claims,claim_match_info.rename(columns={'claim_id':'id'}),on='id',how='left')

# Attach stint info to policies
policies = pd.merge(policies,stint_info,on='id',how='left')
policies = policies.sort_values(by=['stint_id','policyEffectiveDate']).reset_index(drop=True)

# Save to file
outname = os.path.join(pwd,'FimaNfipPolicies_matched.parquet')
policies.to_parquet(outname)

outname = os.path.join(pwd,'FimaNfipClaims_matched.parquet')
claims.to_parquet(outname)

# Generate report
outname = os.path.join(pwd,'report.txt')

n_policies = len(policies)
n_claims = len(claims)
n_stints = len(policies['stint_id'].dropna().unique())

policy_matching_outcomes = str(policies['matching_outcome'].value_counts())
claim_matching_outcomes = str(claims['matching_outcome'].value_counts())

with open('report.txt','w') as f:
    
    f.write('\n\n*** Policy-to-Policy Matching ***\n\n')
    f.write(f'Starting number of policies: {n_policies}\n\n')
    f.write(policy_matching_outcomes)
    f.write(f'\nNumber of unique stints: {n_stints}')
    
    f.write('\n\n*** Claim-to-Policy Matching ***\n\n')
    f.write(f'Starting number of claims: {n_claims}\n\n')
    f.write(claim_matching_outcomes)