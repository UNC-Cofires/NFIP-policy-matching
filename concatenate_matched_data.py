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

# Read in NFIP policy data that includes full-risk premiums obtained via FOIA request
foia_policies_path = '/proj/characklab/flooddata/FOIA/FimaNfipPolicies_FOIA.parquet'
foia_policies = pd.read_parquet(foia_policies_path,engine='pyarrow') 

# Clean certain columns in FOIA data to match OpenFEMA conventions / data types
foia_policies['reportedZipCode'] = foia_policies['reportedZipCode'].apply(lambda x: f'{x:0>5}').astype('string[pyarrow]')
for col in ['policyEffectiveDate','policyTerminationDate','originalNBDate','originalConstructionDate']:
    foia_policies[col] = foia_policies[col].astype('datetime64[us, UTC]')
    
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

# Attach RR2.0 info to policies that have it

match_cols = ['latitude',
              'longitude',
              'countyCode',
              'ratedFloodZone',
              'reportedZipCode',
              'originalNBDate',
              'numberOfFloorsInInsuredBuilding',
              'policyEffectiveDate',
              'policyTerminationDate',
              'totalInsurancePremiumOfThePolicy']

m1 = policies[match_cols].duplicated(keep=False)
m2 = policies[match_cols].isna().any(axis=1)
m3 = ~(m1|m2)

foia_matches = pd.merge(foia_policies[['fullRiskPremium']+match_cols],policies[m3][['id']+match_cols],on=match_cols,how='left')
foia_matches = foia_matches.dropna(subset=['id'])

policies['rr2_foia_match_indicator'] = policies['id'].isin(foia_matches['id']).astype('int64[pyarrow]')
policies = pd.merge(policies,foia_matches[['id','fullRiskPremium']],on='id',how='left')

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

policy_matching_outcomes = str(policies['matching_outcome'].value_counts())
claim_matching_outcomes = str(claims['matching_outcome'].value_counts())

with open('report.txt','w') as f:
    
    f.write('\n\n*** Policy-to-Policy Matching ***\n\n')
    f.write(f'Starting number of policies: {n_policies}\n\n')
    f.write(policy_matching_outcomes)
    
    f.write('\n\n*** Claim-to-Policy Matching ***\n\n')
    f.write(f'Starting number of claims: {n_claims}\n\n')
    f.write(claim_matching_outcomes)