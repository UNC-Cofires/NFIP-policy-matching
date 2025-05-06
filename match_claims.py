import numpy as np
import pandas as pd
import os

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
match_cols = ['latitude',
              'longitude',
              'censusBlockGroupFips',
              'ratedFloodZone',
              'reportedZipCode',
              'originalNBDate',
              'originalConstructionDate',
              'numberOfFloorsInInsuredBuilding',
              'totalBuildingInsuranceCoverage',
              'totalContentsInsuranceCoverage',
              'buildingDeductibleCode',
              'contentsDeductibleCode']

claim_match_cols = ['latitude',
                    'longitude',
                    'censusBlockGroupFips',
                    'ratedFloodZone',
                    'reportedZipCode',
                    'originalNBDate',
                    'originalConstructionDate',
                    'numberOfFloorsInTheInsuredBuilding',
                    'totalBuildingInsuranceCoverage',
                    'totalContentsInsuranceCoverage',
                    'buildingDeductibleCode',
                    'contentsDeductibleCode']

claim_rename_dict = {x:y for x,y in zip(claim_match_cols,match_cols)}

policy_cols = ['id','propertyState','policyEffectiveDate','policyTerminationDate'] + match_cols
claim_cols = ['id','state','dateOfLoss'] + claim_match_cols

policies_path = os.path.join(openfema_dir,'FimaNfipPolicies.parquet')
policies = pd.read_parquet(policies_path,engine='pyarrow',columns=policy_cols,filters=[('propertyState','=',state)])

claims_path = os.path.join(openfema_dir,'FimaNfipClaims.parquet')
claims = pd.read_parquet(claims_path,engine='pyarrow',columns=claim_cols,filters=[('state','=',state)])

# Filter out pre-2009 policy data (doesn't reflect full policy base in force)
cutoff_date = '2010-01-01'

m = (policies['policyEffectiveDate']>=cutoff_date)
policies = policies[m].reset_index(drop=True).set_index('id')
policies.sort_values(by='policyEffectiveDate',inplace=True)

# Filter out pre-2009 claims
m = (claims['dateOfLoss']>=cutoff_date)
claims = claims[m].reset_index(drop=True).set_index('id')
claims.sort_values(by='dateOfLoss',inplace=True)
claims.rename(columns=claim_rename_dict,inplace=True)

original_claim_ids = claims.index.to_list()

# Filter out claims and policies that are missing data on the attributes we'll use for matching

# For deductibles / converage limits, represent "NA" as a value, since some people will voluntarily forgo certain types of coverage 
# (e.g., insure only the building but not the contents)
# We don't really want to count this as missing data since it's still useful information 
claims[['totalBuildingInsuranceCoverage','totalContentsInsuranceCoverage']] = claims[['totalBuildingInsuranceCoverage','totalContentsInsuranceCoverage']].fillna(-9999)
claims[['buildingDeductibleCode','contentsDeductibleCode']] = claims[['buildingDeductibleCode','contentsDeductibleCode']].fillna('-9999')
policies[['totalBuildingInsuranceCoverage','totalContentsInsuranceCoverage']] = policies[['totalBuildingInsuranceCoverage','totalContentsInsuranceCoverage']].fillna(-9999)
policies[['buildingDeductibleCode','contentsDeductibleCode']] = policies[['buildingDeductibleCode','contentsDeductibleCode']].fillna('-9999')

incomplete_data_mask = policies[match_cols].isna().any(axis=1)
policies = policies[~incomplete_data_mask]

incomplete_data_mask = claims[match_cols].isna().any(axis=1)
incomplete_data_ids = claims[incomplete_data_mask].index.to_list()
claims = claims[~incomplete_data_mask]

# Attempt to match claims to policies

claims = claims.reset_index().rename(columns={'id':'claim_id'})
policies = policies.reset_index().rename(columns={'id':'policy_id'})

dates = claims['dateOfLoss'].unique()

match_list = []

for date in dates:
    
    left = claims[claims['dateOfLoss']==date][['claim_id'] + match_cols]
    m = (policies['policyEffectiveDate'] <= date)&(policies['policyTerminationDate'] >= date)
    right = policies[m][['policy_id'] + match_cols]
    
    match_list.append(pd.merge(left,right,on=match_cols,how='left'))
    
matches = pd.concat(match_list)

# Drop claims that aren't matched to a policy, as well as those matched to multiple policies
m1 = matches['claim_id'].duplicated(keep=False)
m2 = matches['policy_id'].isna()
m = ~(m1|m2)

multiple_match_ids = matches[m1]['claim_id'].to_list()
unmatched_ids = matches[m2]['claim_id'].to_list()
matched_ids = matches[m]['claim_id'].to_list()

claim_match_info = matches[m][['claim_id','policy_id']].reset_index(drop=True)
outname = os.path.join(outfolder,f'{state}_claim_match_info.parquet')
claim_match_info.to_parquet(outname)

# Create dataframe that we can use to track what happened to each claim in the original dataset
# (e.g., discarded due to missing data, matched, unmatched, etc.)

selection_flow = pd.DataFrame({'id':original_claim_ids,'outcome':pd.NA})
selection_flow.loc[selection_flow['id'].isin(incomplete_data_ids),'outcome'] = 'Excluded - Missing data'
selection_flow.loc[selection_flow['id'].isin(unmatched_ids),'outcome'] = 'Excluded - No matches'
selection_flow.loc[selection_flow['id'].isin(multiple_match_ids),'outcome'] = 'Excluded - Multiple matches'
selection_flow.loc[selection_flow['id'].isin(matched_ids),'outcome'] = 'Included'

outname = os.path.join(outfolder,f'{state}_claim_selection_flow.parquet')
selection_flow.to_parquet(outname)