import numpy as np
import pandas as pd
import os

### *** HELPER FUNCTIONS *** ###

def truncate_floats(df):
    """
    Attempting to merge datasets on float columns is risky. This function truncates float columns to one decimal place
    and converts the result to a string to make merging easier. 
    """
    
    data_types = df.dtypes
    float_cols = data_types[data_types == 'double[pyarrow]'].index.values
    
    for col in float_cols:
        df[col] = df[col].apply(lambda x: f'{x:.1f}').astype('string[pyarrow]')
    
    return df

def truncate_dates(df,date_cols):
    """
    Attempting to merge datasets on datetime columns can be risky for the same reason as floats.
    This function converts datetime columns to YYYY-MM-DD strings. 
    """

    for col in date_cols:
        df[col] = df[col].dt.strftime('%Y-%m-%d').astype('string[pyarrow]')

    return df

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
# (Leave out censusBlockGroupFips since we'll deal with that separately)

match_cols = ['latitude',
              'longitude',
              'ratedFloodZone',
              'reportedZipCode',
              'originalNBDate',
              'originalConstructionDate',
              'numberOfFloorsInInsuredBuilding']
              

claim_match_cols = ['latitude',
                    'longitude',
                    'ratedFloodZone',
                    'reportedZipCode',
                    'originalNBDate',
                    'originalConstructionDate',
                    'numberOfFloorsInTheInsuredBuilding']

claim_rename_dict = {x:y for x,y in zip(claim_match_cols,match_cols)}

policy_cols = ['id','propertyState','policyEffectiveDate','policyTerminationDate','censusBlockGroupFips'] + match_cols
claim_cols = ['id','state','dateOfLoss','censusBlockGroupFips'] + claim_match_cols

policies_path = os.path.join(openfema_dir,'FimaNfipPolicies.parquet')
policies = pd.read_parquet(policies_path,engine='pyarrow',columns=policy_cols,filters=[('propertyState','=',state)])

claims_path = os.path.join(openfema_dir,'FimaNfipClaims.parquet')
claims = pd.read_parquet(claims_path,engine='pyarrow',columns=claim_cols,filters=[('state','=',state)])

# Filter out pre-2009 policy data (doesn't reflect full policy base in force)
policy_cutoff_date = '2009-01-01'

m = (policies['policyEffectiveDate']>=policy_cutoff_date)
policies = policies[m].reset_index(drop=True).set_index('id')
policies.sort_values(by='policyEffectiveDate',inplace=True)

# Filter out pre-2010 claims
claim_cutoff_date = '2010-01-01'
m = (claims['dateOfLoss']>=claim_cutoff_date)
claims = claims[m].reset_index(drop=True).set_index('id')
claims.sort_values(by='dateOfLoss',inplace=True)
claims.rename(columns=claim_rename_dict,inplace=True)

original_claim_ids = claims.index.to_list()

### *** REPRESENTS FLOATS AND DATETIMES AS STRINGS *** ###

# Merging on floats is risky due to the imprecise nature of how decimals are represented. 
# This same issue also applies to datetimes. 
# For merging purposes, truncate these values and represent them as strings. 

date_cols = ['originalNBDate','originalConstructionDate']

policies = truncate_floats(policies)
policies = truncate_dates(policies,date_cols)

claims = truncate_floats(claims)
claims = truncate_dates(claims,date_cols)

# *** FILTER OUT ENTRIES WITH MISSING DATA *** ###

incomplete_data_mask = policies.isna().any(axis=1)
policies = policies[~incomplete_data_mask]

incomplete_data_mask = claims.isna().any(axis=1)
incomplete_data_ids = claims[incomplete_data_mask].index.to_list()
claims = claims[~incomplete_data_mask]

### *** CREATE DICTIONARY DESCRIBING OVERLAP BETWEEN CENSUS BLOCKGROUPS *** ###

# FEMA used a third-party service to geocode their claim and policy data, 
# which did not provide information on the vintage of the listed census block group (CBG). 
# This is particularly annoying since the same CBG FIPS can be used for different geometries 
# in different census years. For this reason, we'll need to use crosswalks to get a list of 
# any CBG FIPS that might overlap with the one listed in the OpenFEMA data. 

crosswalk_path = os.path.join(pwd,'NHGIS_crosswalks/CBG_intersections.parquet')

state_code = policies['censusBlockGroupFips'].apply(lambda x: x[:2]).mode()[0]
crosswalk = pd.read_parquet(crosswalk_path)
crosswalk = crosswalk[crosswalk['left_GEOID'].str.startswith(state_code)]

vintage_2000_list = crosswalk[crosswalk['left_vintage']==2000]['left_GEOID'].unique()
vintage_2010_list = crosswalk[crosswalk['left_vintage']==2010]['left_GEOID'].unique()
vintage_2020_list = crosswalk[crosswalk['left_vintage']==2020]['left_GEOID'].unique()

claims['CBG_2020_match'] = claims['censusBlockGroupFips'].isin(vintage_2020_list)
claims['CBG_2010_match'] = claims['censusBlockGroupFips'].isin(vintage_2010_list)
claims['CBG_2000_match'] = claims['censusBlockGroupFips'].isin(vintage_2000_list)

# Sometimes, you'll have a claim where the census block group is for a completely different state
# We'll exclude these from the final dataset since it implies something went wrong with the geocoding process
bad_geocode_mask = ~claims[['CBG_2000_match','CBG_2010_match','CBG_2020_match']].any(axis=1)
bad_geocode_ids = claims[bad_geocode_mask].index.to_list()
claims = claims[~bad_geocode_mask]

# From crosswalk data, create dictionary that does the following: 
# For a given CBG FIPS code (of ambiguous year), 
# return a list of CBG FIPS codes from various years that might overlap with input FIPS. 

CBG_dict = crosswalk[['left_GEOID','right_GEOID']].drop_duplicates().groupby('left_GEOID').agg(lambda x: list(x))['right_GEOID'].to_dict()

### *** ATTEMPT TO MATCH CLAIMS TO POLICIES *** ###

claims = claims.reset_index().rename(columns={'id':'claim_id'})
policies = policies.reset_index().rename(columns={'id':'policy_id'})

claims['num_match'] = pd.NA
claims['policy_id'] = pd.NA

for date in claims['dateOfLoss'].unique():

    indices = claims[(claims['dateOfLoss']==date)].index.values
    policies_in_force = policies[(policies['policyEffectiveDate'] <= date)&(policies['policyTerminationDate'] >= date)]

    for idx in indices:

        CBG_FIPS = claims.loc[idx,'censusBlockGroupFips']
        m1 = (policies_in_force['censusBlockGroupFips'].isin(CBG_dict[CBG_FIPS]))
        m2 = (policies_in_force[match_cols] == claims.loc[idx,match_cols]).all(axis=1)

        m = m1&m2

        num_match = np.sum(m)
    
        claims.loc[idx,'num_match'] = num_match
        
        if num_match == 1:
            claims.loc[idx,'policy_id'] = policies_in_force[m]['policy_id'].values[0]

# Drop claims that aren't matched to a policy, as well as those matched to multiple policies

multiple_match_ids = claims[(claims['num_match'] > 1)]['claim_id'].to_list()
unmatched_ids = claims[(claims['num_match'] == 0)]['claim_id'].to_list()
matched_ids = claims[(claims['num_match'] == 1)]['claim_id'].to_list()

claim_match_info = claims[['claim_id','num_match','policy_id']].reset_index(drop=True)
outname = os.path.join(outfolder,f'{state}_claim_match_info.parquet')
claim_match_info.to_parquet(outname)

# Create dataframe that we can use to track what happened to each claim in the original dataset
# (e.g., discarded due to missing data, matched, unmatched, etc.)

selection_flow = pd.DataFrame({'id':original_claim_ids,'outcome':pd.NA})
selection_flow.loc[selection_flow['id'].isin(incomplete_data_ids),'outcome'] = 'Excluded - Missing data'
selection_flow.loc[selection_flow['id'].isin(bad_geocode_ids),'outcome'] = 'Excluded - Bad geocode'
selection_flow.loc[selection_flow['id'].isin(unmatched_ids),'outcome'] = 'Excluded - No matches'
selection_flow.loc[selection_flow['id'].isin(multiple_match_ids),'outcome'] = 'Excluded - Multiple matches'
selection_flow.loc[selection_flow['id'].isin(matched_ids),'outcome'] = 'Included'

outname = os.path.join(outfolder,f'{state}_claim_selection_flow.parquet')
selection_flow.to_parquet(outname)