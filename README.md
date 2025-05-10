# Overview
This project contains code to link individual records from the [OpenFEMA](https://www.fema.gov/about/openfema/data-sets#nfip) NFIP policy and claim datasets that are related to each other. 

## Policy-to-policy matching

The `match_policies.py` script identifies property-level NFIP enrollment "stints" from individual policy records in the [FIMA NFIP Redacted Policies v2](https://www.fema.gov/openfema-data-page/fima-nfip-redacted-policies-v2) dataset. Here, a "stint" is defined as a period of continuous insurance coverage (with no lapses) at a given property location. This is accomplished by first grouping policies based on the time-invariant property characteristics listed below: 

| Variable                        | Description                                                          |
|---------------------------------|----------------------------------------------------------------------|
| latitude                        | Approximate latitude of the insured property (to one decimal place)  |
| longitude                       | Approximate longitude of the insured property (to one decimal place) |
| censusBlockGroupFips            | U.S. Census block group of the insured property                      |
| ratedFloodZone                  | Flood zone used to rate the insured property (e.g., A, V, X, etc.)   |
| reportedZipCode                 | 5-digit postal zip code of the insured property                      |
| originalNBDate                  | The original new business date of the insured property               |
| originalConstructionDate        | The original construction date of the insured property               |
| numberOfFloorsInInsuredBuilding | Code indicating the number of floors in the insured property         |

For a given property, the above characteristics are assumed to remain constant over time as its flood insurance policy is renewed. Within each group, policy renewals are identified based on the start and end dates of the flood insurance policy under the assumption that the new policy goes into effect on the same day that the old policy expires. This matching procedure can result in the following outcomes: 

- 0 matches: There is no matching policy that goes into effect when the current policy expires. The can occur due to a lapse in coverage, or because the expiration date of the current policy occurs after the latest refresh of the OpenFEMA policies dataset. 
- 1 match: There is a single matching policy that goes into effect when the current policy expires. This typically occurs due to policy renewal. 
- ≥2 matches: There are multiple potential policy matches that go into effect when the current policy expires. In this case, we cannot clearly determine which policy is the next member of the stint, and insurance coverage at the property ceases to be tracked over time. This situation is somewhat analogous to the concept of "lost to follow-up" in clinical research. 

Among 69,159,981 policy records from the 2009-2025 period, 65,328,041 (94.5%) were uniquely identifiable based on the time-invariant property characteristics listed above and their start/end dates. After matching policy renewals, a total of 14,270,591 coverage stints were identified, with an average of 4.6 years of follow-up time per stint. A total of 413,408 (0.6%) policies were excluded due to missing property characteristics, while 3,418,532 (4.9%) could not be uniquely distinguished based on their property characteristics and start/end dates, causing them to be exlcuded from coverage stints. 

As part of this analysis, a modified version of the [FIMA NFIP Redacted Policies v2](https://www.fema.gov/openfema-data-page/fima-nfip-redacted-policies-v2) dataset was produced that contains the following additional data fields: 

| Variable         | Description                                                              |
|------------------|--------------------------------------------------------------------------|
| id               | Unique ID assigned to each policy record                                 |
| stint_id         | Unique ID assigned to each coverage stint (blank for excluded records)   |
| matching_outcome | Variable describing why a policy was included or excluded                |
| num_match        | Number of potential matches identified when checking for policy renewals |
| match_id         | If a policy renewal occurs, the ID of the new policy                     |


## Claim-to-policy matching

The `match_claims.py` script identifies policy records associated with claims in the [FIMA NFIP Redacted Claims v2](https://www.fema.gov/openfema-data-page/fima-nfip-redacted-claims-v2) dataset. For each claim, this is accomplished by first filtering out any policies that were not in force on the date when the loss occured. Next, the claim is matched to one or more of the remaining policies based on the following data fields that are common to both datasets: 

| Variable                        | Description                                                          |
|---------------------------------|----------------------------------------------------------------------|
| latitude                        | Approximate latitude of the insured property (to one decimal place)  |
| longitude                       | Approximate longitude of the insured property (to one decimal place) |
| censusBlockGroupFips            | U.S. Census block group of the insured property                      |
| ratedFloodZone                  | Flood zone used to rate the insured property (e.g., A, V, X, etc.)   |
| reportedZipCode                 | 5-digit postal zip code of the insured property                      |
| originalNBDate                  | The original new business date of the insured property               |
| originalConstructionDate        | The original construction date of the insured property               |
| numberOfFloorsInInsuredBuilding | Code indicating the number of floors in the insured property         |
| totalBuildingInsuranceCoverage  | Dollar amount of insurance coverage on the building                  |
| totalContentsInsuranceCoverage  | Dollar amount of insurance coverage on the contents                  |
| buildingDeductibleCode          | Code indicating the deductible amount on the building                |
| contentsDeductibleCode          | Code indicating the deductible amount on the contents                |

This matching procedure can result in the following outcomes: 

- 0 matches: No matching policy records were found. This can potentially occur due to missing data or recording errors in one or more of the data fields listed above, or due to a claim being associated with a policy that is missing entirely from the dataset. Because pre-2009 policy records do not reflect the full policy base in force, many claims from before 2010 do not have a matching policy. 
- 1 match: The claim was matched one-to-one with a policy. This is the ideal outcome that we should expect to see most of the time.  
- ≥2 matches: There are multiple potential policy matches that could be associated with the claim. In this case, the specific policy associated with the claim is ambiguous. 

Among 997,119 claims from the 2009-2025 period, 930,954 (93.4%) were matched one-to-one with a policy. A total of 12,346 (1.2%) claims were excluded due to missing data, 16,777 (1.7%) were excluded because no matching policy record was found, and 37,042 (3.7%) were excluded because multiple potential policy matches were found. 

As part of this analysis, a modified version of the [FIMA NFIP Redacted Claims v2](https://www.fema.gov/openfema-data-page/fima-nfip-redacted-claims-v2) dataset was produced that contains the following additional data fields: 

| Variable         | Description                                                                 |
|------------------|-----------------------------------------------------------------------------|
| id               | Unique ID assigned to each claim                                            |
| matching_outcome | Variable describing whether a claim was successfully matched to a policy    |
| policy_id        | If a claim was successfully matched, the ID of the associated policy record |


