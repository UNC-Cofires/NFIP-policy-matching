# Overview
This project contains code to link individual records from the [OpenFEMA](https://www.fema.gov/about/openfema/data-sets#nfip) NFIP policy and claim datasets that are related to each other. 

## Policy-to-policy matching

The `match_policies.py` script identifies property-level NFIP enrollment "stints" from individual policy records in the [FIMA NFIP Redacted Policies v2](https://www.fema.gov/openfema-data-page/fima-nfip-redacted-policies-v2) dataset. Here, a "stint" is defined as a period of continuous insurance coverage (with no lapses) at a given property location. This is accomplished by first grouping policies based on the property characteristics listed below: 

| Variable                        | Description                                                          |
|---------------------------------|----------------------------------------------------------------------|
| latitude                        | Approximate latitude of the insured property (to one decimal place)  |
| longitude                       | Approximate longitude of the insured property (to one decimal place) |
| censusBlockGroupFips\*          | U.S. Census block group of the insured property (see [note](https://github.com/UNC-Cofires/NFIP-policy-matching#census-geographies)) |
| ratedFloodZone                  | Flood zone used to rate the insured property (e.g., A, V, X, etc.)   |
| reportedZipCode                 | 5-digit postal zip code of the insured property                      |
| originalNBDate                  | The original new business date of the insured property               |
| originalConstructionDate        | The original construction date of the insured property               |
| numberOfFloorsInInsuredBuilding | Code indicating the number of floors in the insured property         |

\*See [note](https://github.com/UNC-Cofires/NFIP-policy-matching#census-geographies) regarding Census geographies.

For a given property, the above characteristics are assumed to remain constant over time as its flood insurance policy is renewed, with the exception of Census block groups (which are allowed to vary within a narrow range of acceptable values). Within each grouping of the above characteristics, policy renewals are identified based on the start and end dates of the flood insurance policy under the assumption that the new policy goes into effect on the same day that the old policy expires. This matching procedure can result in the following outcomes: 

- 0 matches: There is no matching policy that goes into effect when the current policy expires. The can occur due to a lapse in coverage, or because the expiration date of the current policy occurs after the latest refresh of the OpenFEMA policies dataset. 
- 1 match: There is a single matching policy that goes into effect when the current policy expires. This typically occurs due to policy renewal. 
- ≥2 matches: There are multiple potential policy matches that go into effect when the current policy expires. In this case, we cannot clearly determine which policy is the next member of the stint, and insurance coverage at the property ceases to be tracked over time.

Among 69,159,981 policy records from the 2009-2025 period, 65,328,041 (94.5%) were uniquely identifiable based on the property characteristics listed above and their start/end dates. After matching policy renewals, a total of 14,270,591 coverage stints were identified, with an average of 4.6 years of follow-up time per stint. A total of 413,408 (0.6%) policies were excluded due to missing property characteristics, while 3,418,532 (4.9%) could not be uniquely distinguished based on their property characteristics and start/end dates, causing them to be exlcuded from coverage stints. 

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
| censusBlockGroupFips\*          | U.S. Census block group of the insured property (see [note](https://github.com/UNC-Cofires/NFIP-policy-matching/edit/main/README.md#census-geographies)) |
| ratedFloodZone                  | Flood zone used to rate the insured property (e.g., A, V, X, etc.)   |
| reportedZipCode                 | 5-digit postal zip code of the insured property                      |
| originalNBDate                  | The original new business date of the insured property               |
| originalConstructionDate        | The original construction date of the insured property               |
| numberOfFloorsInInsuredBuilding | Code indicating the number of floors in the insured property         |

\*See [note](https://github.com/UNC-Cofires/NFIP-policy-matching#census-geographies) regarding Census geographies.

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
| num_match        | Number of potentially matching policy records identified for the claim      |
| policy_id        | If a claim was successfully matched, the ID of the associated policy record |

## Census geographies

OpenFEMA used a third-party service to geocode their claim and policy data, and unfortunately did not capture the vintage year of Census geographies such as tracts and block groups. An analysis of the `censusBlockGroupFips` data field suggests that the OpenFEMA datasets use a mix of GEOIDs from the 2010, 2020, and 2000 vintage years. However, because GEOIDs are often reused across vintage years, there is no way to unambiguously determine which vintage was used to geocode a specific record. Because the same GEOID can be used to represent different geographic units across vintage years, this implies that the `censusBlockGroupFips` data field might change over time for a given property location. 

To address this issue, we use [NHGIS geographic crosswalks](https://www.nhgis.org/geographic-crosswalks) to create a lookup table that describes the overlap between block groups from various vintage years. For example, the geographic units described by GEOID `440010301001` (which appears in the 2000, 2010, and 2020 vintages) overlap with the geographic units described by the following GEOIDs: `440070107021` and `440070107023` (both of which appear in the 2020 vintage). As such, when matching individual policy and claim records, a record with a GEOID of `440010301001` is allowed to match to records with a GEOID of `440010301001`, `440070107021`, or `440070107023`. 



