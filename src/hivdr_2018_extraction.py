#%%
import bluesquare_data_pipelines.access as blsq
import pandas as pd

#%%
# Connect to HIVDR
hivdr = blsq.dhis_instance("dhis2_cd_hivdr_prod")
print("extracting data")
data = hivdr.get_data(hivdr.dataelement.uid.unique().tolist(), hivdr.orgunitstructure.organisationunituid.unique().tolist(), 2018, "Extraction for Jenny")

d.to_csv("test")