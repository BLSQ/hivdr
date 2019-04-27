#%%
import bluesquare_data_pipelines.access as blsq
import pandas as pd
import numpy as np

#%%
# Connect to HIVDR
hivdr = blsq.dhis_instance("dhis2_cd_hivdr_prod")
#%%
print("extracting data")
fosas =hivdr.orgunitstructure.organisationunituid.unique().tolist()
des = hivdr.dataelement.uid.unique().tolist()

#%%
for i in range(int(np.ceil(len(fosas)/100))):
    fosa_i = fosas[100*i:min(100*i + 99, len(fosas))]
    data = hivdr.get_data(des, fosa_i, 2017, comment="Extraction for Jenny")
    print("writing bach " + str(i))
    with open('extract_2018.csv', 'a') as f:
        data.to_csv(f, header=False)