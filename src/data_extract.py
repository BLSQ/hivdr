import bluesquare_data_pipelines.access as blsq
import pandas as pd

# Connect to HIVDR
hivdr = blsq.dhis_instance("dhis2_cd_hivdr_prod")

# Load IAP data
iap_data = pd.read_excel("IAP_RDC_2010_2016_final.xlsx", sheet_name = "2010_2016")
iap_data.columns = ["province", "fosa", "annee", "on_time_refill" ,"M12_retention", "no_rupture_months", 
                    "good_lines", "viral_load_suppressed", "on_time_visit", "M12_pdv"]

# Standardize FOSA and provinces names
orgunits = hivdr.orgunitstructure[['organisationunituid', 'namelevel2', 'namelevel5']]
orgunits.columns = ["orgunit_id", "province", "fosa"]
orgunits = orgunits.dropna()

for data in [orgunits, iap_data] :
    data.fosa = data.fosa.str.replace("é|è","e")
    data.fosa = data.fosa.str.replace("ô","o")
    for name in ['province', 'fosa'] :
        data.loc[:,name] = data.loc[:,name].str.lower()

# Rewrite FOSA type abbreviations
fosa_dico = {"hgr ":"hopital general de reference",
             "ch ":"centre hospitalier",
             "cs ":"centre de sante",
             "hop ":"hopital",
             "hp ":"hopital",
             "csr ":"centre de sante de reference"}

iap_data["fosa_rebuilt"] = iap_data.fosa
for fosa_type in fosa_dico.keys() :
    print(fosa_type)
    type_select = iap_data.fosa.str.contains(fosa_type)
    iap_data.loc[type_select,"fosa_type"] = fosa_type
    iap_data.loc[type_select,"fosa_simple"] = iap_data[type_select].fosa.str.replace(fosa_type, "").str.strip()
    iap_data.loc[type_select,"fosa_rebuilt"] = iap_data.loc[type_select,"fosa_simple"] + " " + fosa_dico[fosa_type] 

# Manual matching for irregular patterns
manual_fosas = {"butembo hopital general de reference":"nk butembo/kitatumba hopital general de reference",
              "kadutu hopital general de reference":"sk kadutu centre hospitalier",
              "muhungu centre de sante" : "sk muhungu diocesain     centre de sante",
              "kasenga hopital general de reference":"sk kasenga hopital",
              "saint francois centre hospitalier":"sn saint francois d'assise hopital",
              "bodeme. centre de sante":"su bodeme centre de sante",
              "mokili centre de sante de reference": "tp mokili centre de sante reference",
              "makiso hopital general de reference":'tp makiso - kisangani hopital general de reference',
              "mangobo hopital general de reference":"tp mangobo hopital general de reference",
              "matete centre de sante de reference":"tp matete centre de sante",
              "boma hopital general de reference":"kc boma hopital general de reference",
              "kinkanda hopital general de reference":"kc kinkanda prov. general de reference",
              "mvuzi centre de sante de reference":"kc mvuzi centre de sante de reference",
              "alunguli hopital general de reference":"mn alunguli hopital general de reference",
              "kitulizo centre hospitalier":"mn kitulizo bdom centre hospitalier",
              "kikwit nord hopital general de reference":"kl kikwit nord hgr",
              "kikwit sud hopital general de reference":"kl kikwit sud hgr",
              "kokolo hopital general de reference":"kn cmr kokolo hopital",
              "makala hopital general de reference":"kn makala hopital", 
               }

for fosa in manual_fosas.keys():
    iap_data.loc[iap_data.fosa_rebuilt == fosa, "fosa_rebuilt"] = manual_fosas[fosa]

# Run the matching
for province in iap_data.province.unique():
    print(province)
    # Subset data sources on province
    iap_prov = iap_data[iap_data.province == province]
    dat_prov = orgunits[orgunits.province.str.contains(province)]
    # For each fosa in IAP data, check if there is a fosa in HIVDR with similar name
    for fosa in iap_prov.fosa_rebuilt.unique():
        print(fosa)
        matches = dat_prov[dat_prov.fosa.str.contains(fosa)]
        # If we have a unique match, keep it
        if len(matches.fosa.values) == 1:
            print("MATCHED")
            iap_data.loc[(iap_data.province == province) & (iap_data.fosa_rebuilt == fosa),"fosa_dhis"] = matches.fosa.values[0]
            iap_data.loc[(iap_data.province == province) & (iap_data.fosa_rebuilt == fosa),"fosa_id"] = matches.orgunit_id.values[0]
        # In case of multiple matches, print them for evaluation
        if len(matches.fosa.values) > 1:
            print("MULTIPLE MATCHES")
            print("    " + str(matches.fosa.values.tolist()))
        # In case of no match, print simplified name and possible matches for evaluations
        if len(matches.fosa.values) == 0:
            print("NO MATCH")
            simple_fosa = iap_prov.loc[iap_prov.fosa_rebuilt == fosa, "fosa_simple"].values[0]
            if (type(simple_fosa) is str):
                print("  Simplified :" + simple_fosa)
                matches = dat_prov[dat_prov.fosa.str.contains(simple_fosa)]
                print("        " + str(matches.fosa.values.tolist()))

# Write matched FOSAs
iap_data[["province","fosa","fosa_dhis","fosa_id"]].drop_duplicates().to_csv("data/processed/iap_fosa_matched.csv", index = False)

# Get list of FOSAs from which to get data
to_extract_ous = iap_data[~pd.isnull(iap_data.fosa_id)].fosa_id.unique().tolist()

# Get list of data elements from which to get data
to_extract_des = hivdr.dataelement[hivdr.dataelement.name.str.contains("VIH|ARV|PATIENTS - FILE ACTIVE|MED - RS|MED - CMM")]

# Extract data
data  = hivdr.get_data(to_extract_des.uid.unique().tolist(),to_extract_ous)

# Format data elments names, and write their list and the extracted data
data["dataelementname"] = data["dataelementname"].str.replace("\r|\n"," ")
data[["dataelementid","dataelementname"]].drop_duplicates().to_csv("data/processed/processed_de_names.csv", 
                                                                   index = False)
data.to_csv("data/processed/iap_covariates.csv", index = False)
