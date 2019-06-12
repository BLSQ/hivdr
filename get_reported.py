def get_reported_de(self, level = 'uidlevel3'):
        # TODO : allow tailored reported values extraction
        """Get the amount of data reported for each data elements, aggregated at Level 3 level."""
        hierachical_level = (level) # this has to be a tuple

        query = f"""
            SELECT datavalue.periodid, datavalue.dataelementid, _orgunitstructure.{hierachical_level}, 
            count(datavalue) FROM datavalue JOIN _orgunitstructure ON _orgunitstructure.organisationunitid = datavalue.sourceid
            GROUP BY _orgunitstructure.uid{hierachical_level},, datavalue.periodid, datavalue.dataelementid;
        """

        reported_de = pd.read_sql_query(query,
                                        self.connexion)
        reported_de = reported_de.merge(self.organisationunit,
                                        left_on='uidlevel3', right_on='uid',
                                        how='inner')
        reported_de = reported_de.merge(self.dataelement, 
                                        left_on='dataelementid',
                                        right_on='dataelementid',
                                        suffixes=['_orgUnit', '_data_element'])
        reported_de = reported_de.merge(self.periods)
        reported_de = reported_de.merge(self.orgunitstructure,
                                        left_on='uidlevel3',
                                        right_on='organisationunituid')
        reported_de = reported_de[['quarterly', 'monthly',
                                   'uidlevel2', 'namelevel2',
                                   'uidlevel3_x', 'namelevel3',
                                   'count',
                                   'uid_data_element', 'name_data_element']]
        reported_de.columns = ['quarterly', 'monthly',
                               'uidlevel2', 'namelevel2',
                               'uidlevel3', 'namelevel3',
                               'count',
                               'uid_data_element', 'name_data_element']
        return reported_de