import subprocess
import pandas as pd
import datetime
import os
import sys
import csv, copy


class table2qbWrapper(object):
    def __init__(self):
        self._executable = "table2qb.jar"
        self._input_components = "components.csv"
        self._input_observations = "observations.csv"
        self._input_columns = 'columns.csv'
        self._output_files_list = []
        self.codeListHeaders = ['Label', 'Notation', 'Parent Notation']
        self.unique_folder_for_each_run = 'data/'
        self.dimensions_list = []
        self.datasetname = 'myDs'
        self.baseURI = 'http://example.com/dataset/'
        self.slug = 'test'

    def run_full_table2qb_pipes(self):

        self.datasetname = sys.argv[1]
        self.baseURI = sys.argv[2]
        self.slug = sys.argv[3]
        self._input_components = sys.argv[4]
        self._input_observations = sys.argv[5]
        self._input_columns = sys.argv[6]

        # generate code lists
        self.generate_code_lists()

        # execute cube creation pipeline
        subprocess.call(["java", "-jar", self._executable, 'list'])
        subprocess.call(["java", "-jar", self._executable, 'describe', 'cube-pipeline'])
        subprocess.call(["java", "-jar", self._executable, 'describe', 'components-pipeline'])
        subprocess.call(["java", "-jar", self._executable, 'describe', 'codelist-pipeline'])

        # components pipeline
        components_ouptfile = self.unique_folder_for_each_run + 'components__' + self.datasetname + '.ttl'
        subprocess.call(
            ["java", "-jar", self._executable, 'exec', 'components-pipeline', '--input-csv', self._input_components,
             '--base-uri', self.baseURI,
             '--output-file', components_ouptfile])

        # cube pipeline
        cube_ouptfile = self.unique_folder_for_each_run + 'cube__' + self.datasetname + '.ttl'
        subprocess.call(
            ["java", "-jar", self._executable, 'exec', 'cube-pipeline', '--input-csv', self._input_observations,
             '--dataset-name', self.datasetname, '--dataset-slug', 'TEST', '--column-config', self._input_columns,
             '--base-uri', self.baseURI, '--output-file', cube_ouptfile])

        # code list pipeline
        for dim in self.dimensions_list:
            code_list_input = self.unique_folder_for_each_run + dim + '.csv'
            code_list_ouptfile = self.unique_folder_for_each_run + dim + '__codeList__' + self.datasetname + '.ttl'
            subprocess.call(
                ["java", "-jar", self._executable, 'exec', 'codelist-pipeline', '--codelist-csv', code_list_input
                 ,'--codelist-name', dim, '--codelist-slug', dim,
                 '--base-uri',
                 self.baseURI,
                 '--output-file', code_list_ouptfile])

    def generate_code_lists(self):

        # get dimesions names list
        components_df = pd.read_csv(self._input_components)
        dimensions_df = components_df[(components_df['Component Type'] == 'Dimension')]
        # print dimensions_df
        self.dimensions_list = dimensions_df['Label'].tolist()
        # print dimensions_list

        # get observation/dimensions values
        observations_df = pd.read_csv(self._input_observations)

        # do the generation thingy
        self.unique_folder_for_each_run = self.generate_folder_name()
        for dimension in self.dimensions_list:
            # create final code list data frame
            dimCodeList_df = pd.DataFrame(columns=self.codeListHeaders)
            # get dim values
            dim_values_list = observations_df[dimension].tolist()
            # get unique values of dim
            unique_dime_vals_list = list(set(dim_values_list))
            # print unique_dime_vals_list
            dimCodeList_df['Label'] = unique_dime_vals_list
            dimCodeList_df['Notation'] = map(str.lower, unique_dime_vals_list)
            # print dimCodeList_df

            # dataframe to csv
            CodeListcsvFileName = self.unique_folder_for_each_run + dimension + '.csv'
            dimCodeList_df.to_csv(CodeListcsvFileName, sep=',', encoding='utf-8', index=False)

    def decode_output(self):
        pass

    def generate_folder_name(self):
        basename = "data/"
        suffix = datetime.datetime.now().strftime("%y%m%d_%H%M%S")
        folder_name = "_".join([basename, suffix])

        if not os.path.exists(folder_name + '/'):
            os.makedirs(folder_name + '/')
        return folder_name + '/'


if __name__ == "__main__":
    table2qb = table2qbWrapper()
    # table2qb.generate_code_lists()
    # print table2qb.generate_folder_name()
    table2qb.run_full_table2qb_pipes()
