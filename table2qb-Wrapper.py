from __future__ import division   # a/b is float division
import subprocess
import pandas as pd
import datetime
import os
import sys
import copy
import csv
from math import ceil


class table2qbWrapper(object):

    def __init__(self):
        self._executable = "table2qb.jar"
        self.pipelineName = 'components-pipeline'
        self._input_components = "components.csv"
        self._input_observations = "observations.csv"
        self._input_columns = 'columns.csv'
        self._output_files_list = []
        self.codeListHeaders = ['Label', 'Notation', 'Parent Notation']
        self.observationFileHeaders = ['Measure Type', 'value']

        self.unique_folder_for_each_run = 'data/'
        self.dimensions_list = []
        self.measures_list = []
        self.datasetname = 'myDs'
        self.baseURI = 'http://example.com/dataset/'
        self.slug = 'test'
        self.files_count = 1

    def run_full_table2qb_pipes(self):

        self.pipelineName = sys.argv[1]
        self.datasetname = sys.argv[2]
        self.baseURI = sys.argv[3]
        self.slug = sys.argv[4]
        self._input_components = sys.argv[5]
        self._input_observations = sys.argv[6]
        self._input_columns = sys.argv[7]
        # create unique folder to hold outputs
        self.unique_folder_for_each_run = self.generate_folder_name()


        # execute cube creation pipeline
        subprocess.call(["java", "-jar", self._executable, 'list'])

        if self.pipelineName == 'components-pipeline':
            # components pipeline
            subprocess.call(["java", "-jar", self._executable, 'describe', 'components-pipeline'])
            components_ouptfile = self.unique_folder_for_each_run + 'components__' + self.datasetname + '.ttl'
            subprocess.call(
                ["java", "-jar", self._executable, 'exec', 'components-pipeline', '--input-csv', self._input_components,
                 '--base-uri', self.baseURI,
                 '--output-file', components_ouptfile])

        if self.pipelineName == 'cube-pipeline':

            #generate single measure per row observation file
            ready_input_file = self.generate_single_row_observations()


            # cube pipeline
            subprocess.call(["java", "-jar", self._executable, 'describe', 'cube-pipeline'])
            cube_ouptfile = self.unique_folder_for_each_run + 'cube__' + self.datasetname + '.ttl'
            for i in range (0, self.files_count):
                subprocess.call(
                    ["java", "-jar", self._executable, 'exec', 'cube-pipeline', '--input-csv', ready_input_file+'_p'+str(i),
                     '--dataset-name', self.datasetname, '--dataset-slug', self.slug , '--column-config', self._input_columns,
                     '--base-uri', self.baseURI, '--output-file', cube_ouptfile+'_p'+str(i)])

        if self.pipelineName == 'codelist-pipeline':

            # generate code lists
            self.generate_code_lists()

            # code list pipeline
            subprocess.call(["java", "-jar", self._executable, 'describe', 'codelist-pipeline'])
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
        self.dimensions_list = dimensions_df['Label'].tolist()

        # get observation/dimensions values
        observations_df = pd.read_csv(self._input_observations)

        # do the generation thingy
        for dimension in self.dimensions_list:
            # create final code list data frame
            dimCodeList_df = pd.DataFrame(columns=self.codeListHeaders)
            # get dim values
            dim_values_list = observations_df[dimension].tolist()
            # get unique values of dim
            unique_dime_vals_list = list(set(dim_values_list))
            dimCodeList_df['Label'] = unique_dime_vals_list
            dimCodeList_df['Notation'] = unique_dime_vals_list
            # dataframe to csv
            CodeListcsvFileName = self.unique_folder_for_each_run + dimension + '.csv'
            dimCodeList_df.to_csv(CodeListcsvFileName, sep=',', encoding='utf-8', index=False)

    def generate_single_row_observations(self):

        # get measures/dimesnions names list
        components_df = pd.read_csv(self._input_components)
        measures_df = components_df[(components_df['Component Type'] == 'Measure')]
        self.measures_list = measures_df['Label'].tolist()
        dimensions_df = components_df[(components_df['Component Type'] == 'Dimension')]
        self.dimensions_list = dimensions_df['Label'].tolist()

        # get observation/measures values
        observations_df = pd.read_csv(self._input_observations)

        #create new observation data frame to hold new data format
        new_observations_header = []
        new_observations_row = []
        new_observations_list = []

        #prepare header
        for dim in self.dimensions_list:
            new_observations_header.append(dim)
        for new_head in self.observationFileHeaders:
            new_observations_header.append(new_head)
        #append to final list
        #new_observations_list.append(copy.deepcopy(new_observations_header))


        #extract measures values
        for index, row in observations_df.iterrows():
            for measure in self.measures_list:
                #dim values
                for dim in self.dimensions_list:
                    new_observations_row.append(copy.deepcopy(row[dim]))
                #mesure type
                new_observations_row.append(measure)
                #unit
                #new_observations_row.append('')
                #value
                new_observations_row.append(copy.deepcopy(row[measure]))

                # append to final list
                new_observations_list.append(copy.deepcopy(new_observations_row))
                # clean
                del new_observations_row[:]

        #save observations to csv [using pandas]
        #readyObs_df = pd.DataFrame(new_observations_list, columns=new_observations_header)
        readyObsFileName = self.unique_folder_for_each_run + 'input' + '.csv'
        #readyObs_df.to_csv(readyObsFileName, sep=',', chunksize=5000, encoding='utf-8', index=False)

        #save observations to csv [using csv writer]
        total_size = len(new_observations_list) #observations_df.size
        chucnkSize = 500000
        self.files_count = int(ceil(total_size/chucnkSize))


        counter =  self.files_count
        for i in range(0, len(new_observations_list), chucnkSize):
            chucnkList =  new_observations_list[i:i + chucnkSize]
            with open(readyObsFileName+'_p'+str(counter), 'wb') as myfile:
                wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
                #add header
                wr.writerow(new_observations_header)
                for row in chucnkList:
                    wr.writerow(row)
                counter -=1

        return readyObsFileName

    def decode_output(self):
        pass

    def generate_folder_name(self):
        basename = "data/"
        suffix = datetime.datetime.now().strftime("%y%m%d_%H%M%S")
        folder_name = "_".join([basename, suffix])

        if not os.path.exists(folder_name + '/'):
            os.makedirs(folder_name + '/')
        return folder_name + '/'

    def chunks(self, inputList=[], numberOfChuncks = 1):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(inputList), numberOfChuncks):
            yield  inputList[i:i + numberOfChuncks]

if __name__ == "__main__":
    table2qb = table2qbWrapper()
    # table2qb.generate_code_lists()
    # print table2qb.generate_folder_name()
    # print table2qb.generate_single_row_observations()
    table2qb.run_full_table2qb_pipes()
