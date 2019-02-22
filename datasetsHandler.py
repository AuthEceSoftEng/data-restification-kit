import os
import json
import numpy as np
import pandas as pd

class datasetsHandler:
    """
    This class implements a data handler responsible for manipulating the datasets which are to be imported.
    """
    def __init__(self, dataset_name, file_path):
        """
        Initializes this datasets handler. The initialization requires the folder which contains the datasets.
        
        :param dataset_name: the name of the dataset
        :param file_path: the absolute path of the data file
        """
        self.dataset_name = dataset_name
        self.data_file_to_import = file_path
        self.supported_types = ['.csv']

    def read_data(self, columns = 'all', separator = ','):
        """
        Identifies the files that contain the raw data.
        
        :param file_path: the path of the raw data file
        :param columns: the columns that are to be read
        :returns A list containing the paths of raw data files
        """
        
        data = pd.read_csv(self.data_file_to_import, sep = separator)
        if(columns == 'all'):
            return data
        else:
            return data[columns]
        
    def validate(self, columns, separator):
        """
        Validates a certain dataset file
        
        :param df: the data frame that contains the data
        :returns True of no validation error exists. In case of error, it returns the errors 
        """
        
        for extention in self.supported_types:
            if(self.data_file_to_import.endswith(extention)):
                df = self.read_data(columns, separator)
                
                # Check if there are missing values in data
                if(df.isnull().values.any()):
                    missing_values_columns = [key for key in df.keys() if df[key].isnull().values.any()]
                    
                    return {"error": "Validation Error - Missing data in column(s): " + ", ".join(missing_values_columns)}
                else:
                    self.schema = self.schema_extractor(df)
                    return True
        
        return {"error": "Validation Error - Non supported type"}
        
    def schema_extractor(self, df):
        """
        Extracts the data-schema of a given dataset 
        
        :param df: the data frame that contains the data
        :param write_to_file: False/the_schema_file_path
        :returns An object containing the data schema
        """
        schema = {}
        for key in df.keys():
            modified_key = key.replace(' ', '_')
            schema[modified_key] = {}
            if(df[key].dtype == 'object'):
                schema[modified_key]["type"] = 'string'
            else:
                schema[modified_key]["type"] = 'number'
        
        return schema