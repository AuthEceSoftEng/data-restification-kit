# data-restification-kit
> Boilerplate code that can be used for datasets restification. 

The restification procedure uses the following steps: 
1. Register the dataset and retrieve schema.
2. Create a REST service that supports the identified schema.
3. Use the service to populate a MongoDB with the raw data and provide API. 

## Dataset registration

In order to register a certain dataset use the following steps:
1. Create a folder containing the dataset name into the directory ```datasets/``` e.g. (metrics) and place all the data files that contain the raw data. 
2. Create the data schema, which is necessary for the restification service. In order to do so, you can use the following code:
```python
# Initialize datasets handler
dH = datasetsHandler('datasets')

# Get the data files of the first dataset
data_files_path = dH.get_datasets_files_path(dH.datasets_to_import[0])

# Create the schema and store it in schemas directory
df = dH.read_data(data_files_path[0], columns = 'all')
dH.schema_extractor(df, 'schemas/class-metrics.schema')
```

## Run REST Service

In order to run the REST service based on your created schemas, you can simply run `service.py`. The configuration details are documented in `settings.py`.
The service is based on the Python REST API framework [Eve](http://docs.python-eve.org/en/latest/).

## Import data

Once you have run the REST service, you can import the data using the following commands:
```python
# Initialize the datasets handler
dH = datasetsHandler('datasets')

# Initialize the data importer
dI = dataImporter.dataImporter(dH)

# Import data (change the url according with your configuration)
dI.import_data('http://localhost:5000/api/v1/')
```