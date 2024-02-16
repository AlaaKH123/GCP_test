import pandas as pd
from pandas.io import gbq
from google.cloud import bigquery

'''
Python Dependencies to be installed

gcsfs
fsspec
pandas
pandas-gbq

'''

def testpython(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    lst = []
    file_name = event['name']
    table_name = file_name.split('.')[0]

    # Event,File metadata details writing into Big Query
    dct={
         'Event_ID':context.event_id,
         'Event_type':context.event_type,
         'Bucket_name':event['bucket'],
         'File_name':event['name'],
         'Created':event['timeCreated'],
         'Updated':event['updated']
        }
    lst.append(dct)
    df_metadata = pd.DataFrame.from_records(lst)
    df_metadata.to_gbq('test_data_platforme.data_loading_metadata', 
                        project_id='fivetran-408613', 
                        if_exists='append',
                        location='europe-west1')
    
    
    # Actual file data , writing to Big Query
    # Specifying different separators for different types of CSV files
    
    separators = [',', ';', '|']
    for sep in separators:
        try:
            df_data = pd.read_csv('gs://' + event['bucket'] + '/' + file_name, sep=sep)
            break
        except pd.errors.ParserError:
            continue

    df_data.to_gbq('test_data_platforme.' + table_name, 
                        project_id='fivetran-408613', 
                        if_exists='append',
                        location='europe-west1')