# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# OBS precisa ser criar o dataset e projeto antes de criar a tabela

import sys
from google.cloud import bigquery

def create_dataset_table(table_id,dataset_id):

    # Construct a BigQuery client object.
    client = bigquery.Client()
    
    # [START bigquery_create_dataset
    dataset_id = "{}.olist".format(client.project)
    
    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "us-central1"
    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))    

    # TODO(developer): Set table_id to the ID of the table to create.
    print("CREATING TABLE PROCESS...\n")

    table_id = "stack-data-pipeline-gcp.olist.tb_customer"

    schema = [
        bigquery.SchemaField("customer_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("customer_unique_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("customer_zip_code_prefix", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("customer_city", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("customer_state", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("date_load", "DATE", mode="NULLABLE"),        
    ]

    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )
    # [END bigquery_create_table]

if __name__ == "__main__":
    create_dataset_table(dataset_id=sys.argv[1],table_id=sys.argv[2])
    #create_dataset_table()


 
    