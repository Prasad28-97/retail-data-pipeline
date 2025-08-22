import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import mysql.connector
import json

class ReadFromMySQL(beam.DoFn):
    def process(self, element, host, user, password, database, query):
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query)
        for row in cursor.fetchall():
            yield row
        conn.close()

def run():
    options = PipelineOptions(
        runner='DataflowRunner',
        project='thematic-land-467710-p8',
        region='us-east1',
        temp_location='gs://your-bucket/temp',
        job_name='retailer-to-landing'
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Start" >> beam.Create([None])  # dummy element to trigger DoFn
            | "Read MySQL" >> beam.ParDo(
                ReadFromMySQL(),
                host="34.123.45.67",   # your Cloud SQL IP
                user="root",
                password="password",
                database="retailer",
                query="SELECT * FROM customers"
            )
            | "To JSON" >> beam.Map(json.dumps)
            | "Write to GCS" >> beam.io.WriteToText(
                "gs://your-bucket/landing/retailer-db/customers/customers",
                file_name_suffix=".json"
            )
        )

if __name__ == "__main__":
    run()
