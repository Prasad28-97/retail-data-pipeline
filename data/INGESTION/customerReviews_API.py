import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import requests
import json

class ReadFromAPI(beam.DoFn):
    def process(self, element, api_url):
        response = requests.get(api_url)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list):
                for record in data:
                    yield record
            else:
                yield data
        else:
            raise Exception(f"API call failed with status {response.status_code}")

def run():
    options = PipelineOptions(
        runner='DataflowRunner',
        project='thematic-land-467710-p8',
        region='us-east1',
        temp_location='gs://us-central1-demo-composer-603b77d1-bucket/temp',
        job_name='customer-reviews-api'
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Start" >> beam.Create([None])
            | "Read API" >> beam.ParDo(
                ReadFromAPI(),
                api_url="https://api.example.com/customerReviews"   # ✅ Replace with actual API
            )
            | "To JSON" >> beam.Map(json.dumps)
            | "Write to GCS" >> beam.io.WriteToText(
                "gs://us-central1-demo-composer-603b77d1-bucket/landing/retailer-db/customer-reviews/reviews",
                file_name_suffix=".json"
            )
        )

if __name__ == "__main__":
    run()
