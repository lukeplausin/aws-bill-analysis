# aws-bill-analysis

Module to automate ingestion of AWS billing data reports

The goal is to make it easier to ingest AWS billing reports (new format) into another data store such as elasticsearch.

This code is in an early development state.

## TL;DR

```bash
# Install
pip install git+https://github.com/lukeplausin/aws-bill-analysis.git
```

Usage:

```python
# Import report streamer
from aws_bill_analysis import stream_report, parse_report_manifest, get_account_details, get_es_client, ingest_records, logger

# Log into elasticsearch
kwargs = {
    # ES credentials
    'es_user': <Elasticsearch username>,
    'es_pass': <Elasticsearch password>,
    'es_host': <Elasticsearch hostname>,
    'es_port': <Elasticsearch port>,
    's3_bucket_name': 'my-billing-reports',    # Name of S3 bucket where billing reports live
    'key': '/cur/myreports/manifest.json',     # Path to the manifest.json file of the report
    'profile_name': 'my-main-account-profile', # Name of an AWS profile (credentials chain) to use for S3 and Organizations access
}
# Log into elastic
es_client = get_es_client(**kwargs)
# Download billing report manifest
manifest = parse_report_manifest(key=manifest_key, **kwargs)
# Get basic data from organizations api including account names and tags
accounts = get_account_details(**kwargs)

logger.debug(manifest)

# Iterate over report data files
for report_key in manifest['reportKeys']:
    logger.info(f'Downloading report data file from S3: {report_key}')
    # `stream_report` takes a key as input and returns a generator of line items in dictionary format
    stream = stream_report(key=report_key, manifest=manifest, **kwargs)
    # `condition_records` - converts data types of line items based on the spec in the manifest (ie turns date into date, number into number etc...) and augments with metadata from organizations api such as account name
    conditioned_stream = condition_records(
        stream=stream, manifest=manifest, accounts=accounts, **kwargs)
    # Index data into elasticsearch using bulk streaming API
    summary = ingest_records(
        stream=conditioned_stream,
        es_client=es_client,
        index_name=index_name,
        **kwargs
    )
    # Display a summary of what was ingested.
    print(summary)
```

## What does it do?

* Downloads manifest file
* Uses manifest to find data chunks
* Converts data types based on the report manifest
* Strips out null columns etc
* Augments data with additional info from the organizations api
* Can bulk upload into Elasticsearch

## Gotchas

AWS sometimes change the line item IDs at random intervals through the month. So if you are ingesting the daily report every day, you may find that you end up with duplicate rows for line items. The only good solution I've found to this problem is to perform a bulk delete on old data before re-ingesting.
