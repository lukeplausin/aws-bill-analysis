# Common tasks for monitoring workflows and data management

import elasticsearch
import elasticsearch.helpers
import boto3
import re
import datetime
import dateutil.parser
import pytz
from gzip_stream import GZIPCompressedStream
import tempfile

import json
import gzip
import csv
import time

from . import logger


# Helpers
def get_boto_session(profile_name='my-main-account-profile', **kwargs):
    if 'boto_session' in kwargs:
        return kwargs['boto_session']
    else:
        session_args = {}
        if profile_name:
            session_args['profile_name'] = profile_name
        logger.debug(f'Creating boto session with profile {profile_name}')
        session = boto3.session.Session(**session_args)
        return session


def get_s3_client(**kwargs):
    session = get_boto_session(**kwargs)
    s3 = session.client('s3')
    return s3


def get_es_client(es_user, es_pass, es_host, es_port, verify_certs=True):
    # Pull from vault
    logger.info(f'Authenticating to Elasticsearch using credentials.')
    return elasticsearch.Elasticsearch(
        [
            f"https://{es_user}:{es_pass}@{es_host}:{es_port}"
        ],
        verify_certs=verify_certs
    )


def get_available_reports(
            s3_bucket_name="my-billing-reports",
            base_path="cur/cost-report/",
            max_age=5,  # maximum days since end of report
            **kwargs
        ):
    if max_age and max_age > 0:
        oldest_age = datetime.datetime.utcnow() - datetime.timedelta(days=max_age)
    else:
        oldest_age = datetime.datetime(year=2000, month=1, day=1)
    oldest_age = oldest_age.replace(tzinfo=pytz.UTC)
    manifests = []
    s3 = get_s3_client(**kwargs)
    pag = s3.get_paginator('list_objects_v2')
    for page in pag.paginate(Bucket=s3_bucket_name, Prefix=base_path):
        for key in page['Contents']:
            if key['LastModified'] > oldest_age and key['Key'].endswith('Manifest.json'):
                manifests.append(key)

    return manifests


def parse_report_manifest(
            key,
            s3_bucket_name="my-billing-reports",
            **kwargs
        ):
    logger.info(f'Download manifest key from S3: {key}')
    s3 = get_s3_client(**kwargs)
    manifest_obj = s3.get_object(Bucket=s3_bucket_name, Key=key)
    manifest_data = json.load(manifest_obj['Body'])
    return manifest_data


def stream_report(
            key,
            manifest,
            s3_bucket_name="my-billing-reports",
            **kwargs
        ):
    s3 = get_s3_client(**kwargs)
    report_obj = s3.get_object(Bucket=s3_bucket_name, Key=key)
    report_stream = report_obj['Body']
    temp_file_decompress = ""
    if manifest['compression'] == 'GZIP':
        # temp_file_decompress = tempfile.mktemp()
        # logger.debug(f'Report is compressed, writing data to temp file {temp_file_decompress}')
        # with open(temp_file_decompress, 'wb') as f:
        #     for chunk in report_stream:
        #         f.write(chunk)
        # How to ensure file gets closed?
        # report_stream_decompressed = gzip.open(temp_file_decompress, 'rt')
        report_stream_decompressed = GZIPCompressedStream(report_stream)
    else:
        report_stream_decompressed = report_stream

    if manifest['contentType'] == 'text/csv':
        report_stream_parsed = csv.DictReader(report_stream_decompressed)
    else:
        raise NotImplementedError(
            f'Format {manifest["contentType"]} not implemented!')

    for record in report_stream_parsed:
        yield record

    if temp_file_decompress:
        report_stream_decompressed.close()


def augment_account_info(account):
    # Augment data about an account by returning a dictionary with additional data fields for this account.
    return {}


def get_account_details(**kwargs):
    logger.info('Getting information about AWS accounts')
    session = get_boto_session(**kwargs)
    org = session.client('organizations')
    accounts = []
    pag = org.get_paginator('list_accounts')
    for page in pag.paginate():
        accounts = accounts + page['Accounts']
    account_dict = {}
    for account in accounts:
        account_dict[account['Id']] = account
        account_dict[account['Id']].update(augment_account_info(account))
    return account_dict


def condition_records(
            stream, manifest, accounts={},
            remove_empty_fields=True, format_datatypes=True, remove_colon=True,
            special_fixes=True,
            **kwargs
        ):
    # Iterate all parsed rows in a report and format them according to rules.
    cols = {}
    for col in manifest['columns']:
        cols[f"{col['category']}/{col['name']}"] = col

    for record in stream:
        try:
            r_delete = []
            r_update = {}
            if remove_empty_fields:
                empties = [k for k, v in record.items() if not v]
                for k in empties:
                    record.pop(k)

            if format_datatypes:
                for k, v in record.items():
                    col_type = cols[k]['type']
                    if col_type in ('DateTime', 'OptionalDateTime'):
                        if v:
                            record[k] = dateutil.parser.parse(v)
                        else:
                            record[k] = None
                    elif col_type == 'Interval':
                        parts = v.split('/')
                        r_update[f'{k}/start'] = dateutil.parser.parse(parts[0])
                        r_update[f'{k}/end'] = dateutil.parser.parse(parts[1])
                        r_delete.append(k)
                    elif col_type in ('BigDecimal', 'OptionalBigDecimal'):
                        if v:
                            record[k] = float(v)
                        else:
                            record[k] = None
                    elif col_type in ('String', 'OptionalString'):
                        pass
                    else:
                        logger.error(f'Unknown data type {col_type}')

            for k in r_delete:
                record.pop(k)
            record.update(r_update)

            # Add info from the account table
            if accounts:
                if 'bill/PayerAccountId' in record.keys() and record['bill/PayerAccountId']:
                    record['bill/PayerAccountName'] = accounts[record['bill/PayerAccountId']]['Name']

                if 'lineItem/UsageAccountId' in record.keys() and record['lineItem/UsageAccountId']:
                    record['lineItem/UsageAccountName'] = accounts[record['lineItem/UsageAccountId']]['Name']
                    record['lineItem/UsageAccountCustomer'] = accounts[record['lineItem/UsageAccountId']]['Customer']
                    record['customer'] = accounts[record['lineItem/UsageAccountId']]['Customer']
                    record['ownership'] = accounts[record['lineItem/UsageAccountId']]['Ownership']

            # Fix records with : in name
            if remove_colon:
                r_delete = []
                r_update = {}
                for k, v in record.items():
                    if ':' in k:
                        r_update[re.sub(r'\:', '_', k)] = v
                        r_delete.append(k)
                record.update(r_update)
                for k in r_delete:
                    record.pop(k)

            # Special field fixes
            if special_fixes:
                if 'resourceTags/user_Customer' in record.keys():
                    record['customer'] = record['resourceTags/user_Customer']
                if 'resourceTags/user_Environment' in record.keys():
                    record['environment'] = record['resourceTags/user_Environment']
                if 'resourceTags/user_EnvironmentInt' in record.keys():
                    record['environment-internal'] = record['resourceTags/user_EnvironmentInt']
                record['@timestamp'] = record['identity/TimeInterval/start']

            yield record
        except Exception as e:
            logger.exception(e)
            logger.exception(record)


def _stream_to_bulk(stream, index_name, op_type="index"):
    for record in stream:
        record_bulk = {
            "_op_type": op_type,
            "_type": "_doc",
            "_index": index_name.format(timestamp=record['@timestamp']),
            **record
        }
        if 'identity/LineItemId' in record.keys() and record['identity/LineItemId']:
            record_bulk["_id"] = record['identity/LineItemId']
        else:
            logger.debug('Record has no ID')
            logger.debug(record)
        yield record_bulk


def ingest_records(stream, es_client, index_name, chunk_size=250, **kwargs):
    
    logger.info(f'Writing bulk records to index {index_name}')
    es_stream = _stream_to_bulk(stream, index_name=index_name)
    counter = 0
    failures = []
    results = elasticsearch.helpers.streaming_bulk(
        es_client, es_stream, chunk_size=chunk_size,
        raise_on_error=False,
        max_retries=500,
        initial_backoff=10, max_backoff=3000,
        yield_ok=True
    )
    for res in results:
        logger.debug(res)
        if res[0]:
            counter = counter + 1
        else:
            failures.append(res)
        if counter and counter % 100 == 0:
            print('.', end="")
        if failures and len(failures) % 100 == 0:
            print('F', end="")

    n_failures = len(failures)
    logger.info(f'Finished, {counter} ingested and {n_failures} failures.')
    for failure in failures:
        logger.debug(failure)


def ingest_report(manifest_key, index_name='aws-billing-usage-{timestamp:%Y.%m.%d}', **kwargs):
    es_client = get_es_client(**kwargs)
    manifest = parse_report_manifest(key=manifest_key, **kwargs)
    accounts = get_account_details(**kwargs)

    logger.debug(manifest)

    for report_key in manifest['reportKeys']:
        logger.info(f'Downloading report data file from S3: {report_key}')
        stream = stream_report(key=report_key, manifest=manifest, **kwargs)
        conditioned_stream = condition_records(
            stream=stream, manifest=manifest, accounts=accounts, **kwargs)
        # Index data into elasticsearch
        summary = ingest_records(
            stream=conditioned_stream,
            es_client=es_client,
            index_name=index_name,
            **kwargs
        )
        print(summary)
    

def find_and_ingest_cost_reports(**kwargs):
    reports = get_available_reports(**kwargs)
    for report in reports:
        ingest_report(report['Key'], **kwargs)


def list_indices(index_pattern, **kwargs):
    es_client = get_es_client(**kwargs)
    indices = es_client.indices.get_alias(index_pattern)
    for index, index_data in indices.items():
        print(index)


def delete_indices(index_pattern, force=False, **kwargs):
    es_client = get_es_client(**kwargs)
    indices = es_client.indices.get_alias(index_pattern)
    for index, index_data in indices.items():
        print(index)
    if force:
        response = 'Y'
    else:
        response = input('All of the above indices will be deleted. Continue? [Y/n]: ')
    if response == 'Y':
        for index, index_data in indices.items():
            logger.info(f'Deleting index {index}')
            es_client.indices.delete(index)
    else:
        print('Cancelled.')
