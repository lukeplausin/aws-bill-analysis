import os
import sys
import click

from . import logger, log_handler


@click.group(help='Commands to manage AWS report billing data')
@click.option('--debug/--no-debug', default=False, show_default=True)
def cli(debug=False, **kwargs):
    if debug:
        logger.setLevel(logging.DEBUG)
        log_handler.setLevel(logging.DEBUG)
        logger.debug("Debug mode ON, debug level logging will be printed.")

from tabulate import tabulate

from .report_parser import get_available_reports, find_and_ingest_cost_reports, delete_indices, list_indices

######################################################################################
###                            CLI stuff
######################################################################################

# @cli.group(help='Bill data ingest commands')
# # @click.option('--debug/--no-debug', default=False, show_default=True)
# def ingest(**kwargs):
#     pass


@cli.command(help='List cost monitoring reports available in S3.')
@click.option('--max-age', required=False, default=-1, type=int, help='Oldest age of report in days (default - no limit)')
def list_cost_reports(**kwargs):
    reports = get_available_reports(**kwargs)
    print(tabulate(reports))


@cli.command(help='Ingest cost monitoring reports from S3.')
@click.option('--max-age', required=False, default=-1, type=int, help='Oldest age of report in days (default - no limit)')
@click.option('--profile-name', required=False, default="my-main-account-profile", help='Name of AWS profile to use')
@click.option('--s3-bucket-name', required=False, default="my-billing-reports", help='Name of S3 bucket')
@click.option('--base-path', required=False, default="cur/cost-report/", help='Base path (prefix) of S3 bucket where the cost usage reports are located')
@click.option('--remove-empty-fields/--no-remove-empty-fields', required=False, default=True, help='Remove fields which are empty?')
@click.option('--format-datatypes/--no-format-datatypes', required=False, default=True, help='Change the format of data types according to manifest file?')
@click.option('--remove-colon/--no-remove-colon', required=False, default=True, help='Remove the colon from any keys?')
@click.option('--special-fixes/--no-special-fixes', required=False, default=True, help='Apply special fixes to the data?')
@click.option('--index-name', required=False, default='aws-billing-usage-{timestamp:%Y.%m.%d}', help='Name of index (use timestamp and python string formatter).')
@click.option('--es-user', required=True, help='Elasticsearch username')
@click.option('--es-pass', required=True, help='Elasticsearch password')
@click.option('--es-port', required=True, help='Elasticsearch port')
@click.option('--es-host', required=True, help='Elasticsearch hostname')

def ingest_cost_reports(**kwargs):
    find_and_ingest_cost_reports(**kwargs)

@cli.command(help='Delete elasticsearch indices.')
@click.option('--max-age', required=False, default=-1, type=int, help='Oldest age of report in days (default - no limit)')
@click.option('--index-pattern', required=True, help='Provide a regex matching pattern for the index names')
@click.option('--force', required=False, is_flag=True, help='Confirm action (use with caution)', )
@click.option('--es-user', required=True, help='Elasticsearch username')
@click.option('--es-pass', required=True, help='Elasticsearch password')
@click.option('--es-port', required=True, help='Elasticsearch port')
@click.option('--es-host', required=True, help='Elasticsearch hostname')
def delete_es_indices(**kwargs):
    delete_indices(**kwargs)


@cli.command(help='List elasticsearch indices.')
@click.option('--max-age', required=False, default=-1, type=int, help='Oldest age of report in days (default - no limit)')
@click.option('--index-pattern', required=False, default='*', help='Provide a regex matching pattern for the index names')
@click.option('--es-user', required=True, help='Elasticsearch username')
@click.option('--es-pass', required=True, help='Elasticsearch password')
@click.option('--es-port', required=True, help='Elasticsearch port')
@click.option('--es-host', required=True, help='Elasticsearch hostname')
def list_es_indices(**kwargs):
    list_indices(**kwargs)
