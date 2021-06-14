from setuptools import setup, find_packages

setup(
    name='aws-bill-analysis',
    version='0.1.0',
    description='Tool for ingesting AWS billing data',
    packages=find_packages(),
    install_requires=[
        'click', 'boto3', 'elasticsearch', 'gzip-stream', 'tabulate', 'pytz'
    ],
    entry_points={
        'console_scripts': [
            'aws-bill-analysis=aws_bill_analysis.cli:cli',
        ]
    },
    include_package_data=True
)
