from setuptools import setup, find_packages

# read the contents of your README file
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="ofwat-dataprepkit",
    version="0.6",
    author="Ofwat",
    description='ETL Tools for Fabric',
    long_description=long_description,
    long_description_content_type='text/markdown',
    keywords="fabric etl",
    packages=find_packages(),
    url="https://github.com/Ofwat/dataprepkit",
    project_urls={
        'Source': 'https://github.com/Ofwat/dataprepkit',
        'Tracker': 'https://github.com/Ofwat/dataprepkit/issues',
    },
    install_requires=[
        "pandas",
        "numpy",
        "sqlalchemy",
        "pyodbc",
    ],
)