from setuptools import setup, find_packages

# read the contents of your README file
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="ofwat-dataprepkit",
    version="0.19",
    author="Ofwat",
    description="ETL helpers focused on Fabric SQL workloads",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="fabric etl scd2",
    packages=find_packages(include=["dataprepkit", "dataprepkit.*"]),
    package_data={"dataprepkit.validation": ["schema/*.json"]},
    url="https://github.com/Ofwat/dataprepkit",
    project_urls={
        "Source": "https://github.com/Ofwat/dataprepkit",
        "Tracker": "https://github.com/Ofwat/dataprepkit/issues",
    },
    python_requires=">=3.9",
    install_requires=[
        "pandas",
        "numpy",
        "sqlalchemy",
        "pyodbc",
        "pydantic>=1.10.0",
        "pyarrow",
        "openpyxl",
        "PyYAML",
    ],
    extras_require={
        "dev": [
            "pytest",
            "duckdb",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
