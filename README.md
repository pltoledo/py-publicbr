# py-publicbr
[![MIT license](https://img.shields.io/badge/License-MIT-blue.svg)](https://lbesson.mit-license.org/)
[![Documentation Status](https://readthedocs.org/projects/py-publicbr/badge/?version=latest)](https://py-publicbr.readthedocs.io/en/latest/?badge=latest)

`py-publicbr` is a `Python` package used to extract and consolidate public data made available by many Brazilian governmental entities. Currently it supports the following data sources:

* [CNPJ open data](https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj), by Receita Federal

## Dependecies

Aside from the packages explicited in the `requirements.txt`, `py-publicbr` also relies on Spark, as many of the datasets sources are really big. That beign said, the most important dependencies are:

* Java >= 1.8
* Spark >= 3.0

The Spark API used is mainly the Spark SQL lib in the form of `pyspark.sql`.

## Examples

The core idea of the package is to define various sources composed of a crawler and a cleaner. The crawler is responsible for data extraction, while the cleaner performs the data manipulation tasks to prepare datasets for consumption. A `PublicSource` require two arguments: a `SparkSession` to make available all of Spark's funcionalities and a `file_dir` which is the directory where the downloaded and consolidated data will be written. 

An example using the CNPJ source is as follows:

```python
from publicbr.cnpj import CNPJSource
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

source = CNPJSource(spark, 'path/to/dir')
source.create()
```

This way, the data will be downloaded and further consolidated, while also being written in the directory specified. If one wants to only download or only consolidate the data, it is possible to use the individual objects:

### Download Data
```python
from publicbr.cnpj import CNPJCrawler

crawler = CNPJCrawler('path/to/save/dir')
crawler.run()
```

### Consolidate Data
```python
# Cleans Estabelecimentos table of CNPJ source
from publicbr.cnpj import EstabCleaner
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

cleaner = EstabCleaner(
    spark,
    'path/to/raw/data', # Path to raw data directory
    'path/to/save/dir' # Path to save consolidated data 
)
cleaner.clean()
```

## Author
Created by Pedro Toledo. Feel free to contact me!

[![Twitter Badge](https://img.shields.io/badge/-@pedrotol_-1ca0f1?style=flat-square&labelColor=1ca0f1&logo=twitter&logoColor=white&link=https://twitter.com/pedrotol_)](https://twitter.com/pedrotol_)
![Linkedin Badge](https://img.shields.io/badge/-Pedro-blue?style=flat-square&logo=Linkedin&logoColor=white&link=https://www.linkedin.com/in/pedro-toledo/)
[![Gmail Badge](https://img.shields.io/badge/-pedroltoledo@gmail.com-c14438?style=flat-square&logo=Gmail&logoColor=white&link=mailto:tgmarinho@gmail.com)](mailto:pedroltoledo@gmail.com)