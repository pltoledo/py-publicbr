from pyspark.sql.dataframe import DataFrame
from pyspark.sql.column import Column
from unidecode import unidecode
import pyspark.sql.functions as f
import pyspark.sql.types as t
import os
import geocoder

def clean_types(dtype, cols) -> DataFrame:
    """
    Performs a transformation in the columns passed in `cols` based on its data type.
    
    Parameters
    ----------    
    dtype : str
        Data type of the columns. Available data types are:
        * date
        * str
        * int

    cols: List[str]
        List of columns to be transformed
    
    Returns
    -------
    pyspark.sql.dataframe.DataFrame:
        returns the resulting DataFrame
    """
    def _(df):
        nonlocal cols
        cols = [cols] if type(cols) is not list else cols
        if dtype == 'int':
            for c in cols:
                df = df.withColumn(c, f.col(c).cast('int'))
        elif dtype == 'str':
            for c in cols:
                df = df.withColumn(c, f.initcap(f.trim(unidecode_udf(f.col(c)))))
        elif dtype == 'date':
            for c in cols:
                df = df.withColumn(c, f.to_date(f.col(c), 'yyyyMMdd'))
        else:
            raise Exception('Data type not supported.')
        return df
    return _

def renamer(dict) -> DataFrame:
    """
    Renames columns in a Spark DataFrame based on a dictionary mapping.
    
    Parameters
    ----------    
    dict : dict
        Dictionary whose keys are the old column names and values are the new column names.
    
    Returns
    -------
    pyspark.sql.dataframe.DataFrame:
        returns the resulting DataFrame
    """
    def _(df):
        for old, new in dict.items():
            df = df.withColumnRenamed(old, new)
        return df
    return _

def create_dir(path) -> None:
    """
    Creates a directory in the `path`, if it does not already exists.
    
    Parameters
    ----------    
    path : str
        Path to the directory
    
    Returns
    -------
    None
    """
    if not os.path.exists(path):
        os.makedirs(path)

def join_path(*args) -> str:
    """
    Joins strings together as to form a valid direcotry path.
    
    Parameters
    ----------    
    *args : str
    
    Returns
    -------
    str:
        returns the resulting path
    """
    return os.path.join(*args).replace('\\', '/')

# PySpark UDFs

@f.udf(returnType = t.StringType())
def unidecode_udf(string) -> Column:
    """
    Normalizes a string column in a Spark DataFrame by removing special characters, such as accents.
    
    Parameters
    ----------    
    string : pyspark.sql.column.Column
        Column of a Spark DataFrame whose type is string.
    
    Returns
    -------
    pyspark.sql.column.Column:
        returns the resulting column
    """
    if string is None:
        return None
    else:
        return unidecode(string)

@f.udf(returnType = t.ArrayType(t.FloatType()))
def geocoder_udf(rua, cep, bairro, municipio, uf):
    address = f'{rua}, {bairro}, {municipio}, {uf}'
    g = geocoder.arcgis(address)
    if g.address.find(municipio) > -1 and ((g.score == 100) or (g.score >= 80 and g.quality in ["PointAddress", "StreetAddress"])):
        return [g.lat, g.lng]
    else:
        address = f'{bairro}, {municipio}, {uf}'
        g = geocoder.arcgis(address)
        if g.address.find(municipio) > -1 and g.score >= 80:
            return [g.lat, g.lng]
        else:
            address = f'{cep}, {municipio}, {uf}'
            g = geocoder.arcgis(address)
            return [g.lat, g.lng]