from setuptools import setup

with open('README.md', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name = 'py-publicbr',
    packages = ['publicbr', 'publicbr.cnpj'],
    version = '0.1',
    license='MIT',
    description = 'Extract and consolidate Brazilian public datasets',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author = 'Pedro Toledo',
    author_email = 'pedroltoledo@gmail.com',
    url = 'https://github.com/pltoledo/py-publicbr',
    download_url = 'https://github.com/pltoledo/py-publicbr/archive/v_01.tar.gz',
    keywords = ['public data', 'brazil', 'data', 'public', 'etl'],
    install_requires=[
        'tqdm',
        'beautifulsoup4',
        'requests',
        'Unidecode',
        'pyspark',
        'geocoder'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Office/Business',
        'Topic :: Sociology',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.8',
    ],
    python_requires='>=3.8'
)