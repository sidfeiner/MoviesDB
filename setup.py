from setuptools import setup, find_packages

version_info = (0, 0, 2)
version = '.'.join(map(str, version_info))

setup(
    name='movies-db',
    version=version,
    license='For internal usage only',
    description='source code for movies DB design',
    packages=find_packages(include='SRC'),
    install_requires=[
        'fire>=0.1.1',
        'simplejson>=3.10.0',
        'requests>=2.12.1',
        'mysql-connector-python',
        'flasgger',
        'Flask',
        'flask-mysql',
        'memoization'
    ]
)
