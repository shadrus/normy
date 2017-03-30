from distutils.core import setup
from setuptools import find_packages

setup(
    name='normy',
    version='0.11',
    packages=find_packages(exclude=['tests*']),
    install_requires=['pyodbc'],
    url='https://github.com/shadrus/normy',
    license='MIT',
    author='Yury Krylov',
    author_email='',
    description='Not an ORM Yet'
)
