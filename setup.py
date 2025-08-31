from setuptools import setup, find_packages

setup(
    name="awesome_datahub_libs",       # name of your package
    version="0.2.3",                   # start with version 0.1.0
    packages=find_packages(include=["libs", "libs.*"]),
    python_requires=">=3.7",
)
