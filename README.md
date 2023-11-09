## Databend UDF Server SDK
This library provides a SDK for creating user-defined functions (UDF) server in Databend.

![Python](https://img.shields.io/pypi/v/databend-udf)

### Introduction
Databend supports user-defined functions implemented as external functions. With the Databend Python UDF API, users can define custom UDFs using Python and start a Python process as a UDF server. Then users can call the customized UDFs in Databend. Databend will remotely access the UDF server to execute the defined functions.
