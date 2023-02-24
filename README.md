<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Ballista Python Bindings (PyBallista)

This is a Python library that binds to [Apache Arrow](https://arrow.apache.org/) distributed query 
engine [Ballista](https://github.com/apache/arrow-ballista).

## Status

Ballista's Python bindings are currently not very actively maintained and were recently moved out of the main 
Ballista repository to allow that project to move faster.

These bindings are essentially a copy of the DataFusion bindings.

### What works?

- Connect to a Ballista scheduler
- Execute SQL queries
- Use DataFrame API to read files and execute queries
- Support for CSV, Parquet, Avro formats

### What does not work?

- Python UDFs

## Roadmap

- Support reading JSON.
- Support distributed Python UDFs and UDAFs.
- Add support for Substrait, allowing execution against other execution engines that are supported by DataFusion's 
  Python bindings (currently, Polars, Pandas, and cuDF).

## Examples

- [Query a Parquet file using SQL](./examples/sql-parquet.py)
- [Query a Parquet file using DataFrame API](./examples/dataframe-parquet.py)

## How to install (from pip)

```bash
pip install ballista
# or
python -m pip install ballista
```

## How to develop

This assumes that you have rust and cargo installed. We use the workflow recommended by [pyo3](https://github.com/PyO3/pyo3) and [maturin](https://github.com/PyO3/maturin).

Bootstrap:

```bash
# fetch this repo
git clone git@github.com:apache/arrow-ballista-python.git
# change to python directory
cd arrow-ballista-python
# prepare development environment (used to build wheel / install in development)
python3 -m venv venv
# activate the venv
source venv/bin/activate
# update pip itself if necessary
python -m pip install -U pip
# if python -V gives python 3.7
python -m pip install -r requirements-37.txt
# if python -V gives python 3.8/3.9/3.10
python -m pip install -r requirements-310.txt
```

Whenever rust code changes (your changes or via `git pull`):

```bash
# make sure you activate the venv using "source venv/bin/activate" first
maturin develop
python -m pytest
```

## How to update dependencies

To change test dependencies, change the `requirements.in` and run

```bash
# install pip-tools (this can be done only once), also consider running in venv
python -m pip install pip-tools

# change requirements.in and then run
python -m piptools compile --generate-hashes -o requirements-37.txt
# or run this is you are on python 3.8/3.9/3.10
python -m piptools compile --generate-hashes -o requirements.txt
```

To update dependencies, run with `-U`

```bash
python -m piptools compile -U --generate-hashes -o requirements-310.txt
```

More details [here](https://github.com/jazzband/pip-tools)
