pyftdc
==============

[![Gitter][gitter-badge]][gitter-link]

|      CI              | status |
|----------------------|--------|
| pip builds           | [![Pip Actions Status][actions-pip-badge]][actions-pip-link] |



A MongoDB FTDC files parser written in C++ that provides Python bindings using [pybind11](https://github.com/pybind/pybind11) and scikit-build.



[gitter-badge]:            https://badges.gitter.im/pybind/Lobby.svg
[gitter-link]:             https://gitter.im/pybind/Lobby
[actions-badge]:           https://github.com/pybind/pyftdc/workflows/Tests/badge.svg
[actions-conda-link]:      https://github.com/pybind/pyftdc/actions?query=workflow%3AConda
[actions-conda-badge]:     https://github.com/pybind/pyftdc/workflows/Conda/badge.svg
[actions-pip-link]:        https://github.com/pybind/pyftdc/actions?query=workflow%3APip
[actions-pip-badge]:       https://github.com/pybind/pyftdc/workflows/Pip/badge.svg
[actions-wheels-link]:     https://github.com/pybind/pyftdc/actions?query=workflow%3AWheels
[actions-wheels-badge]:    https://github.com/pybind/pyftdc/workflows/Wheels/badge.svg

Installation
------------

**Building on Unix (Ubuntu, Centos, macOS)**

 You will need to install packages. Please see https://github.com/jorge-imperial/mongo_ftdc/blob/main/docs/build.md
 
  
 1. clone this repository and change to the top level directory.
      ```
      git clone git@github.com:jorge-imperial/mongo_ftdc.git 
      cd mongo_ftdc
      ```
      
 2. Install Python libraries to build binaries. Create a virtual environment to make your life easier.
      ```
      python3 -m venv venv
      source venv/bin/activate
      pip install .
      ```
    You will now have built and installed in your virtual environment.
    

Alternatively, you can use setup.py directly, but for that you will need to manually install the required libraries by running
     ```
     pip install -r requirements
     ```
After which you can create a source distribution or a binary wheel:
     ```
     python3 setup.py sdist
     python3 setup.py bdist_wheel
     ```


**Building on Windows**
  
  Not tested yet, but 'It should work'(TM)
  


License
-------

Apache V2

Test call
---------

```python
import pyftdc

# Create a parser object
p = pyftdc.FTDCParser()

diagnostics_path = 'tests/diagnostic.data'   
# Parse a test directory
status = p.parse_dir(diagnostics_path)
 
if status == 0:
    print(f"Parsed sample data dir")
    meta = p.metadata
    if len(meta) > 0:
        print(meta[0])
    print(f"metadata has {len(meta)} elements")

    ts = p.timestamps()
    print(f"There are {len(ts)} timestamps")

    metrics = p.metric_names

    for m in metrics:
        print(f"\tMetric: {m}")
    print(f"There are {len(metrics)} metrics")

    # A metric
    m = p.get_metric( metrics[37] )
    #print(f"Metric values {m}")
    n = p.get_metric(metrics[73])
    #print(f"Another metric  {n}")

    # As Numpy ndarray
    s = p.get_metric_numpy(metrics[15])
    type_of_var = str(type(s))
    print(f"Metric '{metrics[15]}' is a {type_of_var} with {len(s)} elements")

    metric_list = [metrics[15],metrics[16], metrics[17],metrics[18] ]
    ss = p.get_metrics_list_numpy(metric_list)
    type_of_var = str(type(ss))
    print(f"Metric list '{metric_list}' is a {type_of_var} with {len(ss)} elements")

    i = 0
    for element in ss:
        type_of_var = str(type(element))
        print(f"\t{metric_list[i]}type {type_of_var} with {len(element)} elements")
        i += 1

    # Numpy matrix (metrics x samples)
    m = p.get_metrics_list_numpy_matrix(['start', 'end', 'systemMetrics.disks.nvme1n1.reads', 'systemMetrics.cpu.num_cpus', 'serverStatus.connections.current'])
    print(m)

else:
    print(f"foo: status is {status}")


```

[`cibuildwheel`]:          https://cibuildwheel.readthedocs.io
