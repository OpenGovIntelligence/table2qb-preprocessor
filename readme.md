
table2qb-python-wrapper
----------

A- Python wrapper for the clujore tool [table2qb]

B - Auto Generates the codelist inputs for table2qb based on the components description and observations values.

C- change observations formate [from multible measure per row to one measure per row]

D- Handles chunking of big datasets [to resolve memory limitation of table2qb]. 

E- [webservice] Enabling table2qb as webservice -- not yet implemented.

### usage

> $ python table2qb-Wrapper.py [pipeline name] [dataset name] [base uri] [slug] [components file] [observations] [columns config]

### example

> $ python table2qb-Wrapper.py codelist-pipeline test_ds marine.ie/obs test_slug example1/components.csv example1/observations.csv example1/columns.csv