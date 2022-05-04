# pyspark_pipeline
### Installation
Clone the repo
   ```sh
   git clone https://github.com/nagarajuerigi/pyspark_pipeline.git
   ```
## Pre Processing steps

1. Upload the metamodel lookup file and data file to data folder
2. Upload notebook init_setup.ipynb, process_csv.ipynb to your work space in Databricks Community Edition to get started
3. Create Spark Cluster in Databricks with latest Run-time.
4. Run init_setup.ipynb creates the Landing dir, Lookup dir
5. Copies the file from Data dir to Lookup & Landing dir


ToDo:
Implement Header Trailer validation
Implement Audit logging
Tech reconcillation
Database file I/O

Ipython Magic API
```
from IPython.core.magic import (register_line_magic, 
                                register_cell_magic)
                                
import pandas as pd
#from StringIO import StringIO  # Python 2
from io import StringIO  # Python 3

@register_cell_magic
def csv(line, cell):
    # We create a string buffer containing the
    # contents of the cell.
    sio = StringIO(cell)
    # We use Pandas' read_csv function to parse
    # the CSV string.
    return pd.read_csv(sio)

%%csv
col1,col2,col3
0,1,2
3,4,5
7,8,9 
```

##Start adding more components/functionalities.
 Run process_csv.ipynb to test the flow and add more functionalities and add changes to your feature branch
##Working on package/library creation in feature branches with our Hackathon team

Evolution Over Time
- Prepare a UML diagram and a sequence diagram
- Develop High Level Pyspark Based ETL API 
- Write Unit test scripts, and automate the testing.
- Prepare a template driven framework for ETL operation ( Design a low level Ansible library for PySpark Based ETL Pipeline)
- Azure DevOps -> Invoke Ansible Playbook & Role (Develop Pyspark Based template plugin) -> Action: Load Libaries(PySpark ETL Library) & Invoke Databricks via databricks cli to submit job (Batch Mode ) or Access Jupyter Notebook to perform interactive coding (Transform Data from raw to different region on yourself)
- Document, packaging of this feature
- Design Integration system for this ETL framework
- Design Development, Staging, Pre-production and Production Environment to test this feature 
- Figure out how to integrate this framework with ML models

Questions:

What is essential for a ELT/ETL framework to achieve building faster datalake/datalakehouses?
1. Should it be a simplified writing and executing ETL’s
2. Should it be simple YAML Configuration?
3. Should it support developing simple unit test and E2E test?
4. Should we use connectors built on top of apache spark to do source/sink data?
5. How to structure the Spark ETL projects and speed up the development?
6. With No code experience how to setup a data pipeline
7. Should we use a library to build a data lake by configuration & separate input & output operation from transformation?
8. Building Data lakehouse on top of data lake, benefits?
9. Shoud it be simple SQL to build ELT pipelines on a data lakehouse? How to write it as notebook and schedule it?
10. Should we do transformation only by SQL? 
11. Should multi dimenionsional filtering & sampling be done directly on a storage (Data lakehouses) ?
