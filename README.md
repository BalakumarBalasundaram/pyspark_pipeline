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
- Write Unit test scripts, and automation
- Prepare a template driven framework for ETL operation
- Document, packaging of this feature
- Design Integration system for this ETL framework
- Design Development, Staging, Pre-production and Production Environment to test this feature 
- Figure out how to integrate this framework with ML models
