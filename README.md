# NHANES task
A package for creating machine learning tasks based on the [NHANES study](https://wwwn.cdc.gov/Nchs/Nhanes/).

# Structure

The main script files of the package are

```
download_NHANES.py        # Downloads a specified set of files based on year
parse_NHANES.py           # Parses the downloaded documentation (.htm) files to create a .json code list
create_NHANES_dataset.py  # Creates a Pandas dataframe based on the downloaded data files (.xpt) and a configuration file specifying the desired variables
```

The scripts should be executed in the order above. 

# Example: Predicting hypertension

The file ```configs/NHANES_hypertension.yml``` specifies variables used in a hypertension prediction task used by ```create_NHANES_dataset.py```

