# go-dataframe
A simple package to abstract away the process of creating usable DataFrames for data analytics. This package is heavily inspired by the amazing Python library, Pandas.

# Generate DataFrame
User may utilize the CreateDataFrame function to create a DataFrame from an existing CSV file or create an empty DataFrame with the CreateNewDataFrame function. The user can then iterate over the DataFrame to perform the intended tasks. All data in the DataFrame is a string by default. There are various methods to provide additional functionality including: converting data types, update values, filter, concatenate, and more. Please use the below instructions for examples.

# Import Package
![Program Example](https://github.com/kfultz07/go-dataframe/blob/main/assets/images/Import.png)
```go
import (
    "fmt"

    dataframe"github.com/kfultz07/go-dataframe"
)
```

# Read, Create New Field, & Print DataFrame
![Program Example](https://github.com/kfultz07/go-dataframe/blob/main/assets/images/ReadAndPrint.png)

# Filter a DataFrame
![Program Example](https://github.com/kfultz07/go-dataframe/blob/main/assets/images/Filtered.png)

# Filter After to include all records after a specified date
![Program Example](https://github.com/kfultz07/go-dataframe/blob/main/assets/images/FilteredAfter.png)

# Filter Before to include all records before a specified date
![Program Example](https://github.com/kfultz07/go-dataframe/blob/main/assets/images/FilteredBefore.png)

# Filter Between to include only records within specified range
![Program Example](https://github.com/kfultz07/go-dataframe/blob/main/assets/images/FilteredBetween.png)

# Exclude from a DataFrame
![Program Example](https://github.com/kfultz07/go-dataframe/blob/main/assets/images/Exclude.png)

# Keep only specific columns
![Program Example](https://github.com/kfultz07/go-dataframe/blob/main/assets/images/KeepColumns.png)

# Add record to DataFrame
![Program Example](https://github.com/kfultz07/go-dataframe/blob/main/assets/images/AddRecord.png)

# Concatenate a DataFrame
![Program Example](https://github.com/kfultz07/go-dataframe/blob/main/assets/images/Concatenated.png)

# Number of Records in DataFrame
![Program Example](https://github.com/kfultz07/go-dataframe/blob/main/assets/images/Length.png)

# Slice of Unique Values
![Program Example](https://github.com/kfultz07/go-dataframe/blob/main/assets/images/Unique.png)

# Update Value in Field
![Program Example](https://github.com/kfultz07/go-dataframe/blob/main/assets/images/Update.png)