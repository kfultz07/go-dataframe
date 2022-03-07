# go-dataframe
A simple package to abstract away the process of reading in a CSV file and converting it to a usable DataFrame.

# Generate DataFrame
The user specifies the path and filename of the CSV file they wish to read. They then call the CreateDataFrame function to read the CSV file, convert it to a DataFrame, and return both the DataFrame as well as the header names. The user can then iterate over the DataFrame to perform the intended tasks. All data in the DataFrame is by default a string. The ConvertToFloat and ConvertToDate methods are available to change the type if another datatype is needed.

# Update Value in Field
![Program Example](https://github.com/kfultz07/go-dataframe/blob/main/assets/images/Import.png)

# Read & Print DataFrame
![Program Example](https://github.com/kfultz07/go-dataframe/blob/main/assets/images/ReadAndPrint.png)

# Filter a DataFrame
![Program Example](https://github.com/kfultz07/go-dataframe/blob/main/assets/images/Filtered.png)

# Concatenate a DataFrame
![Program Example](https://github.com/kfultz07/go-dataframe/blob/main/assets/images/Concatenated.png)

# Number of Records in DataFrame
![Program Example](https://github.com/kfultz07/go-dataframe/blob/main/assets/images/Length.png)

# Slice of Unique Values
![Program Example](https://github.com/kfultz07/go-dataframe/blob/main/assets/images/Unique.png)

# Update Value in Field
![Program Example](https://github.com/kfultz07/go-dataframe/blob/main/assets/images/Update.png)