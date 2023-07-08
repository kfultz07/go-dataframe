# go-dataframe
A simple package to abstract away the process of creating usable DataFrames for data analytics. This package is heavily inspired by the amazing Python library, Pandas.

## Generate DataFrame
Utilize the CreateDataFrame function to create a DataFrame from an existing CSV file or create an empty DataFrame with the CreateNewDataFrame function. The user can then iterate over the DataFrame to perform the intended tasks. All data in the DataFrame is a string by default. There are various methods to provide additional functionality including: converting data types, update values, filter, concatenate, and more. Please use the below examples or explore the code to learn more.

## Import Package
```go
import (
    "fmt"

    dataframe "github.com/kfultz07/go-dataframe"
)
```

## Load CSV into DataFrame, create a new field, and save
```go
path := "/Users/Name/Desktop/"

// Create the DataFrame
df := dataframe.CreateDataFrame(path, "TestData.csv")

// Create new field
df.NewField("CWT")

// Iterate over DataFrame
for _, row := range df.FrameRecords {
    cost := row.ConvertToFloat("Cost", df.Headers)
    weight := row.ConvertToFloat("Weight", df.Headers)

    // Results must be converted back to string
    result := fmt.Sprintf("%f", cwt(cost, weight))

    // Update the row
    row.Update("CWT", result, df.Headers)
}

df.SaveDataFrame(path, "NewFileName")
```

## Concurrently load multiple CSV files into DataFrames
Tests performed utilized four files with a total of 5,746,452 records and a varing number of columns. Results indicated an average total load time of 8.81 seconds when loaded sequentially and 4.06 seconds when loaded concurrently utilizing the LoadFrames function. An overall 54% speed improvement. Files must all be in the same directory. Results are returned in a
slice in the same order as provided in the files parameter.
```go
filePath := "/Users/Name/Desktop/"
files := []string{
    "One.csv",
    "Two.csv",
    "Three.csv",
    "Four.csv",
    "Five.csv",
}

results, err := LoadFrames(filePath, files)
if err != nil {
    log.Fatal(err)
}

dfOne := results[0]
dfTwo := results[1]
dfThree := results[2]
dfFour := results[3]
dfFive := results[4]
```

## Stream CSV data
Stream rows of data from a csv file to be processed. Streaming data is preferred when dealing with large files and memory usage needs to be considered. Results are streamed via a channel with a StreamingRecord type. A struct with only desired fields could be created and either operated on sequentially or stored in a slice for later use.
```go
type Product struct {
    name string
    cost float64
    weight float64
}

func (p Product) CostPerLb() float64 {
    if p.weight == 0.0 {
        return 0.0
    }
    return p.cost / p.weight
}

filePath := "/Users/Name/Desktop/"

var products []Product

c := make(chan StreamingRecord)
go Stream(filePath, "TestData.csv", c)

for row := range c {
    prod := Product{
        name: row.Val("Name"),
        cost: row.ConvertToFloat("Cost"),
        weight: row.ConvertToInt("Weight"),
    }
    products = append(products, prod)
}
```

## AWS S3 Cloud Storage
```go
// Download a DataFrame from an S3 bucket
path := "/Users/Name/Desktop/" // File path
fileName := "FileName.csv" // File in AWS Bucket must be .csv
bucketName := "BucketName" // Name of the bucket
bucketRegion := "BucketRegion" // Can be found in the Properties tab in the S3 console (ex. us-west-1)
awsAccessKey := "AwsAccessKey" // Access keys can be loaded from environment variables within you program
awsSecretKey := "AwsSecretKey"
df := CreateDataFrameFromAwsS3(path, fileName, bucketName, bucketRegion, awsAccessKey, awsSecretKey)

// Upload a file to an S3 bucket
err := UploadFileToAwsS3(path, fileName, bucket, region)
if err != nil {
    panic(err)
}
```

## Various methods to filter DataFrames
```go
// Variadic methods that generate a new DataFrame
dfFil := df.Filtered("Last Name", "McCarlson", "Benison", "Stephenson")
dfFil := df.Exclude("Last Name", "McCarlson", "Benison", "Stephenson")

// Keep only specific columns
columns := [2]string{"First Name", "Last Name"}
dfFil := df.KeepColumns(columns[:])

// Remove multiple columns
dfFil := df.RemoveColumns("ID", "Cost", "First Name")

// Remove a single column
dfFil := df.RemoveColumns("First Name")

// Filter before, after, or between specified dates
dfFil := df.FilteredAfter("Date", "2022-12-31")
dfFil := df.FilteredBefore("Date", "2022-12-31")
dfFil := df.FilteredBetween("Date", "2022-01-01", "2022-12-31")

// Filter a numerical column based on a provided value
df, err := df.GreaterThanOrEqualTo("Cost", float64(value))
if err != nil {
    panic(err)
}

df, err := df.LessThanOrEqualTo("Weight", float64(value))
if err != nil {
    panic(err)
}
```

## Add record to DataFrame and later update
```go
// Add a new record
data := [6]string{"11", "2022-01-01", "123", "456", "Kevin", "Kevison"}
df = df.AddRecord(data[:])

// Update a value
for _, row := range df.FrameRecords {
    // row.Val() is used to extract the value in a specific column while iterating
    if row.Val("Last Name", df.Headers) == "McPoyle" {
        row.Update("Last Name", "SchmicMcPoyle", df.Headers)
    }
}
```

## Concatenate DataFrames
```go
// ConcatFrames uses a pointer to the DataFrame being appended.
// Both DataFrames must have the same columns in the same order.
df, err := df.ConcatFrames(&dfFil)
if err != nil {
    panic("ConcatFrames Error: ", err)
}
```

## Rename a Column
```go
// Rename an existing column in a DataFrame
// First parameter provides the original column name to be updated.
// The next parameter is the desired new name.
err := df.Rename("Weight", "Total Weight")
if err != nil {
    panic("Rename Column Error: ", err)
}
```

## Merge two DataFrames
```go
df := CreateDataFrame(path, "TestData.csv")
dfRight := CreateDataFrame(path, "TestDataRight.csv")

// Merge all columns found in right DataFrame into left DataFrame.
// User provides the lookup column with the unique values that link the two DataFrames.
df.Merge(&dfRight, "ID")

// Merge only specified columns from right DataFrame into left DataFrame.
// User provides columns immediately after the lookup column.
df.Merge(&dfRight, "ID", "City", "State")

// Inner merge all columns on a specified primary key.
// Results will only include records where the primary key is found in both DataFrames.
df = df.InnerMerge(&dfRight, "ID")
```

## Various Tools
```go
// Total rows
total := df.CountRecords()

// Returns a slice of all unique values in a specified column
lastNames := df.Unique("Last Name")

// Print all columns to console
df.ViewColumns()

// Returns a slice of all columns in order
foundColumns := df.Columns()

// Generates a decoupled copy of an existing DataFrame.
// Changes made in one DataFrame will not be reflected in the other.
df2 := df.Copy()
```

## Mathematics
```go
// Sum a numerical column
sum := df.Sum("Cost")

// Average a numerical column
average := df.Average("Weight")

// Min or Max of a numerical column
minimum := df.Min("Cost")
maximum := df.Max("Cost")

// Calculate the standard deviation of a numerical column
stdev, err := df.StandardDeviation("Cost")
if err != nil {
    panic(err)
}
```
