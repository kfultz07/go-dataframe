# go-dataframe
A simple package to abstract away the process of creating usable DataFrames for data analytics. This package is heavily inspired by the amazing Python library, Pandas.

# Generate DataFrame
User may utilize the CreateDataFrame function to create a DataFrame from an existing CSV file or create an empty DataFrame with the CreateNewDataFrame function. The user can then iterate over the DataFrame to perform the intended tasks. All data in the DataFrame is a string by default. There are various methods to provide additional functionality including: converting data types, update values, filter, concatenate, and more. Please use the below examples or explode the code to learn more.

# Import Package
```go
import (
    "fmt"

    dataframe"github.com/kfultz07/go-dataframe"
)
```

# Read, Create New Field, & Print DataFrame
```go
path := "/Users/Name/Desktop/"

// Create the DataFrame.
df := dataframe.CreateDataFrame(path, "TestData.csv")

// Create new field.
df.NewField("CWT")

// Iterate over DataFrame.
for _, row := range df.FrameRecords {
    cost := row.ConvertToFloat("Cost", df.Headers)
    weight := row.ConvertToFloat("Weight", df.Headers)

    // Results must be converted back to string.
    result := fmt.Sprintf("%f", cwt(cost, weight))

    // Update the row.
    row.Update("CWT", result, df.Headers)
}
```

# Various methods to filter DataFrames
```go
// A variadic methods that generate a new DataFrame.
dfFil := df.Filtered("Last Name", "McCarlson", "Benison", "Stephenson")
dfFil := df.Exclude("Last Name", "McCarlson", "Benison", "Stephenson")

// Keep only specific columns.
columns := [2]string{"First Name", "Last Name"}
dfFil := df.KeepColumns(columns[:])

// Filter before, after, or between specified dates.
dfFil := df.FilteredAfter("Date", "2022-12-31")
dfFil := df.FilteredBefore("Date", "2022.12-31")
dfFil := df.FilteredBetween("Date", "2022-01-01", "2022-12-31")
```

# Add record to DataFrame and later update.
```go
// Add a new record.
data := [6]string{"11", "2022-01-01", "123", "456", "Kevin", "Kevison"}
df = df.AddRecord(data[:])

// Update a value.
for _, row := range df.FrameRecords {
    // row.Val() is used to extract the value in a specific column while iterating.
    if row.Val("Last Name", df.Headers) == "McPoyle" {
        row.Update("Last Name", "SchmicMcPoyle")
    }
}
```

# Concatenate a DataFrame
```go
// ConcatFrames uses a pointer to the second DataFrame to add to the first.
// Both DataFrames must have the same columns.
df = df.ConcatFrames(&dfFil)
```

# Various Metrics & Tools
```go
// Total rows
total := df.CountRecords()

// Sum a numerical field
sum := df.Sum("Cost")

// Average a numerical field
average := df.Average("Weight")

// Min or Max of a numerical field
minimum := df.Min("Cost")
maximum := df.Max("Cost")

// Creates a slice of all unique values in a specified field.
lastNames := df.Unique("Last Name")

// Print all columns to console.
df.ViewColumns()
```