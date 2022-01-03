# go-dataframe
A simple package to abstract away the process of reading in a CSV file and converting it to a usable DataFrame.

# Generate DataFrame
The user specifies the path and filename of the CSV file they wish to read. They then call the CreateDataFrame function to read the CSV file, convert it to a DataFrame, and return both the DataFrame as well as the header names. The user can then iterate over the DataFrame to perform the intended tasks. All data in the DataFrame is by default a string. The ConvertToFloat and ConvertToDate methods are available to change the type if another datatype is needed.

# Example Program
package main

import (
	"fmt"

	dataframe "github.com/kfultz07/go-dataframe"
)

func main() {
	path := "/Users/kevinfultz/Desktop/GoProjects/Playground/"
	fileName := "TestData.csv"
	total := 0.0

	myRecords, headers := dataframe.CreateDataFrame(path, fileName)

	for _, header := range headers {
		fmt.Println(header)
	}

	for i, each := range myRecords {
		fmt.Println(i, each)
		total += each.ConvertToFloat("Cost")
	}

	fmt.Println(total)
	fmt.Println("Complete")
}
