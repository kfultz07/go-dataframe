package main

import (
	"fmt"
)

func main() {
	path := "/Users/kevinfultz/Desktop/GoProjects/go-dataframe/"
	fileName := "TestData.csv"
	df, _ := CreateDataFrame(path, fileName)

	for i, record := range df {
		fmt.Println(i, record.data["First Name"], record.ConvertToDate("Date"), record.ConvertToFloat("Cost"))
	}
}
