package main

import (
	"fmt"

	"github.com/kfultz07/go-dataframe"
)

func main() {
	path := "./"
	fileName := "TestData.csv"
	assignedKey := "ID"

	df, headers := dataframe.CreateDataFrame(path, fileName, assignedKey)

	for _, row := range df {
		costString := row.Val("Cost")
		costFloat := row.ConvertToFloat("Cost")
		fmt.Println(costString, costFloat)
	}
}
