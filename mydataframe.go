package dataframe

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

type Record struct {
	data map[string]string
}

func CreateDataFrame(path, fileName string) (map[string]Record, []string) {
	// Check user entries
	if path[len(path)-1:] != "/" {
		path = path + "/"
	}
	if strings.Contains(fileName, ".csv") != true {
		fileName = fileName + ".csv"
	}
	// 1. Read in file
	// 2. Iterate over rows on CSV file and create Record objects
	// 3. Store Records in a map
	// 4. Returns the map with the records as well as an array of the column headers

	// Open the CSV file
	recordFile, err := os.Open(path + fileName)
	if err != nil {
		fmt.Println("Error opening the file. Please ensure the path and filename are correct.")
		os.Exit(0)
	}

	// Setup the reader
	reader := csv.NewReader(recordFile)

	// Read the records
	header, err := reader.Read()
	if err != nil {
		fmt.Println("Error reading the records")
		os.Exit(0)
	}

	// Assign BOL as the key
	var keyField int
	for i, each := range header {
		if each == "Bill of Lading" {
			keyField = i
		}
	}

	selectedKey := header[keyField]

	// Empty map to store struct objects
	myRecords := make(map[string]Record)

	// Loop over the records and create Record objects.
	fmt.Println("\nBuilding DataFrame...")
	for i := 0; ; i++ {
		record, err := reader.Read()
		if err == io.EOF {
			break // Reached end of file
		} else if err != nil {
			fmt.Println("Error")
		}
		// Create the new Record
		x := Record{make(map[string]string)}

		// Loop over columns and dynamically add column data for each header
		for i, each := range header {
			x.data[each] = record[i]
		}

		// Add Record object to map
		myRecords[x.data[selectedKey]] = x
	}
	fmt.Println("\nDataFrame Ready")
	return myRecords, header
}

func (x Record) ConvertToFloat(fieldName string) float64 {
	// Converts the value from a string to float64
	value, err := strconv.ParseFloat(x.data[fieldName], 64)
	if err != nil {
		fmt.Println("Could Not Convert to Float64")
		os.Exit(0)
	}
	return value
}

func (x Record) ConvertToDate(fieldName string) time.Time {
	// Converts the value from a string to time.Time
	value, err := time.Parse("2006-01-02", x.data[fieldName])
	if err != nil {
		fmt.Println("Could Not Convert to Date")
		os.Exit(0)
	}
	return value
}