package main

import (
	"bufio"
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

func (x Record) convertToFloat(fieldName string) float64 {
	// Converts the value from a string to float64
	value, err := strconv.ParseFloat(x.data[fieldName], 64)
	if err != nil {
		fmt.Println("Could Not Convert to Float64")
		os.Exit(0)
	}
	return value
}

func (x Record) convertToDate(fieldName string) time.Time {
	// Converts the value from a string to time.Time
	value, err := time.Parse("2006-01-01", x.data[fieldName])
	if err != nil {
		fmt.Println("Could Not Convert to Date")
		os.Exit(0)
	}
	return value
}

func createDataFrame(path, fileName string) (map[string]Record, []string) {
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

	// List all found headers as guide to establish key field
	for i, each := range header {
		fmt.Println(i, each)
	}

	// User input for key field
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Printf("Key Field: ")
	scanner.Scan()
	keyFieldString := scanner.Text()
	keyField, err := strconv.Atoi(keyFieldString)
	if err != nil {
		fmt.Println("Unknown Key Field")
		os.Exit(0)
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

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Printf("Path: ")
	scanner.Scan()
	path := scanner.Text()
	fmt.Printf("File Name: ")
	scanner.Scan()
	fileName := scanner.Text()

	// Check user entries
	if path[len(path)-1:] != "/" {
		path = path + "/"
	}
	if strings.Contains(fileName, ".csv") != true {
		fileName = fileName + ".csv"
	}

	myRecords, headers := createDataFrame(path, fileName)

	fmt.Println(len(myRecords), headers)
}
