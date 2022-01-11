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

// Convert back to Upper Case
func CreateDataFrame(path, fileName, assignedKeyField string) (map[string]Record, []string) {
	start := time.Now() // Execution start time

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

	// Assign the key field
	var keyField int
	var keyFound bool
	for i, each := range header {
		if strings.Contains(each, assignedKeyField) {
			keyField = i
			keyFound = true
		}
	}

	// Alert user if key was not found
	if keyFound != true {
		fmt.Println("The specified key field was not found. Please review.")
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

	elapsed := time.Since(start) // Calculate elapsed execution time

	fmt.Printf("DataFrame Ready\nExecution Time: %s", elapsed)
	return myRecords, header
}

func NewField(df map[string]Record, headers []string, fieldName string) (map[string]Record, []string) {
	// Creates a new field with an empty string value.
	// Must pass in the original DataFrame as well as header slice.
	// Returns a tuple with new DataFrame and headers.
	for _, row := range df {
		row.data[fieldName] = ""
	}
	headers = append(headers, fieldName)
	return df, headers
}

func SaveDataFrame(df map[string]Record, headers []string, fileName string, path string) bool {
	start := time.Now() // Execution start time

	// Create the csv file
	csvFile, err := os.Create(path + fileName + ".csv")
	defer csvFile.Close()
	if err != nil {
		fmt.Println("Error Creating CSV file")
		return false
	}

	w := csv.NewWriter(csvFile)
	defer w.Flush()

	var data [][]string
	var row []string

	// Write headers to top of file
	for _, header := range headers {
		row = append(row, header)
	}
	data = append(data, row)

	for _, record := range df {
		var row []string
		for _, header := range headers {
			row = append(row, record.data[header])
		}
		data = append(data, row)
	}
	w.WriteAll(data)

	elapsed := time.Since(start) // Calculate elapsed execution time

	fmt.Printf("DataFrame Saved\nExecution Time: %s", elapsed)

	return true
}

func (x Record) Val(fieldName string) string {
	// Return the value of the specified field
	return x.data[fieldName]
}

func (x Record) Update(fieldName, value string) {
	// Update the value in a specified field
	x.data[fieldName] = value
}

func (x Record) ConvertToFloat(fieldName string) float64 {
	// Converts the value from a string to float64
	value, err := strconv.ParseFloat(x.data[fieldName], 64)
	if err != nil {
		fmt.Println("Could Not Convert to float64")
		os.Exit(0)
	}
	return value
}

func (x Record) ConvertToInt(fieldName string) int64 {
	// Converts the value from a string to int64
	value, err := strconv.ParseInt(x.data[fieldName], 0, 64)
	if err != nil {
		fmt.Println("Could Not Convert to int64")
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

func cwt(cost, weight float64) string {
	result := cost / (weight / 100)
	return strconv.FormatFloat(result, 'E', -1, 64)
}
