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

type DataFrame struct {
	myRecords map[int]Record
	headers   []string
}

// Convert back to Upper Case
func CreateDataFrame(path, fileName string) DataFrame {
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

	// Empty map to store struct objects
	myRecords := make(map[int]Record)

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
		for pos, each := range header {
			x.data[each] = record[pos]
		}

		// Add Record object to map
		myRecords[i] = x
	}

	newFrame := DataFrame{myRecords: myRecords, headers: header}

	elapsed := time.Since(start) // Calculate elapsed execution time

	fmt.Printf("\nDataFrame Ready\nExecution Time: %s\n", elapsed)
	return newFrame
}

// Generates a new filtered DataFrame.
// New DataFrame will be kept in same order as original.
func (frame DataFrame) Filtered(fieldName, value string) DataFrame {
	myRecords := make(map[int]Record)

	pos := 0
	for i := 0; i < len(frame.myRecords); i++ {
		if frame.myRecords[i].data[fieldName] == value {
			x := Record{make(map[string]string)}

			// Loop over columns
			for _, each := range frame.headers {
				x.data[each] = frame.myRecords[i].data[each]
			}

			myRecords[pos] = x
			pos++
		}
	}
	newFrame := DataFrame{myRecords: myRecords, headers: frame.headers}

	return newFrame
}

// Creates a new field and assigns it the provided value.
// Must pass in the original DataFrame as well as header slice.
// Returns a tuple with new DataFrame and headers.
func (frame DataFrame) NewField(fieldName string) DataFrame {
	for _, row := range frame.myRecords {
		row.data[fieldName] = ""
	}
	frame.headers = append(frame.headers, fieldName)
	return frame
}

func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}
	return false
}

// Return a slice of all unique values found in a specified field.
func (frame DataFrame) Unique(fieldName string) []string {
	var results []string

	for _, row := range frame.myRecords {
		if contains(results, row.Val(fieldName)) != true {
			results = append(results, row.Val(fieldName))
		}
	}
	return results
}

// Stack two DataFrames with matching headers.
func (frame DataFrame) ConcatFrames(dfNew DataFrame) DataFrame {
	keyStart := len(frame.myRecords)

	// Iterate over new dataframe in order
	for i := 0; i < len(dfNew.myRecords); i++ {
		// Create new Record
		x := Record{make(map[string]string)}

		// Iterate over headers and add data to Record
		for _, header := range frame.headers {
			x.data[header] = dfNew.myRecords[i].Val(header)
		}
		frame.myRecords[keyStart] = x
		keyStart++
	}
	return frame
}

func (frame DataFrame) CountRecords() int {
	return len(frame.myRecords)
}

func (frame DataFrame) SaveDataFrame(path, fileName string) bool {
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
	for _, header := range frame.headers {
		row = append(row, header)
	}
	data = append(data, row)

	// Iterate over map by order of index or keys.
	for i := 0; i < len(frame.myRecords); i++ {
		var row []string
		for _, header := range frame.headers {
			row = append(row, frame.myRecords[i].data[header])
		}
		data = append(data, row)
	}

	w.WriteAll(data)

	elapsed := time.Since(start) // Calculate elapsed execution time

	fmt.Printf("\nDataFrame Saved\nExecution Time: %s\n", elapsed)

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
