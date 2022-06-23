package dataframe

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type Record struct {
	Data []string
}

type DataFrame struct {
	FrameRecords []Record
	Headers      map[string]int
}

// Generate a new empty DataFrame.
func CreateNewDataFrame(headers []string) DataFrame {
	myRecords := []Record{}
	theHeaders := make(map[string]int)

	// Add headers to map in correct order.
	for i := 0; i < len(headers); i++ {
		theHeaders[headers[i]] = i
	}

	newFrame := DataFrame{FrameRecords: myRecords, Headers: theHeaders}

	return newFrame
}

// Generate a new DataFrame sourced from a csv file.
func CreateDataFrame(path, fileName string) DataFrame {
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
		log.Fatal("Error opening the file. Please ensure the path and filename are correct.")
	}

	// Setup the reader
	reader := csv.NewReader(recordFile)

	// Read the records
	header, err := reader.Read()
	if err != nil {
		log.Fatal("Error reading the records")
	}

	headers := make(map[string]int)
	for i, columnName := range header {
		headers[columnName] = i
	}

	// Remove Byte Order Marker for UTF-8 files.
	for i, each := range header {
		byteSlice := []byte(each)
		if byteSlice[0] == 239 && byteSlice[1] == 187 && byteSlice[2] == 191 {
			header[i] = each[3:]
		}
	}

	// Empty map to store struct objects
	sliceOfSlices := []Record{}

	// Loop over the records and create Record objects.
	for i := 0; ; i++ {
		record, err := reader.Read()
		if err == io.EOF {
			break // Reached end of file
		} else if err != nil {
			log.Fatal("Error in record loop.")
		}
		// Create the new Record
		x := Record{[]string{}}

		// Loop over records and add to Data field of Record struct.
		for _, r := range record {
			x.Data = append(x.Data, r)
		}

		// Add Record object to map
		// myRecords[i] = x
		sliceOfSlices = append(sliceOfSlices, x)
	}

	newFrame := DataFrame{FrameRecords: sliceOfSlices, Headers: headers}

	return newFrame
}

// User specifies columns they want to keep from a preexisting DataFrame.
func (frame DataFrame) KeepColumns(columns []string) DataFrame {
	df := CreateNewDataFrame(columns)

	for _, row := range frame.FrameRecords {
		var newData []string
		for _, column := range columns {
			newData = append(newData, row.Val(column, frame.Headers))
		}
		df.AddRecord(newData)
	}

	return df
}

// Add a new record to the DataFrame.
func (frame DataFrame) AddRecord(newData []string) DataFrame {
	x := Record{[]string{}}

	for _, each := range newData {
		x.Data = append(x.Data, each)
	}

	frame.FrameRecords = append(frame.FrameRecords, x)

	return frame
}

// Generates a new filtered DataFrame.
// New DataFrame will be kept in same order as original.
func (frame DataFrame) Filtered(fieldName string, value ...string) DataFrame {
	headers := []string{}

	for i := 0; i < len(frame.Headers); i++ {
		for k, v := range frame.Headers {
			if v == i {
				headers = append(headers, k)
			}
		}
	}
	newFrame := CreateNewDataFrame(headers)

	for i := 0; i < len(frame.FrameRecords); i++ {
		if contains(value, frame.FrameRecords[i].Data[frame.Headers[fieldName]]) == true {
			newFrame = newFrame.AddRecord(frame.FrameRecords[i].Data)
		}
	}

	return newFrame
}

// Generates a new DataFrame that excludes specified instances.
// New DataFrame will be kept in same order as original.
func (frame DataFrame) Exclude(fieldName string, value ...string) DataFrame {
	headers := []string{}

	for i := 0; i < len(frame.Headers); i++ {
		for k, v := range frame.Headers {
			if v == i {
				headers = append(headers, k)
			}
		}
	}
	newFrame := CreateNewDataFrame(headers)

	for i := 0; i < len(frame.FrameRecords); i++ {
		if contains(value, frame.FrameRecords[i].Data[frame.Headers[fieldName]]) == false {
			newFrame = newFrame.AddRecord(frame.FrameRecords[i].Data)
		}
	}

	return newFrame
}

// Generates a new filtered DataFrame with all records occuring after a specified date provided by the user.
// User must provide the date field as well as the desired date.
// Instances where record dates occur on the same date provided by the user will not be included. Records must occur
// after the specified date.
func (frame DataFrame) FilteredAfter(fieldName, desiredDate string) DataFrame {
	headers := []string{}

	for i := 0; i < len(frame.Headers); i++ {
		for k, v := range frame.Headers {
			if v == i {
				headers = append(headers, k)
			}
		}
	}
	newFrame := CreateNewDataFrame(headers)

	for i := 0; i < len(frame.FrameRecords); i++ {
		recordDate := dateConverter(frame.FrameRecords[i].Data[frame.Headers[fieldName]])
		isAfter := recordDate.After(dateConverter(desiredDate))

		if isAfter {
			newFrame = newFrame.AddRecord(frame.FrameRecords[i].Data)
		}
	}
	return newFrame
}

// Generates a new filtered DataFrame with all records occuring before a specified date provided by the user.
// User must provide the date field as well as the desired date.
// Instances where record dates occur on the same date provided by the user will not be included. Records must occur
// before the specified date.
func (frame DataFrame) FilteredBefore(fieldName, desiredDate string) DataFrame {
	headers := []string{}

	for i := 0; i < len(frame.Headers); i++ {
		for k, v := range frame.Headers {
			if v == i {
				headers = append(headers, k)
			}
		}
	}
	newFrame := CreateNewDataFrame(headers)

	for i := 0; i < len(frame.FrameRecords); i++ {
		recordDate := dateConverter(frame.FrameRecords[i].Data[frame.Headers[fieldName]])
		isBefore := recordDate.Before(dateConverter(desiredDate))

		if isBefore {
			newFrame = newFrame.AddRecord(frame.FrameRecords[i].Data)
		}
	}

	return newFrame
}

// Generates a new filtered DataFrame with all records occuring between a specified date range provided by the user.
// User must provide the date field as well as the desired date.
// Instances where record dates occur on the same date provided by the user will not be included. Records must occur
// between the specified start and end dates.
func (frame DataFrame) FilteredBetween(fieldName, startDate, endDate string) DataFrame {
	headers := []string{}

	for i := 0; i < len(frame.Headers); i++ {
		for k, v := range frame.Headers {
			if v == i {
				headers = append(headers, k)
			}
		}
	}
	newFrame := CreateNewDataFrame(headers)

	for i := 0; i < len(frame.FrameRecords); i++ {
		recordDate := dateConverter(frame.FrameRecords[i].Data[frame.Headers[fieldName]])
		isAfter := recordDate.After(dateConverter(startDate))
		isBefore := recordDate.Before(dateConverter(endDate))

		if isAfter && isBefore {
			newFrame = newFrame.AddRecord(frame.FrameRecords[i].Data)
		}
	}

	return newFrame
}

// Creates a new field and assigns it the provided value.
// Must pass in the original DataFrame as well as header slice.
// Returns a tuple with new DataFrame and headers.
func (frame *DataFrame) NewField(fieldName string) {
	for i, _ := range frame.FrameRecords {
		frame.FrameRecords[i].Data = append(frame.FrameRecords[i].Data, "")
	}
	frame.Headers[fieldName] = len(frame.Headers)
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

	for _, row := range frame.FrameRecords {
		if contains(results, row.Val(fieldName, frame.Headers)) != true {
			results = append(results, row.Val(fieldName, frame.Headers))
		}
	}
	return results
}

// Stack two DataFrames with matching headers.
func (frame DataFrame) ConcatFrames(dfNew *DataFrame) DataFrame {
	// Iterate over new dataframe in order
	for i := 0; i < len(dfNew.FrameRecords); i++ {
		frame.FrameRecords = append(frame.FrameRecords, dfNew.FrameRecords[i])
	}
	return frame
}

func (frame DataFrame) CountRecords() int {
	return len(frame.FrameRecords)
}

func (frame *DataFrame) SaveDataFrame(path, fileName string) bool {
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
	columnLength := len(frame.Headers)

	// Write headers to top of file
	for i := 0; i < columnLength; i++ {
		for k, v := range frame.Headers {
			if v == i {
				row = append(row, k)
			}
		}
	}
	data = append(data, row)

	// Add Data
	for i := 0; i < len(frame.FrameRecords); i++ {
		var row []string
		for pos := 0; pos < columnLength; pos++ {
			row = append(row, frame.FrameRecords[i].Data[pos])
		}
		data = append(data, row)
	}

	w.WriteAll(data)

	return true
}

func (x Record) Val(fieldName string, headers map[string]int) string {
	// Return the value of the specified field
	return x.Data[headers[fieldName]]
}

func (x Record) Update(fieldName, value string, headers map[string]int) {
	// Update the value in a specified field
	x.Data[headers[fieldName]] = value
}

func (x Record) ConvertToFloat(fieldName string, headers map[string]int) float64 {
	// Converts the value from a string to float64
	value, err := strconv.ParseFloat(x.Val(fieldName, headers), 64)
	if err != nil {
		log.Fatal("Could Not Convert to float64")
	}
	return value
}

func (x Record) ConvertToInt(fieldName string, headers map[string]int) int64 {
	// Converts the value from a string to int64
	value, err := strconv.ParseInt(x.Val(fieldName, headers), 0, 64)
	if err != nil {
		log.Fatal("Could Not Convert to int64")
	}
	return value
}

// Converts various date strings into time.Time.
func dateConverter(dateString string) time.Time {
	// Convert date if not in 2006-01-02 format.
	if strings.Contains(dateString, "/") {
		dateSlice := strings.Split(dateString, "/")

		if len(dateSlice[0]) != 2 {
			dateSlice[0] = "0" + dateSlice[0]
		}
		if len(dateSlice[1]) != 2 {
			dateSlice[1] = "0" + dateSlice[1]
		}
		if len(dateSlice[2]) == 2 {
			dateSlice[2] = "20" + dateSlice[2]
		}
		dateString = dateSlice[2] + "-" + dateSlice[0] + "-" + dateSlice[1]
	}

	value, err := time.Parse("2006-01-02", dateString)
	if err != nil {
		log.Fatal("Could Not Convert to time.Time")
	}
	return value
}

// Converts date from specified field to time.Time.
func (x Record) ConvertToDate(fieldName string, headers map[string]int) time.Time {
	result := dateConverter(x.Val(fieldName, headers))
	return result
}
