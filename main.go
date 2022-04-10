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
	Data map[string]string
}

type DataFrame struct {
	FrameRecords map[int]Record
	Headers      []string
}

// Generate a new empty DataFrame.
func CreateNewDataFrame(headers []string) DataFrame {
	myRecords := make(map[int]Record)
	newFrame := DataFrame{FrameRecords: myRecords, Headers: headers}

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

	// Remove Byte Order Marker for UTF-8 files.
	for i, each := range header {
		byteSlice := []byte(each)
		if byteSlice[0] == 239 && byteSlice[1] == 187 && byteSlice[2] == 191 {
			header[i] = each[3:]
		}
	}

	// Empty map to store struct objects
	myRecords := make(map[int]Record)

	// Loop over the records and create Record objects.
	for i := 0; ; i++ {
		record, err := reader.Read()
		if err == io.EOF {
			break // Reached end of file
		} else if err != nil {
			log.Fatal("Error in record loop.")
		}
		// Create the new Record
		x := Record{make(map[string]string)}

		// Loop over columns and dynamically add column data for each header
		for pos, each := range header {
			x.Data[each] = record[pos]
		}

		// Add Record object to map
		myRecords[i] = x
	}

	newFrame := DataFrame{FrameRecords: myRecords, Headers: header}

	return newFrame
}

// User specifies columns they want to keep from a preexisting DataFrame.
func (frame DataFrame) KeepColumns(columns []string) DataFrame {
	df := CreateNewDataFrame(columns)

	for _, row := range frame.FrameRecords {
		var newData []string
		for _, column := range columns {
			newData = append(newData, row.Val(column))
		}
		df.AddRecord(newData)
	}

	return df
}

// Add a new record to the DataFrame.
func (frame DataFrame) AddRecord(newData []string) DataFrame {
	x := Record{make(map[string]string)}

	for i, each := range frame.Headers {
		x.Data[each] = newData[i]
	}

	frame.FrameRecords[len(frame.FrameRecords)] = x

	return frame
}

// Generates a new filtered DataFrame.
// New DataFrame will be kept in same order as original.
func (frame DataFrame) Filtered(fieldName, value string) DataFrame {
	myRecords := make(map[int]Record)

	pos := 0
	for i := 0; i < len(frame.FrameRecords); i++ {
		if frame.FrameRecords[i].Data[fieldName] == value {
			x := Record{make(map[string]string)}

			// Loop over columns
			for _, each := range frame.Headers {
				x.Data[each] = frame.FrameRecords[i].Data[each]
			}

			myRecords[pos] = x
			pos++
		}
	}
	newFrame := DataFrame{FrameRecords: myRecords, Headers: frame.Headers}

	return newFrame
}

// Generates a new DataFrame that excludes specified instances.
func (frame DataFrame) Exclude(fieldName, value string) DataFrame {
	myRecords := make(map[int]Record)

	pos := 0
	for i := 0; i < len(frame.FrameRecords); i++ {
		if frame.FrameRecords[i].Data[fieldName] != value {
			x := Record{make(map[string]string)}

			// Loop over columns
			for _, each := range frame.Headers {
				x.Data[each] = frame.FrameRecords[i].Data[each]
			}

			myRecords[pos] = x
			pos++
		}
	}
	newFrame := DataFrame{FrameRecords: myRecords, Headers: frame.Headers}

	return newFrame
}

// Generates a new filtered DataFrame with all records occuring after a specified date provided by the user.
// User must provide the date field as well as the desired date.
func (frame DataFrame) FilteredAfter(fieldName, desiredDate string) DataFrame {
	myRecords := make(map[int]Record)

	pos := 0

	for i := 0; i < len(frame.FrameRecords); i++ {
		recordDate := dateConverter(frame.FrameRecords[i].Data[fieldName])
		isAfter := recordDate.After(dateConverter(desiredDate))

		if isAfter {
			x := Record{make(map[string]string)}

			// Loop over columns
			for _, each := range frame.Headers {
				x.Data[each] = frame.FrameRecords[i].Data[each]
			}

			myRecords[pos] = x
			pos++
		}
	}
	newFrame := DataFrame{FrameRecords: myRecords, Headers: frame.Headers}

	return newFrame
}

// Generates a new filtered DataFrame with all records occuring before a specified date provided by the user.
// User must provide the date field as well as the desired date.
func (frame DataFrame) FilteredBefore(fieldName, desiredDate string) DataFrame {
	myRecords := make(map[int]Record)

	pos := 0

	for i := 0; i < len(frame.FrameRecords); i++ {
		recordDate := dateConverter(frame.FrameRecords[i].Data[fieldName])
		isBefore := recordDate.Before(dateConverter(desiredDate))

		if isBefore {
			x := Record{make(map[string]string)}

			// Loop over columns
			for _, each := range frame.Headers {
				x.Data[each] = frame.FrameRecords[i].Data[each]
			}

			myRecords[pos] = x
			pos++
		}
	}
	newFrame := DataFrame{FrameRecords: myRecords, Headers: frame.Headers}

	return newFrame
}

// Generates a new filtered DataFrame with all records occuring between a specified date range provided by the user.
// User must provide the date field as well as the desired date.
func (frame DataFrame) FilteredBetween(fieldName, startDate, endDate string) DataFrame {
	myRecords := make(map[int]Record)

	pos := 0

	for i := 0; i < len(frame.FrameRecords); i++ {
		recordDate := dateConverter(frame.FrameRecords[i].Data[fieldName])
		isAfter := recordDate.After(dateConverter(startDate))
		isBefore := recordDate.Before(dateConverter(endDate))

		if isAfter && isBefore {
			x := Record{make(map[string]string)}

			// Loop over columns
			for _, each := range frame.Headers {
				x.Data[each] = frame.FrameRecords[i].Data[each]
			}

			myRecords[pos] = x
			pos++
		}
	}
	newFrame := DataFrame{FrameRecords: myRecords, Headers: frame.Headers}

	return newFrame
}

// Creates a new field and assigns it the provided value.
// Must pass in the original DataFrame as well as header slice.
// Returns a tuple with new DataFrame and headers.
func (frame DataFrame) NewField(fieldName string) DataFrame {
	for _, row := range frame.FrameRecords {
		row.Data[fieldName] = ""
	}
	frame.Headers = append(frame.Headers, fieldName)
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

	for _, row := range frame.FrameRecords {
		if contains(results, row.Val(fieldName)) != true {
			results = append(results, row.Val(fieldName))
		}
	}
	return results
}

// Stack two DataFrames with matching headers.
func (frame DataFrame) ConcatFrames(dfNew DataFrame) DataFrame {
	keyStart := len(frame.FrameRecords)

	// Iterate over new dataframe in order
	for i := 0; i < len(dfNew.FrameRecords); i++ {
		// Create new Record
		x := Record{make(map[string]string)}

		// Iterate over headers and add data to Record
		for _, header := range frame.Headers {
			x.Data[header] = dfNew.FrameRecords[i].Val(header)
		}
		frame.FrameRecords[keyStart] = x
		keyStart++
	}
	return frame
}

func (frame DataFrame) CountRecords() int {
	return len(frame.FrameRecords)
}

func (frame DataFrame) SaveDataFrame(path, fileName string) bool {
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
	for _, header := range frame.Headers {
		row = append(row, header)
	}
	data = append(data, row)

	// Iterate over map by order of index or keys.
	for i := 0; i < len(frame.FrameRecords); i++ {
		var row []string
		for _, header := range frame.Headers {
			row = append(row, frame.FrameRecords[i].Data[header])
		}
		data = append(data, row)
	}

	w.WriteAll(data)

	return true
}

func (x Record) Val(fieldName string) string {
	// Return the value of the specified field
	return x.Data[fieldName]
}

func (x Record) Update(fieldName, value string) {
	// Update the value in a specified field
	x.Data[fieldName] = value
}

func (x Record) ConvertToFloat(fieldName string) float64 {
	// Converts the value from a string to float64
	value, err := strconv.ParseFloat(x.Data[fieldName], 64)
	if err != nil {
		log.Fatal("Could Not Convert to float64")
	}
	return value
}

func (x Record) ConvertToInt(fieldName string) int64 {
	// Converts the value from a string to int64
	value, err := strconv.ParseInt(x.Data[fieldName], 0, 64)
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
func (x Record) ConvertToDate(fieldName string) time.Time {
	dateString := x.Data[fieldName]
	result := dateConverter(dateString)
	return result
}
