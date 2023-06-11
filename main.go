package dataframe

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
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

	// Add headers to map in correct order
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

	// Open the CSV file
	recordFile, err := os.Open(path + fileName)
	if err != nil {
		log.Fatalf("Error opening the file. Please ensure the path and filename are correct. Message: %v", err)
	}

	// Setup the reader
	reader := csv.NewReader(recordFile)

	// Read the records
	header, err := reader.Read()
	if err != nil {
		log.Fatalf("Error reading the records: %v", err)
	}

	// Remove Byte Order Marker for UTF-8 files
	for i, each := range header {
		byteSlice := []byte(each)
		if byteSlice[0] == 239 && byteSlice[1] == 187 && byteSlice[2] == 191 {
			header[i] = each[3:]
		}
	}

	headers := make(map[string]int)
	for i, columnName := range header {
		headers[columnName] = i
	}

	// Empty slice to store Records
	s := []Record{}

	// Loop over the records and create Record objects to be stored
	for i := 0; ; i++ {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("Error in record loop: %v", err)
		}
		// Create new Record
		x := Record{Data: []string{}}

		// Loop over records and add to Data field of Record struct
		for _, r := range record {
			x.Data = append(x.Data, r)
		}
		s = append(s, x)
	}
	newFrame := DataFrame{FrameRecords: s, Headers: headers}
	return newFrame
}

func worker(jobs <-chan string, results chan<- DataFrame, resultsNames chan<- string, filePath string) {
	for n := range jobs {
		df := CreateDataFrame(filePath, n)
		results <- df
		resultsNames <- n
	}
}

// Concurrently loads multiple csv files into DataFrames within the same directory.
// Returns a slice with the DataFrames in the same order as provided in the files parameter.
func LoadFrames(filePath string, files []string) ([]DataFrame, error) {
	numJobs := len(files)

	if numJobs <= 1 {
		return nil, errors.New("LoadFrames requires at least two files")
	}

	jobs := make(chan string, numJobs)
	results := make(chan DataFrame, numJobs)
	resultsNames := make(chan string, numJobs)

	// Generate workers
	for i := 0; i < 4; i++ {
		go worker(jobs, results, resultsNames, filePath)
	}

	// Load up the jobs channel
	for i := 0; i < numJobs; i++ {
		jobs <- files[i]
	}
	close(jobs) // Close jobs channel once loaded

	// Map to store results
	jobResults := make(map[string]DataFrame)

	// Collect results and store in map
	for i := 1; i <= numJobs; i++ {
		jobResults[<-resultsNames] = <-results
	}

	var orderedResults []DataFrame
	for _, f := range files {
		val, ok := jobResults[f]
		if !ok {
			return []DataFrame{}, errors.New("An error occurred while looking up returned DataFrame in the LoadFrames function.")
		}
		orderedResults = append(orderedResults, val)
	}
	return orderedResults, nil
}

// User specifies columns they want to keep from a preexisting DataFrame
func (frame DataFrame) KeepColumns(columns []string) DataFrame {
	df := CreateNewDataFrame(columns)

	for _, row := range frame.FrameRecords {
		var newData []string
		for _, column := range columns {
			newData = append(newData, row.Val(column, frame.Headers))
		}
		df = df.AddRecord(newData)
	}

	return df
}

// User specifies columns they want to remove from a preexisting DataFrame
func (frame DataFrame) RemoveColumns(columns ...string) DataFrame {
	approvedColumns := []string{}

	for _, col := range frame.Columns() {
		if !contains(columns, col) {
			approvedColumns = append(approvedColumns, col)
		}
	}

	return frame.KeepColumns(approvedColumns)
}

// Rename a specified column in the DataFrame
func (frame *DataFrame) Rename(originalColumnName, newColumnName string) error {
	columns := []string{}
	var columnLocation int

	for k, v := range frame.Headers {
		columns = append(columns, k)
		if k == originalColumnName {
			columnLocation = v
		}
	}

	// Check original column name is found in DataFrame
	if contains(columns, originalColumnName) == false {
		return errors.New("The original column name provided was not found in the DataFrame")
	}

	// Check new column name does not already exist
	if contains(columns, newColumnName) == true {
		return errors.New("The provided new column name already exists in the DataFrame and is not allowed")
	}

	// Remove original column name
	delete(frame.Headers, originalColumnName)

	// Add new column name
	frame.Headers[newColumnName] = columnLocation

	return nil
}

// Add a new record to the DataFrame
func (frame DataFrame) AddRecord(newData []string) DataFrame {
	x := Record{Data: []string{}}

	for _, each := range newData {
		x.Data = append(x.Data, each)
	}

	frame.FrameRecords = append(frame.FrameRecords, x)

	return frame
}

// Provides a slice of columns in order
func (frame DataFrame) Columns() []string {
	var columns []string

	for i := 0; i < len(frame.Headers); i++ {
		for k, v := range frame.Headers {
			if v == i {
				columns = append(columns, k)
			}
		}
	}
	return columns
}

// Generates a decoupled copy of an existing DataFrame.
// Changes made to either the original or new copied frame
// will not be reflected in the other.
func (frame DataFrame) Copy() DataFrame {
	headers := []string{}

	for i := 0; i < len(frame.Headers); i++ {
		for k, v := range frame.Headers {
			if v == i {
				headers = append(headers, k)
			}
		}
	}
	df := CreateNewDataFrame(headers)

	for i := 0; i < len(frame.FrameRecords); i++ {
		df = df.AddRecord(frame.FrameRecords[i].Data)
	}
	return df
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

// Generated a new filtered DataFrame that in which a numerical column is either greater than or equal to
// a provided numerical value.
func (frame DataFrame) GreaterThanOrEqualTo(fieldName string, value float64) (DataFrame, error) {
	headers := []string{}

	for i := 0; i < len(frame.Headers); i++ {
		for k, v := range frame.Headers {
			if v == i {
				headers = append(headers, k)
			}
		}
	}
	newFrame := CreateNewDataFrame(headers)

	for i, row := range frame.FrameRecords {
		valString := row.Val(fieldName, frame.Headers)

		val, err := strconv.ParseFloat(valString, 64)
		if err != nil {
			return CreateNewDataFrame([]string{}), err
		}

		if val >= value {
			newFrame = newFrame.AddRecord(frame.FrameRecords[i].Data)
		}
	}
	return newFrame, nil
}

// Generated a new filtered DataFrame that in which a numerical column is either less than or equal to
// a provided numerical value.
func (frame DataFrame) LessThanOrEqualTo(fieldName string, value float64) (DataFrame, error) {
	headers := []string{}

	for i := 0; i < len(frame.Headers); i++ {
		for k, v := range frame.Headers {
			if v == i {
				headers = append(headers, k)
			}
		}
	}
	newFrame := CreateNewDataFrame(headers)

	for i, row := range frame.FrameRecords {
		valString := row.Val(fieldName, frame.Headers)

		val, err := strconv.ParseFloat(valString, 64)
		if err != nil {
			return CreateNewDataFrame([]string{}), err
		}

		if val <= value {
			newFrame = newFrame.AddRecord(frame.FrameRecords[i].Data)
		}
	}
	return newFrame, nil
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
// Instances where record dates occur on the same date provided by the user will not be included.
// Records must occur after the specified date.
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

// Creates a new field and assigns and empty string.
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
func (frame *DataFrame) Unique(fieldName string) []string {
	var results []string

	for _, row := range frame.FrameRecords {
		if contains(results, row.Val(fieldName, frame.Headers)) != true {
			results = append(results, row.Val(fieldName, frame.Headers))
		}
	}
	return results
}

// Stack two DataFrames with matching headers.
func (frame DataFrame) ConcatFrames(dfNew *DataFrame) (DataFrame, error) {
	// Check number of columns in each frame match.
	if len(frame.Headers) != len(dfNew.Headers) {
		return frame, errors.New("Cannot ConcatFrames as columns do not match.")
	}

	// Check columns in both frames are in the same order.
	originalFrame := []string{}
	for i := 0; i <= len(frame.Headers); i++ {
		for k, v := range frame.Headers {
			if v == i {
				originalFrame = append(originalFrame, k)
			}
		}
	}

	newFrame := []string{}
	for i := 0; i <= len(dfNew.Headers); i++ {
		for k, v := range dfNew.Headers {
			if v == i {
				newFrame = append(newFrame, k)
			}
		}
	}

	for i, each := range originalFrame {
		if each != newFrame[i] {
			return frame, errors.New("Cannot ConcatFrames as columns are not in the same order.")
		}
	}

	// Iterate over new dataframe in order
	for i := 0; i < len(dfNew.FrameRecords); i++ {
		frame.FrameRecords = append(frame.FrameRecords, dfNew.FrameRecords[i])
	}
	return frame, nil
}

// Import all columns from right frame into left frame if no columns
// are provided by the user. Process must be done so in order.
func (frame DataFrame) Merge(dfRight *DataFrame, primaryKey string, columns ...string) {
	if len(columns) == 0 {
		for i := 0; i < len(dfRight.Headers); i++ {
			for k, v := range dfRight.Headers {
				if v == i {
					columns = append(columns, k)
				}
			}
		}
	} else {
		// Ensure columns user provided are all found in right frame.
		for _, col := range columns {
			colStatus := false
			for k, _ := range dfRight.Headers {
				if col == k {
					colStatus = true
				}
			}
			// Ensure there are no duplicated columns other than the primary key.
			if colStatus != true {
				panic("Merge Error: User provided column not found in right dataframe.")
			}
		}
	}

	// Check that no columns are duplicated between the two frames (other than primaryKey).
	for _, col := range columns {
		for k, _ := range frame.Headers {
			if col == k && col != primaryKey {
				panic("The following column is duplicated in both frames and is not the specified primary key which is not allowed: " + col)
			}
		}
	}

	// Load map indicating the location of each lookup value in right frame.
	lookup := make(map[string]int)
	for i, row := range dfRight.FrameRecords {
		lookup[row.Val(primaryKey, dfRight.Headers)] = i
	}

	// Create new columns in left frame.
	for _, col := range columns {
		if col != primaryKey {
			frame.NewField(col)
		}
	}

	// Iterate over left frame and add new data.
	for _, row := range frame.FrameRecords {
		lookupVal := row.Val(primaryKey, frame.Headers)

		if val, ok := lookup[lookupVal]; ok {
			for _, col := range columns {
				if col != primaryKey {
					valToAdd := dfRight.FrameRecords[val].Data[dfRight.Headers[col]]
					row.Update(col, valToAdd, frame.Headers)
				}
			}
		}
	}
}

// Performs an inner merge where all columns are consolidated between the two frames but only for records
// where the specified primary key is found in both frames.
func (frame DataFrame) InnerMerge(dfRight *DataFrame, primaryKey string) DataFrame {
	var rightFrameColumns []string

	for i := 0; i < len(dfRight.Headers); i++ {
		for k, v := range dfRight.Headers {
			if v == i {
				rightFrameColumns = append(rightFrameColumns, k)
			}
		}
	}

	var leftFrameColumns []string

	for i := 0; i < len(frame.Headers); i++ {
		for k, v := range frame.Headers {
			if v == i {
				leftFrameColumns = append(leftFrameColumns, k)
			}
		}
	}

	// Ensure the specified primary key is found in both frames.
	var lStatus bool
	var rStatus bool

	for _, col := range leftFrameColumns {
		if col == primaryKey {
			lStatus = true
		}
	}

	for _, col := range rightFrameColumns {
		if col == primaryKey {
			rStatus = true
		}
	}

	if !lStatus || !rStatus {
		panic("The specified primary key was not found in both DataFrames.")
	}

	// Find position of primary key column in right frame.
	var rightFramePrimaryKeyPosition int
	for i, col := range rightFrameColumns {
		if col == primaryKey {
			rightFramePrimaryKeyPosition = i
		}
	}

	// Check that no columns are duplicated between the two frames (other than primaryKey).
	for _, col := range rightFrameColumns {
		for k, _ := range frame.Headers {
			if col == k && col != primaryKey {
				panic("The following column is duplicated in both frames and is not the specified primary key which is not allowed: " + col)
			}
		}
	}

	// Load map indicating the location of each lookup value in right frame.
	rLookup := make(map[string]int)
	for i, row := range dfRight.FrameRecords {
		// Only add if key hasn't already been added. This ensures the first record found in the right
		// frame is what is used instead of the last if duplicates are found.
		currentKey := row.Val(primaryKey, dfRight.Headers)
		_, ok := rLookup[currentKey]
		if !ok {
			rLookup[currentKey] = i
		}
	}

	// New DataFrame to house records found in both frames.
	dfNew := CreateNewDataFrame(leftFrameColumns)

	// Add right frame columns to new DataFrame.
	for i, col := range rightFrameColumns {
		// Skip over primary key column in right frame as it was already included in the left frame.
		if i != rightFramePrimaryKeyPosition {
			dfNew.NewField(col)
		}
	}

	var approvedPrimaryKeys []string

	// Create slice of specified ID's found in both frames.
	for _, lRow := range frame.FrameRecords {
		currentKey := lRow.Val(primaryKey, frame.Headers)

		// Skip blank values as they are not allowed.
		if len(currentKey) == 0 || strings.ToLower(currentKey) == "nan" || strings.ToLower(currentKey) == "null" {
			continue
		}

		for _, rRow := range dfRight.FrameRecords {
			currentRightFrameKey := rRow.Val(primaryKey, dfRight.Headers)
			// Add primary key to approved list if found in right frame.
			if currentRightFrameKey == currentKey {
				approvedPrimaryKeys = append(approvedPrimaryKeys, currentKey)
			}
		}
	}

	// Add approved records to new DataFrame.
	for i, row := range frame.FrameRecords {
		currentKey := row.Val(primaryKey, frame.Headers)
		if contains(approvedPrimaryKeys, currentKey) {
			lData := frame.FrameRecords[i].Data
			rData := dfRight.FrameRecords[rLookup[currentKey]].Data

			// Add left frame data to variable.
			var data []string
			data = append(data, lData...)

			// Add all right frame data while skipping over the primary key column.
			// The primary key column is skipped as it has already been added from the left frame.
			for i, d := range rData {
				if i != rightFramePrimaryKeyPosition {
					data = append(data, d)
				}
			}

			dfNew = dfNew.AddRecord(data)
		}
	}
	return dfNew
}

func (frame *DataFrame) CountRecords() int {
	return len(frame.FrameRecords)
}

// Return a sum of float64 type of a numerical field.
func (frame *DataFrame) Sum(fieldName string) float64 {
	var sum float64

	for _, row := range frame.FrameRecords {
		val, err := strconv.ParseFloat(row.Val(fieldName, frame.Headers), 64)
		if err != nil {
			panic("Could Not Convert String to Float During Sum.")
		}
		sum += val
	}
	return sum
}

// Return an average of type float64 of a numerical field.
func (frame *DataFrame) Average(fieldName string) float64 {
	sum := frame.Sum(fieldName)
	count := frame.CountRecords()

	if count == 0 {
		return 0.0
	}
	return sum / float64(count)
}

// Return the maximum value in a numerical field.
func (frame *DataFrame) Max(fieldName string) float64 {
	maximum := 0.0
	for i, row := range frame.FrameRecords {
		// Set the max to the first value in dataframe.
		if i == 0 {
			initialMax, err := strconv.ParseFloat(row.Val(fieldName, frame.Headers), 64)
			if err != nil {
				panic("Could Not Convert String to Float During Sum.")
			}
			maximum = initialMax
		}
		val, err := strconv.ParseFloat(row.Val(fieldName, frame.Headers), 64)
		if err != nil {
			panic("Could Not Convert String to Float During Sum.")
		}

		if val > maximum {
			maximum = val
		}
	}
	return maximum
}

// Return the minimum value in a numerical field.
func (frame *DataFrame) Min(fieldName string) float64 {
	min := 0.0
	for i, row := range frame.FrameRecords {
		// Set the max to the first value in dataframe.
		if i == 0 {
			initialMin, err := strconv.ParseFloat(row.Val(fieldName, frame.Headers), 64)
			if err != nil {
				panic("Could Not Convert String to Float During Sum.")
			}
			min = initialMin
		}
		val, err := strconv.ParseFloat(row.Val(fieldName, frame.Headers), 64)
		if err != nil {
			panic("Could Not Convert String to Float During Sum.")
		}

		if val < min {
			min = val
		}
	}
	return min
}

func standardDeviation(num []float64) float64 {
	l := float64(len(num))
	sum := 0.0
	var sd float64

	for _, n := range num {
		sum += n
	}

	mean := sum / l

	for j := 0; j < int(l); j++ {
		// The use of Pow math function func Pow(x, y float64) float64
		sd += math.Pow(num[j]-mean, 2)
	}
	// The use of Sqrt math function func Sqrt(x float64) float64
	sd = math.Sqrt(sd / l)

	return sd
}

// Return the standard deviation of a numerical field.
func (frame *DataFrame) StandardDeviation(fieldName string) (float64, error) {
	var nums []float64

	for _, row := range frame.FrameRecords {
		num, err := strconv.ParseFloat(row.Val(fieldName, frame.Headers), 64)
		if err != nil {
			return 0.0, errors.New("Could not convert string to number in specified column to calculate standard deviation.")
		}
		nums = append(nums, num)
	}
	return standardDeviation(nums), nil
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

// Return the value of the specified field
func (x Record) Val(fieldName string, headers map[string]int) string {
	if _, ok := headers[fieldName]; !ok {
		panic(fmt.Errorf("The provided field %s is not a valid field in the dataframe.", fieldName))
	}
	return x.Data[headers[fieldName]]
}

// Update the value in a specified field
func (x Record) Update(fieldName, value string, headers map[string]int) {
	if _, ok := headers[fieldName]; !ok {
		panic(fmt.Errorf("The provided field %s is not a valid field in the dataframe.", fieldName))
	}
	x.Data[headers[fieldName]] = value
}

// Converts the value from a string to float64
func (x Record) ConvertToFloat(fieldName string, headers map[string]int) float64 {
	value, err := strconv.ParseFloat(x.Val(fieldName, headers), 64)
	if err != nil {
		log.Fatalf("Could Not Convert to float64: %v", err)
	}
	return value
}

func (x Record) ConvertToInt(fieldName string, headers map[string]int) int64 {
	// Converts the value from a string to int64
	value, err := strconv.ParseInt(x.Val(fieldName, headers), 0, 64)
	if err != nil {
		log.Fatalf("Could Not Convert to int64: %v", err)
	}
	return value
}

// Converts various date strings into time.Time
func dateConverter(dateString string) time.Time {
	// Convert date if not in 2006-01-02 format
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
		log.Fatalf("Could Not Convert to time.Time: %v", err)
	}
	return value
}

// Converts date from specified field to time.Time
func (x Record) ConvertToDate(fieldName string, headers map[string]int) time.Time {
	result := dateConverter(x.Val(fieldName, headers))
	return result
}
