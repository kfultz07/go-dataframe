package dataframe

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestStream(t *testing.T) {
	firstNameAnswers := []string{"Kevin", "Beth", "Avery", "Peter", "Andy", "Nick", "Bryan", "Brian", "Eric", "Carl"}
	costAnswers := []string{"818", "777", "493", "121", "774", "874", "995", "133", "939", "597"}

	path := "./"
	c := make(chan StreamingRecord)
	go Stream(path, "data/tests/TestData.csv", c)

	i := 0
	for row := range c {
		if row.Val("First Name") != firstNameAnswers[i] {
			t.Error("First name did not match.")
		}
		if row.Val("Cost") != costAnswers[i] {
			t.Error("Cost did not match.")
		}
		i++
	}
}

func TestStreamConvertToInt(t *testing.T) {
	costAnswers := []int64{818, 777, 493, 121, 774, 874, 995, 133, 939, 597}

	path := "./"
	c := make(chan StreamingRecord)
	go Stream(path, "data/tests/TestData.csv", c)

	i := 0
	for row := range c {
		val := row.ConvertToInt("Cost")
		if val != costAnswers[i] {
			t.Error("Could not convert to int64.")
		}
		i++
	}
}

func TestStreamConvertToFloat(t *testing.T) {
	costAnswers := []float64{818.0, 777.0, 493.0, 121.0, 774.0, 874.0, 995.0, 133.0, 939.0, 597.0}

	path := "./"
	c := make(chan StreamingRecord)
	go Stream(path, "data/tests/TestData.csv", c)

	i := 0
	for row := range c {
		val := row.ConvertToFloat("Cost")
		if val != costAnswers[i] {
			t.Error("Could not convert to float64.")
		}
		i++
	}
}

func TestDynamicMetrics(t *testing.T) {
	// Create DataFrame
	columns := []string{"Value"}
	df := CreateNewDataFrame(columns)

	sum := 0.0
	min := 1
	max := 100
	recordedMax := 0.0
	recordedMin := float64(max) + 1.0
	totalRecords := 1_000_000

	for i := 0; i < totalRecords; i++ {
		// Ensures differing values generated on each run.
		rand.Seed(time.Now().UnixNano())
		v := float64(rand.Intn(max-min)+min) + rand.Float64()
		sum = sum + v

		// Add data to DataFrame
		data := []string{fmt.Sprintf("%f", v)}
		df = df.AddRecord(data)

		if v > recordedMax {
			recordedMax = v
		}
		if v < recordedMin {
			recordedMin = v
		}
	}

	dataFrameValue := df.Sum("Value")
	dataFrameAvgValue := math.Round(df.Average("Value")*100) / 100
	dataFrameMaxValue := math.Round(df.Max("Value")*100) / 100
	dataFrameMinValue := math.Round(df.Min("Value")*100) / 100
	avg := math.Round(sum/float64(totalRecords)*100) / 100
	recordedMax = math.Round(recordedMax*100) / 100
	recordedMin = math.Round(recordedMin*100) / 100

	if math.Abs(dataFrameValue-sum) > 0.001 {
		t.Error("Dynamic Metrics: sum float failed", dataFrameValue, sum, math.Abs(dataFrameValue-sum))
	}
	if dataFrameAvgValue != avg {
		t.Error("Dynamic Metrics: average float failed", dataFrameAvgValue, avg)
	}
	if dataFrameMaxValue != recordedMax {
		t.Error("Dynamic Metrics: max value error", dataFrameMaxValue, recordedMax)
	}
	if dataFrameMinValue != recordedMin {
		t.Error("Dynamic Metrics: min value error", dataFrameMinValue, recordedMin)
	}
	if df.CountRecords() != totalRecords {
		t.Error("Dynamic Metrics: count records error", df.CountRecords(), totalRecords)
	}
}

func TestCreateDataFrameCostFloat(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")
	total := 0.0

	for _, row := range df.FrameRecords {
		total += row.ConvertToFloat("Cost", df.Headers)
	}

	if total != 6521.0 {
		t.Error("Cost sum incorrect.")
	}
}

func TestCreateDataFrameCostInt(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")
	var total int64

	for _, row := range df.FrameRecords {
		total += row.ConvertToInt("Cost", df.Headers)
	}

	if total != 6521 {
		t.Error("Cost sum incorrect.")
	}
}

func TestSum(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")

	if df.Sum("Weight") != 3376.0 || df.Sum("Cost") != 6521.0 {
		t.Error("Just sum error...")
	}
}

func TestAverage(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")

	if df.Average("Weight") != 337.60 || df.Average("Cost") != 652.10 {
		t.Error("Not your average error...")
	}
}

func TestMax(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")

	if df.Max("Weight") != 500.0 || df.Max("Cost") != 995.0 {
		t.Error("Error to the max...")
	}
}

func TestMin(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")

	if df.Min("Weight") != 157.0 || df.Min("Cost") != 121.0 {
		t.Error("Error to the min...")
	}
}

func TestStandardDeviationFunction(t *testing.T) {
	nums := []float64{4.27, 23.45, 34.43, 54.76, 65.90, 234.45}
	stdev := standardDeviation(nums)
	expected := 76.42444976721926
	variance := stdev - expected

	if stdev != expected {
		t.Error(fmt.Printf("Standard Deviation calculation error: Expected: %f Result: %f Variance: %f\n", expected, stdev, variance))
	}
}

func TestStandardDeviationMethodPass(t *testing.T) {
	// Create DataFrame
	columns := []string{"ID", "Value"}
	df := CreateNewDataFrame(columns)

	for i := 0; i < 1000; i++ {
		val := strconv.Itoa(i)
		df = df.AddRecord([]string{"ID-" + val, val})
	}

	stdev, err := df.StandardDeviation("Value")
	if err != nil {
		t.Error("Test should have passed without any string to float conversion errors.")
	}

	expected := 288.6749902572095
	variance := stdev - expected

	if stdev != expected {
		t.Error(fmt.Printf("Standard Deviation calculation error: Expected: %f Result: %f Variance: %f\n", expected, stdev, variance))
	}
}

func TestStandardDeviationMethodFail(t *testing.T) {
	// Create DataFrame
	columns := []string{"ID", "Value"}
	df := CreateNewDataFrame(columns)

	for i := 0; i < 1000; i++ {
		// Insert row with value that cannot be converted to float64.
		if i == 500 {
			df = df.AddRecord([]string{"ID-" + "500", "5x0x0x"})
		}
		val := strconv.Itoa(i)
		df = df.AddRecord([]string{"ID-" + val, val})
	}

	_, err := df.StandardDeviation("Value")
	if err == nil {
		t.Error("Test should have failed.")
	}
}

func TestFilteredCount(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")
	dfFil := df.Filtered("Last Name", "Fultz", "Wiedmann")

	if df.CountRecords() != 10 || dfFil.CountRecords() != 5 {
		t.Error("Filtered count incorrect.")
	}
}

func TestFilteredCheck(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")
	dfFil := df.Filtered("Last Name", "Fultz", "Wiedmann")

	for _, row := range dfFil.FrameRecords {
		if row.Val("Last Name", dfFil.Headers) != "Fultz" && row.Val("Last Name", dfFil.Headers) != "Wiedmann" {
			t.Error("Invalid parameter found in Filtered DataFrame.")
		}
	}
}

// Ensures changes made in the original dataframe are not also made in a filtered dataframe.
func TestFilteredChangeToOriginal(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")
	dfFil := df.Filtered("Last Name", "Fultz", "Wiedmann")

	for _, row := range df.FrameRecords {
		if row.Val("ID", df.Headers) == "2" {
			row.Update("Last Name", "Bethany", df.Headers)
		}
		if row.Val("ID", df.Headers) == "5" {
			row.Update("Last Name", "Andyanne", df.Headers)
		}
	}

	// Ensure row was actually updated in the original frame.
	for _, row := range df.FrameRecords {
		if row.Val("ID", df.Headers) == "2" && row.Val("Last Name", df.Headers) != "Bethany" {
			t.Error("Row 2 last name not changed in original frame.")
		}
		if row.Val("ID", df.Headers) == "5" && row.Val("Last Name", df.Headers) != "Andyanne" {
			t.Error("Row 5 last name not changed in original frame.")
		}
	}

	// Check rows in filtered dataframe were not also updated.
	for _, row := range dfFil.FrameRecords {
		if row.Val("ID", df.Headers) == "2" && row.Val("Last Name", df.Headers) != "Fultz" {
			t.Error("Row 2 in filtered dataframe was incorrectly updated with original.")
		}
		if row.Val("ID", df.Headers) == "5" && row.Val("Last Name", df.Headers) != "Wiedmann" {
			t.Error("Row 5 in filtered dataframe was incorrectly updated with original.")
		}
	}
}

func TestGreaterThanOrEqualTo(t *testing.T) {
	path := "./"
	value := float64(597)
	df := CreateDataFrame(path, "data/tests/TestData.csv")
	df, err := df.GreaterThanOrEqualTo("Cost", value)
	if err != nil {
		t.Error("Greater Than Or Equal To: This should not have failed...")
	}

	if df.CountRecords() != 7 {
		t.Error("Greater Than Or Equal To: Record count is not correct.")
	}

	ids := []string{"1", "2", "5", "6", "7", "9", "10"}
	foundIds := df.Unique("ID")

	for i, id := range foundIds {
		if id != ids[i] {
			t.Error("Greater Than Or Equal To: Records do not match.")
		}
	}
}

func TestLessThanOrEqualTo(t *testing.T) {
	path := "./"
	value := float64(436)
	df := CreateDataFrame(path, "data/tests/TestData.csv")
	df, err := df.LessThanOrEqualTo("Weight", value)
	if err != nil {
		t.Error("Less Than Or Equal To: This should not have failed...")
	}

	if df.CountRecords() != 7 {
		t.Error("Less Than Or Equal To: Record count is not correct.")
	}

	ids := []string{"1", "2", "4", "5", "6", "8", "9"}
	foundIds := df.Unique("ID")

	for i, id := range foundIds {
		if id != ids[i] {
			t.Error("Less Than Or Equal To: Records do not match.")
		}
	}
}

func TestExcludeCount(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")
	dfExcl := df.Exclude("Last Name", "Fultz", "Wiedmann")

	if df.CountRecords() != 10 || dfExcl.CountRecords() != 5 {
		t.Error("Excluded count is incorrect.")
	}
}

func TestExcludeCheck(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")
	dfExcl := df.Exclude("Last Name", "Fultz", "Wiedmann")

	for _, row := range dfExcl.FrameRecords {
		if row.Val("Last Name", dfExcl.Headers) == "Fultz" || row.Val("Last Name", dfExcl.Headers) == "Wiedmann" {
			t.Error("Excluded parameter found in DataFrame.")
		}
	}
}

func TestFilteredAfterCount(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")
	dfFil := df.FilteredAfter("Date", "2022-01-08")

	if df.CountRecords() != 10 || dfFil.CountRecords() != 2 {
		t.Error("Filtered After count incorrect.")
	}
}

func TestFilteredAfterCountExcelFormat(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestDataDateFormat.csv")
	dfFil := df.FilteredAfter("Date", "2022-01-08")

	if df.CountRecords() != 10 || dfFil.CountRecords() != 2 {
		t.Error("Filtered After Excel Format count incorrect.")
	}
}

func TestFilteredBeforeCount(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")
	dfFil := df.FilteredBefore("Date", "2022-01-08")

	if df.CountRecords() != 10 || dfFil.CountRecords() != 7 {
		t.Error("Filtered Before count incorrect.")
	}
}

func TestFilteredBeforeCountExcelFormat(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestDataDateFormat.csv")
	dfFil := df.FilteredBefore("Date", "2022-01-08")

	if df.CountRecords() != 10 || dfFil.CountRecords() != 7 {
		t.Error("Filtered Before Excel Format count incorrect.")
	}
}

func TestFilteredBetweenCount(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")
	dfFil := df.FilteredBetween("Date", "2022-01-02", "2022-01-09")

	if df.CountRecords() != 10 || dfFil.CountRecords() != 6 {
		t.Error("Filtered Between count incorrect.")
	}
}

func TestFilteredBetweenExcelFormat(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestDataDateFormat.csv")
	dfFil := df.FilteredBetween("Date", "2022-01-02", "2022-01-09")

	if df.CountRecords() != 10 || dfFil.CountRecords() != 6 {
		t.Error("Filtered Between Excel Format count incorrect.")
	}
}

func TestRecordCheck(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")

	var id string
	var date string
	var cost string
	var weight string
	var firstName string
	var lastName string

	for _, row := range df.FrameRecords {
		if row.Val("ID", df.Headers) == "5" {
			id = row.Val("ID", df.Headers)
			date = row.Val("Date", df.Headers)
			cost = row.Val("Cost", df.Headers)
			weight = row.Val("Weight", df.Headers)
			firstName = row.Val("First Name", df.Headers)
			lastName = row.Val("Last Name", df.Headers)
		}
	}

	if id != "5" {
		t.Error("ID failed")
	} else if date != "2022-01-05" {
		t.Error("Date failed")
	} else if cost != "774" {
		t.Error("Cost failed")
	} else if weight != "415" {
		t.Error("Weight failed")
	} else if firstName != "Andy" {
		t.Error("First Name failed")
	} else if lastName != "Wiedmann" {
		t.Error("Last Name failed")
	}
}

func TestRecordCheckPanic(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")

	for _, row := range df.FrameRecords {
		defer func() { recover() }()

		row.Val("Your Name Here", df.Headers)

		// Never reaches here if `OtherFunctionThatPanics` panics.
		t.Errorf("The row.Val() method should have panicked.")
	}
}

func TestAddRecord(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")
	newData := [6]string{"11", "2022-06-23", "101", "500", "Ben", "Benison"}
	df = df.AddRecord(newData[:])

	if df.CountRecords() != 11 {
		t.Error("Add Record: Count does not match.")
	}

	for _, row := range df.FrameRecords {
		if row.Val("ID", df.Headers) == "11" {
			if row.Val("Date", df.Headers) != "2022-06-23" {
				t.Error("Add Record: date failed")
			}
			if row.Val("Cost", df.Headers) != "101" {
				t.Error("Add Record: cost failed")
			}
			if row.Val("Weight", df.Headers) != "500" {
				t.Error("Add Record: weight failed")
			}
			if row.Val("First Name", df.Headers) != "Ben" {
				t.Error("Add Record: first name failed")
			}
			if row.Val("Last Name", df.Headers) != "Benison" {
				t.Error("Add Record: last name failed")
			}
		}
	}
}

func TestByteOrderMark(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestDataCommaSeparatedValue.csv")
	dfUtf := CreateDataFrame(path, "data/tests/TestData.csv")

	dfTotal := 0.0
	for _, row := range df.FrameRecords {
		dfTotal += row.ConvertToFloat("ID", df.Headers)
	}

	dfUtfTotal := 0.0
	for _, row := range dfUtf.FrameRecords {
		dfUtfTotal += row.ConvertToFloat("ID", dfUtf.Headers)
	}

	if dfTotal != 55.0 || dfUtfTotal != 55.0 {
		t.Error("Byte Order Mark conversion error")
	}
}
func TestKeepColumns(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")

	columns := [3]string{"First Name", "Last Name", "Weight"}
	df = df.KeepColumns(columns[:])

	if df.Headers["First Name"] != 0 || df.Headers["Last Name"] != 1 || df.Headers["Weight"] != 2 || len(df.Headers) > 3 {
		t.Error("Keep Columns failed")
	}
}

func TestRemoveColumnsMultiple(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")

	df = df.RemoveColumns("ID", "Cost", "First Name")

	if df.Headers["Date"] != 0 || df.Headers["Weight"] != 1 || df.Headers["Last Name"] != 2 || len(df.Headers) > 3 {
		t.Error("Remove Multiple Columns failed")
	}
}

func TestRemoveColumnsSingle(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")

	df = df.RemoveColumns("First Name")

	if df.Headers["ID"] != 0 || df.Headers["Date"] != 1 || df.Headers["Cost"] != 2 || df.Headers["Weight"] != 3 || df.Headers["Last Name"] != 4 || len(df.Headers) > 5 {
		t.Error("Remove Single Column failed")
	}
}

func TestDateConverterStandardFormat(t *testing.T) {
	var s interface{} = dateConverter("2022-01-31")
	if _, ok := s.(time.Time); ok != true {
		t.Error("Date Converter Standard Format Failed")
	}
}

func TestDateConverterExcelFormatDoubleDigit(t *testing.T) {
	var s interface{} = dateConverter("01/31/2022")
	if _, ok := s.(time.Time); ok != true {
		t.Error("Date Converter Excel Format Failed")
	}
}

func TestDateConverterExcelFormatSingleMonthDigit(t *testing.T) {
	var s interface{} = dateConverter("1/31/2022")
	if _, ok := s.(time.Time); ok != true {
		t.Error("Date Converter Excel Format Failed")
	}
}

func TestDateConverterExcelFormatSingleDayDigit(t *testing.T) {
	var s interface{} = dateConverter("01/1/2022")
	if _, ok := s.(time.Time); ok != true {
		t.Error("Date Converter Excel Format Failed")
	}
}

func TestDateConverterExcelFormatSingleDigit(t *testing.T) {
	var s interface{} = dateConverter("1/1/2022")
	if _, ok := s.(time.Time); ok != true {
		t.Error("Date Converter Excel Format Failed")
	}
}

func TestDateConverterExcelFormatDoubleYearDigit(t *testing.T) {
	var s interface{} = dateConverter("01/31/22")
	if _, ok := s.(time.Time); ok != true {
		t.Error("Date Converter Excel Format Failed")
	}
}

func TestNewField(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")
	df.NewField("Middle Name")

	if df.Headers["Middle Name"] != 6 {
		fmt.Println(df.Headers)
		t.Error("New field column not added in proper position.")
	}

	for _, row := range df.FrameRecords {
		if row.Val("Middle Name", df.Headers) != "" {
			t.Error("Value in New Field is not set to nil")
		}
	}
}

func TestUnique(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")
	names := df.Unique("Last Name")

	if len(names) != 7 {
		t.Error("Unique slice error.")
	}
}

func TestUpdate(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")

	for _, row := range df.FrameRecords {
		if row.Val("First Name", df.Headers) == "Avery" && row.Val("Last Name", df.Headers) == "Fultz" {
			row.Update("Weight", "30", df.Headers)
		}
	}

	for _, row := range df.FrameRecords {
		if row.Val("First Name", df.Headers) == "Avery" && row.Val("Last Name", df.Headers) == "Fultz" {
			if row.Val("Weight", df.Headers) != "30" {
				t.Error("Update row failed.")
			}
		}
	}
}

func TestUpdatePanic(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")

	for _, row := range df.FrameRecords {
		if row.Val("First Name", df.Headers) == "Avery" && row.Val("Last Name", df.Headers) == "Fultz" {
			defer func() { recover() }()

			row.Update("Your Name Here", "30", df.Headers)

			t.Errorf("Method should have panicked.")
		}
	}
}

func TestMergeFramesAllColumns(t *testing.T) {
	path := "./"

	// Prep left frame
	df := CreateDataFrame(path, "data/tests/TestData.csv")
	newData := [6]string{"11", "2022-06-27", "5467", "9586", "Cassandra", "SchmaSandra"}
	df = df.AddRecord(newData[:])

	// Prep right frame
	dfRight := CreateDataFrame(path, "data/tests/TestMergeData.csv")

	// Merge
	df.Merge(&dfRight, "ID")

	if df.CountRecords() != 11 {
		t.Error("Merge: record count error.")
	}

	m := make(map[string][]string)
	m["2"] = []string{"RICHLAND", "WA", "99354"}
	m["4"] = []string{"VAN BUREN", "AR", "72956"}
	m["6"] = []string{"FISHERS", "NY", "14453"}
	m["10"] = []string{"JEFFERSON CITY", "MO", "65109"}
	m["11"] = []string{"", "", ""}

	for _, row := range df.FrameRecords {
		if val, ok := m[row.Val("ID", df.Headers)]; ok {
			for i, v := range val {
				switch i {
				case 0:
					if row.Val("City", df.Headers) != v {
						t.Error("Merge: city error.")
					}
				case 1:
					if row.Val("State", df.Headers) != v {
						t.Error("Merge: state error.")
					}
				case 2:
					if row.Val("Postal Code", df.Headers) != v {
						t.Error("Merge: postal code error.")
					}
				}
			}
		}
	}
}

func TestMergeFramesSpecifiedColumns(t *testing.T) {
	path := "./"

	// Prep left frame
	df := CreateDataFrame(path, "data/tests/TestData.csv")
	newData := [6]string{"11", "2022-06-27", "5467", "9586", "Cassandra", "SchmaSandra"}
	df = df.AddRecord(newData[:])

	// Prep right frame
	dfRight := CreateDataFrame(path, "data/tests/TestMergeData.csv")

	// Merge
	df.Merge(&dfRight, "ID", "City", "Postal Code")

	if df.CountRecords() != 11 {
		t.Error("Merge: record count error.")
	}

	m := make(map[string][]string)
	m["2"] = []string{"RICHLAND", "99354"}
	m["4"] = []string{"VAN BUREN", "72956"}
	m["6"] = []string{"FISHERS", "14453"}
	m["10"] = []string{"JEFFERSON CITY", "65109"}
	m["11"] = []string{"", ""}

	for _, row := range df.FrameRecords {
		if val, ok := m[row.Val("ID", df.Headers)]; ok {
			for i, v := range val {
				switch i {
				case 0:
					if row.Val("City", df.Headers) != v {
						t.Error("Merge: city error.")
					}
				case 1:
					if row.Val("Postal Code", df.Headers) != v {
						t.Error("Merge: postal code error.")
					}
				}
			}
		}
	}
}

func TestInnerMerge(t *testing.T) {
	path := "./"

	// Prep left frame
	df := CreateDataFrame(path, "data/tests/TestData.csv")

	// Prep right frame
	dfRight := CreateDataFrame(path, "data/tests/TestInnerMergeData.csv")

	// Merge
	df = df.InnerMerge(&dfRight, "ID")

	if df.CountRecords() != 5 {
		t.Error("Inner Merge: record count error.")
	}

	columns := []string{"ID", "Date", "Cost", "Weight", "First Name", "Last Name", "City", "State", "Postal Code"}

	data := make([][]string, 5)
	data[0] = []string{"4", "2022-01-04", "121", "196", "Peter", "Wiedmann", "VAN BUREN", "AR", "72956"}
	data[1] = []string{"5", "2022-01-05", "774", "415", "Andy", "Wiedmann", "TAUNTON", "MA", "2780"}
	data[2] = []string{"7", "2022-01-07", "995", "500", "Bryan", "Curtis", "GOLDSBORO", "NC", "27530"}
	data[3] = []string{"9", "2022-01-09", "939", "157", "Eric", "Petruska", "PHOENIX", "AZ", "85024"}
	data[4] = []string{"10", "2022-01-10", "597", "475", "Carl", "Carlson", "JEFFERSON CITY", "MO", "65109"}

	for i, row := range df.FrameRecords {
		if len(row.Data) != len(data[i]) {
			t.Error("Inner Merge: Column count does not match.")
		}
		for i2, col := range columns {
			val := row.Val(col, df.Headers)
			if val != data[i][i2] {
				t.Error("Inner Merge: Data results to not match what is expected.")
			}
		}
	}
}

func TestInnerMergeLeftFrameDuplicates(t *testing.T) {
	path := "./"

	// Prep left frame
	df := CreateDataFrame(path, "data/tests/TestDataInnerDuplicate.csv")

	// Prep right frame
	dfRight := CreateDataFrame(path, "data/tests/TestInnerMergeData.csv")

	// Merge
	df = df.InnerMerge(&dfRight, "ID")

	if df.CountRecords() != 6 {
		t.Error("Inner Merge: record count error.")
	}

	columns := []string{"ID", "Date", "Cost", "Weight", "First Name", "Last Name", "City", "State", "Postal Code"}

	data := make([][]string, 6)
	data[0] = []string{"4", "2022-01-04", "121", "196", "Peter", "Wiedmann", "VAN BUREN", "AR", "72956"}
	data[1] = []string{"5", "2022-01-05", "774", "415", "Andy", "Wiedmann", "TAUNTON", "MA", "2780"}
	data[2] = []string{"7", "2022-01-07", "995", "500", "Bryan", "Curtis", "GOLDSBORO", "NC", "27530"}
	data[3] = []string{"9", "2022-01-09", "939", "157", "Eric", "Petruska", "PHOENIX", "AZ", "85024"}
	data[4] = []string{"9", "2022-01-09", "12345", "6789", "Eric", "Petruska", "PHOENIX", "AZ", "85024"}
	data[5] = []string{"10", "2022-01-10", "597", "475", "Carl", "Carlson", "JEFFERSON CITY", "MO", "65109"}

	for i, row := range df.FrameRecords {
		if len(row.Data) != len(data[i]) {
			t.Error("Inner Merge: Column count does not match.")
		}
		for i2, col := range columns {
			val := row.Val(col, df.Headers)
			if val != data[i][i2] {
				t.Error("Inner Merge: Data results to not match what is expected.")
			}
		}
	}
}

func TestConcatFrames(t *testing.T) {
	path := "./"
	dfOne := CreateDataFrame(path, "data/tests/TestData.csv")
	df := CreateDataFrame(path, "data/tests/TestDataConcat.csv")

	lastNames := [20]string{
		"Fultz",
		"Fultz",
		"Fultz",
		"Wiedmann",
		"Wiedmann",
		"Wilfong",
		"Curtis",
		"Wenck",
		"Petruska",
		"Carlson",
		"Benny",
		"Kenny",
		"McCarlson",
		"Jeffery",
		"Stephenson",
		"Patrickman",
		"Briarson",
		"Ericson",
		"Asherton",
		"Highman",
	}

	dfOne, err := dfOne.ConcatFrames(&df)
	if err != nil {
		t.Error("Concat Frames: ", err)
	}
	var totalCost int64
	var totalWeight int64

	for i, row := range dfOne.FrameRecords {
		if row.Val("Last Name", dfOne.Headers) != lastNames[i] {
			t.Error("Concat Frames Failed: Last Names")
		}
		totalCost += row.ConvertToInt("Cost", dfOne.Headers)
		totalWeight += row.ConvertToInt("Weight", dfOne.Headers)
	}

	if totalCost != 7100 || totalWeight != 3821 {
		t.Error("Concat Frames Failed: Values")
	}

	if dfOne.CountRecords() != 20 {
		t.Error("Concat Frames Failed: Row Count")
	}
}

func TestConcatFramesAddress(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")
	df2 := CreateDataFrame(path, "data/tests/TestDataConcat.csv")

	df3, err := df.ConcatFrames(&df2)
	if err != nil {
		t.Error(err)
	}

	if &df == &df3 || &df2 == &df3 {
		t.Error("ConcatFrames did not create a truly decoupled new dataframe")
	}
	if df3.CountRecords() != 20 {
		t.Error("ConcatFrames did not properly append")
	}
}

func TestConcatFramesColumnCount(t *testing.T) {
	path := "./"
	dfOne := CreateDataFrame(path, "data/tests/TestData.csv")
	columns := []string{"one", "two", "three"}
	dfTwo := CreateNewDataFrame(columns)

	dfOne, err := dfOne.ConcatFrames(&dfTwo)
	if err == nil {
		t.Error("Concat Frames Did Not Fail --> ", err)
	}
}

func TestConcatFramesColumnOrder(t *testing.T) {
	path := "./"
	dfOne := CreateDataFrame(path, "data/tests/TestData.csv")
	columns := []string{
		"ID",
		"Date",
		"Cost",
		"Weight",
		"Last Name",
		"First Name",
	}
	dfTwo := CreateNewDataFrame(columns)

	dfOne, err := dfOne.ConcatFrames(&dfTwo)
	if err == nil {
		t.Error("Concat Frames Did Not Fail --> ", err)
	}
}

// Ensures once a new filtered DataFrame is created, if records are updated in the original
// it will not affect the records in the newly created filtered version.
func TestCopiedFrame(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")

	df2 := df.Filtered("Last Name", "Wiedmann")

	// Update data in original frame.
	for _, row := range df.FrameRecords {
		if row.Val("First Name", df.Headers) == "Peter" && row.Val("Last Name", df.Headers) == "Wiedmann" {
			row.Update("Last Name", "New Last Name", df.Headers)
		}
	}

	// Check value did not change in newly copied frame.
	for _, row := range df2.FrameRecords {
		if row.Val("ID", df2.Headers) == "4" {
			if row.Val("First Name", df2.Headers) != "Peter" || row.Val("Last Name", df2.Headers) != "Wiedmann" {
				t.Error("Copied Frame: name appears to have changed in second frame.")
			}
		}
	}
}

func TestSaveDataFrame(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")

	if df.SaveDataFrame(path, "Testing") != true {
		t.Error("Failed to save dataframe.")
	}

	t.Logf("Cleaning up %s...", "Testing.csv")
	if err := os.Remove("Testing.csv"); err != nil {
		t.Error("Failed to clean up.")
	}
}

func TestAssortment(t *testing.T) {
	path := "./"

	// Concatenate Frames
	dfOne := CreateDataFrame(path, "data/tests/TestData.csv")
	df := CreateDataFrame(path, "data/tests/TestDataConcat.csv")
	df, err := df.ConcatFrames(&dfOne)
	if err != nil {
		log.Fatal("Concat Frames: ", err)
	}

	// Add Records
	newData := [6]string{"21", "2022-01-01", "200", "585", "Tommy", "Thompson"}
	df = df.AddRecord(newData[:])
	newDataTwo := [6]string{"22", "2022-01-31", "687", "948", "Sarah", "McSarahson"}
	df = df.AddRecord(newDataTwo[:])

	if df.CountRecords() != 22 {
		t.Error("Assortment: concat count incorrect.")
	}

	df = df.Exclude("Last Name", "Fultz", "Highman", "Stephenson")

	if df.CountRecords() != 17 {
		t.Error("Assortment: excluded count incorrect.")
	}

	df = df.FilteredAfter("Date", "2022-01-08")

	if df.CountRecords() != 4 {
		t.Error("Assortment: filtered after count incorrect.")
	}

	lastNames := df.Unique("Last Name")
	checkLastNames := [4]string{"Petruska", "Carlson", "Asherton", "McSarahson"}

	if len(lastNames) != 4 {
		t.Error("Assortment: last name count failed")
	}

	for _, name := range lastNames {
		var status bool
		for _, cName := range checkLastNames {
			if name == cName {
				status = true
			}
		}
		if status != true {
			t.Error("Assortment: last name not found.")
		}
	}

}

func TestCopy(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")
	df2 := df.Copy()

	for _, row := range df2.FrameRecords {
		if row.Val("First Name", df2.Headers) == "Bryan" && row.Val("Last Name", df2.Headers) == "Curtis" {
			row.Update("First Name", "Brian", df2.Headers)
		}
		if row.Val("First Name", df2.Headers) == "Carl" && row.Val("Last Name", df2.Headers) == "Carlson" {
			row.Update("First Name", "McCarlson", df2.Headers)
		}
	}

	// Test original frame did not change.
	for _, row := range df.FrameRecords {
		if row.Val("Last Name", df.Headers) == "Curtis" {
			if row.Val("First Name", df.Headers) != "Bryan" {
				t.Error("First Name in original frame is not correct.")
			}
		}
		if row.Val("Last Name", df.Headers) == "Carlson" {
			if row.Val("First Name", df.Headers) != "Carl" {
				t.Error("First Name in original frame is not correct.")
			}
		}
	}

	// Test copied frame contains changes.
	for _, row := range df2.FrameRecords {
		if row.Val("Last Name", df2.Headers) == "Curtis" {
			if row.Val("First Name", df2.Headers) != "Brian" {
				t.Error("First Name in copied frame is not correct.")
			}
		}
		if row.Val("Last Name", df2.Headers) == "Carlson" {
			if row.Val("First Name", df2.Headers) != "McCarlson" {
				t.Error("First Name in copied frame is not correct.")
			}
		}
	}
}

func TestCopyAddress(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")
	df2 := df.Copy()

	if &df == &df2 {
		t.Error("Copy did not create a truly decoupled copy.")
	}
}

func TestColumns(t *testing.T) {
	path := "./"
	requiredColumns := []string{
		"ID",
		"Date",
		"Cost",
		"Weight",
		"First Name",
		"Last Name",
	}
	df := CreateDataFrame(path, "data/tests/TestData.csv")
	foundColumns := df.Columns()

	if len(foundColumns) != 6 {
		t.Error("Length of found columns does not match")
	}

	for i := 0; i < len(requiredColumns); i++ {
		if foundColumns[i] != requiredColumns[i] {
			t.Error("Order of found columns does not match")
		}
	}
}

func TestAutoCount(t *testing.T) {
	columns := []string{"id", "number", "value"}
	df := CreateNewDataFrame(columns)

	for i := 0; i < 1_000; i++ {
		val := float64(i + 1)
		sq := val * val
		data := []string{
			strconv.Itoa(i),
			fmt.Sprintf("%f", val),
			fmt.Sprintf("%f", sq),
		}
		df = df.AddRecord(data)
	}

	if df.CountRecords() != 1_000 {
		t.Error("Test Auto: count is not 1,000,000")
	}
}

func TestAutoSum(t *testing.T) {
	columns := []string{"id", "number", "value"}
	df := CreateNewDataFrame(columns)

	for i := 0; i < 1_000; i++ {
		val := float64(i + 1)
		sq := val * val
		data := []string{
			strconv.Itoa(i),
			fmt.Sprintf("%f", val),
			fmt.Sprintf("%f", sq),
		}
		df = df.AddRecord(data)
	}

	if df.Sum("value") != 333_833_500.0 {
		t.Error("Test Auto: sum is not correct")
	}
}

func TestLoadFrames(t *testing.T) {
	filePath := "./"
	files := []string{
		"data/tests/TestData.csv",
		"data/tests/TestDataCommaSeparatedValue.csv",
		"data/tests/TestDataConcat.csv",
		"data/tests/TestDataDateFormat.csv",
		"data/tests/TestMergeData.csv",
	}

	results, err := LoadFrames(filePath, files)
	if err != nil {
		log.Fatal(err)
	}

	dfTd := results[0]
	dfComma := results[1]
	dfConcat := results[2]
	dfDate := results[3]
	dfMerge := results[4]

	if dfTd.CountRecords() != 10 || dfTd.Sum("Weight") != 3376.0 || len(dfTd.Columns()) != 6 {
		t.Error("LoadFrames: TestData.csv is not correct")
	}
	if dfComma.CountRecords() != 10 || dfComma.Sum("Cost") != 6521.0 || len(dfComma.Columns()) != 6 {
		t.Error("LoadFrames: TestDataCommaSeparatedValue.csv is not correct")
	}
	if dfConcat.CountRecords() != 10 || dfConcat.Sum("Weight") != 445.0 || len(dfConcat.Columns()) != 6 {
		t.Error("LoadFrames: TestDataConcat.csv is not correct")
	}
	if dfDate.CountRecords() != 10 || dfDate.Average("Cost") != 652.1 || len(dfDate.Columns()) != 6 {
		t.Error("LoadFrames: TestDataDateFormat.csv is not correct")
	}
	if dfMerge.CountRecords() != 10 || dfMerge.Sum("Postal Code") != 495735.0 || len(dfMerge.Columns()) != 4 {
		t.Error("LoadFrames: TestMergeData.csv is not correct")
	}

	dfFilterTest := dfTd.Filtered("Last Name", "Fultz")
	if dfTd.CountRecords() == dfFilterTest.CountRecords() {
		t.Error("LoadFrame: variable referencing map value")
	}
}

func TestLoadFramesError(t *testing.T) {
	filePath := "./"
	files := []string{"data/tests/TestData.csv"}

	_, err := LoadFrames(filePath, files)
	if err == nil {
		t.Error("LoadFrames did not fail as expected")
	}
}

func TestRename(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")

	err := df.Rename("Weight", "Total Weight")
	if err != nil {
		t.Error(err)
	}

	for _, row := range df.FrameRecords {
		if row.Val("First Name", df.Headers) == "Andy" && row.Val("Last Name", df.Headers) == "Wiedmann" {
			row.Update("Total Weight", "1000", df.Headers)
		}
	}

	for _, row := range df.FrameRecords {
		if row.Val("First Name", df.Headers) == "Andy" && row.Val("Last Name", df.Headers) == "Wiedmann" {
			if row.Val("Total Weight", df.Headers) != "1000" {
				t.Error("Value in new column did not update correctly")
			}
		}
	}

	foundColumns := []string{}
	newColumnStatus := false
	for k, _ := range df.Headers {
		foundColumns = append(foundColumns, k)
		if k == "Total Weight" {
			newColumnStatus = true
		}
	}

	if newColumnStatus != true {
		t.Error("New column was not found")
	}
	if len(foundColumns) != 6 {
		t.Error("Wrong number of columns found")
	}
}

func TestRenameOriginalNotFound(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")

	err := df.Rename("The Weight", "Total Weight")
	if err == nil {
		t.Error(err)
	}
}

func TestRenameDuplicate(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "data/tests/TestData.csv")

	err := df.Rename("Weight", "Cost")
	if err == nil {
		t.Error(err)
	}
}
