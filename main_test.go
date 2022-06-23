package dataframe

import (
	"fmt"
	"testing"
	"time"
)

func TestCreateDataFrameCostFloat(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "TestData.csv")
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
	df := CreateDataFrame(path, "TestData.csv")
	var total int64

	for _, row := range df.FrameRecords {
		total += row.ConvertToInt("Cost", df.Headers)
	}

	if total != 6521 {
		t.Error("Cost sum incorrect.")
	}
}

func TestFilteredCount(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "TestData.csv")
	dfFil := df.Filtered("Last Name", "Fultz", "Wiedmann")

	if df.CountRecords() != 10 || dfFil.CountRecords() != 5 {
		t.Error("Filtered count incorrect.")
	}
}

func TestFilteredCheck(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "TestData.csv")
	dfFil := df.Filtered("Last Name", "Fultz", "Wiedmann")

	for _, row := range dfFil.FrameRecords {
		if row.Val("Last Name", dfFil.Headers) != "Fultz" && row.Val("Last Name", dfFil.Headers) != "Wiedmann" {
			t.Error("Invalid parameter found in Filtered DataFrame.")
		}
	}
}

func TestExcludeCount(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "TestData.csv")
	dfExcl := df.Exclude("Last Name", "Fultz", "Wiedmann")

	if df.CountRecords() != 10 || dfExcl.CountRecords() != 5 {
		t.Error("Excluded count is incorrect.")
	}
}

func TestExcludeCheck(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "TestData.csv")
	dfExcl := df.Exclude("Last Name", "Fultz", "Wiedmann")

	for _, row := range dfExcl.FrameRecords {
		if row.Val("Last Name", dfExcl.Headers) == "Fultz" || row.Val("Last Name", dfExcl.Headers) == "Wiedmann" {
			t.Error("Excluded parameter found in DataFrame.")
		}
	}
}

func TestFilteredAfterCount(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "TestData.csv")
	dfFil := df.FilteredAfter("Date", "2022-01-08")

	if df.CountRecords() != 10 || dfFil.CountRecords() != 2 {
		t.Error("Filtered After count incorrect.")
	}
}

func TestFilteredAfterCountExcelFormat(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "TestDataDateFormat.csv")
	dfFil := df.FilteredAfter("Date", "2022-01-08")

	if df.CountRecords() != 10 || dfFil.CountRecords() != 2 {
		t.Error("Filtered After Excel Format count incorrect.")
	}
}

func TestFilteredBeforeCount(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "TestData.csv")
	dfFil := df.FilteredBefore("Date", "2022-01-08")

	if df.CountRecords() != 10 || dfFil.CountRecords() != 7 {
		t.Error("Filtered Before count incorrect.")
	}
}

func TestFilteredBeforeCountExcelFormat(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "TestDataDateFormat.csv")
	dfFil := df.FilteredBefore("Date", "2022-01-08")

	if df.CountRecords() != 10 || dfFil.CountRecords() != 7 {
		t.Error("Filtered Before Excel Format count incorrect.")
	}
}

func TestFilteredBetweenCount(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "TestData.csv")
	dfFil := df.FilteredBetween("Date", "2022-01-02", "2022-01-09")

	if df.CountRecords() != 10 || dfFil.CountRecords() != 6 {
		t.Error("Filtered Between count incorrect.")
	}
}

func TestFilteredBetweenExcelFormat(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "TestDataDateFormat.csv")
	dfFil := df.FilteredBetween("Date", "2022-01-02", "2022-01-09")

	if df.CountRecords() != 10 || dfFil.CountRecords() != 6 {
		t.Error("Filtered Between Excel Format count incorrect.")
	}
}

func TestRecordCheck(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "TestData.csv")

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

func TestAddRecord(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "TestData.csv")
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
	df := CreateDataFrame(path, "TestDataCommaSeparatedValue.csv")
	dfUtf := CreateDataFrame(path, "TestData.csv")

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

// --NEW--
func TestKeepColumns(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "TestData.csv")

	columns := [3]string{"First Name", "Last Name", "Weight"}
	df = df.KeepColumns(columns[:])

	if df.Headers["First Name"] != 0 || df.Headers["Last Name"] != 1 || df.Headers["Weight"] != 2 || len(df.Headers) > 3 {
		t.Error("Keep Columns failed")
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
	df := CreateDataFrame(path, "TestData.csv")
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
	df := CreateDataFrame(path, "TestData.csv")
	names := df.Unique("Last Name")

	if len(names) != 7 {
		t.Error("Unique slice error.")
	}
}

func TestUpdate(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "TestData.csv")

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

func TestConcatFrames(t *testing.T) {
	path := "./"
	dfOne := CreateDataFrame(path, "TestData.csv")
	df := CreateDataFrame(path, "TestDataConcat.csv")

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

	dfOne = dfOne.ConcatFrames(&df)
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

func TestSaveDataFrame(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "TestData.csv")

	if df.SaveDataFrame(path, "Testing") != true {
		t.Error("Failed to save dataframe.")
	}
}
