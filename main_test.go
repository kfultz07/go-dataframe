package dataframe

import (
	"testing"
	"time"
)

func TestCreateDataFrameCostFloat(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "TestData.csv")
	total := 0.0

	for _, row := range df.FrameRecords {
		total += row.ConvertToFloat("Cost")
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
		total += row.ConvertToInt("Cost")
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
		if row.Val("Last Name") != "Fultz" && row.Val("Last Name") != "Wiedmann" {
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
		if row.Val("Last Name") == "Fultz" || row.Val("Last Name") == "Wiedmann" {
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
		if row.Val("ID") == "5" {
			id = row.Val("ID")
			date = row.Val("Date")
			cost = row.Val("Cost")
			weight = row.Val("Weight")
			firstName = row.Val("First Name")
			lastName = row.Val("Last Name")
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

func TestByteOrderMark(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "TestDataCommaSeparatedValue.csv")
	dfUtf := CreateDataFrame(path, "TestData.csv")

	dfTotal := 0.0
	for _, row := range df.FrameRecords {
		dfTotal += row.ConvertToFloat("ID")
	}

	dfUtfTotal := 0.0
	for _, row := range dfUtf.FrameRecords {
		dfUtfTotal += row.ConvertToFloat("ID")
	}

	if dfTotal != 55.0 || dfUtfTotal != 55.0 {
		t.Error("Byte Order Mark conversion error")
	}
}

func TestKeepColumns(t *testing.T) {
	path := "./"
	df := CreateDataFrame(path, "TestData.csv")

	columns := [3]string{"First Name", "Last Name", "Weight"}
	df = df.KeepColumns(columns[:])

	if df.Headers[0] != "First Name" || df.Headers[1] != "Last Name" || df.Headers[2] != "Weight" || len(df.Headers) > 3 {
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
