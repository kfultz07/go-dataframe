package dataframe

import (
	"testing"
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
	dfFil := df.Filtered("Last Name", "Fultz")

	if df.CountRecords() != 10 || dfFil.CountRecords() != 3 {
		t.Error("Filtered cound incorrect.")
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
