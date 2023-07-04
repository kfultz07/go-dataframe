package dataframe

import (
	"fmt"
	"strconv"
	"time"
)

func calculateSpaces(val string, maxColumnWidth int) string {
	valLength := len(val)

	if len(val)%2 != 0 {
		val += " "
	}

	if len(val) == maxColumnWidth {
		return "|" + val + "| ---> " + strconv.Itoa(valLength)
	}

	for len(val) < maxColumnWidth {
		val = " " + val + " "
	}

	return "|" + val + "| ---> " + strconv.Itoa(valLength)
}

func calculateMaxColumnWidth(headers []string) int {
	maxWidth := len(headers[0])

	for _, each := range headers {
		if len(each) > maxWidth {
			maxWidth = len(each)
		}
	}
	return maxWidth
}

// Dynamically generate the column headers for the table.
func generateTableColumns(headers []string, maxColumnWidth int) string {
	var head string
	var columnCount int

	for _, h := range headers {
		val := calculateSpaces(h, maxColumnWidth)
		head += val + "\n"
		columnCount++
	}

	head = "\n" + head

	border := " "

	for i := 0; i < maxColumnWidth; i++ {
		border += "-"
	}

	head = "Column Count: " + strconv.Itoa(columnCount) + "\n" + border + head + border

	return head
}

// Method to print all columns in a viewable table within the terminal.
func (frame DataFrame) ViewColumns() {
	var columns []string

	// Add columns in order from map.
	for i := 0; i < len(frame.Headers); i++ {
		for k, v := range frame.Headers {
			if v == i {
				columns = append(columns, k)
			}
		}
	}

	maxColumnWidth := calculateMaxColumnWidth(columns)

	head := generateTableColumns(columns, maxColumnWidth)
	fmt.Println(head)
}

func loading(quit <-chan bool) {
	char := []string{
		"| L",
		"/ LO",
		"- LOA",
		"\\ LOAD",
		"| LOADI",
		"/ LOADIN",
		"- LOADING",
		"\\ LOADING.",
		"| LOADING..",
		"/ LOADING...",
		"-           ",
	}

	for {
		select {
		case <-quit:
			fmt.Printf("\r")
			return
		default:
			for _, c := range char {
				fmt.Printf("\r%s", c)
				time.Sleep(time.Millisecond * 75)
			}
		}
	}
}
