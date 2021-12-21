package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

type Row struct {
	firstName string
	lastName  string
	address   string
	city      string
	state     string
	zipCode   string
}

var dataframe map[int]Row

func main() {
	// Create the map
	dataframe = make(map[int]Row)

	// Open the CSV file
	recordFile, err := os.Open("/Users/kevinfultz/Desktop/DumpBin/ActiveCasa.csv")
	if err != nil {
		fmt.Println("An error encountered ::", err)
	}

	// Setup the reader
	reader := csv.NewReader(recordFile)

	// Read the records
	header, err := reader.Read()
	if err != nil {
		fmt.Println("An error encountered ::", err)
	}
	fmt.Printf("Headers : %v \n", header)

	// Loop over records and add to DataFrame
	for i := 0; ; i++ {
		record, err := reader.Read()
		if err == io.EOF {
			break // reached end of the file
		} else if err != nil {
			fmt.Println("An error encountered ::", err)
		}
		firstName := record[0]
		lastName := record[1]
		address := record[2]
		city := record[3]
		state := record[4]
		zipCode := record[5]

		// Add to DataFrame
		dataframe[i] = Row{firstName, lastName, address, city, state, zipCode}
	}

	// Create New CSV file to write to
	file, err := os.Create("CasaResults.csv")
	defer file.Close()
	if err != nil {
		log.Fatalln("Failed to open file", err)
	}

	w := csv.NewWriter(file)
	defer w.Flush()

	// Add the column headers
	record := []string{"Key", "First Name", "Last Name", "Address", "City", "State", "Zip Code"}
	if err := w.Write(record); err != nil {
		log.Fatalln("Error", err)
	}

	for key, x := range dataframe {
		// Create the record and write to the CSV file
		record := []string{strconv.Itoa(key), x.firstName, x.lastName, x.address, x.city, x.state, x.zipCode}
		if err := w.Write(record); err != nil {
			log.Fatalln("Error", err)
		}
	}
	fmt.Println("\nComplete")
}
