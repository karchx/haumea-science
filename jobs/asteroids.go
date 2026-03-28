package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
)

const (
	APIKEY = "esebnPHu2VfTAKwHma6JKRsSsC1CARxBcBpt6YRr"
)

func main() {
	startDate := "2026-03-20"
	endDate := "2026-03-26"
	url := fmt.Sprintf("https://api.nasa.gov/neo/rest/v1/feed?start_date=%s&end_date=%s&api_key=%s", startDate, endDate, APIKEY)
	resp, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(body))
}
