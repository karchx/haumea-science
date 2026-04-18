package main

import (
	log "github.com/gothew/l-og"
	godotenv "github.com/joho/godotenv"
	"github.com/karchx/haumea-science/pipelines/cmd"
)

func loadEnv() {
	// Load environment variables from .env file if it exists
	if err := godotenv.Load(); err != nil {
		log.Infof("No .env file found: %v", err)
	}
}

func main() {
	loadEnv()
	cmd.Execute()
}
