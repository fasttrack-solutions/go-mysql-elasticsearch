package api

import (
	"encoding/json"
	"log"
	"net/http"
)

func respond(data interface{}, statusCode int, w http.ResponseWriter) {
	// Return JSON type.
	w.Header().Set("Content-Type", "application/json")

	// Marshal response.
	resp, respErr := json.Marshal(data)
	if respErr != nil {
		log.Printf("failed to generate the API response, data: %v", data)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(statusCode)
	w.Write(resp)
}
