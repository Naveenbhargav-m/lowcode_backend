package workflowengine

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func WorkflowHandler(w http.ResponseWriter, r *http.Request) {
	// // Check if the request is a POST method
	// if r.Method != http.MethodPost {
	// 	http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	// 	return
	// }

	Usage()
	// Parse the JSON from the request body
	var workflowRequest struct {
		Schema string                 `json:"schema"`
		Input  map[string]interface{} `json:"input"`
	}

	if err := json.NewDecoder(r.Body).Decode(&workflowRequest); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Create a new workflow engine
	engine := NewEngine()

	// Register default blocks
	engine.RegisterBlock("add", AddBlock)
	engine.RegisterBlock("stringTransform", StringTransformBlock)
	engine.RegisterBlock("noop", func(input map[string]interface{}, output map[string]interface{}) error {
		return nil
	})

	// Load schema
	schema, err := LoadSchema([]byte(workflowRequest.Schema))
	if err != nil {
		http.Error(w, fmt.Sprintf("Error loading schema: %v", err), http.StatusBadRequest)
		return
	}

	// Execute workflow
	start := time.Now()
	result, err := engine.Execute(schema, workflowRequest.Input)
	elapsed := time.Since(start)

	if err != nil {
		http.Error(w, fmt.Sprintf("Error executing workflow: %v", err), http.StatusInternalServerError)
		return
	}

	// Create the response
	response := struct {
		Result        map[string]interface{} `json:"result"`
		ExecutionTime string                 `json:"executionTime"`
	}{
		Result:        result,
		ExecutionTime: elapsed.String(),
	}

	// Set content type and write response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}
}
