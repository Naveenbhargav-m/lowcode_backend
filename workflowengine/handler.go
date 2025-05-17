package workflowengine

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

func WorkflowHandler(w http.ResponseWriter, r *http.Request) {
	temp := r.Context().Value("db_conn")
	workflowID := r.PathValue("workflow_id")
	db, ok := temp.(*pgxpool.Pool)
	if !ok {
		http.Error(w, "Database connection not found in context", http.StatusInternalServerError)
		return
	}

	data := map[string]interface{}{}
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		log.Printf("error decoding request body: %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	dbname := r.Context().Value("db_name")
	dbnamestr, _ := dbname.(string)

	// Query the workflow
	query := "SELECT * FROM _workflows WHERE id = $1"
	rows, err := db.Query(r.Context(), query, workflowID)
	if err != nil {
		log.Printf("error querying the rows: %v", err)
		http.Error(w, "Database query error", http.StatusInternalServerError)
		return
	}
	defer rows.Close() // Important: always close rows

	// Check if we have any results
	if !rows.Next() {
		log.Printf("no workflow found with ID: %v", workflowID)
		http.Error(w, "Workflow not found", http.StatusNotFound)
		return
	}

	// Get column descriptions to properly handle the row data
	colDescs := rows.FieldDescriptions()
	values := make([]interface{}, len(colDescs))

	// Create scan targets
	scanTargets := make([]interface{}, len(colDescs))
	for i := range values {
		scanTargets[i] = &values[i]
	}

	// Scan the row into values
	if err := rows.Scan(scanTargets...); err != nil {
		log.Printf("error scanning row: %v", err)
		http.Error(w, "Error retrieving workflow data", http.StatusInternalServerError)
		return
	}

	// Build the input map from column names and values
	input := make(map[string]interface{})
	for i, colDesc := range colDescs {
		colName := string(colDesc.Name)
		input[colName] = values[i]
	}
	flowData, _ := input["flow_data"].(map[string]interface{})
	Schema, err := LoadSchema(flowData)
	if err != nil {
		log.Printf("error loading schema data: %v", err)
		http.Error(w, "Error parsing schema", http.StatusInternalServerError)
		return
	}

	engine := NewEngine(r.Context(), db, dbnamestr)
	RegisterBlocks(engine)
	resp, err := engine.Execute(Schema, data)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error executing workflow: %v", err), http.StatusInternalServerError)
		return
	}

	// Set content type and write response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}
}
