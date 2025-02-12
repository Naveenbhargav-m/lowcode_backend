package query

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"lowcode.com/backend/types"
)

func QueryHandler(w http.ResponseWriter, r *http.Request) {
	appid := r.PathValue("app_id")
	if appid == "" {
		http.Error(w, "app_id is required", http.StatusBadRequest)
		return
	}

	queryParams := types.QueryParams{}
	if err := json.NewDecoder(r.Body).Decode(&queryParams); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	connPool := r.Context().Value("db_conn").(*pgxpool.Pool)

	dbConn, err := connPool.Acquire(r.Context())
	if err != nil {
		http.Error(w, "Failed to acquire database connection", http.StatusInternalServerError)
		return
	}
	defer dbConn.Release()

	rows, err := dbConn.Query(r.Context(), queryParams.Code)
	if err != nil {
		http.Error(w, fmt.Sprintf("Query execution failed: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Get column names dynamically
	fieldDescriptions := rows.FieldDescriptions()
	columns := make([]string, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		columns[i] = string(fd.Name)
	}

	// Fetch all rows dynamically
	var results []map[string]interface{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			http.Error(w, "Failed to read query results", http.StatusInternalServerError)
			return
		}
		rowData := make(map[string]interface{})
		for i, value := range values {
			rowData[columns[i]] = value
		}
		results = append(results, rowData)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}
