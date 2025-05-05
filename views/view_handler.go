package views

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/v5/pgxpool"
	"lowcode.com/backend/types"
)

func ViewHandler(w http.ResponseWriter, r *http.Request) {
	appid := r.PathValue("app_id") // Use PathValue if it's part of the route
	if appid == "" {
		http.Error(w, "app_id is required", http.StatusBadRequest)
		return
	}

	viewparams := types.ViewParams{}
	if err := json.NewDecoder(r.Body).Decode(&viewparams); err != nil {
		fmt.Printf("error: unable to parse the body %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	connPool := r.Context().Value("db_conn").(*pgxpool.Pool)

	// Acquire database connection
	dbConn, err := connPool.Acquire(r.Context())
	if err != nil {
		http.Error(w, "Failed to acquire database connection", http.StatusInternalServerError)
		return
	}
	defer dbConn.Release()

	// Set search_path
	searchPath := pgx.Identifier{"public"}.Sanitize()
	_, err = dbConn.Exec(r.Context(), fmt.Sprintf("SET search_path TO %s", searchPath))
	if err != nil {
		http.Error(w, "Failed to set search_path", http.StatusInternalServerError)
		return
	}

	// Execute the provided SQL query (creating or altering the view)
	sqlCode := viewparams.Code
	_, err = dbConn.Exec(r.Context(), sqlCode)
	if err != nil {
		http.Error(w, "Failed to execute view creation/update query", http.StatusInternalServerError)
		return
	}

	// Extract the view name from the SQL query (assuming it starts with CREATE OR ALTER VIEW)
	viewName := extractViewName(sqlCode)
	if viewName == "" {
		http.Error(w, "Failed to determine view name", http.StatusBadRequest)
		return
	}

	// Fetch column names of the created/altered view
	query := `
		SELECT column_name, data_type 
		FROM information_schema.columns 
		WHERE table_schema = 'public' 
		AND table_name = $1
	`

	rows, err := dbConn.Query(r.Context(), query, viewName)
	if err != nil {
		http.Error(w, "Failed to fetch view columns", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var columns []map[string]string
	for rows.Next() {
		var columnName, dataType string
		if err := rows.Scan(&columnName, &dataType); err != nil {
			http.Error(w, "Failed to parse columns", http.StatusInternalServerError)
			return
		}
		columns = append(columns, map[string]string{
			"name": columnName,
			"type": dataType,
		})
	}

	// Return the columns as JSON
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"view_name": viewName,
		"fields":    columns,
		"sqlCode":   sqlCode,
	})
}

func extractViewName(sqlCode string) string {
	// Check for CREATE OR REPLACE VIEW (case-insensitive)
	if strings.HasPrefix(strings.ToUpper(sqlCode), "CREATE OR REPLACE VIEW") {
		// Split the string and return the part after "CREATE OR REPLACE VIEW"
		parts := strings.Fields(sqlCode)
		if len(parts) > 4 {
			return parts[4] // Return the view name
		}
	}

	// Check for CREATE VIEW (case-insensitive)
	if strings.HasPrefix(strings.ToUpper(sqlCode), "CREATE VIEW") {
		// Split the string and return the part after "CREATE VIEW"
		parts := strings.Fields(sqlCode)
		if len(parts) > 2 {
			return parts[2] // Return the view name
		}
	}

	// Check for ALTER VIEW (case-insensitive)
	if strings.HasPrefix(strings.ToUpper(sqlCode), "ALTER VIEW") {
		// Split the string and return the part after "ALTER VIEW"
		parts := strings.Fields(sqlCode)
		if len(parts) > 2 {
			return parts[2] // Return the view name
		}
	}

	// Return an empty string if no match is found
	return ""
}

func RawQueryHandler(w http.ResponseWriter, r *http.Request) {
	appid := r.PathValue("app_id") // Use PathValue if it's part of the route
	if appid == "" {
		http.Error(w, "app_id is required", http.StatusBadRequest)
		return
	}

	viewparams := types.ViewParams{}
	if err := json.NewDecoder(r.Body).Decode(&viewparams); err != nil {
		fmt.Printf("error: unable to parse the body %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	connPool := r.Context().Value("db_conn").(*pgxpool.Pool)

	// Acquire database connection
	dbConn, err := connPool.Acquire(r.Context())
	if err != nil {
		http.Error(w, "Failed to acquire database connection", http.StatusInternalServerError)
		return
	}
	defer dbConn.Release()

	// Set search_path
	searchPath := pgx.Identifier{"public"}.Sanitize()
	_, err = dbConn.Exec(r.Context(), fmt.Sprintf("SET search_path TO %s", searchPath))
	if err != nil {
		http.Error(w, "Failed to set search_path", http.StatusInternalServerError)
		return
	}

	// Execute the provided SQL query (creating or altering the view)
	sqlCode := viewparams.Code
	rows, err := dbConn.Query(r.Context(), sqlCode)
	if err != nil {
		http.Error(w, "Failed to execute query", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Get field descriptions (column names)
	fieldDescriptions := rows.FieldDescriptions()
	if len(fieldDescriptions) == 0 {
		// If no columns are returned, it might be a scalar result
		var result interface{}
		if err := dbConn.QueryRow(r.Context(), sqlCode).Scan(&result); err != nil {
			http.Error(w, "Error retrieving scalar result", http.StatusInternalServerError)
			return
		}

		// Return scalar result (aggregation like COUNT, SUM, etc.)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(result)
		return
	}

	// For normal row-based results, process and return rows
	var result []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(fieldDescriptions))
		valuePtrs := make([]interface{}, len(fieldDescriptions))
		for i := range fieldDescriptions {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			http.Error(w, "Failed to scan row", http.StatusInternalServerError)
			return
		}

		row := make(map[string]interface{})
		for i, col := range fieldDescriptions {
			row[string(col.Name)] = values[i]
		}
		result = append(result, row)
	}

	// Check for any error during row iteration
	if err := rows.Err(); err != nil {
		http.Error(w, "Error reading rows", http.StatusInternalServerError)
		return
	}

	// Return row-based result as JSON
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(result)
}
