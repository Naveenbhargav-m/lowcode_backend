package views

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"

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
		"columns":   columns,
	})
}

// Helper function to extract the view name from SQL code
func extractViewName(sqlCode string) string {
	re := regexp.MustCompile(`(?i)CREATE\s+VIEW\s+(\w+)|ALTER\s+VIEW\s+(\w+)`)
	matches := re.FindStringSubmatch(sqlCode)
	if matches == nil {
		return ""
	}
	if matches[1] != "" {
		return matches[1] // CREATE VIEW case
	}
	return matches[2] // ALTER VIEW case
}
