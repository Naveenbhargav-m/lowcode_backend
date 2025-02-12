package triggers

import (
	"encoding/json"
	"net/http"
	"regexp"

	"github.com/jackc/pgx/v5/pgxpool"
	"lowcode.com/backend/types"
)

func TriggerHandler(w http.ResponseWriter, r *http.Request) {
	appid := r.PathValue("app_id")
	if appid == "" {
		http.Error(w, "app_id is required", http.StatusBadRequest)
		return
	}

	triggerParams := types.TriggerParams{}
	if err := json.NewDecoder(r.Body).Decode(&triggerParams); err != nil {
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

	// Execute the function creation SQL first
	if triggerParams.FunctionCode != "" {
		_, err = dbConn.Exec(r.Context(), triggerParams.FunctionCode)
		if err != nil {
			http.Error(w, "Failed to create trigger function", http.StatusInternalServerError)
			return
		}
	}

	// Execute the trigger creation SQL
	_, err = dbConn.Exec(r.Context(), triggerParams.Code)
	if err != nil {
		http.Error(w, "Failed to create/update trigger", http.StatusInternalServerError)
		return
	}

	triggerName := extractTriggerName(triggerParams.Code)
	if triggerName == "" {
		http.Error(w, "Failed to determine trigger name", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"message":      "Trigger created successfully",
		"trigger_name": triggerName,
	})
}

// Helper function to extract the trigger name from SQL
func extractTriggerName(sqlCode string) string {
	re := regexp.MustCompile(`(?i)CREATE\s+TRIGGER\s+(\w+)`)
	matches := re.FindStringSubmatch(sqlCode)
	if len(matches) > 1 {
		return matches[1]
	}
	return ""
}
