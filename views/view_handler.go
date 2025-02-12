package views

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

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

	// Switch to the database specified by appid
	dbConn, err := connPool.Acquire(context.Background())
	if err != nil {
		http.Error(w, "Failed to acquire database connection", http.StatusInternalServerError)
		return
	}
	defer dbConn.Release()
	searchPath := pgx.Identifier{"public"}.Sanitize()
	_, err = dbConn.Exec(context.Background(), fmt.Sprintf("SET search_path TO %s", searchPath))
	if err != nil {
		http.Error(w, "Failed to set search_path", http.StatusInternalServerError)
		return
	}

	sqlCode := viewparams.Code
	_, err = dbConn.Exec(r.Context(), sqlCode)
	if err != nil {

	}

}
