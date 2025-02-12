package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"lowcode.com/backend/dbstore"
	"lowcode.com/backend/middleware"
	"lowcode.com/backend/tables"
	"lowcode.com/backend/triggers"
	"lowcode.com/backend/views"
)

// Utility: Validate PostgreSQL identifiers
var identifierRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func validateIdentifier(name string) error {
	if !identifierRegex.MatchString(name) {
		return fmt.Errorf("invalid identifier: %s", name)
	}
	return nil
}

// Utility: Validate column types (customize as needed)
func validateColumnType(colType string) error {
	validTypes := map[string]bool{
		"SERIAL": true, "VARCHAR": true, "TEXT": true, "BOOLEAN": true, "INT": true,
		"BIGINT": true, "DATE": true, "TIMESTAMP": true,
	}
	if !validTypes[colType] {
		return fmt.Errorf("invalid column type: %s", colType)
	}
	return nil
}

// Function to create a new database and tables
func createDatabaseHandler(w http.ResponseWriter, r *http.Request) {
	type request struct {
		DatabaseName string `json:"name"`
	}

	var req request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		log.Println("Error decoding JSON:", err)
		return
	}

	// Validate database name
	if err := validateIdentifier(req.DatabaseName); err != nil {
		http.Error(w, fmt.Sprintf("Invalid database name: %v", err), http.StatusBadRequest)
		log.Println("Validation error:", err)
		return
	}

	// Create the database
	dbname := strings.ToLower(req.DatabaseName)
	createDBQuery := fmt.Sprintf("CREATE DATABASE %s", dbname)
	dbpool, _ := dbstore.GlobalPoolManager.GetPool("nokodo_creator")
	if _, err := dbpool.Exec(context.Background(), createDBQuery); err != nil {
		http.Error(w, fmt.Sprintf("Failed to create database: %v", err), http.StatusInternalServerError)
		log.Println("Database creation error:", err)
		return
	}
	log.Printf("Database %s created successfully", dbname)

	// Close the existing pool connection to the `postgres` database
	// dbPool.Close()

	// Create a new connection to the newly created database
	newDBConnString := fmt.Sprintf("postgres://bhargav:Bhargav123@localhost:5432/%s?sslmode=disable", dbname)
	var newDB *pgxpool.Pool
	var err error
	for i := 0; i < 5; i++ {
		// Attempt to establish a new connection pool to the new database
		newDB, err = pgxpool.New(context.Background(), newDBConnString)
		if err == nil {
			break
		}
		log.Printf("Failed to connect to the newly created database %s, retrying... (%d/5)", dbname, i+1)
		time.Sleep(2 * time.Second)
	}

	// If all retries fail
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to connect to the new database after retries: %v", err), http.StatusInternalServerError)
		log.Println("Database connection error:", err)
		return
	}
	defer newDB.Close()

	// Create tables in the new database
	queries := []string{
		"CREATE TABLE screens (id SERIAL PRIMARY KEY, screen_name VARCHAR(255), tags TEXT[], configs JSONB);",
		"CREATE TABLE forms (id SERIAL PRIMARY KEY, form_name VARCHAR(255), table_name VARCHAR(255), fields JSONB);",
		"CREATE TABLE global_states (id SERIAL PRIMARY KEY, state_name TEXT, default_value JSONB, screen_name TEXT, screen_id INT);",
		"CREATE TABLE tables_data (id SERIAL PRIMARY KEY, table_name TEXT, label TEXT, fields JSONB);",
	}

	for _, query := range queries {
		if _, err := newDB.Exec(context.Background(), query); err != nil {
			http.Error(w, fmt.Sprintf("Failed to create table: %v", err), http.StatusInternalServerError)
			log.Println("Table creation error:", err)
			return
		}
	}

	log.Printf("All tables created successfully in database %s", req.DatabaseName)

	// Respond with success
	w.WriteHeader(http.StatusCreated)
	resp := map[string]string{
		"status": "success",
		"msg":    fmt.Sprintf("Database %s and its tables created successfully", req.DatabaseName),
	}
	respJson, _ := json.Marshal(resp)
	w.Write(respJson)
}

// API handler to create a table
func createTableHandler(w http.ResponseWriter, r *http.Request) {
	dbName := r.URL.Query().Get("dbName")
	if dbName == "" {
		http.Error(w, "Database name is required", http.StatusBadRequest)
		return
	}

	type Column struct {
		Name     string `json:"name"`
		Type     string `json:"type"`
		Nullable bool   `json:"nullable"`
	}
	type request struct {
		TableName string   `json:"tableName"`
		Columns   []Column `json:"columns"`
	}

	var req request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		log.Println("Error decoding JSON:", err)
		return
	}

	// Validate table name
	if err := validateIdentifier(req.TableName); err != nil {
		http.Error(w, fmt.Sprintf("Invalid table name: %v", err), http.StatusBadRequest)
		log.Println("Validation error:", err)
		return
	}

	// Reconnect to the specified database
	newDBConnString := fmt.Sprintf("postgres://bhargav:Bhargav123@localhost:5432/%s?sslmode=disable", dbName)
	newDB, err := pgxpool.New(context.Background(), newDBConnString)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to connect to the database: %v", err), http.StatusInternalServerError)
		log.Println("Database connection error:", err)
		return
	}
	defer newDB.Close()

	// Construct column definitions
	columnDefs := ""
	for i, col := range req.Columns {
		// Validate column name and type
		if err := validateIdentifier(col.Name); err != nil {
			http.Error(w, fmt.Sprintf("Invalid column name: %v", err), http.StatusBadRequest)
			log.Println("Validation error:", err)
			return
		}
		if err := validateColumnType(col.Type); err != nil {
			http.Error(w, fmt.Sprintf("Invalid column type: %v", err), http.StatusBadRequest)
			log.Println("Validation error:", err)
			return
		}

		nullable := "NOT NULL"
		if col.Nullable {
			nullable = "NULL"
		}
		columnDefs += fmt.Sprintf("%s %s %s", col.Name, col.Type, nullable)
		if i < len(req.Columns)-1 {
			columnDefs += ", "
		}
	}

	// Construct and execute query
	query := fmt.Sprintf("CREATE TABLE %s (%s)", req.TableName, columnDefs)
	_, err = newDB.Exec(context.Background(), query)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create table: %v", err), http.StatusInternalServerError)
		log.Println("Table creation error:", err)
		return
	}

	w.WriteHeader(http.StatusCreated)
	msg := fmt.Sprintf("Table %s created successfully", req.TableName)
	log.Println(msg)
	w.Write([]byte(msg))
}

func main() {
	cmd := exec.Command("./prestd")
	err := cmd.Start()
	if err != nil {
		log.Fatalf("error starting prestd binary:err: %v\n", err)
	}
	fmt.Println("started prestd in 8000!")
	// Initialize the global pool manager with a 10-minute inactivity TTL
	dbstore.InitPoolManager(10 * time.Minute)

	// HTTP server setup
	mux := http.NewServeMux()
	mux.HandleFunc("/api/create-database", createDatabaseHandler)
	mux.HandleFunc("/api/create-table", createTableHandler)
	mux.Handle("/api/update-table/{app_id}", registerwithDB(tables.TablesModifierHandler))
	mux.Handle("/api/tables/{app_id}", registerwithDB(tables.TablesModifierHandler))
	mux.Handle("/api/views/{app_id}", registerwithDB(views.ViewHandler))
	mux.Handle("/api/triggers/{app_id}", registerwithDB(triggers.TriggerHandler))
	// dbmux := middleware.DBInjectionMiddleware(mux)
	newmux := middleware.CorsMiddleware(mux)
	server := http.Server{
		Addr:    ":8001",
		Handler: newmux,
	}
	log.Println("Server is running on http://localhost:8001")
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server failed: %v\n", err)
	}
}

func registerwithDB(handlerfunc func(w http.ResponseWriter, r *http.Request)) http.Handler {
	return middleware.DBInjectionMiddleware(http.HandlerFunc(handlerfunc))
}
