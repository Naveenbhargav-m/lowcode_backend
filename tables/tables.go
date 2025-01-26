package tables

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/v5/pgxpool"
	"lowcode.com/backend/types"
)

func TablesModifierHandler(w http.ResponseWriter, r *http.Request) {
	appid := r.PathValue("app_id") // Use PathValue if it's part of the route
	if appid == "" {
		http.Error(w, "app_id is required", http.StatusBadRequest)
		return
	}

	tablesData := types.Schema{}
	// tablesData := map[string]interface{}{}
	if err := json.NewDecoder(r.Body).Decode(&tablesData); err != nil {
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

	for _, table := range tablesData.Tables {
		// Sanitize the table name
		tableName := pgx.Identifier{table.Label}.Sanitize()

		query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY)", tableName)
		if _, err := dbConn.Exec(context.Background(), query); err != nil {
			http.Error(w, fmt.Sprintf("Error creating table %s: %v", table.Label, err), http.StatusInternalServerError)
			return
		}
	}

	alterqueries := []string{}
	// Process each table in the Schema
	for _, table := range tablesData.Tables {
		// Process fields in the table
		for _, field := range table.Fields {
			fieldType := mapFieldType(field.Type)
			field.Type = fieldType
			alterQuery, _ := generateQueryForField(field, table.Label)
			alterqueries = append(alterqueries, alterQuery)

		}
	}
	new_queries := []string{}
	relations := []string{}
	for _, query := range alterqueries {
		if strings.Contains(query, "REFERENCES") && strings.Contains(query, "CONSTRAINT") {
			relations = append(relations, query)
			continue
		}
		new_queries = append(new_queries, query)
	}

	new_queries = append(new_queries, relations...)

	for _, query := range new_queries {
		if _, err := dbConn.Exec(context.Background(), query); err != nil {
			http.Error(w, fmt.Sprintf("Error executing query %s: %v", query, err), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Database schema updated successfully"))
}

func mapFieldType(fieldType string) string {
	lower := strings.ToLower(fieldType)
	switch lower {
	case "primary":
		return "SERIAL PRIMARY KEY"
	case "string":
		return "TEXT"
	case "int", "integer":
		return "INTEGER"
	case "bool", "boolean":
		return "BOOLEAN"
	case "float":
		return "REAL"
	case "date":
		return "DATE"
	case "text[]":
		return "TEXT[]"
	case "integer[]":
		return "INTEGER[]"
	case "json":
		return "JSON"
	case "jsonb":
		return "JSONB"
	case "interval":
		return "INTERVAL"
	case "point (x,y)":
		return "JSONB"
	default:
		return "TEXT"
	}
}

func generateQueryForField(field types.Field, tableName string) (string, error) {
	queryParts := []string{}
	operationLower := strings.ToLower(field.Operation)

	sanitize := func(input string) string {
		return pgx.Identifier{input}.Sanitize()
	}

	switch operationLower {
	case "add":
		// Add column with constraints
		part := fmt.Sprintf("ADD COLUMN IF NOT EXISTS %s %s",
			sanitize(field.Name),
			field.Type,
		)
		if field.Required {
			part += " NOT NULL"
		}
		if field.Default != nil {
			part += fmt.Sprintf(" DEFAULT %s", *field.Default)
		}
		queryParts = append(queryParts, part)

		// Add foreign key constraint if Relation is defined
		if field.Relation != nil {
			relationParts := strings.Split(*field.Relation, ".")
			if len(relationParts) == 2 {
				relationTable := relationParts[0]
				relationColumn := relationParts[1]
				constraintName := fmt.Sprintf("fk_%s_%s", tableName, field.Name)

				part := fmt.Sprintf(
					"ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s)",
					sanitize(constraintName),
					sanitize(field.Name),
					sanitize(relationTable),
					sanitize(relationColumn),
				)
				queryParts = append(queryParts, part)
			}
		}

	default:
		// Handle NOT NULL constraint
		if field.DropRelation {
			queryParts = append(queryParts, fmt.Sprintf(
				"ALTER COLUMN %s DROP NOT NULL",
				sanitize(field.Name),
			))
		} else if field.Required {
			queryParts = append(queryParts, fmt.Sprintf(
				"ALTER COLUMN %s SET NOT NULL",
				sanitize(field.Name),
			))
		}

		if field.OldName != "" {
			// Rename column if OldName is provided
			queryParts = append(queryParts, fmt.Sprintf(
				"RENAME COLUMN %s TO %s",
				sanitize(field.OldName),
				sanitize(field.Name),
			))
		}

		// Handle default value constraint
		if field.DropDefault {
			queryParts = append(queryParts, fmt.Sprintf(
				"ALTER COLUMN %s DROP DEFAULT",
				sanitize(field.Name),
			))
		} else if field.Default != nil {
			queryParts = append(queryParts, fmt.Sprintf(
				"ALTER COLUMN %s SET DEFAULT %s",
				sanitize(field.Name),
				*field.Default,
			))
		}

		// Handle foreign key constraint
		if field.Relation != nil {
			relationParts := strings.Split(*field.Relation, ".")
			if len(relationParts) == 2 {
				relationTable := relationParts[0]
				relationColumn := relationParts[1]
				constraintName := fmt.Sprintf("fk_%s_%s", tableName, field.Name)

				queryParts = append(queryParts, fmt.Sprintf(
					"ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s)",
					sanitize(constraintName),
					sanitize(field.Name),
					sanitize(relationTable),
					sanitize(relationColumn),
				))
			}
		}

		// Handle type cast
		if field.Type != "" {
			queryParts = append(queryParts, fmt.Sprintf(
				"ALTER COLUMN %s TYPE %s USING %s::%s",
				sanitize(field.Name),
				field.Type,
				sanitize(field.Name),
				field.Type,
			))
		}
	}

	if len(queryParts) > 0 {
		return fmt.Sprintf(
			"ALTER TABLE %s %s",
			sanitize(tableName),
			strings.Join(queryParts, ", "),
		), nil
	}
	return "", fmt.Errorf("no valid operation specified")
}
