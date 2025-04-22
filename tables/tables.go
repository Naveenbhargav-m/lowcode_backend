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
	var payload types.TransactionPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}
	queries, err := ProcessTransactions(payload.Transactions)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error processing transactions: %v", err), http.StatusInternalServerError)
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
	for _, query := range queries {
		_, err = dbConn.Exec(context.Background(), query)
		if err != nil {
			msg := fmt.Sprintf("failed to run query :%v", err)
			http.Error(w, msg, http.StatusInternalServerError)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	configsJSon, _ := json.Marshal(&payload.Configs)
	query := "update _tables set tables_data = $1 where id = 1"
	_, _ = dbConn.Exec(context.Background(), query, configsJSon)
	json.NewEncoder(w).Encode(payload.Configs)
}

// ProcessTransactions processes all transactions and returns the generated SQL queries
func ProcessTransactions(transactions []types.Transaction) ([]string, error) {
	var queries []string
	for _, transaction := range transactions {
		query, err := GenerateSQLQuery(transaction)
		if err != nil {
			return nil, fmt.Errorf("error processing transaction %s: %v", transaction.ID, err)
		}
		queries = append(queries, query)
	}
	return queries, nil
}

// GenerateSQLQuery generates an SQL query based on the transaction type
func GenerateSQLQuery(transaction types.Transaction) (string, error) {
	switch transaction.Type {
	case "drop_foreign_key":
		return GenerateDropForeignKeySQL(transaction), nil
	case "drop_field":
		return GenerateDropFieldSQL(transaction), nil
	case "drop_table":
		return GenerateDropTableSQL(transaction), nil
	case "create_table":
		return GenerateCreateTableSQL(transaction)
	case "rename_field":
		return GenerateRenameFieldSQL(transaction), nil
	case "add_field":
		return GenerateAddFieldSQL(transaction)
	case "alter_field":
		return GenerateAlterFieldSQL(transaction)
	case "create_foreign_key":
		return GenerateCreateForeignKeySQL(transaction), nil
	default:
		return "", fmt.Errorf("unknown transaction type: %s", transaction.Type)
	}
}

// GenerateDropForeignKeySQL generates SQL to drop a foreign key constraint
func GenerateDropForeignKeySQL(transaction types.Transaction) string {
	return fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;",
		FormatName(transaction.SourceTable),
		FormatName(transaction.ConstraintName))
}

// GenerateDropFieldSQL generates SQL to drop a field from a table
func GenerateDropFieldSQL(transaction types.Transaction) string {
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;",
		FormatName(transaction.Table),
		FormatName(transaction.Field.(string)))
}

// GenerateDropTableSQL generates SQL to drop a table
func GenerateDropTableSQL(transaction types.Transaction) string {
	return fmt.Sprintf("DROP TABLE %s;", FormatName(transaction.Table))
}

// GenerateCreateTableSQL generates SQL to create a new table
func GenerateCreateTableSQL(transaction types.Transaction) (string, error) {
	if len(transaction.Fields) == 0 {
		return "", fmt.Errorf("no fields defined for table %s", transaction.Table)
	}
	var fieldDefs []string
	var primaryKeys []string

	for _, field := range transaction.Fields {
		fieldDef, err := FormatFieldDefinition(field, false)
		if err != nil {
			return "", err
		}
		fieldDefs = append(fieldDefs, fieldDef)

		if field.PrimaryKey {
			primaryKeys = append(primaryKeys, FormatName(field.Name))
		}
	}

	// Add primary key constraint if there are primary keys
	if len(primaryKeys) > 0 {
		pkConstraint := fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primaryKeys, ", "))
		fieldDefs = append(fieldDefs, pkConstraint)
	}

	return fmt.Sprintf("CREATE TABLE %s (\n  %s\n);",
		FormatName(transaction.Table),
		strings.Join(fieldDefs, ",\n  ")), nil
}

// GenerateRenameFieldSQL generates SQL to rename a field
func GenerateRenameFieldSQL(transaction types.Transaction) string {
	return fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s;",
		FormatName(transaction.Table),
		FormatName(transaction.OldName),
		FormatName(transaction.NewName))
}

// GenerateAddFieldSQL generates SQL to add a field to a table
func GenerateAddFieldSQL(transaction types.Transaction) (string, error) {
	// Convert the field from interface{} to Field
	var field types.Field
	fieldBytes, err := json.Marshal(transaction.Field)
	if err != nil {
		return "", fmt.Errorf("error marshaling field: %v", err)
	}

	if err := json.Unmarshal(fieldBytes, &field); err != nil {
		return "", fmt.Errorf("error unmarshaling field: %v", err)
	}

	fieldDef, err := FormatFieldDefinition(field, true)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;",
		FormatName(transaction.Table),
		fieldDef), nil
}

// GenerateAlterFieldSQL generates SQL to alter a field's properties
func GenerateAlterFieldSQL(transaction types.Transaction) (string, error) {
	var modifications map[string]interface{}
	if err := json.Unmarshal(transaction.Modifications, &modifications); err != nil {
		return "", fmt.Errorf("error unmarshaling modifications: %v", err)
	}
	var alterCommands []string
	tableName := FormatName(transaction.Table)
	fieldName := FormatName(transaction.Field.(string))

	if length, ok := modifications["length"]; ok {
		alterCommands = append(alterCommands,
			fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s(%v);",
				tableName, fieldName, "varchar", length))
	}

	if defaultValue, ok := modifications["defaultValue"]; ok {
		if defaultValue == nil {
			alterCommands = append(alterCommands,
				fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;",
					tableName, fieldName))
		} else {
			alterCommands = append(alterCommands,
				fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;",
					tableName, fieldName, FormatDefaultValue(defaultValue)))
		}
	}

	if nullable, ok := modifications["nullable"].(bool); ok {
		if nullable {
			alterCommands = append(alterCommands,
				fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;",
					tableName, fieldName))
		} else {
			alterCommands = append(alterCommands,
				fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;",
					tableName, fieldName))
		}
	}

	return strings.Join(alterCommands, "\n"), nil
}

// GenerateCreateForeignKeySQL generates SQL to create a foreign key constraint
func GenerateCreateForeignKeySQL(transaction types.Transaction) string {
	return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s);",
		FormatName(transaction.SourceTable),
		FormatName(transaction.ConstraintName),
		FormatName(transaction.SourceColumn),
		FormatName(transaction.TargetTable),
		FormatName(transaction.TargetColumn))
}

// FormatFieldDefinition formats a field definition for CREATE TABLE or ALTER TABLE ADD COLUMN
func FormatFieldDefinition(field types.Field, isAlterTable bool) (string, error) {
	var parts []string
	parts = append(parts, FormatName(field.Name))
	// Convert dataType to lowercase for consistency
	dataType := strings.ToLower(field.DataType)

	// Add data type with length if applicable
	if field.Length != nil && field.Length != "" {
		parts = append(parts, fmt.Sprintf("%s(%v)", dataType, field.Length))
	} else if dataType == "serial" {
		// Handle SERIAL type specially for better cross-database compatibility
		parts = append(parts, "integer")
	} else {
		parts = append(parts, dataType)
	}

	// Add NOT NULL constraint if field is not nullable
	if !field.Nullable {
		parts = append(parts, "NOT NULL")
	}

	// Add DEFAULT if present
	if field.DefaultValue != nil {
		parts = append(parts, fmt.Sprintf("DEFAULT %s", FormatDefaultValue(field.DefaultValue)))
	}

	// Add PRIMARY KEY constraint if this is a primary key and we're in an ALTER TABLE
	// In CREATE TABLE, we add the PRIMARY KEY constraint separately
	if field.PrimaryKey && isAlterTable {
		parts = append(parts, "PRIMARY KEY")
	}

	// Handle SERIAL for PostgreSQL (auto-increment for other databases)
	if dataType == "serial" && !isAlterTable {
		// For PostgreSQL, we need to add auto-increment functionality
		// This is specific to PostgreSQL syntax
		if field.PrimaryKey {
			return strings.Join(parts, " "), nil
		}
	}

	return strings.Join(parts, " "), nil
}

// FormatDefaultValue formats a default value based on its type
func FormatDefaultValue(value interface{}) string {
	switch v := value.(type) {
	case string:
		// Handle function calls like NOW() by checking if it ends with ()
		if strings.HasSuffix(v, "()") {
			return v
		}
		return fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
	case nil:
		return "NULL"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// FormatName converts an identifier to lowercase and removes quotes
// This ensures consistency across all SQL statements
func FormatName(name string) string {
	return strings.ToLower(name)
}
