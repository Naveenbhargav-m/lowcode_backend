package types

import "encoding/json"

type ViewParams struct {
	Code string `json:"code"`
}

type TriggerParams struct {
	FunctionCode string `json:"function_code"` // Optional: SQL for function creation
	Code         string `json:"code"`          // SQL for trigger creation
}

// QueryParams is used for executing arbitrary queries.
type QueryParams struct {
	Code string `json:"code"`
}

// Transaction represents a generic database transaction
type Transaction struct {
	ID             string          `json:"id"`
	Type           string          `json:"type"`
	Table          string          `json:"table,omitempty"`
	ConstraintName string          `json:"constraintName,omitempty"`
	SourceTable    string          `json:"sourceTable,omitempty"`
	SourceColumn   string          `json:"sourceColumn,omitempty"`
	TargetTable    string          `json:"targetTable,omitempty"`
	TargetColumn   string          `json:"targetColumn,omitempty"`
	Field          interface{}     `json:"field,omitempty"`
	Fields         []Field         `json:"fields,omitempty"`
	OldName        string          `json:"oldName,omitempty"`
	NewName        string          `json:"newName,omitempty"`
	Modifications  json.RawMessage `json:"modifications,omitempty"`
}

// Field represents a database field definition
type Field struct {
	Name         string      `json:"name"`
	DataType     string      `json:"dataType"`
	Length       interface{} `json:"length"`
	PrimaryKey   bool        `json:"primaryKey"`
	Nullable     bool        `json:"nullable"`
	DefaultValue interface{} `json:"defaultValue"`
}

// TransactionPayload represents the JSON payload containing transactions
type TransactionPayload struct {
	Configs      map[string]interface{} `json:"config"`
	Transactions []Transaction          `json:"transactions"`
}

// SQLResponse is the structure for the SQL response
type SQLResponse struct {
	Queries []string `json:"queries"`
}
