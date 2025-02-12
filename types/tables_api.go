package types

type TablesAPIStruct struct {
}

type Field struct {
	Name         string  `json:"name"`
	OldName      string  `json:"old_name"`
	UID          string  `json:"uid"`
	Type         string  `json:"type"`
	Order        int     `json:"order"`
	SQLType      string  `json:"sql_type,omitempty"`
	Table        string  `json:"table"`
	Required     bool    `json:"required"`
	DropRelation bool    `json:"drop_required"`
	DropDefault  bool    `json:"drop_default"`
	Operation    string  `json:"operation"`
	Default      *string `json:"default"`
	Relation     *string `json:"relation"`
	FromName     string  `json:"from_name"`
}

type Table struct {
	ID        string           `json:"id"`
	Label     string           `json:"label"`
	Fields    map[string]Field `json:"fields"`
	OldName   string           `json:"old_name"`
	Operation string           `json:"rename_table"`
}

type Schema struct {
	Tables map[string]Table `json:"tables"`
}

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
