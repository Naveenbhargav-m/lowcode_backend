package workflowengine

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	appcrud "lowcode.com/backend/app_crud"
	datahelpers "lowcode.com/backend/data_helpers"
)

func InsertRows(ctx context.Context, configs map[string]interface{},
	input, schema, output map[string]interface{}) error {

	// Get global variables
	// emyblock := GetCurBlockData(schema)
	// blockconfigs := myblock.BlockConfig

	// Only resolve references if needed
	needsResolving := false
	for _, v := range input {
		if str, ok := v.(string); ok && strings.Contains(str, "[") && strings.Contains(str, "]") {
			needsResolving = true
			break
		}
	}

	var resolvedInput map[string]interface{} = input
	var err error

	if needsResolving {
		// Only resolve if there might be references
		resolvedInput, err = datahelpers.ResolveVariableReferences(output, schema, input)
		if err != nil {
			return fmt.Errorf("error resolving variable references: %w", err)
		}
	}

	table, _ := resolvedInput["table"].(string)
	table_schema, _ := resolvedInput["schema"].(string)
	rowsData := resolvedInput["rows"]

	db, _ := configs["db"].(*pgxpool.Pool)
	resp, err := appcrud.InsertRecords(ctx, db, table_schema, table, rowsData)
	if err != nil {
		return err
	}
	curname, _ := configs["id"].(string)
	output[curname] = resp
	return nil
}

func UpdateRows(ctx context.Context, configs map[string]interface{}, input, schema, output map[string]interface{}) error {
	return nil
}

func DeletRows(ctx context.Context, configs map[string]interface{}, input, schema, output map[string]interface{}) error {
	return nil
}

func ReadRows(ctx context.Context, configs map[string]interface{}, input, schema, output map[string]interface{}) error {
	return nil
}
