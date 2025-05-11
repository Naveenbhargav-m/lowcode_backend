package workflowengine

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	appcrud "lowcode.com/backend/app_crud"
)

func InsertRows(ctx context.Context, configs map[string]interface{},
	input, schema, output map[string]interface{}) error {
	myblock := GetCurBlockData(schema)
	curName, _ := schema["current_block_name"].(string)
	blockConfig := myblock.BlockConfig
	table, _ := blockConfig["table"].(string)
	table_schema, _ := blockConfig["schema"].(string)
	rowsID, _ := blockConfig["rows"].(string)
	rowsData := input[rowsID]
	db, _ := configs["db"].(*pgxpool.Pool)
	// dbname, _ := configs["db_name"].(string)
	resp, err := appcrud.InsertRecords(ctx, db, table_schema, table, rowsData)
	if err != nil {
		return err
	}
	output[curName] = resp
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
