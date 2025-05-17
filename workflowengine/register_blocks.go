package workflowengine

func RegisterBlocks(e *Engine) {
	e.RegisterBlock("insert_rows", InsertRows)
	e.RegisterBlock("update_rows", UpdateRows)
	e.RegisterBlock("read_rows", ReadRows)
	e.RegisterBlock("delete_rows", DeletRows)
	e.RegisterBlock("code_block", JSBlock)
}
