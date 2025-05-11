package workflowengine

func GetCurBlockData(schema map[string]interface{}) *Block {
	curName, _ := schema["current_block_name"].(string)
	myschema, _ := schema["schema"].(*WorkflowSchema)
	if myschema == nil {
		return nil
	}

	myblock := myschema.Blocks[curName]
	return myblock
}
