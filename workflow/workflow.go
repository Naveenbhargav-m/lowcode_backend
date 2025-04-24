package workflow

/*
We need to create a workflow engine in golang , that takes list of blocks and edges and data map.
take the start block, take the edge that start from it and start exeucting the next connected blocks.
condition block, insert_record , update_record etc like blocks would be there.

sample data blocks -> [{"id": "1234", "type": "insert_record"}] ,
edges [{"id": "12344", "source": "1234", "target": "next_block_id" }]
data {"id": "1234", "input": {"name": "Some name", "age": 24}, "output": {}},

it should handle the infinite curcular loops with circuit breaking , timeouts etc.
it should pre sort or perform heavy operations before proceeding to workflow execution.

it should be very fast , performant, scalable , easy to extend, use and understand.

you understood it?
*/
// type BlockData struct {
// 	Inputs          map[string]interface{}
// 	PreviousStep    map[string]interface{}
// 	Configs         map[string]interface{}
// 	CurrentBlockID  string
// 	CurrentEdgeID   string
// 	Options         map[string]interface{}
// 	GlobalStateCopy map[string]interface{}
// }

// type OutputData struct {
// 	Output map[string]interface{}
// }

// func StateBlock(input *BlockData) *OutputData {
// 	return &OutputData{}
// }
