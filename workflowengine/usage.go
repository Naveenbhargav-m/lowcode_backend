package workflowengine

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// Usage function
func Usage() {
	// Create a new workflow engine
	engine := NewEngine()

	// Register blocks
	engine.RegisterBlock("add", AddBlock)
	engine.RegisterBlock("stringTransform", StringTransformBlock)

	// Register a no-op handler for the end block
	engine.RegisterBlock("noop", func(input, output map[string]interface{}) error {
		// Copy input to output
		for k, v := range input {
			output[k] = v
		}
		return nil
	})

	// Define a simple workflow schema
	schemaJSON := []byte(`{
		"name": "SimpleCalculationWorkflow",
		"startBlock": "addBlock",
		"blocks": {
			"addBlock": {
				"name": "addBlock",
				"handler": "add",
				"nextBlocks": ["transformBlock"]
			},
			"transformBlock": {
				"name": "transformBlock",
				"handler": "stringTransform",
				"nextBlocks": ["endBlock"] 
			},
			"endBlock": {
				"name": "endBlock",
				"handler": "noop"
			}
		},
		"endBlock": "endBlock"
	}`)

	// Load schema
	schema, err := LoadSchema(schemaJSON)
	if err != nil {
		log.Fatalf("Error loading schema: %v", err)
	}

	// Prepare input data
	input := map[string]interface{}{
		"num1":      5.0,
		"num2":      10.0,
		"text":      "hello workflow",
		"operation": "uppercase",
	}

	// Execute workflow
	start := time.Now()
	result, err := engine.Execute(schema, input)
	elapsed := time.Since(start)

	if err != nil {
		log.Fatalf("Error executing workflow: %v", err)
	}

	// Print result
	resultJSON, _ := json.MarshalIndent(result, "", "  ")
	fmt.Printf("Workflow result:\n%s\n", string(resultJSON))
	fmt.Printf("Execution time: %v\n", elapsed)

	// Example with a more complex workflow with input/output mapping
	complexSchemaJSON := []byte(`{
		"name": "ComplexWorkflow",
		"startBlock": "addBlock",
		"blocks": {
			"addBlock": {
				"name": "addBlock",
				"handler": "add",
				"inputMap": ["num1", "num2"],
				"outputMap": ["result"],
				"nextBlocks": ["transformBlock"]
			},
			"transformBlock": {
				"name": "transformBlock",
				"handler": "stringTransform",
				"inputMap": ["text", "operation"],
				"outputMap": ["transformedText"],
				"nextBlocks": ["endBlock"]
			},
			"endBlock": {
				"name": "endBlock",
				"handler": "noop"
			}
		},
		"endBlock": "endBlock"
	}`)

	// Load complex schema
	complexSchema, err := LoadSchema(complexSchemaJSON)
	if err != nil {
		log.Fatalf("Error loading complex schema: %v", err)
	}

	// Execute complex workflow
	start = time.Now()
	complexResult, err := engine.Execute(complexSchema, input)
	complexElapsed := time.Since(start)

	if err != nil {
		log.Fatalf("Error executing complex workflow: %v", err)
	}

	// Print complex result
	complexResultJSON, _ := json.MarshalIndent(complexResult, "", "  ")
	fmt.Printf("\nComplex workflow result:\n%s\n", string(complexResultJSON))
	fmt.Printf("Complex execution time: %v\n", complexElapsed)
}
