package workflowengine

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Usage function
func Usage(ctx context.Context, db *pgxpool.Pool, dbname string) {
	// Create a new workflow engine
	engine := NewEngine(ctx, db, dbname)

	// Register blocks
	engine.RegisterBlock("add", AddBlock)
	engine.RegisterBlock("stringTransform", StringTransformBlock)
	engine.RegisterBlock("code", JSBlock)
	// Register a no-op handler for the end block
	engine.RegisterBlock("noop", func(ctx context.Context, dbconfigs, input, schema, output map[string]interface{}) error {
		// Copy input to output
		for k, v := range input {
			output[k] = v
		}
		return nil
	})

	// Define a simple workflow schema
	schemaJSON := []byte(`{
		"name": "SimpleCalculationWorkflow",
		"startBlock": "jsCode",
		"blocks": {
		  "jsCode": {
			"name": "code",
			"handler": "code",
			"blockConfig": {
			  "js_code": "function ProcessTemplate() {\n  let template = inputs[\"template\"];\n  let copy = { ...inputs };\n  delete copy[\"template\"];\n  const result = template.replace(/{{\\s*([\\w.]+)\\s*}}/g, (match, key) => {\n    return key in copy ? copy[key] : match;\n  });\n  return result;\n}\nProcessTemplate();"
			},
			"inputMap": [
			  "name",
			  "age",
			  "gender",
			  "template"
			],
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
		"name":     "NaveenBhargav",
		"age":      25,
		"gender":   "Male",
		"template": "Hello, my name is {{name}}. I am {{age}} years old and identify as {{gender}}.",
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

	newinputs := map[string]interface{}{
		"num1":            10.0,
		"num2":            20.0,
		"text":            "Some sample text",
		"operation":       "uppercase",
		"transformedText": "",
		"result":          0.0,
	}
	// Load complex schema
	complexSchema, err := LoadSchema(complexSchemaJSON)
	if err != nil {
		log.Fatalf("Error loading complex schema: %v", err)
	}

	// Execute complex workflow
	start = time.Now()
	complexResult, err := engine.Execute(complexSchema, newinputs)
	complexElapsed := time.Since(start)

	if err != nil {
		log.Fatalf("Error executing complex workflow: %v", err)
	}

	// Print complex result
	complexResultJSON, _ := json.MarshalIndent(complexResult, "", "  ")
	fmt.Printf("\nComplex workflow result:\n%s\n", string(complexResultJSON))
	fmt.Printf("Complex execution time: %v\n", complexElapsed)
}
