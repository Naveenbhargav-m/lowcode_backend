package workflowengine

// future optimizations:
//https://claude.ai/share/603452a6-a548-4549-8d6a-187ca2af029c

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// BlockFunc defines the signature for a block function
type BlockFunc func(input, output map[string]interface{}) error

// Block represents a processing unit in the workflow
type Block struct {
	Name       string   `json:"name"`
	Handler    string   `json:"handler"`
	InputMap   []string `json:"inputMap,omitempty"`   // Optional: maps from global state to block input
	OutputMap  []string `json:"outputMap,omitempty"`  // Optional: maps from block output to global state
	NextBlocks []string `json:"nextBlocks,omitempty"` // Optional: for conditional flows
}

// WorkflowSchema defines the structure of the workflow
type WorkflowSchema struct {
	Name       string            `json:"name"`
	StartBlock string            `json:"startBlock"`
	Blocks     map[string]*Block `json:"blocks"`
	EndBlock   string            `json:"endBlock"`
}

// Engine is the workflow execution engine
type Engine struct {
	registry   map[string]BlockFunc
	registryMu sync.RWMutex
	// Pre-allocate these maps for reuse
	blockInput  map[string]interface{}
	blockOutput map[string]interface{}
}

// NewEngine creates a new workflow engine
func NewEngine() *Engine {
	return &Engine{
		registry:    make(map[string]BlockFunc),
		blockInput:  make(map[string]interface{}),
		blockOutput: make(map[string]interface{}),
	}
}

// RegisterBlock registers a block handler with the engine
func (e *Engine) RegisterBlock(name string, handler BlockFunc) {
	e.registryMu.Lock()
	e.registry[name] = handler
	e.registryMu.Unlock()
}

// getHandler retrieves a handler safely
func (e *Engine) getHandler(name string) (BlockFunc, error) {
	e.registryMu.RLock()
	handler, exists := e.registry[name]
	e.registryMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("handler %s not registered", name)
	}
	return handler, nil
}

// Execute runs a workflow with the given schema and input
func (e *Engine) Execute(schema *WorkflowSchema, input map[string]interface{}) (map[string]interface{}, error) {
	if schema == nil {
		return nil, errors.New("workflow schema is nil")
	}

	// Initialize global state with input
	globalState := make(map[string]interface{}, len(input))
	for k, v := range input {
		globalState[k] = v
	}

	// Get the start block
	currentBlockName := schema.StartBlock
	if _, exists := schema.Blocks[currentBlockName]; !exists {
		return nil, fmt.Errorf("start block %s not found", schema.StartBlock)
	}

	// Process blocks until we reach the end block
	for currentBlockName != schema.EndBlock {
		currentBlock := schema.Blocks[currentBlockName]
		if currentBlock == nil {
			return nil, fmt.Errorf("block %s not found", currentBlockName)
		}

		// Get the handler for the current block
		handler, err := e.getHandler(currentBlock.Handler)
		if err != nil {
			return nil, err
		}

		// Clear maps for reuse instead of allocating new ones
		clearMap(e.blockInput)
		clearMap(e.blockOutput)

		// Create input for the block from global state using inputMap
		if len(currentBlock.InputMap) > 0 {
			for _, key := range currentBlock.InputMap {
				if val, ok := globalState[key]; ok {
					e.blockInput[key] = val
				}
			}
		} else {
			// If no input map is specified, pass the entire global state
			for k, v := range globalState {
				e.blockInput[k] = v
			}
		}

		// Execute the block
		if err := handler(e.blockInput, e.blockOutput); err != nil {
			return nil, fmt.Errorf("error executing block %s: %w", currentBlock.Name, err)
		}

		// Update global state with block output using outputMap
		if len(currentBlock.OutputMap) > 0 {
			for _, key := range currentBlock.OutputMap {
				if val, ok := e.blockOutput[key]; ok {
					globalState[key] = val
				}
			}
		} else {
			// If no output map is specified, update the entire global state
			for k, v := range e.blockOutput {
				globalState[k] = v
			}
		}

		// Determine the next block
		if len(currentBlock.NextBlocks) > 0 {
			currentBlockName = currentBlock.NextBlocks[0] // For now, just take the first next block
		} else {
			// If no next blocks specified and we haven't reached the end, this is an error
			return nil, fmt.Errorf("block %s does not specify next blocks and is not the end block", currentBlockName)
		}
	}

	return globalState, nil
}

// Helper function to clear a map without reallocating
func clearMap(m map[string]interface{}) {
	for k := range m {
		delete(m, k)
	}
}

// LoadSchema loads a workflow schema from JSON
func LoadSchema(schemaJSON []byte) (*WorkflowSchema, error) {
	var schema WorkflowSchema
	d := json.NewDecoder(bytes.NewReader(schemaJSON))
	d.UseNumber() // Use json.Number instead of float64 for numbers
	err := d.Decode(&schema)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling schema: %w", err)
	}
	return &schema, nil
}
