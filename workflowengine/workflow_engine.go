package workflowengine

// future optimizations:
//https://claude.ai/share/603452a6-a548-4549-8d6a-187ca2af029c

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"lowcode.com/backend/utilities"
)

// BlockFunc defines the signature for a block function
type BlockFunc func(ctx context.Context, dbconfigs, input, schema, output map[string]interface{}) error

// Block represents a processing unit in the workflow
type Block struct {
	Name        string                 `json:"name"`
	Handler     string                 `json:"handler"`
	BlockConfig map[string]interface{} `json:"block_config"`
	InputMap    map[string]interface{} `json:"input_map,omitempty"`  // Optional: maps from global state to block input
	OutputMap   map[string]interface{} `json:"output_map,omitempty"` // Optional: maps from block output to global state
	NextBlocks  []string               `json:"next_block,omitempty"` // Optional: for conditional flows
}

// WorkflowSchema defines the structure of the workflow
type WorkflowSchema struct {
	Name       string            `json:"name"`
	StartBlock string            `json:"start_block"`
	Blocks     map[string]*Block `json:"blocks"`
	EndBlock   string            `json:"end_block"`
}

// Engine is the workflow execution engine
type Engine struct {
	registry   map[string]BlockFunc
	registryMu sync.RWMutex
	// Pre-allocate these maps for reuse
	blockInput  map[string]interface{}
	blockOutput map[string]interface{}
	schemaData  map[string]interface{}
	ctx         context.Context
	db          *pgxpool.Pool
	dbname      string
}

// NewEngine creates a new workflow engine
func NewEngine(ctx context.Context, db *pgxpool.Pool, dbname string) *Engine {
	return &Engine{
		registry:    make(map[string]BlockFunc),
		blockInput:  make(map[string]interface{}),
		blockOutput: make(map[string]interface{}),
		schemaData:  make(map[string]interface{}),
		ctx:         ctx,
		db:          db,
		dbname:      dbname,
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

	configs := map[string]interface{}{
		"db":      e.db,
		"db_name": e.dbname,
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
		clearMap(e.schemaData)

		// Create input for the block from global state using inputMap
		if len(currentBlock.InputMap) > 0 {
			for key, value := range currentBlock.InputMap {
				e.blockInput[key] = value
			}
		} else {
			// If no input map is specified, pass the entire global state
			for k, v := range globalState {
				e.blockInput[k] = v
			}
		}

		// Execute the block
		e.schemaData = map[string]interface{}{
			"schema":             schema,
			"current_block_name": currentBlockName,
			"current_block":      currentBlock,
		}
		if err := handler(e.ctx, configs, e.blockInput, e.schemaData, globalState); err != nil {
			return nil, fmt.Errorf("error executing block %s: %w", currentBlock.Name, err)
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
func LoadSchema(schemaJSON map[string]interface{}) (*WorkflowSchema, error) {
	var schema WorkflowSchema
	err := utilities.ConvertToType(schemaJSON, &schema)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling schema: %w", err)
	}
	return &schema, nil
}
