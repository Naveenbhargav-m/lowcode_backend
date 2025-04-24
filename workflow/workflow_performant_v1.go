package workflow

import (
	"sync"
	"sync/atomic"
)

// FieldType defines the possible data types for block fields
type FieldType int

const (
	StringField FieldType = iota
	IntField
	FloatField
	BoolField
	ObjectField
)

// BlockSchema defines the structure of inputs and outputs for a block
type BlockSchema struct {
	// Maps field names to array indices for typed access
	StringFieldIndices map[string]int
	IntFieldIndices    map[string]int
	FloatFieldIndices  map[string]int
	BoolFieldIndices   map[string]int
	ObjectFieldIndices map[string]int

	// Total counts for pre-allocating arrays
	StringFieldCount int
	IntFieldCount    int
	FloatFieldCount  int
	BoolFieldCount   int
	ObjectFieldCount int
}

// SchemaRegistry manages all block schemas in the system
type SchemaRegistry struct {
	mu      sync.RWMutex
	Schemas map[string]*BlockSchema
}

// NewSchemaRegistry creates a new schema registry
func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{
		Schemas: make(map[string]*BlockSchema),
	}
}

// RegisterBlockType adds a new block type with its schema to the registry
func (r *SchemaRegistry) RegisterBlockType(blockType string, fields map[string]FieldType) *BlockSchema {
	r.mu.Lock()
	defer r.mu.Unlock()

	schema := &BlockSchema{
		StringFieldIndices: make(map[string]int),
		IntFieldIndices:    make(map[string]int),
		FloatFieldIndices:  make(map[string]int),
		BoolFieldIndices:   make(map[string]int),
		ObjectFieldIndices: make(map[string]int),
	}

	// Assign indices for each field type
	for fieldName, fieldType := range fields {
		switch fieldType {
		case StringField:
			schema.StringFieldIndices[fieldName] = schema.StringFieldCount
			schema.StringFieldCount++
		case IntField:
			schema.IntFieldIndices[fieldName] = schema.IntFieldCount
			schema.IntFieldCount++
		case FloatField:
			schema.FloatFieldIndices[fieldName] = schema.FloatFieldCount
			schema.FloatFieldCount++
		case BoolField:
			schema.BoolFieldIndices[fieldName] = schema.BoolFieldCount
			schema.BoolFieldCount++
		case ObjectField:
			schema.ObjectFieldIndices[fieldName] = schema.ObjectFieldCount
			schema.ObjectFieldCount++
		}
	}

	r.Schemas[blockType] = schema
	return schema
}

// GetSchema returns a schema for a given block type
func (r *SchemaRegistry) GetSchema(blockType string) *BlockSchema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.Schemas[blockType]
}

// GlobalState represents the shared state across the workflow
type GlobalState struct {
	// Use atomic.Value for concurrent access without locking
	StringData atomic.Value // holds map[string]string
	IntData    atomic.Value // holds map[string]int64
	FloatData  atomic.Value // holds map[string]float64
	BoolData   atomic.Value // holds map[string]bool
	ObjectData atomic.Value // holds map[string]any
}

// NewGlobalState creates a new global state container
func NewGlobalState() *GlobalState {
	state := &GlobalState{}
	state.StringData.Store(make(map[string]string))
	state.IntData.Store(make(map[string]int64))
	state.FloatData.Store(make(map[string]float64))
	state.BoolData.Store(make(map[string]bool))
	state.ObjectData.Store(make(map[string]any))
	return state
}

// TypedData contains arrays for strongly-typed access
type TypedData struct {
	BlockType    string
	Schema       *BlockSchema
	StringValues []string
	IntValues    []int64
	FloatValues  []float64
	BoolValues   []bool
	ObjectValues []any
}

// NewTypedData creates a new typed data container based on a schema
func NewTypedData(blockType string, schema *BlockSchema) *TypedData {
	return &TypedData{
		BlockType:    blockType,
		Schema:       schema,
		StringValues: make([]string, schema.StringFieldCount),
		IntValues:    make([]int64, schema.IntFieldCount),
		FloatValues:  make([]float64, schema.FloatFieldCount),
		BoolValues:   make([]bool, schema.BoolFieldCount),
		ObjectValues: make([]any, schema.ObjectFieldCount),
	}
}

// Accessors for TypedData
func (d *TypedData) SetString(fieldName string, value string) bool {
	idx, exists := d.Schema.StringFieldIndices[fieldName]
	if !exists {
		return false
	}
	d.StringValues[idx] = value
	return true
}

func (d *TypedData) GetString(fieldName string) (string, bool) {
	idx, exists := d.Schema.StringFieldIndices[fieldName]
	if !exists {
		return "", false
	}
	return d.StringValues[idx], true
}

func (d *TypedData) SetInt(fieldName string, value int64) bool {
	idx, exists := d.Schema.IntFieldIndices[fieldName]
	if !exists {
		return false
	}
	d.IntValues[idx] = value
	return true
}

func (d *TypedData) GetInt(fieldName string) (int64, bool) {
	idx, exists := d.Schema.IntFieldIndices[fieldName]
	if !exists {
		return 0, false
	}
	return d.IntValues[idx], true
}

// Add similar methods for other types...

// BlockConfig contains block configuration parameters
type BlockConfig struct {
	*TypedData
}

// BlockOptions contains runtime options for block execution
type BlockOptions struct {
	*TypedData
}

// BlockData combines all inputs needed for block execution
type BlockData struct {
	// Typed inputs based on schema
	Inputs *TypedData

	// Previous step output
	PreviousStep *OutputData

	// Block configuration
	Configs *BlockConfig

	// Block runtime options
	Options *BlockOptions

	// Block identification
	CurrentBlockID string
	CurrentEdgeID  string

	// Reference to global state
	GlobalState *GlobalState
}

// OutputData represents block execution results
type OutputData struct {
	*TypedData
}

// Create object pool for performance
var blockDataPool = sync.Pool{
	New: func() interface{} {
		return &BlockData{}
	},
}

// GetBlockData gets a BlockData from the pool
func GetBlockData() *BlockData {
	return blockDataPool.Get().(*BlockData)
}

// ReleaseBlockData returns a BlockData to the pool
func ReleaseBlockData(data *BlockData) {
	// Clear references but keep allocated arrays
	data.Inputs = nil
	data.PreviousStep = nil
	data.Configs = nil
	data.Options = nil
	data.CurrentBlockID = ""
	data.CurrentEdgeID = ""
	data.GlobalState = nil

	blockDataPool.Put(data)
}

// StateBlock is a block function that processes input data
func StateBlock(input *BlockData) *OutputData {
	// Create output using schema from input if available
	var output *OutputData
	if input.Inputs != nil && input.Inputs.Schema != nil {
		typedData := NewTypedData(input.Inputs.BlockType, input.Inputs.Schema)
		output = &OutputData{typedData}
	} else {
		// Fallback if no schema is available
		output = &OutputData{&TypedData{}}
	}

	// Process block logic here
	// For example:
	if input.Inputs != nil {
		if nameVal, ok := input.Inputs.GetString("name"); ok {
			output.SetString("greeting", "Hello, "+nameVal)
		}
	}

	return output
}

// WorkflowEngine manages the execution of blocks
type WorkflowEngine struct {
	Registry    *SchemaRegistry
	GlobalState *GlobalState
	BlockFuncs  map[string]func(*BlockData) *OutputData
}

// NewWorkflowEngine creates a new workflow engine
func NewWorkflowEngine() *WorkflowEngine {
	return &WorkflowEngine{
		Registry:    NewSchemaRegistry(),
		GlobalState: NewGlobalState(),
		BlockFuncs:  make(map[string]func(*BlockData) *OutputData),
	}
}

// RegisterBlock adds a block function to the engine
func (e *WorkflowEngine) RegisterBlock(blockType string, fn func(*BlockData) *OutputData) {
	e.BlockFuncs[blockType] = fn
}

// CreateBlockData prepares a BlockData for execution
func (e *WorkflowEngine) CreateBlockData(
	blockType string,
	inputValues map[string]interface{},
	configValues map[string]interface{},
	optionValues map[string]interface{},
	previousOutput *OutputData,
	blockID string,
	edgeID string,
) *BlockData {
	schema := e.Registry.GetSchema(blockType)
	if schema == nil {
		return nil
	}

	// Create BlockData with all components
	blockData := GetBlockData()
	blockData.Inputs = NewTypedData(blockType, schema)
	blockData.Configs = &BlockConfig{NewTypedData(blockType, schema)}
	blockData.Options = &BlockOptions{NewTypedData(blockType, schema)}
	blockData.PreviousStep = previousOutput
	blockData.CurrentBlockID = blockID
	blockData.CurrentEdgeID = edgeID
	blockData.GlobalState = e.GlobalState

	// Populate inputs (simplified for demonstration)
	for key, value := range inputValues {
		switch v := value.(type) {
		case string:
			blockData.Inputs.SetString(key, v)
		case int64:
			blockData.Inputs.SetInt(key, v)
			// Handle other types...
		}
	}

	// Similar population for configs and options...

	return blockData
}

// ExecuteBlock runs a block function with the given data
func (e *WorkflowEngine) ExecuteBlock(blockType string, data *BlockData) *OutputData {
	blockFunc, exists := e.BlockFuncs[blockType]
	if !exists {
		return nil
	}

	return blockFunc(data)
}

// Example usage
func ExampleUsage() *WorkflowEngine {
	// Setup workflow engine
	engine := NewWorkflowEngine()

	// Register schemas
	engine.Registry.RegisterBlockType("userInfo", map[string]FieldType{
		"name":     StringField,
		"email":    StringField,
		"age":      IntField,
		"isActive": BoolField,
	})

	// Register block functions
	engine.RegisterBlock("userInfo", func(data *BlockData) *OutputData {
		output := &OutputData{NewTypedData(data.Inputs.BlockType, data.Inputs.Schema)}

		name, _ := data.Inputs.GetString("name")
		email, _ := data.Inputs.GetString("email")

		output.SetString("greeting", "Hello, "+name)
		output.SetString("contactInfo", "Email: "+email)

		return output
	})

	return engine
}
