package workflow

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// Common errors
var (
	ErrSchemaNotFound    = errors.New("schema not found")
	ErrFieldNotFound     = errors.New("field not found")
	ErrBlockNotFound     = errors.New("block not found")
	ErrInvalidFieldType  = errors.New("invalid field type")
	ErrStepNotFound      = errors.New("workflow step not found")
	ErrWorkflowExecution = errors.New("workflow execution error")
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

// NewBlockSchema creates a new block schema with initialized maps
func NewBlockSchema() *BlockSchema {
	return &BlockSchema{
		StringFieldIndices: make(map[string]int),
		IntFieldIndices:    make(map[string]int),
		FloatFieldIndices:  make(map[string]int),
		BoolFieldIndices:   make(map[string]int),
		ObjectFieldIndices: make(map[string]int),
	}
}

// SchemaRegistry manages all block schemas in the system
type SchemaRegistry struct {
	mu      sync.RWMutex
	Schemas map[string]*BlockSchema

	// Global state schema
	GlobalSchema *BlockSchema
	// User fields schema
	UserFieldsSchema *BlockSchema
}

// NewSchemaRegistry creates a new schema registry
func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{
		Schemas:          make(map[string]*BlockSchema),
		GlobalSchema:     NewBlockSchema(),
		UserFieldsSchema: NewBlockSchema(),
	}
}

// RegisterBlockType adds a new block type with its schema to the registry
func (r *SchemaRegistry) RegisterBlockType(blockType string, fields map[string]FieldType) (*BlockSchema, error) {
	if blockType == "" {
		return nil, errors.New("blockType cannot be empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	schema := NewBlockSchema()

	// Assign indices for each field type
	for fieldName, fieldType := range fields {
		if fieldName == "" {
			return nil, errors.New("field name cannot be empty")
		}

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
		default:
			return nil, fmt.Errorf("%w: %v", ErrInvalidFieldType, fieldType)
		}
	}

	r.Schemas[blockType] = schema
	return schema, nil
}

// RegisterGlobalField adds a field to the global state schema
func (r *SchemaRegistry) RegisterGlobalField(fieldName string, fieldType FieldType) error {
	if fieldName == "" {
		return errors.New("field name cannot be empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	schema := r.GlobalSchema
	switch fieldType {
	case StringField:
		if _, exists := schema.StringFieldIndices[fieldName]; !exists {
			schema.StringFieldIndices[fieldName] = schema.StringFieldCount
			schema.StringFieldCount++
		}
	case IntField:
		if _, exists := schema.IntFieldIndices[fieldName]; !exists {
			schema.IntFieldIndices[fieldName] = schema.IntFieldCount
			schema.IntFieldCount++
		}
	case FloatField:
		if _, exists := schema.FloatFieldIndices[fieldName]; !exists {
			schema.FloatFieldIndices[fieldName] = schema.FloatFieldCount
			schema.FloatFieldCount++
		}
	case BoolField:
		if _, exists := schema.BoolFieldIndices[fieldName]; !exists {
			schema.BoolFieldIndices[fieldName] = schema.BoolFieldCount
			schema.BoolFieldCount++
		}
	case ObjectField:
		if _, exists := schema.ObjectFieldIndices[fieldName]; !exists {
			schema.ObjectFieldIndices[fieldName] = schema.ObjectFieldCount
			schema.ObjectFieldCount++
		}
	default:
		return fmt.Errorf("%w: %v", ErrInvalidFieldType, fieldType)
	}
	return nil
}

// RegisterUserField adds a field to the user fields schema
func (r *SchemaRegistry) RegisterUserField(fieldName string, fieldType FieldType) error {
	if fieldName == "" {
		return errors.New("field name cannot be empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	schema := r.UserFieldsSchema
	switch fieldType {
	case StringField:
		if _, exists := schema.StringFieldIndices[fieldName]; !exists {
			schema.StringFieldIndices[fieldName] = schema.StringFieldCount
			schema.StringFieldCount++
		}
	case IntField:
		if _, exists := schema.IntFieldIndices[fieldName]; !exists {
			schema.IntFieldIndices[fieldName] = schema.IntFieldCount
			schema.IntFieldCount++
		}
	case FloatField:
		if _, exists := schema.FloatFieldIndices[fieldName]; !exists {
			schema.FloatFieldIndices[fieldName] = schema.FloatFieldCount
			schema.FloatFieldCount++
		}
	case BoolField:
		if _, exists := schema.BoolFieldIndices[fieldName]; !exists {
			schema.BoolFieldIndices[fieldName] = schema.BoolFieldCount
			schema.BoolFieldCount++
		}
	case ObjectField:
		if _, exists := schema.ObjectFieldIndices[fieldName]; !exists {
			schema.ObjectFieldIndices[fieldName] = schema.ObjectFieldCount
			schema.ObjectFieldCount++
		}
	default:
		return fmt.Errorf("%w: %v", ErrInvalidFieldType, fieldType)
	}
	return nil
}

// GetSchema returns a schema for a given block type
func (r *SchemaRegistry) GetSchema(blockType string) (*BlockSchema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	schema, exists := r.Schemas[blockType]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrSchemaNotFound, blockType)
	}
	return schema, nil
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
	if schema == nil {
		// Create an empty schema to prevent nil pointer dereference
		schema = NewBlockSchema()
	}

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
	if d == nil || d.Schema == nil {
		return false
	}

	idx, exists := d.Schema.StringFieldIndices[fieldName]
	if !exists {
		return false
	}

	if idx < 0 || idx >= len(d.StringValues) {
		log.Printf("Warning: String index out of bounds for field %s: index=%d, len=%d",
			fieldName, idx, len(d.StringValues))
		return false
	}

	d.StringValues[idx] = value
	return true
}

func (d *TypedData) GetString(fieldName string) (string, bool) {
	if d == nil || d.Schema == nil {
		return "", false
	}

	idx, exists := d.Schema.StringFieldIndices[fieldName]
	if !exists {
		return "", false
	}

	if idx < 0 || idx >= len(d.StringValues) {
		log.Printf("Warning: String index out of bounds for field %s: index=%d, len=%d",
			fieldName, idx, len(d.StringValues))
		return "", false
	}

	return d.StringValues[idx], true
}

func (d *TypedData) SetInt(fieldName string, value int64) bool {
	if d == nil || d.Schema == nil {
		return false
	}

	idx, exists := d.Schema.IntFieldIndices[fieldName]
	if !exists {
		return false
	}

	if idx < 0 || idx >= len(d.IntValues) {
		log.Printf("Warning: Int index out of bounds for field %s: index=%d, len=%d",
			fieldName, idx, len(d.IntValues))
		return false
	}

	d.IntValues[idx] = value
	return true
}

func (d *TypedData) GetInt(fieldName string) (int64, bool) {
	if d == nil || d.Schema == nil {
		return 0, false
	}

	idx, exists := d.Schema.IntFieldIndices[fieldName]
	if !exists {
		return 0, false
	}

	if idx < 0 || idx >= len(d.IntValues) {
		log.Printf("Warning: Int index out of bounds for field %s: index=%d, len=%d",
			fieldName, idx, len(d.IntValues))
		return 0, false
	}

	return d.IntValues[idx], true
}

func (d *TypedData) SetFloat(fieldName string, value float64) bool {
	if d == nil || d.Schema == nil {
		return false
	}

	idx, exists := d.Schema.FloatFieldIndices[fieldName]
	if !exists {
		return false
	}

	if idx < 0 || idx >= len(d.FloatValues) {
		log.Printf("Warning: Float index out of bounds for field %s: index=%d, len=%d",
			fieldName, idx, len(d.FloatValues))
		return false
	}

	d.FloatValues[idx] = value
	return true
}

func (d *TypedData) GetFloat(fieldName string) (float64, bool) {
	if d == nil || d.Schema == nil {
		return 0, false
	}

	idx, exists := d.Schema.FloatFieldIndices[fieldName]
	if !exists {
		return 0, false
	}

	if idx < 0 || idx >= len(d.FloatValues) {
		log.Printf("Warning: Float index out of bounds for field %s: index=%d, len=%d",
			fieldName, idx, len(d.FloatValues))
		return 0, false
	}

	return d.FloatValues[idx], true
}

func (d *TypedData) SetBool(fieldName string, value bool) bool {
	if d == nil || d.Schema == nil {
		return false
	}

	idx, exists := d.Schema.BoolFieldIndices[fieldName]
	if !exists {
		return false
	}

	if idx < 0 || idx >= len(d.BoolValues) {
		log.Printf("Warning: Bool index out of bounds for field %s: index=%d, len=%d",
			fieldName, idx, len(d.BoolValues))
		return false
	}

	d.BoolValues[idx] = value
	return true
}

func (d *TypedData) GetBool(fieldName string) (bool, bool) {
	if d == nil || d.Schema == nil {
		return false, false
	}

	idx, exists := d.Schema.BoolFieldIndices[fieldName]
	if !exists {
		return false, false
	}

	if idx < 0 || idx >= len(d.BoolValues) {
		log.Printf("Warning: Bool index out of bounds for field %s: index=%d, len=%d",
			fieldName, idx, len(d.BoolValues))
		return false, false
	}

	return d.BoolValues[idx], true
}

func (d *TypedData) SetObject(fieldName string, value any) bool {
	if d == nil || d.Schema == nil {
		return false
	}

	idx, exists := d.Schema.ObjectFieldIndices[fieldName]
	if !exists {
		return false
	}

	if idx < 0 || idx >= len(d.ObjectValues) {
		log.Printf("Warning: Object index out of bounds for field %s: index=%d, len=%d",
			fieldName, idx, len(d.ObjectValues))
		return false
	}

	d.ObjectValues[idx] = value
	return true
}

func (d *TypedData) GetObject(fieldName string) (any, bool) {
	if d == nil || d.Schema == nil {
		return nil, false
	}

	idx, exists := d.Schema.ObjectFieldIndices[fieldName]
	if !exists {
		return nil, false
	}

	if idx < 0 || idx >= len(d.ObjectValues) {
		log.Printf("Warning: Object index out of bounds for field %s: index=%d, len=%d",
			fieldName, idx, len(d.ObjectValues))
		return nil, false
	}

	return d.ObjectValues[idx], true
}

// Clone creates a deep copy of the TypedData
func (d *TypedData) Clone() *TypedData {
	if d == nil {
		return nil
	}

	clone := NewTypedData(d.BlockType, d.Schema)

	// Copy each array
	copy(clone.StringValues, d.StringValues)
	copy(clone.IntValues, d.IntValues)
	copy(clone.FloatValues, d.FloatValues)
	copy(clone.BoolValues, d.BoolValues)

	// Deep copy of objects might be needed depending on usage
	copy(clone.ObjectValues, d.ObjectValues)

	return clone
}

// WorkflowConfig contains workflow configuration parameters
type WorkflowConfig struct {
	*TypedData
	// Additional workflow-specific configuration
	DefaultNextStep map[string]string // Maps from block ID to next step ID
}

// NewWorkflowConfig creates a new workflow configuration
func NewWorkflowConfig(schema *BlockSchema) *WorkflowConfig {
	return &WorkflowConfig{
		TypedData:       NewTypedData("workflowConfig", schema),
		DefaultNextStep: make(map[string]string),
	}
}

// GlobalData represents the shared state across the workflow
type GlobalData struct {
	mu        sync.RWMutex
	TypedData *TypedData
}

// NewGlobalData creates a new global data container
func NewGlobalData(schema *BlockSchema) *GlobalData {
	return &GlobalData{
		TypedData: NewTypedData("global", schema),
	}
}

// GetString retrieves a string value from global data
func (g *GlobalData) GetString(fieldName string) (string, bool) {
	if g == nil || g.TypedData == nil {
		return "", false
	}

	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.TypedData.GetString(fieldName)
}

// SetString sets a string value in global data
func (g *GlobalData) SetString(fieldName string, value string) bool {
	if g == nil || g.TypedData == nil {
		return false
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	return g.TypedData.SetString(fieldName, value)
}

// GetInt retrieves an int value from global data
func (g *GlobalData) GetInt(fieldName string) (int64, bool) {
	if g == nil || g.TypedData == nil {
		return 0, false
	}

	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.TypedData.GetInt(fieldName)
}

// SetInt sets an int value in global data
func (g *GlobalData) SetInt(fieldName string, value int64) bool {
	if g == nil || g.TypedData == nil {
		return false
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	return g.TypedData.SetInt(fieldName, value)
}

// GetFloat retrieves a float value from global data
func (g *GlobalData) GetFloat(fieldName string) (float64, bool) {
	if g == nil || g.TypedData == nil {
		return 0, false
	}

	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.TypedData.GetFloat(fieldName)
}

// SetFloat sets a float value in global data
func (g *GlobalData) SetFloat(fieldName string, value float64) bool {
	if g == nil || g.TypedData == nil {
		return false
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	return g.TypedData.SetFloat(fieldName, value)
}

// GetBool retrieves a bool value from global data
func (g *GlobalData) GetBool(fieldName string) (bool, bool) {
	if g == nil || g.TypedData == nil {
		return false, false
	}

	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.TypedData.GetBool(fieldName)
}

// SetBool sets a bool value in global data
func (g *GlobalData) SetBool(fieldName string, value bool) bool {
	if g == nil || g.TypedData == nil {
		return false
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	return g.TypedData.SetBool(fieldName, value)
}

// GetObject retrieves an object value from global data
func (g *GlobalData) GetObject(fieldName string) (any, bool) {
	if g == nil || g.TypedData == nil {
		return nil, false
	}

	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.TypedData.GetObject(fieldName)
}

// SetObject sets an object value in global data
func (g *GlobalData) SetObject(fieldName string, value any) bool {
	if g == nil || g.TypedData == nil {
		return false
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	return g.TypedData.SetObject(fieldName, value)
}

// UserFields contains user-specific fields
type UserFields struct {
	mu        sync.RWMutex
	TypedData *TypedData
}

// NewUserFields creates a new user fields container
func NewUserFields(schema *BlockSchema) *UserFields {
	return &UserFields{
		TypedData: NewTypedData("userFields", schema),
	}
}

// GetString retrieves a string value from user fields
func (u *UserFields) GetString(fieldName string) (string, bool) {
	if u == nil || u.TypedData == nil {
		return "", false
	}

	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.TypedData.GetString(fieldName)
}

// SetString sets a string value in user fields
func (u *UserFields) SetString(fieldName string, value string) bool {
	if u == nil || u.TypedData == nil {
		return false
	}

	u.mu.Lock()
	defer u.mu.Unlock()
	return u.TypedData.SetString(fieldName, value)
}

// GetInt retrieves an int value from user fields
func (u *UserFields) GetInt(fieldName string) (int64, bool) {
	if u == nil || u.TypedData == nil {
		return 0, false
	}

	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.TypedData.GetInt(fieldName)
}

// SetInt sets an int value in user fields
func (u *UserFields) SetInt(fieldName string, value int64) bool {
	if u == nil || u.TypedData == nil {
		return false
	}

	u.mu.Lock()
	defer u.mu.Unlock()
	return u.TypedData.SetInt(fieldName, value)
}

// GetFloat retrieves a float value from user fields
func (u *UserFields) GetFloat(fieldName string) (float64, bool) {
	if u == nil || u.TypedData == nil {
		return 0, false
	}

	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.TypedData.GetFloat(fieldName)
}

// SetFloat sets a float value in user fields
func (u *UserFields) SetFloat(fieldName string, value float64) bool {
	if u == nil || u.TypedData == nil {
		return false
	}

	u.mu.Lock()
	defer u.mu.Unlock()
	return u.TypedData.SetFloat(fieldName, value)
}

// GetBool retrieves a bool value from user fields
func (u *UserFields) GetBool(fieldName string) (bool, bool) {
	if u == nil || u.TypedData == nil {
		return false, false
	}

	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.TypedData.GetBool(fieldName)
}

// SetBool sets a bool value in user fields
func (u *UserFields) SetBool(fieldName string, value bool) bool {
	if u == nil || u.TypedData == nil {
		return false
	}

	u.mu.Lock()
	defer u.mu.Unlock()
	return u.TypedData.SetBool(fieldName, value)
}

// GetObject retrieves an object value from user fields
func (u *UserFields) GetObject(fieldName string) (any, bool) {
	if u == nil || u.TypedData == nil {
		return nil, false
	}

	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.TypedData.GetObject(fieldName)
}

// SetObject sets an object value in user fields
func (u *UserFields) SetObject(fieldName string, value any) bool {
	if u == nil || u.TypedData == nil {
		return false
	}

	u.mu.Lock()
	defer u.mu.Unlock()
	return u.TypedData.SetObject(fieldName, value)
}

// BlockConfig contains block configuration parameters
type BlockConfig struct {
	*TypedData
}

// BlockOptions contains runtime options for block execution
type BlockOptions struct {
	*TypedData
}

// BlockContext contains all data needed for block execution
type BlockContext struct {
	// Typed inputs based on schema
	Inputs *TypedData

	// Previous step output
	PreviousOutput *OutputData

	// Block configuration
	Config *BlockConfig

	// Block runtime options
	Options *BlockOptions

	// Block identification
	BlockID string
	EdgeID  string

	// Reference to global data and user fields
	GlobalData *GlobalData
	UserFields *UserFields

	// Workflow configuration - allows blocks to change workflow path
	WorkflowConfig *WorkflowConfig
}

// OutputData represents block execution results
type OutputData struct {
	*TypedData
	Error error // Captures any errors during block execution
}

// BlockFunc defines the signature for block execution functions
type BlockFunc func(*BlockContext) *OutputData

// Create object pool for performance
var blockContextPool = sync.Pool{
	New: func() interface{} {
		return &BlockContext{}
	},
}

// GetBlockContext gets a BlockContext from the pool
func GetBlockContext() *BlockContext {
	return blockContextPool.Get().(*BlockContext)
}

// ReleaseBlockContext returns a BlockContext to the pool
func ReleaseBlockContext(ctx *BlockContext) {
	// Clear references but keep allocated arrays
	ctx.Inputs = nil
	ctx.PreviousOutput = nil
	ctx.Config = nil
	ctx.Options = nil
	ctx.BlockID = ""
	ctx.EdgeID = ""
	ctx.GlobalData = nil
	ctx.UserFields = nil
	ctx.WorkflowConfig = nil

	blockContextPool.Put(ctx)
}

// WorkflowEngine manages the execution of blocks
type WorkflowEngine struct {
	Registry    *SchemaRegistry
	BlockFuncs  map[string]BlockFunc
	ErrorLogger func(error)
}

// NewWorkflowEngine creates a new workflow engine
func NewWorkflowEngine() *WorkflowEngine {
	return &WorkflowEngine{
		Registry:   NewSchemaRegistry(),
		BlockFuncs: make(map[string]BlockFunc),
		ErrorLogger: func(err error) {
			if err != nil {
				log.Println("Workflow error:", err)
			}
		},
	}
}

// RegisterBlock adds a block function to the engine
func (e *WorkflowEngine) RegisterBlock(blockType string, fn BlockFunc) error {
	if blockType == "" {
		return errors.New("blockType cannot be empty")
	}

	if fn == nil {
		return errors.New("block function cannot be nil")
	}

	e.BlockFuncs[blockType] = fn
	return nil
}

// CreateBlockContext prepares a BlockContext for execution
func (e *WorkflowEngine) CreateBlockContext(
	blockType string,
	inputValues map[string]interface{},
	configValues map[string]interface{},
	optionValues map[string]interface{},
	previousOutput *OutputData,
	blockID string,
	edgeID string,
	globalData *GlobalData,
	userFields *UserFields,
	workflowConfig *WorkflowConfig,
) (*BlockContext, error) {
	schema, err := e.Registry.GetSchema(blockType)
	if err != nil {
		return nil, err
	}

	// Create BlockContext with all components
	blockContext := GetBlockContext()
	blockContext.Inputs = NewTypedData(blockType, schema)
	blockContext.Config = &BlockConfig{NewTypedData(blockType, schema)}
	blockContext.Options = &BlockOptions{NewTypedData(blockType, schema)}
	blockContext.PreviousOutput = previousOutput
	blockContext.BlockID = blockID
	blockContext.EdgeID = edgeID
	blockContext.GlobalData = globalData
	blockContext.UserFields = userFields
	blockContext.WorkflowConfig = workflowConfig

	// Populate inputs
	for key, value := range inputValues {
		if key == "" {
			continue
		}

		switch v := value.(type) {
		case string:
			blockContext.Inputs.SetString(key, v)
		case int:
			blockContext.Inputs.SetInt(key, int64(v))
		case int64:
			blockContext.Inputs.SetInt(key, v)
		case float64:
			blockContext.Inputs.SetFloat(key, v)
		case bool:
			blockContext.Inputs.SetBool(key, v)
		default:
			blockContext.Inputs.SetObject(key, v)
		}
	}

	// Populate configs
	for key, value := range configValues {
		if key == "" {
			continue
		}

		switch v := value.(type) {
		case string:
			blockContext.Config.SetString(key, v)
		case int:
			blockContext.Config.SetInt(key, int64(v))
		case int64:
			blockContext.Config.SetInt(key, v)
		case float64:
			blockContext.Config.SetFloat(key, v)
		case bool:
			blockContext.Config.SetBool(key, v)
		default:
			blockContext.Config.SetObject(key, v)
		}
	}

	// Populate options
	for key, value := range optionValues {
		if key == "" {
			continue
		}

		switch v := value.(type) {
		case string:
			blockContext.Options.SetString(key, v)
		case int:
			blockContext.Options.SetInt(key, int64(v))
		case int64:
			blockContext.Options.SetInt(key, v)
		case float64:
			blockContext.Options.SetFloat(key, v)
		case bool:
			blockContext.Options.SetBool(key, v)
		default:
			blockContext.Options.SetObject(key, v)
		}
	}

	return blockContext, nil
}

// ExecuteBlock runs a block function with the given context
func (e *WorkflowEngine) ExecuteBlock(blockType string, context *BlockContext) *OutputData {
	blockFunc, exists := e.BlockFuncs[blockType]
	if !exists {
		err := fmt.Errorf("%w: %s", ErrBlockNotFound, blockType)
		e.ErrorLogger(err)
		return &OutputData{
			TypedData: NewTypedData("error", nil),
			Error:     err,
		}
	}

	// Execute the block and catch any panics
	var output *OutputData
	func() {
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("panic in block %s: %v", blockType, r)
				e.ErrorLogger(err)
				output = &OutputData{
					TypedData: NewTypedData("error", nil),
					Error:     err,
				}
			}
		}()
		output = blockFunc(context)
	}()

	// Ensure we always return a valid output even if the block function returns nil
	if output == nil {
		output = &OutputData{
			TypedData: NewTypedData(blockType, nil),
			Error:     fmt.Errorf("block %s returned nil output", blockType),
		}
	}

	return output
}

// WorkflowStep defines a single step in a workflow
type WorkflowStep struct {
	BlockType     string
	BlockID       string
	EdgeID        string
	InputValues   map[string]interface{}
	ConfigValues  map[string]interface{}
	OptionValues  map[string]interface{}
	NextStepID    string            // Default next step
	BranchStepIDs map[string]string // Conditional branching based on output values
}

// Workflow defines a complete workflow configuration
type Workflow struct {
	ID           string
	Name         string
	Description  string
	StartStepID  string
	Steps        map[string]*WorkflowStep
	ConfigSchema *BlockSchema           // Schema for workflow configuration
	ConfigValues map[string]interface{} // Configuration values
}

// WorkflowContext contains all data for workflow execution
type WorkflowContext struct {
	GlobalData     *GlobalData
	UserFields     *UserFields
	WorkflowConfig *WorkflowConfig
	ExecutionState map[string]interface{} // Additional state for workflow execution
}

// NewWorkflowContext creates a new workflow context
func NewWorkflowContext(registry *SchemaRegistry) *WorkflowContext {
	configSchema := NewBlockSchema()
	return &WorkflowContext{
		GlobalData:     NewGlobalData(registry.GlobalSchema),
		UserFields:     NewUserFields(registry.UserFieldsSchema),
		WorkflowConfig: NewWorkflowConfig(configSchema),
		ExecutionState: make(map[string]interface{}),
	}
}

// WorkflowResult contains the results of a workflow execution
type WorkflowResult struct {
	StepResults   map[string]*OutputData
	FinalOutput   *OutputData
	Error         error
	ExecutionPath []string // List of executed steps in order
	ExecutionTime time.Duration
}

// ExecuteWorkflow runs a complete workflow with the given context
func (e *WorkflowEngine) ExecuteWorkflow(workflow *Workflow, context *WorkflowContext) *WorkflowResult {
	if workflow == nil {
		return &WorkflowResult{
			Error: errors.New("workflow cannot be nil"),
		}
	}

	// Initialize workflow result
	result := &WorkflowResult{
		StepResults:   make(map[string]*OutputData),
		ExecutionPath: make([]string, 0),
	}

	startTime := time.Now()

	// Apply workflow configuration
	configSchema := workflow.ConfigSchema
	if configSchema == nil {
		// Use default schema if not provided
		configSchema = NewBlockSchema()
	}

	workflowConfig := NewWorkflowConfig(configSchema)

	// Add default next steps from workflow definition
	for stepID, step := range workflow.Steps {
		if step.NextStepID != "" {
			workflowConfig.DefaultNextStep[stepID] = step.NextStepID
		}
	}

	// Apply config values
	for key, value := range workflow.ConfigValues {
		switch v := value.(type) {
		case string:
			workflowConfig.SetString(key, v)
		case int:
			workflowConfig.SetInt(key, int64(v))
		case int64:
			workflowConfig.SetInt(key, v)
		case float64:
			workflowConfig.SetFloat(key, v)
		case bool:
			workflowConfig.SetBool(key, v)
		default:
			workflowConfig.SetObject(key, v)
		}
	}

	// Use the provided context if available, otherwise create new one
	if context == nil {
		context = NewWorkflowContext(e.Registry)
	}

	// Set the workflow config in the context
	context.WorkflowConfig = workflowConfig

	currentStepID := workflow.StartStepID
	var previousOutput *OutputData

	// Execute steps until we reach the end or an error occurs
	for currentStepID != "" {
		step, exists := workflow.Steps[currentStepID]
		if !exists {
			err := fmt.Errorf("%w: %s", ErrStepNotFound, currentStepID)
			e.ErrorLogger(err)
			result.Error = err
			break
		}

		// Add step to execution path
		result.ExecutionPath = append(result.ExecutionPath, currentStepID)

		// Create block context for this step
		blockContext, err := e.CreateBlockContext(
			step.BlockType,
			step.InputValues,
			step.ConfigValues,
			step.OptionValues,
			previousOutput,
			step.BlockID,
			step.EdgeID,
			context.GlobalData,
			context.UserFields,
			context.WorkflowConfig,
		)

		if err != nil {
			e.ErrorLogger(err)
			result.Error = err
			break
		}

		// Execute the block
		output := e.ExecuteBlock(step.BlockType, blockContext)

		// Store result
		result.StepResults[currentStepID] = output
		previousOutput = output

		// Check for errors
		if output.Error != nil {
			result.Error = output.Error
			break
		}

		// Determine next step
		nextStepID := ""

		// Check if any dynamic routing condition is met
		// This allows blocks to modify the workflow path by updating the WorkflowConfig
		if context.WorkflowConfig != nil {
			if dynamicNext, exists := context.WorkflowConfig.DefaultNextStep[step.BlockID]; exists {
				nextStepID = dynamicNext
			}
		}

		// If no dynamic routing is defined, use the static routing from workflow definition
		if nextStepID == "" {
			// First check branch conditions (can be extended with more complex logic)
			for condition, branchStepID := range step.BranchStepIDs {
				// Simple string output matching for now
				if outputValue, ok := output.GetString(condition); ok && outputValue == "true" {
					nextStepID = branchStepID
					break
				}
			}

			// If no branch condition matched, use default next step
			if nextStepID == "" {
				nextStepID = step.NextStepID
			}
		}

		// Prepare for next step
		currentStepID = nextStepID

		// Release block context back to pool
		ReleaseBlockContext(blockContext)
	}

	// Set final output and execution time
	result.FinalOutput = previousOutput
	result.ExecutionTime = time.Since(startTime)

	return result
}

// GetNextStepID returns the next step ID for a given step
func (w *Workflow) GetNextStepID(currentStepID string, output *OutputData) string {
	step, exists := w.Steps[currentStepID]
	if !exists {
		return ""
	}

	// Check branch conditions first
	for condition, branchStepID := range step.BranchStepIDs {
		if output != nil {
			// Simple string matching for conditions
			if outputValue, ok := output.GetString(condition); ok && outputValue == "true" {
				return branchStepID
			}
		}
	}

	// Fall back to default next step
	return step.NextStepID
}

// LoadWorkflow loads a workflow from configuration data
func LoadWorkflow(workflowData map[string]interface{}) (*Workflow, error) {
	// A simple implementation - in a real system this would parse from JSON or a database
	id, _ := workflowData["id"].(string)
	name, _ := workflowData["name"].(string)
	description, _ := workflowData["description"].(string)
	startStepID, _ := workflowData["startStepID"].(string)

	if id == "" || startStepID == "" {
		return nil, errors.New("workflow must have ID and start step")
	}

	workflow := &Workflow{
		ID:           id,
		Name:         name,
		Description:  description,
		StartStepID:  startStepID,
		Steps:        make(map[string]*WorkflowStep),
		ConfigSchema: NewBlockSchema(),
		ConfigValues: make(map[string]interface{}),
	}

	// Load steps
	stepsData, _ := workflowData["steps"].(map[string]interface{})
	for stepID, stepDataRaw := range stepsData {
		stepData, _ := stepDataRaw.(map[string]interface{})

		blockType, _ := stepData["blockType"].(string)
		blockID, _ := stepData["blockID"].(string)
		edgeID, _ := stepData["edgeID"].(string)
		nextStepID, _ := stepData["nextStepID"].(string)

		if blockType == "" || blockID == "" {
			return nil, fmt.Errorf("step %s missing required fields", stepID)
		}

		step := &WorkflowStep{
			BlockType:     blockType,
			BlockID:       blockID,
			EdgeID:        edgeID,
			NextStepID:    nextStepID,
			InputValues:   make(map[string]interface{}),
			ConfigValues:  make(map[string]interface{}),
			OptionValues:  make(map[string]interface{}),
			BranchStepIDs: make(map[string]string),
		}

		// Load input values
		if inputData, ok := stepData["inputValues"].(map[string]interface{}); ok {
			for k, v := range inputData {
				step.InputValues[k] = v
			}
		}

		// Load config values
		if configData, ok := stepData["configValues"].(map[string]interface{}); ok {
			for k, v := range configData {
				step.ConfigValues[k] = v
			}
		}

		// Load option values
		if optionData, ok := stepData["optionValues"].(map[string]interface{}); ok {
			for k, v := range optionData {
				step.OptionValues[k] = v
			}
		}

		// Load branch steps
		if branchData, ok := stepData["branchStepIDs"].(map[string]interface{}); ok {
			for k, v := range branchData {
				if vs, ok := v.(string); ok {
					step.BranchStepIDs[k] = vs
				}
			}
		}

		workflow.Steps[stepID] = step
	}

	// Load workflow config
	if configData, ok := workflowData["configValues"].(map[string]interface{}); ok {
		for k, v := range configData {
			workflow.ConfigValues[k] = v
		}
	}

	return workflow, nil
}

// Helper function to log workflow results
func LogWorkflowResults(result *WorkflowResult) {
	if result == nil {
		log.Println("Workflow result is nil")
		return
	}

	log.Printf("Workflow execution completed in %v", result.ExecutionTime)
	log.Printf("Execution path: %v", result.ExecutionPath)

	if result.Error != nil {
		log.Printf("Workflow error: %v", result.Error)
	}

	log.Println("Step results:")
	for stepID, output := range result.StepResults {
		log.Printf("  Step %s:", stepID)

		if output == nil || output.TypedData == nil || output.Schema == nil {
			log.Printf("    <nil output>")
			continue
		}

		// Print string values
		for fieldName, idx := range output.Schema.StringFieldIndices {
			if idx >= 0 && idx < len(output.StringValues) {
				log.Printf("    %s: %s", fieldName, output.StringValues[idx])
			}
		}

		// Print int values
		for fieldName, idx := range output.Schema.IntFieldIndices {
			if idx >= 0 && idx < len(output.IntValues) {
				log.Printf("    %s: %d", fieldName, output.IntValues[idx])
			}
		}

		// Print float values
		for fieldName, idx := range output.Schema.FloatFieldIndices {
			if idx >= 0 && idx < len(output.FloatValues) {
				log.Printf("    %s: %.2f", fieldName, output.FloatValues[idx])
			}
		}

		// Print bool values
		for fieldName, idx := range output.Schema.BoolFieldIndices {
			if idx >= 0 && idx < len(output.BoolValues) {
				log.Printf("    %s: %t", fieldName, output.BoolValues[idx])
			}
		}

		// Object values often need custom handling
		for fieldName, idx := range output.Schema.ObjectFieldIndices {
			if idx >= 0 && idx < len(output.ObjectValues) {
				log.Printf("    %s: %v", fieldName, output.ObjectValues[idx])
			}
		}
	}
}

// Register example blocks
func RegisterExampleBlocks(engine *WorkflowEngine) error {
	// Register global state fields
	if err := engine.Registry.RegisterGlobalField("username", StringField); err != nil {
		return err
	}
	if err := engine.Registry.RegisterGlobalField("userEmail", StringField); err != nil {
		return err
	}
	if err := engine.Registry.RegisterGlobalField("userAge", IntField); err != nil {
		return err
	}
	if err := engine.Registry.RegisterGlobalField("isPremiumUser", BoolField); err != nil {
		return err
	}
	if err := engine.Registry.RegisterGlobalField("lastLogin", StringField); err != nil {
		return err
	}
	if err := engine.Registry.RegisterGlobalField("requestCount", IntField); err != nil {
		return err
	}
	if err := engine.Registry.RegisterGlobalField("processingTime", FloatField); err != nil {
		return err
	}

	// Register user fields
	if err := engine.Registry.RegisterUserField("name", StringField); err != nil {
		return err
	}
	if err := engine.Registry.RegisterUserField("email", StringField); err != nil {
		return err
	}
	if err := engine.Registry.RegisterUserField("age", IntField); err != nil {
		return err
	}
	if err := engine.Registry.RegisterUserField("isActive", BoolField); err != nil {
		return err
	}

	// 1. UserInfo Block
	if _, err := engine.Registry.RegisterBlockType("userInfo", map[string]FieldType{
		"name":     StringField,
		"email":    StringField,
		"age":      IntField,
		"isActive": BoolField,
	}); err != nil {
		return err
	}

	if err := engine.RegisterBlock("userInfo", func(ctx *BlockContext) *OutputData {
		if ctx == nil || ctx.Inputs == nil || ctx.GlobalData == nil || ctx.UserFields == nil {
			return &OutputData{
				TypedData: NewTypedData("error", nil),
				Error:     errors.New("invalid block context"),
			}
		}

		// Get input values with null-safety
		name, hasName := ctx.Inputs.GetString("name")
		email, hasEmail := ctx.Inputs.GetString("email")
		age, hasAge := ctx.Inputs.GetInt("age")
		isActive, _ := ctx.Inputs.GetBool("isActive")

		// Create output schema matching the block type
		schema, err := engine.Registry.GetSchema("userInfo")
		if err != nil {
			return &OutputData{
				TypedData: NewTypedData("error", nil),
				Error:     err,
			}
		}

		output := &OutputData{TypedData: NewTypedData("userInfo", schema)}

		// Store in user fields
		if hasName {
			ctx.UserFields.SetString("name", name)
		}
		if hasEmail {
			ctx.UserFields.SetString("email", email)
		}
		if hasAge {
			ctx.UserFields.SetInt("age", age)
		}
		ctx.UserFields.SetBool("isActive", isActive)

		// Store in global state
		if hasName {
			ctx.GlobalData.SetString("username", name)
		}
		if hasEmail {
			ctx.GlobalData.SetString("userEmail", email)
		}
		if hasAge {
			ctx.GlobalData.SetInt("userAge", age)
		}

		// Create output
		if hasName {
			output.SetString("greeting", "Hello, "+name)
		} else {
			output.SetString("greeting", "Hello, Guest")
		}

		if hasEmail {
			output.SetString("contactInfo", "Email: "+email)
		} else {
			output.SetString("contactInfo", "Email: Not provided")
		}

		if hasAge {
			output.SetInt("ageNextYear", age+1)
		} else {
			output.SetInt("ageNextYear", 0)
		}

		return output
	}); err != nil {
		return err
	}

	// 2. Register a decision block
	if _, err := engine.Registry.RegisterBlockType("decision", map[string]FieldType{
		"condition":   StringField,
		"trueOutput":  StringField,
		"falseOutput": StringField,
	}); err != nil {
		return err
	}

	if err := engine.RegisterBlock("decision", func(ctx *BlockContext) *OutputData {
		if ctx == nil || ctx.Inputs == nil {
			return &OutputData{
				TypedData: NewTypedData("error", nil),
				Error:     errors.New("invalid block context"),
			}
		}

		// Get input values
		condition, hasCondition := ctx.Inputs.GetString("condition")
		trueOutput, hasTrueOutput := ctx.Inputs.GetString("trueOutput")
		falseOutput, hasFalseOutput := ctx.Inputs.GetString("falseOutput")

		// Create output
		schema, _ := engine.Registry.GetSchema("decision")
		output := &OutputData{TypedData: NewTypedData("decision", schema)}

		// Default values
		result := "false"
		outputValue := ""

		// Evaluate condition (simplified for example)
		if hasCondition && condition == "true" {
			result = "true"
			if hasTrueOutput {
				outputValue = trueOutput
			}
		} else {
			if hasFalseOutput {
				outputValue = falseOutput
			}
		}

		// Set outputs
		output.SetString("result", result)
		output.SetString("output", outputValue)

		// Dynamically modify workflow path if needed
		if ctx.WorkflowConfig != nil && ctx.BlockID != "" {
			// In a real implementation, you could set the next step based on the decision
			// ctx.WorkflowConfig.DefaultNextStep[ctx.BlockID] = someNextStepID
		}

		return output
	}); err != nil {
		return err
	}

	return nil
}

// CreateExampleWorkflow creates a sample workflow
func CreateExampleWorkflow() *Workflow {
	return &Workflow{
		ID:          "wf-001",
		Name:        "User Registration Workflow",
		Description: "Process new user registration",
		StartStepID: "step1",
		Steps: map[string]*WorkflowStep{
			"step1": {
				BlockType: "userInfo",
				BlockID:   "block1",
				EdgeID:    "edge1",
				InputValues: map[string]interface{}{
					"name":     "John Doe",
					"email":    "john@example.com",
					"age":      35,
					"isActive": true,
				},
				ConfigValues: map[string]interface{}{},
				OptionValues: map[string]interface{}{},
				NextStepID:   "step2",
			},
			"step2": {
				BlockType: "decision",
				BlockID:   "block2",
				EdgeID:    "edge2",
				InputValues: map[string]interface{}{
					"condition":   "true",
					"trueOutput":  "Premium user path",
					"falseOutput": "Standard user path",
				},
				ConfigValues: map[string]interface{}{},
				OptionValues: map[string]interface{}{},
				BranchStepIDs: map[string]string{
					"result": "step3", // If result is "true", go to step3
				},
				NextStepID: "", // End workflow if no branches match
			},
			"step3": {
				BlockType: "userInfo",
				BlockID:   "block3",
				EdgeID:    "edge3",
				InputValues: map[string]interface{}{
					"name":     "Jane Doe",
					"email":    "jane@example.com",
					"age":      28,
					"isActive": true,
				},
				ConfigValues: map[string]interface{}{},
				OptionValues: map[string]interface{}{},
				NextStepID:   "", // End of workflow
			},
		},
		ConfigSchema: NewBlockSchema(),
		ConfigValues: map[string]interface{}{
			"workflowTimeout": 30, // seconds
		},
	}
}

// ExecuteSampleWorkflow demonstrates the workflow execution
func ExecuteSampleWorkflow() *WorkflowResult {
	// Create and configure the workflow engine
	engine := NewWorkflowEngine()
	if err := RegisterExampleBlocks(engine); err != nil {
		log.Printf("Error registering blocks: %v", err)
		return &WorkflowResult{Error: err}
	}

	// Create the sample workflow
	workflow := CreateExampleWorkflow()

	// Create workflow context
	context := NewWorkflowContext(engine.Registry)

	// Execute the workflow
	result := engine.ExecuteWorkflow(workflow, context)

	// Log results
	LogWorkflowResults(result)

	return result
}

// Example handler for HTTP integration
func HandleWorkflowExecution(workflowID string, inputData map[string]interface{}) *WorkflowResult {
	// In a real application, this would load workflow from storage based on ID
	workflow := CreateExampleWorkflow()

	// Create engine
	engine := NewWorkflowEngine()
	if err := RegisterExampleBlocks(engine); err != nil {
		return &WorkflowResult{Error: err}
	}

	// Create context
	context := NewWorkflowContext(engine.Registry)

	// Apply input data to user fields
	for k, v := range inputData {
		switch val := v.(type) {
		case string:
			context.UserFields.SetString(k, val)
		case int:
			context.UserFields.SetInt(k, int64(val))
		case int64:
			context.UserFields.SetInt(k, val)
		case float64:
			context.UserFields.SetFloat(k, val)
		case bool:
			context.UserFields.SetBool(k, val)
		default:
			context.UserFields.SetObject(k, val)
		}
	}

	// Execute workflow
	return engine.ExecuteWorkflow(workflow, context)
}

// For HTTP handler integration
func HandleWorkflowHandler(w http.ResponseWriter, r *http.Request) {
	result := ExecuteSampleWorkflow()
	if result.Error != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Workflow execution failed: %v", result.Error)
		return
	}

	// In a real application, you would return a proper JSON response
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Workflow executed successfully")
}
