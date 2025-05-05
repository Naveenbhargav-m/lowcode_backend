package workflow

import (
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"
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

	// Global state schema
	GlobalSchema *BlockSchema
}

// NewSchemaRegistry creates a new schema registry
func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{
		Schemas: make(map[string]*BlockSchema),
		GlobalSchema: &BlockSchema{
			StringFieldIndices: make(map[string]int),
			IntFieldIndices:    make(map[string]int),
			FloatFieldIndices:  make(map[string]int),
			BoolFieldIndices:   make(map[string]int),
			ObjectFieldIndices: make(map[string]int),
		},
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

// RegisterGlobalField adds a field to the global state schema
func (r *SchemaRegistry) RegisterGlobalField(fieldName string, fieldType FieldType) {
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
	}
}

// GetSchema returns a schema for a given block type
func (r *SchemaRegistry) GetSchema(blockType string) *BlockSchema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.Schemas[blockType]
}

// GlobalState represents the shared state across the workflow
type GlobalState struct {
	mu        sync.RWMutex
	TypedData *TypedData
}

// NewGlobalState creates a new global state container
func NewGlobalState(schema *BlockSchema) *GlobalState {
	return &GlobalState{
		TypedData: NewTypedData("global", schema),
	}
}

// GetString retrieves a string value from global state
func (s *GlobalState) GetString(fieldName string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.TypedData.GetString(fieldName)
}

// SetString sets a string value in global state
func (s *GlobalState) SetString(fieldName string, value string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.TypedData.SetString(fieldName, value)
}

// GetInt retrieves an int value from global state
func (s *GlobalState) GetInt(fieldName string) (int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.TypedData.GetInt(fieldName)
}

// SetInt sets an int value in global state
func (s *GlobalState) SetInt(fieldName string, value int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.TypedData.SetInt(fieldName, value)
}

// GetFloat retrieves a float value from global state
func (s *GlobalState) GetFloat(fieldName string) (float64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.TypedData.GetFloat(fieldName)
}

// SetFloat sets a float value in global state
func (s *GlobalState) SetFloat(fieldName string, value float64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.TypedData.SetFloat(fieldName, value)
}

// GetBool retrieves a bool value from global state
func (s *GlobalState) GetBool(fieldName string) (bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.TypedData.GetBool(fieldName)
}

// SetBool sets a bool value in global state
func (s *GlobalState) SetBool(fieldName string, value bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.TypedData.SetBool(fieldName, value)
}

// GetObject retrieves an object value from global state
func (s *GlobalState) GetObject(fieldName string) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.TypedData.GetObject(fieldName)
}

// SetObject sets an object value in global state
func (s *GlobalState) SetObject(fieldName string, value any) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.TypedData.SetObject(fieldName, value)
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

func (d *TypedData) SetFloat(fieldName string, value float64) bool {
	idx, exists := d.Schema.FloatFieldIndices[fieldName]
	if !exists {
		return false
	}
	d.FloatValues[idx] = value
	return true
}

func (d *TypedData) GetFloat(fieldName string) (float64, bool) {
	idx, exists := d.Schema.FloatFieldIndices[fieldName]
	if !exists {
		return 0, false
	}
	return d.FloatValues[idx], true
}

func (d *TypedData) SetBool(fieldName string, value bool) bool {
	idx, exists := d.Schema.BoolFieldIndices[fieldName]
	if !exists {
		return false
	}
	d.BoolValues[idx] = value
	return true
}

func (d *TypedData) GetBool(fieldName string) (bool, bool) {
	idx, exists := d.Schema.BoolFieldIndices[fieldName]
	if !exists {
		return false, false
	}
	return d.BoolValues[idx], true
}

func (d *TypedData) SetObject(fieldName string, value any) bool {
	idx, exists := d.Schema.ObjectFieldIndices[fieldName]
	if !exists {
		return false
	}
	d.ObjectValues[idx] = value
	return true
}

func (d *TypedData) GetObject(fieldName string) (any, bool) {
	idx, exists := d.Schema.ObjectFieldIndices[fieldName]
	if !exists {
		return nil, false
	}
	return d.ObjectValues[idx], true
}

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

// WorkflowEngine manages the execution of blocks
type WorkflowEngine struct {
	Registry    *SchemaRegistry
	GlobalState *GlobalState
	BlockFuncs  map[string]func(*BlockData) *OutputData
}

// NewWorkflowEngine creates a new workflow engine
func NewWorkflowEngine() *WorkflowEngine {
	registry := NewSchemaRegistry()
	return &WorkflowEngine{
		Registry:    registry,
		GlobalState: NewGlobalState(registry.GlobalSchema),
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

	// Populate inputs
	for key, value := range inputValues {
		switch v := value.(type) {
		case string:
			blockData.Inputs.SetString(key, v)
		case int:
			blockData.Inputs.SetInt(key, int64(v))
		case int64:
			blockData.Inputs.SetInt(key, v)
		case float64:
			blockData.Inputs.SetFloat(key, v)
		case bool:
			blockData.Inputs.SetBool(key, v)
		default:
			blockData.Inputs.SetObject(key, v)
		}
	}

	// Populate configs
	for key, value := range configValues {
		switch v := value.(type) {
		case string:
			blockData.Configs.SetString(key, v)
		case int:
			blockData.Configs.SetInt(key, int64(v))
		case int64:
			blockData.Configs.SetInt(key, v)
		case float64:
			blockData.Configs.SetFloat(key, v)
		case bool:
			blockData.Configs.SetBool(key, v)
		default:
			blockData.Configs.SetObject(key, v)
		}
	}

	// Populate options
	for key, value := range optionValues {
		switch v := value.(type) {
		case string:
			blockData.Options.SetString(key, v)
		case int:
			blockData.Options.SetInt(key, int64(v))
		case int64:
			blockData.Options.SetInt(key, v)
		case float64:
			blockData.Options.SetFloat(key, v)
		case bool:
			blockData.Options.SetBool(key, v)
		default:
			blockData.Options.SetObject(key, v)
		}
	}

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

// WorkflowStep defines a single step in a workflow
type WorkflowStep struct {
	BlockType     string
	BlockID       string
	EdgeID        string
	InputValues   map[string]interface{}
	ConfigValues  map[string]interface{}
	OptionValues  map[string]interface{}
	NextStepID    string
	BranchStepIDs map[string]string // Conditional branching based on output values
}

// Workflow defines a complete workflow configuration
type Workflow struct {
	ID          string
	Name        string
	Description string
	StartStepID string
	Steps       map[string]*WorkflowStep
}

// ExecuteWorkflow runs a complete workflow
func (e *WorkflowEngine) ExecuteWorkflow(workflow *Workflow) map[string]*OutputData {
	results := make(map[string]*OutputData)

	currentStepID := workflow.StartStepID
	var previousOutput *OutputData

	for currentStepID != "" {
		step := workflow.Steps[currentStepID]
		if step == nil {
			break
		}

		blockData := e.CreateBlockData(
			step.BlockType,
			step.InputValues,
			step.ConfigValues,
			step.OptionValues,
			previousOutput,
			step.BlockID,
			step.EdgeID,
		)

		output := e.ExecuteBlock(step.BlockType, blockData)
		results[currentStepID] = output
		previousOutput = output

		// Determine next step (simple linear flow for now)
		currentStepID = step.NextStepID

		// Handle branching logic in a more advanced implementation
		// if len(step.BranchStepIDs) > 0 {
		//    // Determine branch based on output values
		// }

		// Release block data back to pool
		ReleaseBlockData(blockData)
	}

	return results
}

// Register example blocks
func RegisterExampleBlocks(engine *WorkflowEngine) {
	// Register global state fields
	engine.Registry.RegisterGlobalField("username", StringField)
	engine.Registry.RegisterGlobalField("userEmail", StringField)
	engine.Registry.RegisterGlobalField("userAge", IntField)
	engine.Registry.RegisterGlobalField("isPremiumUser", BoolField)
	engine.Registry.RegisterGlobalField("lastLogin", StringField)
	engine.Registry.RegisterGlobalField("requestCount", IntField)
	engine.Registry.RegisterGlobalField("processingTime", FloatField)

	// 1. UserInfo Block
	engine.Registry.RegisterBlockType("userInfo", map[string]FieldType{
		"name":     StringField,
		"email":    StringField,
		"age":      IntField,
		"isActive": BoolField,
	})

	engine.RegisterBlock("userInfo", func(data *BlockData) *OutputData {
		output := &OutputData{NewTypedData(data.Inputs.BlockType, data.Inputs.Schema)}

		name, _ := data.Inputs.GetString("name")
		email, _ := data.Inputs.GetString("email")
		age, _ := data.Inputs.GetInt("age")

		// Store in global state
		data.GlobalState.SetString("username", name)
		data.GlobalState.SetString("userEmail", email)
		data.GlobalState.SetInt("userAge", age)

		// Create output
		output.SetString("greeting", "Hello, "+name)
		output.SetString("contactInfo", "Email: "+email)
		output.SetInt("ageNextYear", age+1)

		return output
	})

	// 2. Calculator Block
	engine.Registry.RegisterBlockType("calculator", map[string]FieldType{
		"operation": StringField,
		"valueA":    FloatField,
		"valueB":    FloatField,
		"result":    FloatField,
	})

	engine.RegisterBlock("calculator", func(data *BlockData) *OutputData {
		output := &OutputData{NewTypedData(data.Inputs.BlockType, data.Inputs.Schema)}

		operation, _ := data.Inputs.GetString("operation")
		valueA, _ := data.Inputs.GetFloat("valueA")
		valueB, _ := data.Inputs.GetFloat("valueB")

		var result float64
		switch operation {
		case "add":
			result = valueA + valueB
		case "subtract":
			result = valueA - valueB
		case "multiply":
			result = valueA * valueB
		case "divide":
			if valueB != 0 {
				result = valueA / valueB
			} else {
				result = math.NaN()
			}
		case "power":
			result = math.Pow(valueA, valueB)
		default:
			result = math.NaN()
		}

		output.SetFloat("result", result)
		output.SetString("equation", fmt.Sprintf("%.2f %s %.2f = %.2f", valueA, operation, valueB, result))

		return output
	})

	// 3. TextProcessor Block
	engine.Registry.RegisterBlockType("textProcessor", map[string]FieldType{
		"text":           StringField,
		"operation":      StringField,
		"processedText":  StringField,
		"wordCount":      IntField,
		"characterCount": IntField,
	})

	engine.RegisterBlock("textProcessor", func(data *BlockData) *OutputData {
		output := &OutputData{NewTypedData(data.Inputs.BlockType, data.Inputs.Schema)}

		text, _ := data.Inputs.GetString("text")
		operation, _ := data.Inputs.GetString("operation")

		var processedText string
		switch operation {
		case "uppercase":
			processedText = strings.ToUpper(text)
		case "lowercase":
			processedText = strings.ToLower(text)
		case "capitalize":
			words := strings.Fields(text)
			for i, word := range words {
				if len(word) > 0 {
					words[i] = strings.ToUpper(word[:1]) + word[1:]
				}
			}
			processedText = strings.Join(words, " ")
		case "reverse":
			runes := []rune(text)
			for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
				runes[i], runes[j] = runes[j], runes[i]
			}
			processedText = string(runes)
		default:
			processedText = text
		}

		wordCount := int64(len(strings.Fields(text)))
		charCount := int64(len(text))

		output.SetString("processedText", processedText)
		output.SetInt("wordCount", wordCount)
		output.SetInt("characterCount", charCount)

		return output
	})

	// 4. DataLogger Block
	engine.Registry.RegisterBlockType("dataLogger", map[string]FieldType{
		"message":    StringField,
		"severity":   StringField,
		"timestamp":  StringField,
		"successful": BoolField,
	})

	engine.RegisterBlock("dataLogger", func(data *BlockData) *OutputData {
		output := &OutputData{NewTypedData(data.Inputs.BlockType, data.Inputs.Schema)}

		message, _ := data.Inputs.GetString("message")
		severity, _ := data.Inputs.GetString("severity")

		// Default to "info" if severity is not provided
		if severity == "" {
			severity = "info"
		}

		timestamp := time.Now().Format(time.RFC3339)

		// Increment request count in global state
		requestCount, exists := data.GlobalState.GetInt("requestCount")
		if exists {
			data.GlobalState.SetInt("requestCount", requestCount+1)
		} else {
			data.GlobalState.SetInt("requestCount", 1)
		}

		// Log format: [SEVERITY] [TIMESTAMP] MESSAGE
		logMessage := fmt.Sprintf("[%s] [%s] %s", strings.ToUpper(severity), timestamp, message)

		// In a real implementation, this would write to a log file or service
		fmt.Println(logMessage)

		output.SetString("message", message)
		output.SetString("severity", severity)
		output.SetString("timestamp", timestamp)
		output.SetBool("successful", true)

		return output
	})

	// 5. EmailSender Block
	engine.Registry.RegisterBlockType("emailSender", map[string]FieldType{
		"recipient":    StringField,
		"subject":      StringField,
		"body":         StringField,
		"sent":         BoolField,
		"errorMessage": StringField,
		"messageID":    StringField,
	})

	engine.RegisterBlock("emailSender", func(data *BlockData) *OutputData {
		output := &OutputData{NewTypedData(data.Inputs.BlockType, data.Inputs.Schema)}

		recipient, _ := data.Inputs.GetString("recipient")
		subject, _ := data.Inputs.GetString("subject")
		body, _ := data.Inputs.GetString("body")

		// In a real implementation, this would use an email service
		// For now, we'll simulate success
		sent := true
		var errorMessage string
		messageID := fmt.Sprintf("MSG-%d-%s", time.Now().Unix(), recipient[:3])

		// Pull from global state if recipient is empty
		if recipient == "" {
			if userEmail, exists := data.GlobalState.GetString("userEmail"); exists {
				recipient = userEmail
			}
		}

		// For demo purposes, simulate an error for a specific address
		if recipient == "error@example.com" {
			sent = false
			errorMessage = "Failed to send email: Invalid recipient"
			messageID = ""
		}

		output.SetString("recipient", recipient)
		output.SetString("subject", subject)
		output.SetString("body", body)
		output.SetBool("sent", sent)
		output.SetString("errorMessage", errorMessage)
		output.SetString("messageID", messageID)

		return output
	})
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
				BlockType: "textProcessor",
				BlockID:   "block2",
				EdgeID:    "edge2",
				InputValues: map[string]interface{}{
					"text":      "Welcome to our platform!",
					"operation": "uppercase",
				},
				ConfigValues: map[string]interface{}{},
				OptionValues: map[string]interface{}{},
				NextStepID:   "step3",
			},
			"step3": {
				BlockType: "calculator",
				BlockID:   "block3",
				EdgeID:    "edge3",
				InputValues: map[string]interface{}{
					"operation": "add",
					"valueA":    10.5,
					"valueB":    5.25,
				},
				ConfigValues: map[string]interface{}{},
				OptionValues: map[string]interface{}{},
				NextStepID:   "step4",
			},
			"step4": {
				BlockType: "dataLogger",
				BlockID:   "block4",
				EdgeID:    "edge4",
				InputValues: map[string]interface{}{
					"message":  "User registration completed",
					"severity": "info",
				},
				ConfigValues: map[string]interface{}{},
				OptionValues: map[string]interface{}{},
				NextStepID:   "step5",
			},
			"step5": {
				BlockType: "emailSender",
				BlockID:   "block5",
				EdgeID:    "edge5",
				InputValues: map[string]interface{}{
					"subject": "Welcome to Our Service",
					"body":    "Thank you for registering with our service. We're excited to have you on board!",
				},
				ConfigValues: map[string]interface{}{},
				OptionValues: map[string]interface{}{},
				NextStepID:   "",
			},
		},
	}
}

// ExecuteSampleWorkflow demonstrates the workflow execution
func ExecuteSampleWorkflow() map[string]*OutputData {
	// Create and configure the workflow engine
	engine := NewWorkflowEngine()
	RegisterExampleBlocks(engine)

	// Create the sample workflow
	workflow := CreateExampleWorkflow()

	// Execute the workflow
	return engine.ExecuteWorkflow(workflow)
}

// Example main function
func ExampleMain() {
	fmt.Println("Starting workflow execution...")

	results := ExecuteSampleWorkflow()

	fmt.Println("\nWorkflow Results:")
	for stepID, output := range results {
		fmt.Printf("\nStep %s Output:\n", stepID)

		// Print string values
		for fieldName, idx := range output.Schema.StringFieldIndices {
			fmt.Printf("  %s: %s\n", fieldName, output.StringValues[idx])
		}

		// Print int values
		for fieldName, idx := range output.Schema.IntFieldIndices {
			fmt.Printf("  %s: %d\n", fieldName, output.IntValues[idx])
		}

		// Print float values
		for fieldName, idx := range output.Schema.FloatFieldIndices {
			fmt.Printf("  %s: %.2f\n", fieldName, output.FloatValues[idx])
		}

		// Print bool values
		for fieldName, idx := range output.Schema.BoolFieldIndices {
			fmt.Printf("  %s: %t\n", fieldName, output.BoolValues[idx])
		}
	}

	fmt.Println("\nWorkflow execution completed.")
}

func HandleWorkflowHandler(w http.ResponseWriter, r *http.Request) {
	ExampleMain()
}
