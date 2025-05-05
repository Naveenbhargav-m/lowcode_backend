package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Common errors
var (
	ErrBlockNotFound2   = errors.New("block not found")
	ErrInvalidBlockType = errors.New("invalid block type")
	ErrExecutionTimeout = errors.New("workflow execution timeout")
	ErrMaxIterations    = errors.New("maximum iterations exceeded")
)

// Block represents a workflow block interface that all block types must implement
type Block interface {
	Execute(ctx context.Context, data map[string]interface{}) (map[string]interface{}, error)
	GetID() string
	GetType() string
}

// BasicBlock provides common functionality for all blocks
type BasicBlock struct {
	ID   string
	Type string
}

func (b *BasicBlock) GetID() string {
	return b.ID
}

func (b *BasicBlock) GetType() string {
	return b.Type
}

// Edge represents a directed connection between blocks
type Edge struct {
	ID     string
	Source string
	Target string
}

// ExecutionOptions contains configuration for workflow execution
type ExecutionOptions struct {
	Timeout       time.Duration
	MaxIterations int
}

// DefaultOptions returns sensible default execution options
func DefaultOptions() ExecutionOptions {
	return ExecutionOptions{
		Timeout:       30 * time.Second,
		MaxIterations: 1000,
	}
}

// Executor is a lightweight, stateless workflow executor
type Executor struct {
	// BlockRegistry allows custom block type registration
	BlockRegistry map[string]func(blockID string, config map[string]interface{}) (Block, error)
}

// NewExecutor creates a new workflow executor
func NewExecutor() *Executor {
	return &Executor{
		BlockRegistry: make(map[string]func(blockID string, config map[string]interface{}) (Block, error)),
	}
}

// RegisterBlockType adds a new block type to the executor
func (e *Executor) RegisterBlockType(blockType string, factory func(blockID string, config map[string]interface{}) (Block, error)) {
	e.BlockRegistry[blockType] = factory
}

// buildBlocksMap constructs a map of blocks from block definitions
func (e *Executor) buildBlocksMap(blockDefs []map[string]interface{}) (map[string]Block, error) {
	blocks := make(map[string]Block)

	for _, def := range blockDefs {
		blockID, ok := def["id"].(string)
		if !ok {
			return nil, errors.New("block missing id")
		}

		blockType, ok := def["type"].(string)
		if !ok {
			return nil, errors.New("block missing type")
		}

		factory, exists := e.BlockRegistry[blockType]
		if !exists {
			return nil, fmt.Errorf("%w: %s", ErrInvalidBlockType, blockType)
		}

		block, err := factory(blockID, def)
		if err != nil {
			return nil, fmt.Errorf("failed to create block %s: %w", blockID, err)
		}

		blocks[blockID] = block
	}

	return blocks, nil
}

// buildEdgesMap constructs an adjacency list from edge definitions
func (e *Executor) buildEdgesMap(edgeDefs []map[string]interface{}) (map[string][]string, error) {
	edgesMap := make(map[string][]string)

	for _, def := range edgeDefs {
		source, ok := def["source"].(string)
		if !ok {
			return nil, errors.New("edge missing source")
		}

		target, ok := def["target"].(string)
		if !ok {
			return nil, errors.New("edge missing target")
		}

		if _, exists := edgesMap[source]; !exists {
			edgesMap[source] = make([]string, 0)
		}

		edgesMap[source] = append(edgesMap[source], target)
	}

	return edgesMap, nil
}

// findStartBlock identifies the starting block for workflow execution
func (e *Executor) findStartBlock(blocks map[string]Block, edgesMap map[string][]string) (string, error) {
	// First look for incoming edges for each block
	hasIncoming := make(map[string]bool)
	for _, targets := range edgesMap {
		for _, target := range targets {
			hasIncoming[target] = true
		}
	}

	// Find blocks with no incoming edges
	for blockID := range blocks {
		if !hasIncoming[blockID] {
			return blockID, nil
		}
	}

	// If no clear start block, look for a block with type "start"
	for blockID, block := range blocks {
		if block.GetType() == "start" {
			return blockID, nil
		}
	}

	return "", errors.New("no start block found")
}

// Execute runs the workflow defined by blocks and edges using the provided input data
func (e *Executor) Execute(
	ctx context.Context,
	blockDefs []map[string]interface{},
	edgeDefs []map[string]interface{},
	inputData map[string]interface{},
	options ExecutionOptions,
) (map[string]interface{}, []string, error) {
	// Apply timeout
	ctx, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()

	// Build blocks map
	blocks, err := e.buildBlocksMap(blockDefs)
	if err != nil {
		return nil, nil, err
	}

	// Build edges adjacency list
	edgesMap, err := e.buildEdgesMap(edgeDefs)
	if err != nil {
		return nil, nil, err
	}

	// Find start block
	startBlockID, err := e.findStartBlock(blocks, edgesMap)
	if err != nil {
		return nil, nil, err
	}

	// Initialize execution state
	data := make(map[string]interface{})
	for k, v := range inputData {
		data[k] = v
	}

	// Initialize output tracking
	output := make(map[string]interface{})
	executionPath := make([]string, 0)

	// Execute workflow
	err = e.executeBlock(
		ctx,
		startBlockID,
		blocks,
		edgesMap,
		data,
		&output,
		&executionPath,
		0,
		options.MaxIterations,
	)

	return output, executionPath, err
}

// executeBlock processes a single block and follows its outgoing edges
func (e *Executor) executeBlock(
	ctx context.Context,
	blockID string,
	blocks map[string]Block,
	edgesMap map[string][]string,
	data map[string]interface{},
	output *map[string]interface{},
	executionPath *[]string,
	currentDepth int,
	maxIterations int,
) error {
	// Check for context cancellation
	select {
	case <-ctx.Done():
		return ErrExecutionTimeout
	default:
		// Continue execution
	}

	// Check max iterations
	if currentDepth >= maxIterations {
		return ErrMaxIterations
	}

	// Get block to execute
	block, exists := blocks[blockID]
	if !exists {
		return fmt.Errorf("%w: %s", ErrBlockNotFound, blockID)
	}

	// Add to execution path
	*executionPath = append(*executionPath, blockID)

	// Execute block
	blockOutput, err := block.Execute(ctx, data)
	if err != nil {
		return err
	}

	// Merge block output into workflow data and output
	for k, v := range blockOutput {
		data[k] = v
		(*output)[k] = v
	}

	// Find next blocks
	nextBlocks, hasNext := edgesMap[blockID]
	if !hasNext || len(nextBlocks) == 0 {
		// End of workflow path
		return nil
	}

	// Execute next blocks
	for _, nextBlockID := range nextBlocks {
		err := e.executeBlock(
			ctx,
			nextBlockID,
			blocks,
			edgesMap,
			data,
			output,
			executionPath,
			currentDepth+1,
			maxIterations,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

// ExecuteParallel runs the workflow with parallel execution where possible
func (e *Executor) ExecuteParallel(
	ctx context.Context,
	blockDefs []map[string]interface{},
	edgeDefs []map[string]interface{},
	inputData map[string]interface{},
	options ExecutionOptions,
) (map[string]interface{}, []string, error) {
	// Apply timeout
	ctx, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()

	// Build blocks map
	blocks, err := e.buildBlocksMap(blockDefs)
	if err != nil {
		return nil, nil, err
	}

	// Build edges adjacency list
	edgesMap, err := e.buildEdgesMap(edgeDefs)
	if err != nil {
		return nil, nil, err
	}

	// Find start block
	startBlockID, err := e.findStartBlock(blocks, edgesMap)
	if err != nil {
		return nil, nil, err
	}

	// Initialize execution state
	data := make(map[string]interface{})
	for k, v := range inputData {
		data[k] = v
	}

	// Create thread-safe data store
	safeData := &syncMap{
		data: data,
	}

	// Create thread-safe output and execution path
	output := &syncMap{data: make(map[string]interface{})}
	var executionMu sync.Mutex
	executionPath := make([]string, 0)

	// Create wait group to track execution
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	doneCh := make(chan struct{})

	// Start execution
	wg.Add(1)
	go func() {
		err := e.executeBlockParallel(
			ctx,
			startBlockID,
			blocks,
			edgesMap,
			safeData,
			output,
			&executionPath,
			&executionMu,
			&wg,
			errCh,
			0,
			options.MaxIterations,
		)
		if err != nil {
			select {
			case errCh <- err:
			default:
				// Another error was already sent
			}
		}
		wg.Done()
	}()

	// Wait for completion or error
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case err := <-errCh:
		return output.getAll(), executionPath, err
	case <-doneCh:
		return output.getAll(), executionPath, nil
	case <-ctx.Done():
		return output.getAll(), executionPath, ErrExecutionTimeout
	}
}

// syncMap provides thread-safe access to a map
type syncMap struct {
	mu   sync.RWMutex
	data map[string]interface{}
}

func (m *syncMap) get(key string) (interface{}, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, exists := m.data[key]
	return value, exists
}

func (m *syncMap) getAll() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[string]interface{})
	for k, v := range m.data {
		result[k] = v
	}
	return result
}

func (m *syncMap) set(key string, value interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
}

func (m *syncMap) setAll(data map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range data {
		m.data[k] = v
	}
}

// executeBlockParallel processes blocks in parallel where possible
func (e *Executor) executeBlockParallel(
	ctx context.Context,
	blockID string,
	blocks map[string]Block,
	edgesMap map[string][]string,
	data *syncMap,
	output *syncMap,
	executionPath *[]string,
	executionMu *sync.Mutex,
	wg *sync.WaitGroup,
	errCh chan error,
	currentDepth int,
	maxIterations int,
) error {
	// Check for context cancellation
	select {
	case <-ctx.Done():
		return ErrExecutionTimeout
	default:
		// Continue execution
	}

	// Check max iterations
	if currentDepth >= maxIterations {
		return ErrMaxIterations
	}

	// Get block to execute
	block, exists := blocks[blockID]
	if !exists {
		return fmt.Errorf("%w: %s", ErrBlockNotFound, blockID)
	}

	// Record execution path
	executionMu.Lock()
	*executionPath = append(*executionPath, blockID)
	executionMu.Unlock()

	// Execute block with a copy of the current data
	blockData := data.getAll()
	blockOutput, err := block.Execute(ctx, blockData)
	if err != nil {
		return err
	}

	// Update data and output
	data.setAll(blockOutput)
	output.setAll(blockOutput)

	// Find next blocks
	nextBlocks, hasNext := edgesMap[blockID]
	if !hasNext || len(nextBlocks) == 0 {
		// End of workflow path
		return nil
	}

	// Execute next blocks in parallel if there are multiple
	if len(nextBlocks) > 1 {
		var nextWg sync.WaitGroup
		for _, nextBlockID := range nextBlocks {
			nextBlockID := nextBlockID // Create local variable for goroutine
			nextWg.Add(1)
			wg.Add(1)

			go func() {
				defer nextWg.Done()
				err := e.executeBlockParallel(
					ctx,
					nextBlockID,
					blocks,
					edgesMap,
					data,
					output,
					executionPath,
					executionMu,
					wg,
					errCh,
					currentDepth+1,
					maxIterations,
				)
				if err != nil {
					select {
					case errCh <- err:
					default:
						// Another error was already sent
					}
				}
				wg.Done()
			}()
		}
		nextWg.Wait()
	} else if len(nextBlocks) == 1 {
		// Execute single next block in the same goroutine
		return e.executeBlockParallel(
			ctx,
			nextBlocks[0],
			blocks,
			edgesMap,
			data,
			output,
			executionPath,
			executionMu,
			wg,
			errCh,
			currentDepth+1,
			maxIterations,
		)
	}

	return nil
}

// Sample block implementations for common use cases

// StartBlock is a simple starting point for workflows
type StartBlock struct {
	BasicBlock
}

func (b *StartBlock) Execute(ctx context.Context, data map[string]interface{}) (map[string]interface{}, error) {
	// Start blocks typically don't modify data
	return map[string]interface{}{
		"__workflow_start_time": time.Now(),
	}, nil
}

// EndBlock marks the end of a workflow path
type EndBlock struct {
	BasicBlock
	Status string
}

func (b *EndBlock) Execute(ctx context.Context, data map[string]interface{}) (map[string]interface{}, error) {
	return map[string]interface{}{
		"__workflow_status":   b.Status,
		"__workflow_end_time": time.Now(),
	}, nil
}

// LogicBlock executes custom logic
type LogicBlock struct {
	BasicBlock
	Logic func(ctx context.Context, data map[string]interface{}) (map[string]interface{}, error)
}

func (b *LogicBlock) Execute(ctx context.Context, data map[string]interface{}) (map[string]interface{}, error) {
	return b.Logic(ctx, data)
}

// ConditionBlock evaluates conditions to determine execution path
type ConditionBlock struct {
	BasicBlock
	Condition func(ctx context.Context, data map[string]interface{}) (bool, error)
	// Paths are handled by edge definitions
}

func (b *ConditionBlock) Execute(ctx context.Context, data map[string]interface{}) (map[string]interface{}, error) {
	result, err := b.Condition(ctx, data)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"__condition_result_" + b.ID: result,
	}, nil
}

// TransformBlock applies transformations to data
type TransformBlock struct {
	BasicBlock
	Fields       []string
	Transformer  func(value interface{}) (interface{}, error)
	OutputPrefix string
}

func (b *TransformBlock) Execute(ctx context.Context, data map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	for _, field := range b.Fields {
		if value, exists := data[field]; exists {
			transformed, err := b.Transformer(value)
			if err != nil {
				return nil, fmt.Errorf("transform error on field %s: %w", field, err)
			}

			outputKey := field
			if b.OutputPrefix != "" {
				outputKey = b.OutputPrefix + "_" + field
			}

			result[outputKey] = transformed
		}
	}

	return result, nil
}

// Helper functions to create standard blocks

func NewStartBlock(id string) Block {
	return &StartBlock{
		BasicBlock: BasicBlock{
			ID:   id,
			Type: "start",
		},
	}
}

func NewEndBlock(id string, status string) Block {
	return &EndBlock{
		BasicBlock: BasicBlock{
			ID:   id,
			Type: "end",
		},
		Status: status,
	}
}

func NewLogicBlock(id string, logic func(ctx context.Context, data map[string]interface{}) (map[string]interface{}, error)) Block {
	return &LogicBlock{
		BasicBlock: BasicBlock{
			ID:   id,
			Type: "logic",
		},
		Logic: logic,
	}
}

func NewConditionBlock(id string, condition func(ctx context.Context, data map[string]interface{}) (bool, error)) Block {
	return &ConditionBlock{
		BasicBlock: BasicBlock{
			ID:   id,
			Type: "condition",
		},
		Condition: condition,
	}
}

func NewTransformBlock(id string, fields []string, transformer func(value interface{}) (interface{}, error), outputPrefix string) Block {
	return &TransformBlock{
		BasicBlock: BasicBlock{
			ID:   id,
			Type: "transform",
		},
		Fields:       fields,
		Transformer:  transformer,
		OutputPrefix: outputPrefix,
	}
}
