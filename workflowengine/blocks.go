package workflowengine

import (
	"encoding/json"
	"fmt"
	"strings"
)

var (
	errNum1NotFound    = fmt.Errorf("num1 not found in input")
	errNum2NotFound    = fmt.Errorf("num2 not found in input")
	errTextNotFound    = fmt.Errorf("text not found in input")
	errInvalidNumType  = fmt.Errorf("num must be a number")
	errInvalidTextType = fmt.Errorf("text must be a string")
	errInvalidOpType   = fmt.Errorf("operation must be a string")
)

// Sample block functions for demonstration
// AddBlock adds two numbers from the input
func AddBlock(input, output map[string]interface{}) error {
	// Copy input to output (only when needed)
	for k, v := range input {
		output[k] = v
	}

	// Get the numbers to add
	num1Val, exists := input["num1"]
	if !exists {
		return errNum1NotFound
	}

	num1, err := toFloat64(num1Val)
	if err != nil {
		return err
	}

	num2Val, exists := input["num2"]
	if !exists {
		return errNum2NotFound
	}

	num2, err := toFloat64(num2Val)
	if err != nil {
		return err
	}

	// Perform addition
	output["result"] = num1 + num2

	return nil
}

// StringTransformBlock transforms strings in the input
func StringTransformBlock(input, output map[string]interface{}) error {
	// Copy input to output (only when needed)
	for k, v := range input {
		output[k] = v
	}

	// Get the string to transform
	textVal, exists := input["text"]
	if !exists {
		return errTextNotFound
	}

	text, ok := textVal.(string)
	if !ok {
		return errInvalidTextType
	}

	// Get the transform operation
	operation := "uppercase" // Default
	if opVal, exists := input["operation"]; exists {
		var ok bool
		operation, ok = opVal.(string)
		if !ok {
			return errInvalidOpType
		}
	}

	// Perform the transformation (switch once instead of multiple string comparisons)
	var transformedText string
	switch operation {
	case "uppercase":
		transformedText = toUpperInPlace(text)
	case "lowercase":
		transformedText = toLowerInPlace(text)
	case "reverse":
		transformedText = reverseInPlace(text)
	default:
		return fmt.Errorf("unsupported operation: %s", operation)
	}

	output["transformedText"] = transformedText
	return nil
}

// Helper function to convert to float64 more efficiently
func toFloat64(val interface{}) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case json.Number:
		// Parse once as float64 directly
		return v.Float64()
	default:
		return 0, errInvalidNumType
	}
}

// String manipulation functions optimized for performance
func toUpperInPlace(s string) string {
	if s == "" {
		return s
	}

	// For small strings, just use the standard library
	if len(s) < 64 {
		return strings.ToUpper(s)
	}

	// For larger strings, convert to []byte to avoid reallocations
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			b[i] = c - 32
		} else {
			b[i] = c
		}
	}
	return string(b)
}

func toLowerInPlace(s string) string {
	if s == "" {
		return s
	}

	// For small strings, just use the standard library
	if len(s) < 64 {
		return strings.ToLower(s)
	}

	// For larger strings, convert to []byte to avoid reallocations
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		} else {
			b[i] = c
		}
	}
	return string(b)
}

func reverseInPlace(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}
