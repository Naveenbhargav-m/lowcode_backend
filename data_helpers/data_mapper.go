package datahelpers

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

var refRegex = regexp.MustCompile(`^([a-zA-Z_]+)\["([^"]+)"\](?:\["([^"]+)"\])*$`)

// ResolveVariableReferences optimized for performance
func ResolveVariableReferences(global map[string]interface{},
	overallSchema map[string]interface{},
	input map[string]interface{}) (map[string]interface{}, error) {
	// Pre-compile the regex pattern once

	// Process the input map in-place to avoid unnecessary copying
	return processMap(input, global, overallSchema, refRegex)
}

// processMap handles map processing with minimal allocations
func processMap(inputMap map[string]interface{},
	global map[string]interface{},
	overallSchema map[string]interface{},
	refRegex *regexp.Regexp) (map[string]interface{}, error) {

	// Create result map only if changes are needed
	result := inputMap
	madeChanges := false

	for key, value := range inputMap {
		switch v := value.(type) {
		case string:
			// Only check strings that might be references (quick check before regex)
			if strings.Contains(v, "[") && strings.Contains(v, "]") && strings.Contains(v, "\"") {
				if refRegex.MatchString(v) {
					resolvedValue, err := resolveReference(v, global, overallSchema, refRegex)
					if err != nil {
						return nil, fmt.Errorf("error resolving reference for key %s: %w", key, err)
					}

					// Only create a new map if this is the first change
					if !madeChanges {
						madeChanges = true
						// Now we need to copy the map
						result = make(map[string]interface{}, len(inputMap))
						for k, v := range inputMap {
							result[k] = v
						}
					}
					result[key] = resolvedValue
				}
			}

		case map[string]interface{}:
			// Process nested map
			resolvedMap, err := processMap(v, global, overallSchema, refRegex)
			if err != nil {
				return nil, err
			}

			// We need to track if the processMap function modified the map
			// We can do this by checking if the madeChanges flag inside processMap was set
			// which would cause it to return a new map instance

			// Create a simple tracking variable to check if the returned map is different
			mapWasModified := false

			// If it's a different map reference, then changes were made
			if resolvedMap != nil && v != nil {
				// Compare pointers - if different instances (one is a new copy), then it was modified
				mapWasModified = !sameMapReference(resolvedMap, v)
			}

			if mapWasModified {
				// Only create a new map if this is the first change
				if !madeChanges {
					madeChanges = true
					result = make(map[string]interface{}, len(inputMap))
					for k, v := range inputMap {
						result[k] = v
					}
				}
				result[key] = resolvedMap
			}

		case []interface{}:
			// Process each item in the slice
			needSliceUpdate := false
			sliceCopy := v

			for i, item := range v {
				if itemMap, ok := item.(map[string]interface{}); ok {
					resolvedItem, err := processMap(itemMap, global, overallSchema, refRegex)
					if err != nil {
						return nil, err
					}

					// We can't directly compare maps with !=
					// Instead, check if processMap returned a different map reference
					itemMapWasModified := false

					if resolvedItem != nil && itemMap != nil {
						// Compare pointers to see if they're different instances
						itemMapWasModified = !sameMapReference(resolvedItem, itemMap)
					}

					if itemMapWasModified {
						// Copy the slice on first change
						if !needSliceUpdate {
							needSliceUpdate = true
							sliceCopy = make([]interface{}, len(v))
							copy(sliceCopy, v)
						}
						sliceCopy[i] = resolvedItem
					}
				}
			}

			// Only update the map if the slice changed
			if needSliceUpdate {
				// Only create a new map if this is the first change
				if !madeChanges {
					madeChanges = true
					result = make(map[string]interface{}, len(inputMap))
					for k, v := range inputMap {
						result[k] = v
					}
				}
				result[key] = sliceCopy
			}
		}
	}

	return result, nil
}

// Helper function to safely compare map references
// This checks if two maps are the same instance (same memory reference)
func sameMapReference(a, b map[string]interface{}) bool {
	// Convert the maps to interface{} values which can be compared with unsafe.Pointer
	// if we really want to check the exact memory address, but a simpler approach is to:

	// Use reflection for pointer comparison
	return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer()
}

func resolveReference(reference string, global, overallSchema map[string]interface{},
	refRegex *regexp.Regexp) (interface{}, error) {

	matches := refRegex.FindStringSubmatch(reference)
	if len(matches) < 3 {
		return reference, nil // Not a valid reference pattern
	}

	// Determine source map
	var sourceMap map[string]interface{}
	switch matches[1] {
	case "state":
		sourceMap = global
	case "schema":
		sourceMap = overallSchema
	default:
		return reference, nil // Not a recognized source, return as is
	}

	// Navigate through nested keys more efficiently
	var current interface{} = sourceMap

	// Explicit navigation for first key which we know exists
	firstKey := matches[2]
	if currentMap, ok := current.(map[string]interface{}); ok {
		if val, exists := currentMap[firstKey]; exists {
			current = val
		} else {
			return nil, fmt.Errorf("key '%s' not found", firstKey)
		}
	} else {
		return nil, fmt.Errorf("expected map at root but got %T", current)
	}

	// Now handle any additional keys
	// Extract remaining keys using direct string operations instead of regex
	remaining := reference[len(matches[1])+2+len(firstKey)+2:] // Skip past prefix["firstkey"]

	// Fast path for common case with exactly one more key level
	if strings.HasPrefix(remaining, "[\"") && strings.HasSuffix(remaining, "\"]") {
		secondKey := remaining[2 : len(remaining)-2]
		if currentMap, ok := current.(map[string]interface{}); ok {
			if val, exists := currentMap[secondKey]; exists {
				return val, nil
			}
			return nil, fmt.Errorf("key '%s' not found", secondKey)
		}
		return nil, fmt.Errorf("expected map at key '%s' but got %T", firstKey, current)
	}

	// Slower but more general path for multiple levels
	if len(remaining) > 0 {
		// Simple state machine to extract keys
		var key strings.Builder
		inKey := false

		for i := 0; i < len(remaining); i++ {
			char := remaining[i]

			if !inKey && char == '[' {
				// Skip the opening bracket and quote
				if i+2 < len(remaining) && remaining[i+1] == '"' {
					i += 1 // Skip to quote
					inKey = true
					key.Reset()
				}
			} else if inKey && char == '"' && i+1 < len(remaining) && remaining[i+1] == ']' {
				// End of key
				inKey = false
				i++ // Skip the closing bracket

				// Navigate to the next level
				if currentMap, ok := current.(map[string]interface{}); ok {
					nextKey := key.String()
					if val, exists := currentMap[nextKey]; exists {
						current = val
					} else {
						return nil, fmt.Errorf("key '%s' not found", nextKey)
					}
				} else {
					return nil, fmt.Errorf("expected map but got %T", current)
				}

			} else if inKey {
				key.WriteByte(char)
			}
		}
	}

	return current, nil
}
