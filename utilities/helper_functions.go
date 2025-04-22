package utilities

import (
	"encoding/json"
	"fmt"
)

// ConvertToType converts any interface{} to a specific struct type.
// `source` can be a map[string]interface{} or another struct.
// `target` must be a pointer to the target struct.
func ConvertToType(source interface{}, target interface{}) error {
	bytes, err := json.Marshal(source)
	if err != nil {
		return fmt.Errorf("failed to marshal source: %w", err)
	}

	err = json.Unmarshal(bytes, target)
	if err != nil {
		return fmt.Errorf("failed to unmarshal into target: %w", err)
	}

	return nil
}
