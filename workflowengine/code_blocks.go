package workflowengine

import (
	"context"
	"encoding/json"
	"strings"

	"rogchap.com/v8go"
	v8 "rogchap.com/v8go"
)

func JSBlock(ctx context.Context, dbconfigs, input, schema, output map[string]interface{}) error {
	myblock := GetCurBlockData(schema)
	configs := myblock.BlockConfig
	jscode, _ := configs["js_code"].(string)
	v8ctx := v8.NewContext()
	global := v8ctx.Global()
	// Convert Go maps to V8 values - optimize by only passing needed data
	// Only create necessary V8 values to minimize V8 GC pressure
	v8inp, err := GoToV8Value(v8ctx, input)
	if err != nil {
		return err
	}
	global.Set("inputs", v8inp)

	// For simple template processing, schema and output might not be needed
	// Only create them if your JS code actually uses them
	if strings.Contains(jscode, "schema") {
		v8Schema, err := GoToV8Value(v8ctx, schema)
		if err != nil {
			return err
		}
		global.Set("schema", v8Schema)
	}

	if strings.Contains(jscode, "outputs") {
		v8out, err := GoToV8Value(v8ctx, output)
		if err != nil {
			return err
		}
		global.Set("outputs", v8out)
	}

	resp, err := v8ctx.RunScript(jscode, "flow.js")
	if err != nil {
		return err
	}
	if resp != nil && !resp.IsUndefined() {
		// Process result if needed
		if len(output) > 0 {
			bytes, _ := resp.MarshalJSON()
			var result interface{}
			json.Unmarshal(bytes, &result)
			output["result"] = result
		}
	}

	return nil

}

func GoToV8Value(ctx *v8go.Context, value interface{}) (*v8go.Value, error) {
	bytes, _ := json.Marshal(value)
	val, err := v8go.JSONParse(ctx, string(bytes))
	return val, err
}
