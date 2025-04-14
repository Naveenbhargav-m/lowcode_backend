package appcrud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// QueryParams holds the parsed query parameters for database operations
type QueryParams struct {
	Fields       []string                     // Selected fields with optional aliases
	Filters      []Filter                     // Filter conditions
	Joins        map[string]string            // Join conditions
	OrderBy      []OrderByClause              // Order by clauses
	Limit        int                          // Limit results
	Offset       int                          // Offset results
	GroupBy      []string                     // Group by fields
	Having       []string                     // Having conditions
	Aggregations map[string]AggregationClause // Aggregate functions with aliases
}

// Filter represents a filter condition with support for AND/OR logic
type Filter struct {
	Conditions []Condition // List of conditions to be combined
	Operator   string      // "AND" or "OR" - default is "AND"
}

// Condition represents a single filter condition
type Condition struct {
	Field    string      // Field name
	Operator string      // Comparison operator (=, >, <, !=, etc.)
	Value    interface{} // Value to compare against
}

// OrderByClause represents an order by directive
type OrderByClause struct {
	Field     string // Field name
	Direction string // ASC or DESC
}

// AggregationClause represents an aggregation function
type AggregationClause struct {
	Function string // Aggregation function (SUM, AVG, COUNT, etc.)
	Field    string // Field to aggregate
	Alias    string // Result alias
}

// HandleDatabaseRequest is the main handler for dynamic database operations
func HandleDatabaseRequest(w http.ResponseWriter, r *http.Request) {
	// Get database connection from context (set by middleware)
	temp := r.Context().Value("db_conn")
	db, ok := temp.(*pgxpool.Pool)
	if !ok {
		http.Error(w, "Database connection not found in context", http.StatusInternalServerError)
		return
	}

	// Extract path parameters
	schema := r.PathValue("schema")
	table := r.PathValue("table_name")

	// Validate schema and table to prevent SQL injection
	if !isValidIdentifier(schema) || !isValidIdentifier(table) {
		http.Error(w, "Invalid schema or table name", http.StatusBadRequest)
		return
	}

	// Parse query parameters for filtering, field selection, etc.
	params, err := parseQueryParams(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error parsing query parameters: %v", err), http.StatusBadRequest)
		return
	}

	var result interface{}

	// Process request based on HTTP method
	switch r.Method {
	case http.MethodGet:
		result, err = handleRead(r.Context(), db, schema, table, params)
	case http.MethodPost:
		result, err = handleCreate(r.Context(), db, schema, table, r)
	case http.MethodPut:
		result, err = handleUpdate(r.Context(), db, schema, table, r, params)
	case http.MethodDelete:
		result, err = handleDelete(r.Context(), db, schema, table, params)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err != nil {
		log.Printf("Error processing request: %v", err)
		statusCode := http.StatusInternalServerError

		// Check for specific error types to return appropriate status codes
		if errors.Is(err, pgx.ErrNoRows) {
			statusCode = http.StatusNotFound
		} else if strings.Contains(err.Error(), "permission denied") {
			statusCode = http.StatusForbidden
		} else if strings.Contains(err.Error(), "validation") {
			statusCode = http.StatusBadRequest
		}

		http.Error(w, fmt.Sprintf("Error: %v", err), statusCode)
		return
	}

	// Return the result as JSON
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		log.Printf("Error encoding response: %v", err)
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
	}
}

// parseQueryParams parses URL query parameters into QueryParams structure
func parseQueryParams(r *http.Request) (QueryParams, error) {
	params := QueryParams{
		Fields:       []string{},
		Filters:      []Filter{},
		Joins:        make(map[string]string),
		OrderBy:      []OrderByClause{},
		GroupBy:      []string{},
		Having:       []string{},
		Aggregations: make(map[string]AggregationClause),
	}

	// Parse selected fields and aggregations
	if selectParam := r.URL.Query().Get("select"); selectParam != "" {
		fields := strings.Split(selectParam, ",")
		for _, field := range fields {
			field = strings.TrimSpace(field)

			// Check if this is an aggregation with alias
			if strings.Contains(field, ":") {
				parts := strings.Split(field, ":")
				fieldExpr := strings.TrimSpace(parts[0])
				alias := strings.TrimSpace(parts[1])

				// Check if this is an aggregation function
				if matches := regexp.MustCompile(`(\w+)\(([^)]+)\)`).FindStringSubmatch(fieldExpr); len(matches) > 0 {
					function := matches[1]
					aggField := matches[2]
					params.Aggregations[alias] = AggregationClause{
						Function: function,
						Field:    aggField,
						Alias:    alias,
					}
				} else {
					// Just a field with alias
					params.Fields = append(params.Fields, fmt.Sprintf("%s AS %s", fieldExpr, alias))
				}
			} else if matches := regexp.MustCompile(`(\w+)\(([^)]+)\)`).FindStringSubmatch(field); len(matches) > 0 {
				// Aggregation without alias
				function := matches[1]
				aggField := matches[2]
				alias := strings.ToLower(function) + "_" + strings.ToLower(aggField)
				params.Aggregations[alias] = AggregationClause{
					Function: function,
					Field:    aggField,
					Alias:    alias,
				}
			} else {
				// Regular field
				params.Fields = append(params.Fields, field)
			}
		}
	}

	// Parse where conditions
	if whereParam := r.URL.Query().Get("where"); whereParam != "" {
		filters, err := parseWhereClause(whereParam)
		if err != nil {
			return params, fmt.Errorf("invalid where clause: %v", err)
		}
		params.Filters = filters
	}

	// Parse order by
	if orderParam := r.URL.Query().Get("order_by"); orderParam != "" {
		orderClauses := strings.Split(orderParam, ",")
		for _, clause := range orderClauses {
			clause = strings.TrimSpace(clause)
			direction := "ASC"

			if strings.HasPrefix(clause, "-") {
				direction = "DESC"
				clause = strings.TrimPrefix(clause, "-")
			}

			params.OrderBy = append(params.OrderBy, OrderByClause{
				Field:     clause,
				Direction: direction,
			})
		}
	}

	// Parse limit
	if limitParam := r.URL.Query().Get("limit"); limitParam != "" {
		limit, err := strconv.Atoi(limitParam)
		if err != nil || limit < 0 {
			return params, fmt.Errorf("invalid limit parameter: %v", limitParam)
		}
		params.Limit = limit
	}

	// Parse offset
	if offsetParam := r.URL.Query().Get("offset"); offsetParam != "" {
		offset, err := strconv.Atoi(offsetParam)
		if err != nil || offset < 0 {
			return params, fmt.Errorf("invalid offset parameter: %v", offsetParam)
		}
		params.Offset = offset
	}

	// Parse group by
	if groupByParam := r.URL.Query().Get("group_by"); groupByParam != "" {
		groupFields := strings.Split(groupByParam, ",")
		for _, field := range groupFields {
			params.GroupBy = append(params.GroupBy, strings.TrimSpace(field))
		}
	}

	// Parse having clause
	if havingParam := r.URL.Query().Get("having"); havingParam != "" {
		havingClauses := strings.Split(havingParam, ",")
		for _, clause := range havingClauses {
			params.Having = append(params.Having, strings.TrimSpace(clause))
		}
	}

	// Parse joins
	if joinParam := r.URL.Query().Get("join"); joinParam != "" {
		joins := strings.Split(joinParam, ",")
		for _, join := range joins {
			parts := strings.Split(join, ":")
			if len(parts) == 2 {
				table := strings.TrimSpace(parts[0])
				condition := strings.TrimSpace(parts[1])
				params.Joins[table] = condition
			}
		}
	}

	return params, nil
}

// parseWhereClause parses complex where conditions with support for AND/OR logic
func parseWhereClause(whereClause string) ([]Filter, error) {
	var filters []Filter

	// Handle empty where clause
	whereClause = strings.TrimSpace(whereClause)
	if whereClause == "" {
		return filters, nil
	}

	// First, check if the entire clause is wrapped in brackets
	if strings.HasPrefix(whereClause, "[") && strings.HasSuffix(whereClause, "]") {
		// Remove outermost brackets
		innerContent := whereClause[1 : len(whereClause)-1]

		// Try to parse as a complex expression with .or. at the top level
		orFilters, err := parseComplexExpression(innerContent, ".or.")
		if err != nil {
			return nil, err
		}

		if len(orFilters.Conditions) > 0 {
			// We successfully parsed as an OR expression
			filters = append(filters, orFilters)
			return filters, nil
		}

		// Try to parse as a complex expression with .and. at the top level
		andFilters, err := parseComplexExpression(innerContent, ".and.")
		if err != nil {
			return nil, err
		}

		if len(andFilters.Conditions) > 0 {
			// We successfully parsed as an AND expression
			filters = append(filters, andFilters)
			return filters, nil
		}

		// If it's not a complex expression, try to parse as a single condition
		condition, err := parseCondition(innerContent)
		if err != nil {
			return nil, err
		}

		filter := Filter{
			Operator:   "AND", // Default operator
			Conditions: []Condition{condition},
		}
		filters = append(filters, filter)
	} else {
		// Split by comma for top-level AND conditions if not enclosed in brackets
		andGroups := strings.Split(whereClause, ",")

		for _, andGroup := range andGroups {
			andGroup = strings.TrimSpace(andGroup)

			// Skip empty groups
			if andGroup == "" {
				continue
			}

			// Parse the AND group
			condition, err := parseCondition(andGroup)
			if err != nil {
				return nil, err
			}

			filter := Filter{
				Operator:   "AND",
				Conditions: []Condition{condition},
			}

			filters = append(filters, filter)
		}
	}

	return filters, nil
}

// parseComplexExpression parses an expression with the specified delimiter (.and. or .or.)
// taking nested brackets into account
func parseComplexExpression(expr string, delimiter string) (Filter, error) {
	parts := tokenizeExpression(expr, delimiter)

	// If there's only one part, it's not the expected type of expression
	if len(parts) <= 1 {
		return Filter{}, nil
	}

	var operator string
	if delimiter == ".and." {
		operator = "AND"
	} else {
		operator = "OR"
	}

	filter := Filter{
		Operator:   operator,
		Conditions: []Condition{},
	}

	for _, part := range parts {
		part = strings.TrimSpace(part)

		// Skip empty parts
		if part == "" {
			continue
		}

		// Handle nested groups
		if strings.HasPrefix(part, "[") && strings.HasSuffix(part, "]") {
			// Recursively parse the nested group
			nestedFilters, err := parseWhereClause(part)
			if err != nil {
				return Filter{}, err
			}

			// For each nested filter, add all its conditions
			for _, nf := range nestedFilters {
				for _, cond := range nf.Conditions {
					filter.Conditions = append(filter.Conditions, cond)
				}
			}
		} else if strings.Contains(part, ".and.") {
			// This is an AND expression
			andFilter, err := parseComplexExpression(part, ".and.")
			if err != nil {
				return Filter{}, err
			}

			if len(andFilter.Conditions) > 0 {
				// Create a nested filter
				nestedFilter := Filter{
					Operator:   "AND",
					Conditions: andFilter.Conditions,
				}
				// Add as a single combined condition
				condition := Condition{
					Field:    "_nested",
					Operator: "AND",
					Value:    nestedFilter,
				}
				filter.Conditions = append(filter.Conditions, condition)
			}
		} else if strings.Contains(part, ".or.") {
			// This is an OR expression
			orFilter, err := parseComplexExpression(part, ".or.")
			if err != nil {
				return Filter{}, err
			}

			if len(orFilter.Conditions) > 0 {
				// Create a nested filter
				nestedFilter := Filter{
					Operator:   "OR",
					Conditions: orFilter.Conditions,
				}
				// Add as a single combined condition
				condition := Condition{
					Field:    "_nested",
					Operator: "OR",
					Value:    nestedFilter,
				}
				filter.Conditions = append(filter.Conditions, condition)
			}
		} else {
			// Parse as a simple condition
			condition, err := parseCondition(part)
			if err != nil {
				return Filter{}, err
			}
			filter.Conditions = append(filter.Conditions, condition)
		}
	}

	return filter, nil
}

// tokenizeExpression splits an expression by delimiter, respecting brackets
func tokenizeExpression(expr string, delimiter string) []string {
	var result []string
	var currentToken strings.Builder
	bracketLevel := 0

	i := 0
	for i < len(expr) {
		// Check if we've found the delimiter at the current position
		if bracketLevel == 0 && i+len(delimiter) <= len(expr) && expr[i:i+len(delimiter)] == delimiter {
			// Add the current token to the result
			result = append(result, currentToken.String())
			// Reset the current token
			currentToken.Reset()
			// Skip the delimiter
			i += len(delimiter)
		} else {
			// Handle brackets
			if expr[i] == '[' {
				bracketLevel++
			} else if expr[i] == ']' {
				bracketLevel--
			}

			// Add the current character to the token
			currentToken.WriteByte(expr[i])
			i++
		}
	}

	// Add the last token if there's anything
	if currentToken.Len() > 0 {
		result = append(result, currentToken.String())
	}

	return result
}

// parseCondition parses a single condition like "field=value" or "field>=value"
func parseCondition(condStr string) (Condition, error) {
	// Match different operators: =, !=, >, <, >=, <=, LIKE, IN
	operatorPattern := regexp.MustCompile(`(.*?)(=|!=|>=|<=|>|<|LIKE|IN)(.*)`)
	matches := operatorPattern.FindStringSubmatch(condStr)

	if len(matches) != 4 {
		return Condition{}, fmt.Errorf("invalid condition format: %s", condStr)
	}

	field := strings.TrimSpace(matches[1])
	operator := strings.TrimSpace(matches[2])
	valueStr := strings.TrimSpace(matches[3])

	// Process the value based on operator
	var value interface{}

	// Handle the IN operator specially
	if operator == "IN" && strings.HasPrefix(valueStr, "(") && strings.HasSuffix(valueStr, ")") {
		// Remove parentheses and split by comma
		valueStr = valueStr[1 : len(valueStr)-1]
		values := strings.Split(valueStr, ",")

		// Clean up each value
		cleanValues := make([]string, 0, len(values))
		for _, v := range values {
			cleanValues = append(cleanValues, strings.TrimSpace(v))
		}

		value = cleanValues
	} else {
		// Try to convert to int or float if possible
		if intVal, err := strconv.Atoi(valueStr); err == nil {
			value = intVal
		} else if floatVal, err := strconv.ParseFloat(valueStr, 64); err == nil {
			value = floatVal
		} else if valueStr == "true" || valueStr == "false" {
			value = valueStr == "true"
		} else if valueStr == "null" {
			value = nil
		} else {
			// Handle date format (YYYY-MM-DD)
			if datePattern := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`); datePattern.MatchString(valueStr) {
				value = valueStr // Keep as string, will be converted in SQL
			} else {
				// Treat as string, removing quotes if present
				if (strings.HasPrefix(valueStr, "'") && strings.HasSuffix(valueStr, "'")) ||
					(strings.HasPrefix(valueStr, "\"") && strings.HasSuffix(valueStr, "\"")) {
					value = valueStr[1 : len(valueStr)-1]
				} else {
					value = valueStr
				}
			}
		}
	}

	return Condition{
		Field:    field,
		Operator: operator,
		Value:    value,
	}, nil
}

// handleRead processes GET requests to retrieve data
func handleRead(ctx context.Context, db *pgxpool.Pool, schema, table string, params QueryParams) (interface{}, error) {
	query, args := buildSelectQuery(schema, table, params)

	log.Printf("Executing query: %s with args: %v", query, args)

	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()

	// Parse the rows into a slice of maps
	result := make([]map[string]interface{}, 0)

	// Get column descriptions
	fieldDescriptions := rows.FieldDescriptions()
	columnNames := make([]string, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		columnNames[i] = string(fd.Name)
	}

	// Iterate through the rows
	for rows.Next() {
		values := make([]interface{}, len(columnNames))
		valuePtrs := make([]interface{}, len(columnNames))

		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}

		row := make(map[string]interface{})
		for i, colName := range columnNames {
			row[colName] = values[i]
		}

		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	// If we have a limit=1, return a single object instead of an array with one element
	if params.Limit == 1 && len(result) == 1 {
		return result[0], nil
	}

	return result, nil
}

// handleCreate processes POST requests to insert new records
func handleCreate(ctx context.Context, db *pgxpool.Pool, schema, table string, r *http.Request) (interface{}, error) {
	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading request body: %w", err)
	}

	// Determine if we're dealing with a single object or an array
	var data interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("error parsing JSON: %w", err)
	}

	// Handle both single object and array of objects
	switch v := data.(type) {
	case map[string]interface{}:
		// Single object
		return insertRecord(ctx, db, schema, table, v)
	case []interface{}:
		// Array of objects
		records := make([]map[string]interface{}, 0, len(v))
		for _, item := range v {
			if record, ok := item.(map[string]interface{}); ok {
				result, err := insertRecord(ctx, db, schema, table, record)
				if err != nil {
					return nil, err
				}
				if resultMap, ok := result.(map[string]interface{}); ok {
					records = append(records, resultMap)
				}
			} else {
				return nil, fmt.Errorf("invalid item in array: %v", item)
			}
		}
		return records, nil
	default:
		return nil, fmt.Errorf("invalid JSON data format")
	}
}

// insertRecord inserts a single record into the database
func insertRecord(
	ctx context.Context,
	db *pgxpool.Pool,
	schema, table string,
	record map[string]interface{},
) (interface{}, error) {
	// Extract column names and values
	columns := make([]string, 0, len(record))
	values := make([]interface{}, 0, len(record))
	placeholders := make([]string, 0, len(record))

	i := 1
	for col, val := range record {
		// Skip null values
		if val == nil {
			continue
		}

		// Validate column name to prevent SQL injection
		if !isValidIdentifier(col) {
			return nil, fmt.Errorf("invalid column name: %s", col)
		}

		columns = append(columns, col)
		values = append(values, val)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		i++
	}

	// Build and execute query
	query := fmt.Sprintf(
		"INSERT INTO \"%s\".\"%s\" (\"%s\") VALUES (%s) RETURNING *",
		schema, table, strings.Join(columns, "\", \""), strings.Join(placeholders, ", "),
	)

	log.Printf("Executing query: %s with values: %v", query, values)

	row, err := db.Query(ctx, query, values...)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}
	// Parse the returned row into a map
	fieldDescriptions := row.FieldDescriptions()
	returnedColumns := make([]string, len(fieldDescriptions))
	returnedValues := make([]interface{}, len(fieldDescriptions))
	returnedValuePtrs := make([]interface{}, len(fieldDescriptions))

	for i, fd := range fieldDescriptions {
		returnedColumns[i] = string(fd.Name)
		returnedValuePtrs[i] = &returnedValues[i]
	}

	if err := row.Scan(returnedValuePtrs...); err != nil {
		return nil, fmt.Errorf("error scanning inserted row: %w", err)
	}

	result := make(map[string]interface{})
	for i, colName := range returnedColumns {
		result[colName] = returnedValues[i]
	}

	return result, nil
}

// handleUpdate processes PUT requests to update existing records
func handleUpdate(
	ctx context.Context,
	db *pgxpool.Pool,
	schema, table string,
	r *http.Request,
	params QueryParams,
) (interface{}, error) {
	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading request body: %w", err)
	}

	// Parse the update data
	var updateData map[string]interface{}
	if err := json.Unmarshal(body, &updateData); err != nil {
		return nil, fmt.Errorf("error parsing JSON: %w", err)
	}

	// Build update query
	query, args := buildUpdateQuery(schema, table, updateData, params)

	log.Printf("Executing query: %s with args: %v", query, args)

	// Execute the update
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error executing update: %w", err)
	}
	defer rows.Close()

	// Parse the returned rows into a slice of maps
	result := make([]map[string]interface{}, 0)

	// Get column descriptions
	fieldDescriptions := rows.FieldDescriptions()
	columnNames := make([]string, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		columnNames[i] = string(fd.Name)
	}

	// Iterate through the rows
	for rows.Next() {
		values := make([]interface{}, len(columnNames))
		valuePtrs := make([]interface{}, len(columnNames))

		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}

		row := make(map[string]interface{})
		for i, colName := range columnNames {
			row[colName] = values[i]
		}

		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	// If no rows were updated, return an error
	if len(result) == 0 {
		return nil, fmt.Errorf("no records found matching the filter criteria")
	}

	return result, nil
}

// handleDelete processes DELETE requests to remove records
func handleDelete(
	ctx context.Context,
	db *pgxpool.Pool,
	schema, table string,
	params QueryParams,
) (interface{}, error) {
	// Build delete query
	query, args := buildDeleteQuery(schema, table, params)

	log.Printf("Executing query: %s with args: %v", query, args)

	// Execute the delete
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error executing delete: %w", err)
	}
	defer rows.Close()

	// Parse the returned rows into a slice of maps
	result := make([]map[string]interface{}, 0)

	// Get column descriptions
	fieldDescriptions := rows.FieldDescriptions()
	columnNames := make([]string, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		columnNames[i] = string(fd.Name)
	}

	// Iterate through the rows
	for rows.Next() {
		values := make([]interface{}, len(columnNames))
		valuePtrs := make([]interface{}, len(columnNames))

		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}

		row := make(map[string]interface{})
		for i, colName := range columnNames {
			row[colName] = values[i]
		}

		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	// If no rows were deleted, return an error
	if len(result) == 0 {
		return nil, fmt.Errorf("no records found matching the filter criteria")
	}

	return result, nil
}

// buildSelectQuery builds a SELECT query with the given parameters
func buildSelectQuery(schema, table string, params QueryParams) (string, []interface{}) {
	var queryBuilder strings.Builder
	var args []interface{}
	var argIndex int = 1

	// Start building the query
	queryBuilder.WriteString("SELECT ")

	// Add fields
	if len(params.Fields) == 0 && len(params.Aggregations) == 0 {
		queryBuilder.WriteString("*")
	} else {
		var fieldClauses []string

		// Add regular fields
		for _, field := range params.Fields {
			fieldClauses = append(fieldClauses, field)
		}

		// Add aggregation functions
		for _, agg := range params.Aggregations {
			aggClause := fmt.Sprintf("%s(%s) AS %s", agg.Function, agg.Field, agg.Alias)
			fieldClauses = append(fieldClauses, aggClause)
		}

		queryBuilder.WriteString(strings.Join(fieldClauses, ", "))
	}

	// Add FROM clause
	queryBuilder.WriteString(fmt.Sprintf(" FROM \"%s\".\"%s\"", schema, table))

	// Add JOIN clauses
	for joinTable, joinCondition := range params.Joins {
		if !isValidIdentifier(joinTable) {
			continue // Skip invalid identifiers
		}
		queryBuilder.WriteString(fmt.Sprintf(" JOIN \"%s\" ON %s", joinTable, joinCondition))
	}

	// Add WHERE clause
	if len(params.Filters) > 0 {
		whereClause, whereArgs, newArgIndex := buildWhereClause(params.Filters, argIndex)
		if whereClause != "" {
			queryBuilder.WriteString(" ")
			queryBuilder.WriteString(whereClause)
			args = append(args, whereArgs...)
			argIndex = newArgIndex
		}
	}

	// Add GROUP BY clause
	if len(params.GroupBy) > 0 {
		queryBuilder.WriteString(" GROUP BY ")
		queryBuilder.WriteString(strings.Join(params.GroupBy, ", "))
	}

	// Add HAVING clause
	if len(params.Having) > 0 {
		queryBuilder.WriteString(" HAVING ")

		for i, havingClause := range params.Having {
			if i > 0 {
				queryBuilder.WriteString(" AND ")
			}

			// Replace function calls with placeholders
			pattern := regexp.MustCompile(`([A-Za-z0-9_]+)\(([^)]+)\)([><=!]+)([0-9.]+)`)
			replaced := pattern.ReplaceAllStringFunc(havingClause, func(match string) string {
				submatches := pattern.FindStringSubmatch(match)
				if len(submatches) < 5 {
					return match
				}

				function := submatches[1]
				field := submatches[2]
				operator := submatches[3]
				value := submatches[4]

				// Try to convert value to numeric type
				if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
					args = append(args, floatVal)
					return fmt.Sprintf("%s(%s) %s $%d", function, field, operator, argIndex)
				}

				args = append(args, value)
				return fmt.Sprintf("%s(%s) %s $%d", function, field, operator, argIndex)
			})

			queryBuilder.WriteString(replaced)
			argIndex++
		}
	}

	// Add ORDER BY clause
	if len(params.OrderBy) > 0 {
		queryBuilder.WriteString(" ORDER BY ")

		orderClauses := make([]string, 0, len(params.OrderBy))
		for _, order := range params.OrderBy {
			orderClauses = append(orderClauses, fmt.Sprintf("%s %s", order.Field, order.Direction))
		}

		queryBuilder.WriteString(strings.Join(orderClauses, ", "))
	}

	// Add LIMIT clause
	if params.Limit > 0 {
		queryBuilder.WriteString(fmt.Sprintf(" LIMIT $%d", argIndex))
		args = append(args, params.Limit)
		argIndex++
	}

	// Add OFFSET clause
	if params.Offset > 0 {
		queryBuilder.WriteString(fmt.Sprintf(" OFFSET $%d", argIndex))
		args = append(args, params.Offset)
		argIndex++
	}

	return queryBuilder.String(), args
}

// buildUpdateQuery builds an UPDATE query with the given parameters
func buildUpdateQuery(
	schema, table string,
	updateData map[string]interface{},
	params QueryParams,
) (string, []interface{}) {
	var queryBuilder strings.Builder
	var args []interface{}
	var argIndex int = 1

	// Start building the query
	queryBuilder.WriteString(fmt.Sprintf("UPDATE \"%s\".\"%s\" SET ", schema, table))

	// Add SET clause
	setClauses := make([]string, 0, len(updateData))
	for field, value := range updateData {
		if !isValidIdentifier(field) {
			continue // Skip invalid identifiers
		}

		setClauses = append(setClauses, fmt.Sprintf("\"%s\" = $%d", field, argIndex))
		args = append(args, value)
		argIndex++
	}

	queryBuilder.WriteString(strings.Join(setClauses, ", "))

	// Add WHERE clause
	if len(params.Filters) > 0 {
		whereClause, whereArgs, newArgIndex := buildWhereClause(params.Filters, argIndex)
		if whereClause != "" {
			queryBuilder.WriteString(" WHERE ")
			queryBuilder.WriteString(whereClause)
			args = append(args, whereArgs...)
			argIndex = newArgIndex
		}
	}

	// Add RETURNING clause
	queryBuilder.WriteString(" RETURNING *")

	return queryBuilder.String(), args
}

// buildWhereClause constructs a SQL WHERE clause from a slice of filters
func buildWhereClause(filters []Filter, argIndex int) (string, []interface{}, int) {
	if len(filters) == 0 {
		return "", nil, argIndex
	}

	var args []interface{}
	var clauses []string

	for _, filter := range filters {
		if len(filter.Conditions) == 0 {
			continue
		}

		var conditionClauses []string
		for _, condition := range filter.Conditions {
			if !isValidIdentifier(condition.Field) {
				continue
			}

			placeholder := fmt.Sprintf("$%d", argIndex)
			argIndex++

			// Handle different operators
			var clause string
			switch condition.Operator {
			case "eq", "=", "==":
				clause = fmt.Sprintf("%s = %s", condition.Field, placeholder)
			case "neq", "!=", "<>":
				clause = fmt.Sprintf("%s != %s", condition.Field, placeholder)
			case "gt", ">":
				clause = fmt.Sprintf("%s > %s", condition.Field, placeholder)
			case "gte", ">=":
				clause = fmt.Sprintf("%s >= %s", condition.Field, placeholder)
			case "lt", "<":
				clause = fmt.Sprintf("%s < %s", condition.Field, placeholder)
			case "lte", "<=":
				clause = fmt.Sprintf("%s <= %s", condition.Field, placeholder)
			case "like":
				clause = fmt.Sprintf("%s LIKE %s", condition.Field, placeholder)
			case "ilike":
				clause = fmt.Sprintf("%s ILIKE %s", condition.Field, placeholder)
			case "in":
				// For IN operator, we handle array values
				clause = fmt.Sprintf("%s IN (%s)", condition.Field, placeholder)
			default:
				// Default to equality
				clause = fmt.Sprintf("%s = %s", condition.Field, placeholder)
			}

			conditionClauses = append(conditionClauses, clause)
			args = append(args, condition.Value)
		}

		if len(conditionClauses) > 0 {
			operator := "AND"
			if filter.Operator == "OR" {
				operator = "OR"
			}

			filterClause := strings.Join(conditionClauses, " "+operator+" ")
			if len(conditionClauses) > 1 {
				filterClause = "(" + filterClause + ")"
			}

			clauses = append(clauses, filterClause)
		}
	}

	if len(clauses) == 0 {
		return "", nil, argIndex
	}

	whereClause := "WHERE " + strings.Join(clauses, " AND ")
	return whereClause, args, argIndex
}

// isValidIdentifier checks if a string is a valid SQL identifier
func isValidIdentifier(identifier string) bool {
	if identifier == "" {
		return false
	}

	// Check if the identifier is quoted already
	if strings.HasPrefix(identifier, "\"") && strings.HasSuffix(identifier, "\"") {
		return true
	}

	// Basic validation for PostgreSQL identifiers
	// First character must be a letter or underscore
	if !unicode.IsLetter(rune(identifier[0])) && identifier[0] != '_' {
		return false
	}

	// Remaining characters must be letters, numbers, or underscores
	for i := 1; i < len(identifier); i++ {
		char := rune(identifier[i])
		if !unicode.IsLetter(char) && !unicode.IsDigit(char) && char != '_' && char != '.' {
			return false
		}
	}

	// Check for SQL keywords
	sqlKeywords := map[string]bool{
		"SELECT": true, "FROM": true, "WHERE": true, "INSERT": true,
		"UPDATE": true, "DELETE": true, "DROP": true, "CREATE": true,
		"TABLE": true, "ALTER": true, "INDEX": true, "VIEW": true,
		"GROUP": true, "ORDER": true, "BY": true, "HAVING": true,
		"JOIN": true, "INNER": true, "OUTER": true, "LEFT": true,
		"RIGHT": true, "FULL": true, "ON": true, "AS": true,
		"DISTINCT": true, "COUNT": true, "SUM": true, "AVG": true,
		"MIN": true, "MAX": true, "AND": true, "OR": true,
		"IN": true, "BETWEEN": true, "LIKE": true, "ILIKE": true,
		"LIMIT": true, "OFFSET": true, "NULL": true, "NOT": true,
	}

	if sqlKeywords[strings.ToUpper(identifier)] {
		return false
	}

	return true
}

// buildDeleteQuery constructs a DELETE SQL query
func buildDeleteQuery(schema, table string, params QueryParams) (string, []interface{}) {
	if !isValidIdentifier(schema) || !isValidIdentifier(table) {
		return "", nil
	}

	// Start building the query with the DELETE statement
	query := fmt.Sprintf("DELETE FROM %s.%s", schema, table)

	// Build the WHERE clause
	whereClause, whereArgs, _ := buildWhereClause(params.Filters, 1)

	if whereClause != "" {
		query += " " + whereClause
	}

	// Add RETURNING clause if there are fields to return
	if len(params.Fields) > 0 {
		returningFields := make([]string, 0, len(params.Fields))
		for _, field := range params.Fields {
			if isValidIdentifier(strings.Split(field, ":")[0]) {
				returningFields = append(returningFields, field)
			}
		}

		if len(returningFields) > 0 {
			query += " RETURNING " + strings.Join(returningFields, ", ")
		}
	}

	return query, whereArgs
}
