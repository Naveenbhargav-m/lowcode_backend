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
	"time"
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
	IsNested bool        // Flag to indicate if this is a nested condition
	Nested   *Filter     // Pointer to a nested filter (if IsNested is true)
}

// OrderByClause represents an order by instruction
type OrderByClause struct {
	Field     string
	Direction string // ASC or DESC
}

// AggregationClause represents an aggregation function
type AggregationClause struct {
	Function string
	Field    string
	Alias    string
}

// HandleDatabaseRequest is the main handler for dynamic database operations
//
// // Database API Endpoints
//
// Base URL pattern: /{schema}/{table_name}
//
// Supported methods:
//   - GET: Retrieve records
//   - POST: Create new records
//   - PUT/PATCH: Update existing records
//   - DELETE: Remove records
//
// Query Parameters:
//
//   - select: Specifies fields to return
//     Format: ?select=field1,field2,alias:field3,COUNT(field):count_alias
//     Example: ?select=id,name,total:price*quantity,COUNT(id):count
//
//   - where: Filters results with conditions
//     Format: ?where=field=value,field2!=value2
//     Complex format: ?where=[condition1.and.condition2.or.condition3]
//     Operators: =, !=, >, <, >=, <=, LIKE, IN
//     Example: ?where=age>=18,status=active
//     Example: ?where=[name=John.and.age>30]
//     Example: ?where=[status=active.or.[age>30.and.role=admin]]
//     Example: ?where=field1=value1,field2!=value2
//     Example: ?where=age>20.and.name=John
//     Example: ?where=[status=active.or.status=pending]
//     Example: ?where=[created_at>2023-01-01.and.[status=active.or.priority>3]]
//
//   - order_by: Sorts results
//     Format: ?order_by=field1,-field2
//     Prefix with - for descending order
//     Example: ?order_by=created_at,-priority
//
//   - limit: Restricts number of returned records
//     Format: ?limit=N
//     Example: ?limit=100
//
//   - offset: Skips N records
//     Format: ?offset=N
//     Example: ?offset=50
//
//   - group_by: Groups results
//     Format: ?group_by=field1,field2
//     Example: ?group_by=department,status
//
//   - having: Filters grouped results
//     Format: ?having=COUNT(id)>10,SUM(amount)<1000
//     Example: ?having=COUNT(id)>5
//
//   - join: Joins related tables
//     Format: ?join=table:condition,table2:condition2
//     Example: ?join=orders:orders.user_id=users.id
//
// Combined Example:
// GET /public/users?select=id,name,email&where=status=active,age>=21&order_by=-created_at&limit=10&offset=0
func HandleDatabaseRequest(w http.ResponseWriter, r *http.Request) {
	// Get database connection from context (set by middleware)
	startime := time.Now()
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
	parsetime := time.Now()
	params, err := parseQueryParams(r)
	fmt.Printf("time taken to parsequeryParams: %v \n", time.Since(parsetime))
	if err != nil {
		http.Error(w, fmt.Sprintf("Error parsing query parameters: %v", err), http.StatusBadRequest)
		return
	}

	var result interface{}

	// Process request based on HTTP method
	switch r.Method {
	case http.MethodGet:
		result, err = HandleRead(r.Context(), db, schema, table, params)
	case http.MethodPost:
		result, err = handleCreate(r.Context(), db, schema, table, r)
	case http.MethodPut, http.MethodPatch:
		result, err = handleUpdate(r.Context(), db, schema, table, r, params)
	case http.MethodDelete:
		result, err = HandleDelete(r.Context(), db, schema, table, params)
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
		fmt.Printf("total time for request:%v\n", time.Since(startime))
		return
	}

	// Return the result as JSON
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		log.Printf("Error encoding response: %v", err)
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
	}
	fmt.Printf("total time for request:%v\n", time.Since(startime))
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
	return InsertRecords(ctx, db, schema, table, data)
	// Handle both single object and array of objects
}

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
	return HandleUpdateRecords(ctx, db, table, schema, updateData, params)
}

// handleRead processes GET requests to retrieve data
func HandleRead(ctx context.Context, db *pgxpool.Pool, schema, table string, params QueryParams) (interface{}, error) {
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

func InsertRecords(ctx context.Context, db *pgxpool.Pool, schema, table string, data interface{}) (interface{}, error) {
	switch v := data.(type) {
	case map[string]interface{}:
		// Single object
		return InsertRecord(ctx, db, schema, table, v)
	case []interface{}:
		// Array of objects
		records := make([]map[string]interface{}, 0, len(v))
		for _, item := range v {
			if record, ok := item.(map[string]interface{}); ok {
				result, err := InsertRecord(ctx, db, schema, table, record)
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
func InsertRecord(
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

func HandleUpdateRecords(ctx context.Context, db *pgxpool.Pool,
	table, schema string, updateData map[string]interface{}, params QueryParams) (interface{}, error) {
	// Build update query
	buidtime := time.Now()
	query, args := buildUpdateQuery(schema, table, updateData, params)
	fmt.Printf("time taken to build the update query:%v\n", time.Since(buidtime))
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
func HandleDelete(
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

// Pre-compile all regular expressions
var (
	aggregationRegex = regexp.MustCompile(`(\w+)\(([^)]+)\)`)
	jsonFieldRegex   = regexp.MustCompile(`^([\w_]+->>'?[\w_]+'?)(=|!=|>=|<=|>|<|LIKE|IN)(.*)$`)
	nestedJsonRegex  = regexp.MustCompile(`^([\w_]+->'?[\w_]+'?->>'?[\w_]+'?)(=|!=|>=|<=|>|<|LIKE|IN)(.*)$`)
	operatorRegex    = regexp.MustCompile(`^([\w_.]+)(=|!=|>=|<=|>|<|LIKE|IN)(.*)$`)
	dateRegex        = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
)

func parseQueryParams(r *http.Request) (QueryParams, error) {
	query := r.URL.Query()
	params := QueryParams{
		Fields:       make([]string, 0, 8),    // Pre-allocate with reasonable capacity
		Filters:      make([]Filter, 0, 4),    // Pre-allocate with reasonable capacity
		Joins:        make(map[string]string), // Maps don't need capacity hints
		OrderBy:      make([]OrderByClause, 0, 4),
		GroupBy:      make([]string, 0, 4),
		Having:       make([]string, 0, 4),
		Aggregations: make(map[string]AggregationClause),
	}

	// Parse selected fields and aggregations
	if selectParam := query.Get("select"); selectParam != "" {
		// Fast path: If there are no commas, we can avoid the split
		if !strings.Contains(selectParam, ",") {
			parseSelectField(&params, selectParam)
		} else {
			fields := strings.Split(selectParam, ",")
			for _, field := range fields {
				parseSelectField(&params, strings.TrimSpace(field))
			}
		}
	}

	// Parse where conditions
	if whereParam := query.Get("where"); whereParam != "" {
		filters, err := parseWhereClause(whereParam)
		if err != nil {
			return params, fmt.Errorf("invalid where clause: %v", err)
		}
		params.Filters = filters
	}

	// Parse order by
	if orderParam := query.Get("order_by"); orderParam != "" {
		orderClauses := strings.Split(orderParam, ",")
		for _, clause := range orderClauses {
			clause = strings.TrimSpace(clause)
			direction := "ASC"

			if strings.HasPrefix(clause, "-") {
				direction = "DESC"
				clause = clause[1:] // More efficient than TrimPrefix
			}

			params.OrderBy = append(params.OrderBy, OrderByClause{
				Field:     clause,
				Direction: direction,
			})
		}
	}

	// Parse limit
	if limitParam := query.Get("limit"); limitParam != "" {
		limit, err := strconv.Atoi(limitParam)
		if err != nil || limit < 0 {
			return params, fmt.Errorf("invalid limit parameter: %v", limitParam)
		}
		params.Limit = limit
	}

	// Parse offset
	if offsetParam := query.Get("offset"); offsetParam != "" {
		offset, err := strconv.Atoi(offsetParam)
		if err != nil || offset < 0 {
			return params, fmt.Errorf("invalid offset parameter: %v", offsetParam)
		}
		params.Offset = offset
	}

	// Parse group by
	if groupByParam := query.Get("group_by"); groupByParam != "" {
		groupFields := strings.Split(groupByParam, ",")
		for _, field := range groupFields {
			params.GroupBy = append(params.GroupBy, strings.TrimSpace(field))
		}
	}

	// Parse having clause
	if havingParam := query.Get("having"); havingParam != "" {
		havingClauses := strings.Split(havingParam, ",")
		for _, clause := range havingClauses {
			params.Having = append(params.Having, strings.TrimSpace(clause))
		}
	}

	// Parse joins
	if joinParam := query.Get("join"); joinParam != "" {
		joins := strings.Split(joinParam, ",")
		for _, join := range joins {
			if idx := strings.Index(join, ":"); idx != -1 {
				table := strings.TrimSpace(join[:idx])
				condition := strings.TrimSpace(join[idx+1:])
				params.Joins[table] = condition
			}
		}
	}

	return params, nil
}

// parseSelectField processes a single field from the select parameter
func parseSelectField(params *QueryParams, field string) {
	// Check if this is an aggregation with alias
	if idx := strings.Index(field, ":"); idx != -1 {
		fieldExpr := strings.TrimSpace(field[:idx])
		alias := strings.TrimSpace(field[idx+1:])

		// Check if this is an aggregation function
		if matches := aggregationRegex.FindStringSubmatch(fieldExpr); len(matches) > 0 {
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
	} else if matches := aggregationRegex.FindStringSubmatch(field); len(matches) > 0 {
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

// parseWhereClause parses complex where conditions with support for nested AND/OR logic
func parseWhereClause(whereClause string) ([]Filter, error) {
	whereClause = strings.TrimSpace(whereClause)
	if whereClause == "" {
		return []Filter{}, nil
	}

	// Check if we're dealing with a complex bracketed expression
	if strings.HasPrefix(whereClause, "[") && strings.HasSuffix(whereClause, "]") {
		// Strip the outer brackets and pass to complex expression parser
		filter, err := parseComplexExpression(whereClause[1 : len(whereClause)-1])
		if err != nil {
			return nil, err
		}
		return []Filter{filter}, nil
	}

	// Handle simpler comma-separated conditions as AND clause
	if strings.Contains(whereClause, ",") {
		parts := strings.Split(whereClause, ",")
		conditions := make([]Condition, 0, len(parts))

		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}

			condition, err := parseCondition(part)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, condition)
		}

		return []Filter{{Operator: "AND", Conditions: conditions}}, nil
	}

	// Check for logical operators (.and. or .or.) in a non-bracketed expression
	if strings.Contains(whereClause, ".and.") || strings.Contains(whereClause, ".or.") {
		filter, err := parseComplexExpression(whereClause)
		if err != nil {
			return nil, err
		}
		return []Filter{filter}, nil
	}

	// Single condition
	condition, err := parseCondition(whereClause)
	if err != nil {
		return nil, err
	}

	return []Filter{{Operator: "AND", Conditions: []Condition{condition}}}, nil
}

// parseComplexExpression parses a complex expression containing .and. and .or. operators
func parseComplexExpression(expr string) (Filter, error) {
	expr = strings.TrimSpace(expr)

	// First check for OR operator at top level (higher precedence than AND)
	if hasTopLevelOperator(expr, ".or.") {
		parts := splitByOperator(expr, ".or.")
		conditions := make([]Condition, 0, len(parts))

		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}

			// Check if part is another complex expression
			if strings.HasPrefix(part, "[") && strings.HasSuffix(part, "]") {
				// Recursively parse nested expression
				nestedFilter, err := parseComplexExpression(part[1 : len(part)-1])
				if err != nil {
					return Filter{}, err
				}

				// Add as nested condition
				conditions = append(conditions, Condition{
					IsNested: true,
					Nested:   &nestedFilter,
				})
			} else if strings.Contains(part, ".and.") {
				// Handle AND expressions
				nestedFilter, err := parseComplexExpression(part)
				if err != nil {
					return Filter{}, err
				}

				conditions = append(conditions, Condition{
					IsNested: true,
					Nested:   &nestedFilter,
				})
			} else {
				// Simple condition
				condition, err := parseCondition(part)
				if err != nil {
					return Filter{}, err
				}
				conditions = append(conditions, condition)
			}
		}

		return Filter{Operator: "OR", Conditions: conditions}, nil
	}

	// Then check for AND operator at top level
	if hasTopLevelOperator(expr, ".and.") {
		parts := splitByOperator(expr, ".and.")
		conditions := make([]Condition, 0, len(parts))

		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}

			// Check if part is another complex expression
			if strings.HasPrefix(part, "[") && strings.HasSuffix(part, "]") {
				// Recursively parse nested expression
				nestedFilter, err := parseComplexExpression(part[1 : len(part)-1])
				if err != nil {
					return Filter{}, err
				}

				// Add as nested condition
				conditions = append(conditions, Condition{
					IsNested: true,
					Nested:   &nestedFilter,
				})
			} else if strings.Contains(part, ".or.") {
				// Handle OR expressions
				nestedFilter, err := parseComplexExpression(part)
				if err != nil {
					return Filter{}, err
				}

				conditions = append(conditions, Condition{
					IsNested: true,
					Nested:   &nestedFilter,
				})
			} else {
				// Simple condition
				condition, err := parseCondition(part)
				if err != nil {
					return Filter{}, err
				}
				conditions = append(conditions, condition)
			}
		}

		return Filter{Operator: "AND", Conditions: conditions}, nil
	}

	// Single condition
	condition, err := parseCondition(expr)
	if err != nil {
		return Filter{}, err
	}

	return Filter{Operator: "AND", Conditions: []Condition{condition}}, nil
}

// hasTopLevelOperator checks if the operator exists at the top level (not inside brackets)
func hasTopLevelOperator(expr string, operator string) bool {
	bracketLevel := 0
	opLen := len(operator)
	exprLen := len(expr)

	for i := 0; i <= exprLen-opLen; i++ {
		if expr[i] == '[' {
			bracketLevel++
		} else if expr[i] == ']' {
			bracketLevel--
		} else if bracketLevel == 0 && i+opLen <= exprLen && expr[i:i+opLen] == operator {
			return true
		}
	}
	return false
}

// splitByOperator splits a string by an operator, respecting nested brackets
func splitByOperator(expr string, operator string) []string {
	result := make([]string, 0, 4) // Preallocate with reasonable capacity
	var currentPart strings.Builder
	bracketLevel := 0
	i := 0
	opLen := len(operator)
	exprLen := len(expr)

	// Pre-size the builder to avoid reallocations
	currentPart.Grow(len(expr) / 2)

	for i < exprLen {
		// Check for opening bracket
		if expr[i] == '[' {
			bracketLevel++
			currentPart.WriteByte(expr[i])
			i++
		} else if expr[i] == ']' {
			// Check for closing bracket
			bracketLevel--
			currentPart.WriteByte(expr[i])
			i++
		} else if bracketLevel == 0 && i+opLen <= exprLen && expr[i:i+opLen] == operator {
			// We found the operator at top level
			result = append(result, currentPart.String())
			currentPart.Reset()
			i += opLen // Skip the operator
		} else {
			// Regular character
			currentPart.WriteByte(expr[i])
			i++
		}
	}

	// Don't forget the last part
	if currentPart.Len() > 0 {
		result = append(result, currentPart.String())
	}

	return result
}

// parseCondition parses a single condition like "field=value" or "field>=value"
func parseCondition(condStr string) (Condition, error) {
	condStr = strings.TrimSpace(condStr)

	// Handle JSON field conditions (first level)
	if jsonMatches := jsonFieldRegex.FindStringSubmatch(condStr); len(jsonMatches) == 4 {
		// JSON field condition found
		field := strings.TrimSpace(jsonMatches[1])
		operator := strings.TrimSpace(jsonMatches[2])
		valueStr := strings.TrimSpace(jsonMatches[3])

		// Optimize JSON field formatting
		field = formatJsonField(field)

		value, err := parseValue(valueStr, operator)
		if err != nil {
			return Condition{}, err
		}

		return Condition{
			Field:    field,
			Operator: operator,
			Value:    value,
			IsNested: false,
		}, nil
	}

	// Handle nested JSON field conditions (second level)
	if nestedMatches := nestedJsonRegex.FindStringSubmatch(condStr); len(nestedMatches) == 4 {
		// Nested JSON field condition found
		field := strings.TrimSpace(nestedMatches[1])
		operator := strings.TrimSpace(nestedMatches[2])
		valueStr := strings.TrimSpace(nestedMatches[3])

		// Optimize nested JSON field formatting
		field = formatNestedJsonField(field)

		value, err := parseValue(valueStr, operator)
		if err != nil {
			return Condition{}, err
		}

		return Condition{
			Field:    field,
			Operator: operator,
			Value:    value,
			IsNested: false,
		}, nil
	}

	// Standard field pattern
	if matches := operatorRegex.FindStringSubmatch(condStr); len(matches) == 4 {
		field := strings.TrimSpace(matches[1])
		operator := strings.TrimSpace(matches[2])
		valueStr := strings.TrimSpace(matches[3])

		value, err := parseValue(valueStr, operator)
		if err != nil {
			return Condition{}, err
		}

		return Condition{
			Field:    field,
			Operator: operator,
			Value:    value,
			IsNested: false,
		}, nil
	}

	return Condition{}, fmt.Errorf("invalid condition format: %s", condStr)
}

// formatJsonField optimizes the JSON field formatting
func formatJsonField(field string) string {
	parts := strings.Split(field, "->>")
	if len(parts) <= 1 {
		return field
	}

	// Faster string building than using append and join
	var result strings.Builder
	result.WriteString(parts[0])
	result.WriteString("->>")

	if !strings.HasPrefix(parts[1], "'") && !strings.HasSuffix(parts[1], "'") {
		result.WriteByte('\'')
		result.WriteString(parts[1])
		result.WriteByte('\'')
	} else {
		result.WriteString(parts[1])
	}

	return result.String()
}

// formatNestedJsonField optimizes the nested JSON field formatting
func formatNestedJsonField(field string) string {
	parts := strings.Split(field, "->>")
	if len(parts) <= 1 {
		return field
	}

	secondParts := strings.Split(parts[0], "->")
	if len(secondParts) <= 1 {
		return field
	}

	var result strings.Builder
	result.WriteString(secondParts[0])
	result.WriteString("->")

	if !strings.HasPrefix(secondParts[1], "'") && !strings.HasSuffix(secondParts[1], "'") {
		result.WriteByte('\'')
		result.WriteString(secondParts[1])
		result.WriteByte('\'')
	} else {
		result.WriteString(secondParts[1])
	}

	result.WriteString("->>")

	if len(parts) > 1 {
		if !strings.HasPrefix(parts[1], "'") && !strings.HasSuffix(parts[1], "'") {
			result.WriteByte('\'')
			result.WriteString(parts[1])
			result.WriteByte('\'')
		} else {
			result.WriteString(parts[1])
		}
	}

	return result.String()
}

// parseValue parses the value part of a condition based on the operator
func parseValue(valueStr string, operator string) (interface{}, error) {
	// Handle the IN operator specially
	if operator == "IN" && strings.HasPrefix(valueStr, "(") && strings.HasSuffix(valueStr, ")") {
		// Remove parentheses and split by comma
		valueStr = valueStr[1 : len(valueStr)-1]
		values := strings.Split(valueStr, ",")

		// Pre-allocate the result array with the right capacity
		cleanValues := make([]string, 0, len(values))
		for _, v := range values {
			v = strings.TrimSpace(v)
			// Remove quotes if present
			if (strings.HasPrefix(v, "'") && strings.HasSuffix(v, "'")) ||
				(strings.HasPrefix(v, "\"") && strings.HasSuffix(v, "\"")) {
				v = v[1 : len(v)-1]
			}
			cleanValues = append(cleanValues, v)
		}

		return cleanValues, nil
	}

	// Try to convert to appropriate type
	if intVal, err := strconv.Atoi(valueStr); err == nil {
		return intVal, nil
	} else if floatVal, err := strconv.ParseFloat(valueStr, 64); err == nil {
		return floatVal, nil
	} else if valueStr == "true" || valueStr == "false" {
		return valueStr == "true", nil
	} else if valueStr == "null" {
		return nil, nil
	} else if strings.HasPrefix(valueStr, "{") && strings.HasSuffix(valueStr, "}") {
		// Try to parse as JSON
		var jsonValue map[string]interface{}
		if err := json.Unmarshal([]byte(valueStr), &jsonValue); err == nil {
			return jsonValue, nil
		}
	} else if strings.HasPrefix(valueStr, "[") && strings.HasSuffix(valueStr, "]") && operator != "IN" {
		// Try to parse as JSON array
		var jsonArray []interface{}
		if err := json.Unmarshal([]byte(valueStr), &jsonArray); err == nil {
			return jsonArray, nil
		}
	}

	// Handle date format (YYYY-MM-DD)
	if dateRegex.MatchString(valueStr) {
		return valueStr, nil // Keep as string, will be converted in SQL
	}

	// Treat as string, removing quotes if present
	if (strings.HasPrefix(valueStr, "'") && strings.HasSuffix(valueStr, "'")) ||
		(strings.HasPrefix(valueStr, "\"") && strings.HasSuffix(valueStr, "\"")) {
		return valueStr[1 : len(valueStr)-1], nil
	}

	return valueStr, nil
}

// BuildWhereClause builds a SQL WHERE clause from the filter conditions
func BuildWhereClause(filters []Filter, startParamIndex int) (string, []interface{}, int) {
	if len(filters) == 0 {
		return "", nil, startParamIndex
	}

	var clauses []string
	var params []interface{}
	paramIndex := startParamIndex

	for _, filter := range filters {
		clause, filterParams, newParamIndex := buildFilterClause(filter, paramIndex)
		if clause != "" {
			clauses = append(clauses, clause)
			params = append(params, filterParams...)
			paramIndex = newParamIndex
		}
	}

	if len(clauses) == 0 {
		return "", nil, paramIndex
	}

	// whereClause := "WHERE " + strings.Join(clauses, " AND ")
	whereClause := strings.Join(clauses, " AND ")
	return whereClause, params, paramIndex
}

// buildFilterClause builds a SQL clause for a single filter
func buildFilterClause(filter Filter, startParamIndex int) (string, []interface{}, int) {
	if len(filter.Conditions) == 0 {
		return "", nil, startParamIndex
	}

	var clauses []string
	var params []interface{}
	paramIndex := startParamIndex

	for _, condition := range filter.Conditions {
		if condition.IsNested && condition.Nested != nil {
			// Handle nested filter
			nestedClause, nestedParams, newParamIndex := buildFilterClause(*condition.Nested, paramIndex)
			if nestedClause != "" {
				clauses = append(clauses, "("+nestedClause+")")
				params = append(params, nestedParams...)
				paramIndex = newParamIndex
			}
		} else {
			// Handle regular condition
			clause, condParams, newParamIndex := buildConditionClause(condition, paramIndex)
			if clause != "" {
				clauses = append(clauses, clause)
				params = append(params, condParams...)
				paramIndex = newParamIndex
			}
		}
	}

	if len(clauses) == 0 {
		return "", nil, paramIndex
	}

	operator := " AND "
	if filter.Operator == "OR" {
		operator = " OR "
	}

	return strings.Join(clauses, operator), params, paramIndex
}

// buildConditionClause builds a SQL clause for a single condition
func buildConditionClause(condition Condition, paramIndex int) (string, []interface{}, int) {
	if condition.Field == "_nested" {
		// This shouldn't happen with the new structure, but keeping for backwards compatibility
		return "", nil, paramIndex
	}

	newParamIndex := paramIndex
	placeholder := fmt.Sprintf("$%d", newParamIndex)
	paramIndex = paramIndex + 1
	newParamIndex = paramIndex
	// Handle special operators
	switch condition.Operator {
	case "IN":
		if values, ok := condition.Value.([]string); ok {
			placeholders := make([]string, len(values))
			params := make([]interface{}, len(values))
			for i, v := range values {
				placeholders[i] = fmt.Sprintf("$%d", paramIndex+i+1)
				params[i] = v
			}
			newParamIndex = paramIndex + len(values)
			return fmt.Sprintf("%s IN (%s)", condition.Field, strings.Join(placeholders, ", ")), params, newParamIndex
		}
		return fmt.Sprintf("%s IN (%s)", condition.Field, placeholder), []interface{}{condition.Value}, newParamIndex
	case "LIKE":
		return fmt.Sprintf("%s LIKE %s", condition.Field, placeholder), []interface{}{condition.Value}, newParamIndex
	case "NESTED":
		// This shouldn't happen with the new structure, but keeping for backwards compatibility
		return "", nil, paramIndex
	default:
		return fmt.Sprintf("%s %s %s", condition.Field, condition.Operator, placeholder), []interface{}{condition.Value}, newParamIndex
	}
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
		whereClause, whereArgs, newArgIndex := BuildWhereClause(params.Filters, argIndex)
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

	// whereClause := "WHERE " + strings.Join(clauses, " AND ")
	whereClause := strings.Join(clauses, " AND ")
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
