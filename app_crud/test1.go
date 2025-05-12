package appcrud

/* this is back up functions just in case:


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
// with proper handling of nested expressions
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
	for i := 0; i <= len(expr)-len(operator); i++ {
		if expr[i] == '[' {
			bracketLevel++
		} else if expr[i] == ']' {
			bracketLevel--
		} else if bracketLevel == 0 && i+len(operator) <= len(expr) && expr[i:i+len(operator)] == operator {
			return true
		}
	}
	return false
}

// splitByOperator splits a string by an operator, respecting nested brackets
func splitByOperator(expr string, operator string) []string {
	var result []string
	var currentPart strings.Builder
	bracketLevel := 0
	i := 0

	for i < len(expr) {
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
		} else if bracketLevel == 0 && i+len(operator) <= len(expr) && expr[i:i+len(operator)] == operator {
			// We found the operator at top level
			result = append(result, currentPart.String())
			currentPart.Reset()
			i += len(operator) // Skip the operator
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
	jsonFieldPattern := regexp.MustCompile(`^([\w_]+->>'?[\w_]+'?)(=|!=|>=|<=|>|<|LIKE|IN)(.*)$`)
	jsonMatches := jsonFieldPattern.FindStringSubmatch(condStr)

	if len(jsonMatches) == 4 {
		// JSON field condition found
		field := strings.TrimSpace(jsonMatches[1])
		parts := strings.Split(field, "->>")
		newparts := []string{}
		for i, part := range parts {
			if i == 0 {
				newparts = append(newparts, part)
				continue
			}
			temp := fmt.Sprintf("'%v'", part)
			newparts = append(newparts, temp)
		}
		field = strings.Join(newparts, "->>")
		operator := strings.TrimSpace(jsonMatches[2])
		valueStr := strings.TrimSpace(jsonMatches[3])

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
	nestedJsonPattern := regexp.MustCompile(`^([\w_]+->'?[\w_]+'?->>'?[\w_]+'?)(=|!=|>=|<=|>|<|LIKE|IN)(.*)$`)
	nestedMatches := nestedJsonPattern.FindStringSubmatch(condStr)

	if len(nestedMatches) == 4 {
		// Nested JSON field condition found
		field := strings.TrimSpace(nestedMatches[1])
		operator := strings.TrimSpace(nestedMatches[2])
		valueStr := strings.TrimSpace(nestedMatches[3])
		parts := strings.Split(field, "->>")
		secondpart := strings.Split(parts[0], "->")
		secondparts := []string{}
		for i, part := range secondpart {
			if i == 0 {
				secondparts = append(secondparts, part)
				continue
			}
			temp := fmt.Sprintf("'%v'", part)
			secondparts = append(secondparts, temp)
		}
		joinedpart := strings.Join(secondparts, "->")
		lastparrt := ""
		if len(parts) > 1 {
			lastparrt = fmt.Sprintf("'%v'", parts[1])
		}
		field = strings.Join([]string{joinedpart, lastparrt}, "->>")
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
	operatorPattern := regexp.MustCompile(`^([\w_.]+)(=|!=|>=|<=|>|<|LIKE|IN)(.*)$`)
	matches := operatorPattern.FindStringSubmatch(condStr)

	if len(matches) != 4 {
		return Condition{}, fmt.Errorf("invalid condition format: %s", condStr)
	}

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

// parseValue parses the value part of a condition based on the operator
func parseValue(valueStr string, operator string) (interface{}, error) {
	// Handle the IN operator specially
	if operator == "IN" && strings.HasPrefix(valueStr, "(") && strings.HasSuffix(valueStr, ")") {
		// Remove parentheses and split by comma
		valueStr = valueStr[1 : len(valueStr)-1]
		values := strings.Split(valueStr, ",")

		// Clean up each value
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
	if datePattern := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`); datePattern.MatchString(valueStr) {
		return valueStr, nil // Keep as string, will be converted in SQL
	}

	// Treat as string, removing quotes if present
	if (strings.HasPrefix(valueStr, "'") && strings.HasSuffix(valueStr, "'")) ||
		(strings.HasPrefix(valueStr, "\"") && strings.HasSuffix(valueStr, "\"")) {
		return valueStr[1 : len(valueStr)-1], nil
	}

	return valueStr, nil
}


*/
