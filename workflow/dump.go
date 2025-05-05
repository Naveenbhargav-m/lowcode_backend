package workflow

/*
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
*/
