package appcrud

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strings"
)

// APIRequest contains all possible input types for the API call.
type APIRequest struct {
	Method      string
	URL         string
	Headers     map[string]string
	QueryParams map[string]string
	Body        interface{}
	FormParams  map[string]string
	Files       map[string]string // key = field name, value = file path
}

// APICall makes a generic HTTP request supporting various data types.
func APICall(req APIRequest) ([]byte, error) {
	client := &http.Client{}

	// Build URL with Query Parameters
	reqURL, err := url.Parse(req.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	query := reqURL.Query()
	for key, value := range req.QueryParams {
		query.Set(key, value)
	}
	reqURL.RawQuery = query.Encode()

	var body io.Reader

	// Multipart Form (for files)
	if len(req.Files) > 0 {
		var buf bytes.Buffer
		writer := multipart.NewWriter(&buf)

		// Add form fields
		for key, value := range req.FormParams {
			_ = writer.WriteField(key, value)
		}

		// Add files
		for field, filePath := range req.Files {
			file, err := os.Open(filePath)
			if err != nil {
				return nil, fmt.Errorf("error opening file %s: %w", filePath, err)
			}
			defer file.Close()

			part, err := writer.CreateFormFile(field, filePath)
			if err != nil {
				return nil, err
			}
			_, err = io.Copy(part, file)
			if err != nil {
				return nil, err
			}
		}

		writer.Close()
		body = &buf
		req.Headers["Content-Type"] = writer.FormDataContentType()
	} else if req.FormParams != nil { // Regular Form Data
		form := url.Values{}
		for key, value := range req.FormParams {
			form.Set(key, value)
		}
		body = strings.NewReader(form.Encode())
		req.Headers["Content-Type"] = "application/x-www-form-urlencoded"
	} else if req.Body != nil { // JSON Body
		jsonBody, err := json.Marshal(req.Body)
		if err != nil {
			return nil, fmt.Errorf("JSON encoding error: %w", err)
		}
		body = bytes.NewBuffer(jsonBody)
		req.Headers["Content-Type"] = "application/json"
	}

	// Create HTTP Request
	request, err := http.NewRequest(req.Method, reqURL.String(), body)
	if err != nil {
		return nil, fmt.Errorf("request creation error: %w", err)
	}

	// Add Headers
	for key, value := range req.Headers {
		request.Header.Set(key, value)
	}

	// Execute Request
	resp, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("request execution error: %w", err)
	}
	defer resp.Body.Close() // Ensure response body is closed

	// Read response body
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}

	// Handle HTTP errors
	if resp.StatusCode >= 400 {
		return respBytes, fmt.Errorf("HTTP error: %s (%d)", resp.Status, resp.StatusCode)
	}

	return respBytes, nil
}
