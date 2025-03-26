package middleware

import (
	"context"
	"net/http"

	"lowcode.com/backend/dbstore"
)

// CORS middleware
func CorsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Add CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// middleware to add the db connection of respective app to the request context.
//
// The app_id will be read from path param.
func DBInjectionMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		appID := r.PathValue("app_id") // Adjust based on how app_id is passed
		if appID == "" {
			http.Error(w, "app_id is required", http.StatusBadRequest)
			return
		}

		pool, err := dbstore.GlobalPoolManager.GetPool(appID)
		if err != nil {
			http.Error(w, "Failed to get database connection pool", http.StatusInternalServerError)
			return
		}

		ctx := context.WithValue(r.Context(), "db_conn", pool)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
