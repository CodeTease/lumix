package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/meilisearch/meilisearch-go"
	"golang.org/x/time/rate"
)

// SearchResult struct
type SearchResult struct {
	URL       string  `json:"url"`
	Title     string  `json:"title"`
	Snippet   string  `json:"snippet"`
	MetaDesc  string  `json:"meta_description"`
	LithScore float64 `json:"lith_score"` // Display Lith score
}

var dbPool *pgxpool.Pool
var meiliClient meilisearch.ServiceManager

const configKey = "default"

func main() {
	// ... (Code kết nối DB) ...
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL must be set")
	}
	var err error
	dbPool, err = pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("DB Error: %v", err)
	}
	defer dbPool.Close()

	// Initialize Config Table
	if err := initConfigTable(dbPool); err != nil {
		log.Fatalf("Failed to init config table: %v", err)
	}

	// --- Meilisearch Setup ---
	meiliHost := os.Getenv("MEILI_HOST")
	if meiliHost == "" {
		meiliHost = "http://meilisearch:7700"
	}
	meiliKey := os.Getenv("MEILI_API_KEY")

	meiliClient = meilisearch.New(meiliHost, meilisearch.WithAPIKey(meiliKey))

	// --- Enforce Ranking Rules on Startup ---
	go func() {
		// Wait for Meili to start
		for i := 0; i < 5; i++ {
			if _, err := meiliClient.Health(); err == nil {
				log.Println("Configuring Meilisearch Ranking Rules for Lith Ranker...")
				meiliClient.Index("pages").UpdateSettings(&meilisearch.Settings{
					RankingRules: []string{
						"words",
						"typo",
						"proximity",
						"attribute",
						"sort",
						"exactness",
						"lith_score:desc",
					},
					SortableAttributes: []string{"lith_score", "crawled_at"},
				})
				return
			}
			// Sleep handled by Docker restart policy or user patience
		}
	}()

	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	// Rate Limiting (20 requests per second)
	e.Use(middleware.RateLimiter(middleware.NewRateLimiterMemoryStoreWithConfig(
		middleware.RateLimiterMemoryStoreConfig{Rate: rate.Limit(20), Burst: 50, ExpiresIn: 3 * time.Minute},
	)))

	// Configurable CORS
	allowedOrigins := []string{"*"}
	if envOrigins := os.Getenv("CORS_ALLOWED_ORIGINS"); envOrigins != "" {
		allowedOrigins = []string{envOrigins}
	}

	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: allowedOrigins,
		AllowMethods: []string{http.MethodGet, http.MethodPut, http.MethodPost, http.MethodDelete},
	}))

	e.GET("/api/search", searchHandler)

	// Admin API with Basic Auth
	adminGroup := e.Group("/api/config")
	adminGroup.Use(middleware.BasicAuth(func(username, password string, c echo.Context) (bool, error) {
		adminUser := os.Getenv("ADMIN_USERNAME")
		if adminUser == "" {
			adminUser = "admin" // Default fallback, but ENV is preferred
		}
		adminPass := os.Getenv("ADMIN_PASSWORD")

		// Require ADMIN_PASSWORD to be set for security
		if adminPass == "" {
			return false, nil
		}

		if subtle.ConstantTimeCompare([]byte(username), []byte(adminUser)) == 1 &&
			subtle.ConstantTimeCompare([]byte(password), []byte(adminPass)) == 1 {
			return true, nil
		}
		return false, nil
	}))

	adminGroup.GET("", getConfigHandler)
	adminGroup.POST("", updateConfigHandler)

	e.Static("/", "/app/go-backend/static")

	// Graceful Shutdown
	go func() {
		if err := e.Start(":8080"); err != nil && err != http.ErrServerClosed {
			e.Logger.Fatal("shutting down the server")
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server with a timeout of 10 seconds.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.Shutdown(ctx); err != nil {
		e.Logger.Fatal(err)
	}
}

func initConfigTable(pool *pgxpool.Pool) error {
	ctx := context.Background()

	// Create table if not exists
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS crawler_config (
			id SERIAL PRIMARY KEY,
			key VARCHAR(255) UNIQUE NOT NULL,
			value JSONB NOT NULL,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return err
	}

	// Insert default config if not exists
	defaultConfig := `{
	  "start_urls": ["https://teaserverse.dev"],
	  "max_depth": 1,
	  "max_concurrent_requests": 5,
	  "delay_per_domain": 1.0,
	  "user_agent": "TeaserBot/LocalDev",
	  "max_pages": 5,
	  "save_to_db": true,
	  "save_to_json": false,
	  "ssl_verify": false,
	  "minio_storage": {"enabled": false}
	}`

	_, err = pool.Exec(ctx, `
		INSERT INTO crawler_config (key, value) VALUES ($1, $2)
		ON CONFLICT (key) DO NOTHING
	`, configKey, defaultConfig)

	return err
}

func searchHandler(c echo.Context) error {
	query := c.QueryParam("q")
	if query == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "query param 'q' required"})
	}

	searchRequest := &meilisearch.SearchRequest{
		Limit:                 20,
		AttributesToRetrieve:  []string{"url", "title", "meta_description", "lith_score"},
		AttributesToHighlight: []string{"*"},
		HighlightPreTag:       "<b>",
		HighlightPostTag:      "</b>",
		AttributesToCrop:      []string{"body_text"},
		CropLength:            150,
	}

	searchRes, err := meiliClient.Index("pages").Search(query, searchRequest)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "search failed"})
	}

	results := []SearchResult{}
	for _, hit := range searchRes.Hits {
		var hitMap map[string]interface{}
		hitJSON, _ := json.Marshal(hit)
		json.Unmarshal(hitJSON, &hitMap)

		res := SearchResult{}
		if url, ok := hitMap["url"].(string); ok {
			res.URL = url
		}
		if title, ok := hitMap["title"].(string); ok {
			res.Title = title
		}
		if meta, ok := hitMap["meta_description"].(string); ok {
			res.MetaDesc = meta
		}
		if score, ok := hitMap["lith_score"].(float64); ok {
			res.LithScore = score
		}

		if formatted, ok := hitMap["_formatted"].(map[string]interface{}); ok {
			if hSnippet, ok := formatted["body_text"].(string); ok {
				res.Snippet = hSnippet
			}
		}
		if res.Snippet == "" {
			res.Snippet = res.MetaDesc
		}

		results = append(results, res)
	}

	return c.JSON(http.StatusOK, results)
}

func getConfigHandler(c echo.Context) error {
	var valueBytes []byte
	err := dbPool.QueryRow(context.Background(), "SELECT value FROM crawler_config WHERE key = $1", configKey).Scan(&valueBytes)
	if err != nil {
		if err == pgx.ErrNoRows {
			return c.JSON(http.StatusNotFound, map[string]string{"error": "Config not found"})
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to read config from DB"})
	}

	var config map[string]interface{}
	if err := json.Unmarshal(valueBytes, &config); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to parse config JSON"})
	}

	return c.JSON(http.StatusOK, config)
}

func updateConfigHandler(c echo.Context) error {
	var newConfig map[string]interface{}
	if err := c.Bind(&newConfig); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid JSON"})
	}

	configJSON, err := json.Marshal(newConfig)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to marshal config"})
	}

	_, err = dbPool.Exec(context.Background(), `
		UPDATE crawler_config 
		SET value = $1, updated_at = CURRENT_TIMESTAMP 
		WHERE key = $2
	`, configJSON, configKey)

	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to update config in DB"})
	}

	return c.JSON(http.StatusOK, map[string]string{"status": "updated"})
}
