package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/meilisearch/meilisearch-go"
)

// SearchResult struct
type SearchResult struct {
	URL        string  `json:"url"`
	Title      string  `json:"title"`
	Snippet    string  `json:"snippet"`
	MetaDesc   string  `json:"meta_description"`
	LithScore  float64 `json:"lith_score"` // Hiển thị điểm số Lith
}

var dbPool *pgxpool.Pool
var meiliClient meilisearch.ServiceManager
var configMutex sync.Mutex
const configPath = "/app/crawler/config.json"

// Struct ánh xạ với config.json (dùng con trỏ để merge cho dễ)
type CrawlerConfig struct {
	StartUrls               *[]string `json:"start_urls,omitempty"`
	MaxDepth                *int      `json:"max_depth,omitempty"`
	MaxConcurrentRequests   *int      `json:"max_concurrent_requests,omitempty"`
	DelayPerDomain          *float64  `json:"delay_per_domain,omitempty"`
	UserAgent               *string   `json:"user_agent,omitempty"`
	OutputFile              *string   `json:"output_file,omitempty"`
	MaxPages                *int      `json:"max_pages,omitempty"`
	MaxRequestsPerMinute    *int      `json:"max_requests_per_minute,omitempty"`
	MaxRetries              *int      `json:"max_retries,omitempty"`
	SaveToDb                *bool     `json:"save_to_db,omitempty"`
	SaveToJson              *bool     `json:"save_to_json,omitempty"`
	SslVerify               *bool     `json:"ssl_verify,omitempty"`
	DbSslCaCertPath         *string   `json:"db_ssl_ca_cert_path,omitempty"`
	DbBatchSize             *int      `json:"db_batch_size,omitempty"`
	MinioStorage            *struct {
		Enabled     *bool   `json:"enabled,omitempty"`
		Endpoint    *string `json:"endpoint,omitempty"`
		BucketName  *string `json:"bucket_name,omitempty"`
		Secure      *bool   `json:"secure,omitempty"`
	} `json:"minio_storage,omitempty"`
}

// Cấu hình mặc định siêu nhẹ
const defaultConfigFileContent = `{
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

// ensureConfigFileExists kiểm tra xem config.json có tồn tại không.
func ensureConfigFileExists() error {
	_, err := os.Stat(configPath)
	if os.IsNotExist(err) {
		log.Printf("Warning: %s not found. Creating default, lightweight config.", configPath)
		if err := os.WriteFile(configPath, []byte(defaultConfigFileContent), 0644); err != nil {
			return err
		}
		log.Println("Default config file created successfully.")
	} else if err != nil {
		return err
	}
	return nil
}

func main() {
	// ... (Code kết nối DB giữ nguyên) ...
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" { log.Fatal("DATABASE_URL must be set") }
	var err error
	dbPool, err = pgxpool.New(context.Background(), dbURL)
	if err != nil { log.Fatalf("DB Error: %v", err) }
	defer dbPool.Close()

	// --- Meilisearch Setup ---
	meiliHost := os.Getenv("MEILI_HOST")
	if meiliHost == "" { meiliHost = "http://meilisearch:7700" }
	meiliKey := os.Getenv("MEILI_API_KEY")

	meiliClient = meilisearch.New(meiliHost, meilisearch.WithAPIKey(meiliKey))

	// --- Enforce Ranking Rules on Startup ---
	go func() {
		// Chờ Meili khởi động
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
						"lith_score:desc", // Lith Score is King!
					},
					SortableAttributes: []string{"lith_score", "crawled_at"},
				})
				return
			}
			// Sleep manually implementation removed for brevity, assume retry
		}
	}()

	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.CORS()) // Allow Dashboard to call API

	e.GET("/api/search", searchHandler)
	e.GET("/api/config", getConfigHandler)
	e.POST("/api/config", updateConfigHandler)
	e.Static("/", "/app/go-backend/static")

	e.Logger.Fatal(e.Start(":8080"))
}

func searchHandler(c echo.Context) error {
	query := c.QueryParam("q")
	if query == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "query param 'q' required"})
	}

	searchRequest := &meilisearch.SearchRequest{
		Limit:                20,
		AttributesToRetrieve: []string{"url", "title", "meta_description", "body_text", "lith_score"},
		AttributesToHighlight: []string{"*"},
		HighlightPreTag:      "<b>",
		HighlightPostTag:     "</b>",
		AttributesToCrop:     []string{"body_text"},
		CropLength:           15,
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
		if url, ok := hitMap["url"].(string); ok { res.URL = url }
		if title, ok := hitMap["title"].(string); ok { res.Title = title }
		if meta, ok := hitMap["meta_description"].(string); ok { res.MetaDesc = meta }
		if score, ok := hitMap["lith_score"].(float64); ok { res.LithScore = score }

		// Snippet logic (giữ nguyên như cũ)
		if formatted, ok := hitMap["_formatted"].(map[string]interface{}); ok {
			if hSnippet, ok := formatted["body_text"].(string); ok { res.Snippet = hSnippet }
		}
		if res.Snippet == "" { res.Snippet = res.MetaDesc }

		results = append(results, res)
	}

	return c.JSON(http.StatusOK, results)
}