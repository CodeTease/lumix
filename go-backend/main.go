package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	// Bỏ "io" vì Go không thích import thừa

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/meilisearch/meilisearch-go"
)

// Struct để chứa kết quả tìm kiếm
type SearchResult struct {
	URL        string `json:"url"`
	Title      string `json:"title"`
	Snippet    string `json:"snippet"` // Đoạn trích nổi bật
	MetaDesc   string `json:"meta_description"`
}

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


var dbPool *pgxpool.Pool
var meiliClient meilisearch.ServiceManager // Meilisearch client
var configMutex sync.Mutex // Mutex để bảo vệ việc đọc/ghi file config

// Đường dẫn đến file config trong container (nhất quán)
const configPath = "/app/crawler/config.json"

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
	if err := ensureConfigFileExists(); err != nil {
		log.Fatalf("Fatal Error: Could not ensure config file exists or read permissions: %v\n", err)
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL must be set")
	}

	var err error
	dbPool, err = pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer dbPool.Close()

	log.Println("Successfully connected to PostgreSQL!")

	// --- Khởi tạo Meilisearch Client ---
	meiliHost := os.Getenv("MEILI_HOST")
	if meiliHost == "" {
		meiliHost = "http://meilisearch:7700" // Default for Docker Compose
	}
	meiliKey := os.Getenv("MEILI_API_KEY") // Can be empty for local dev

	meiliClient = meilisearch.New(meiliHost, meilisearch.WithAPIKey(meiliKey))

	// Kiểm tra kết nối tới Meilisearch (optional but good practice)
	if _, err := meiliClient.Health(); err != nil {
		log.Printf("Warning: Could not connect to Meilisearch: %v\n", err)
	} else {
		log.Println("Successfully connected to Meilisearch!")
	}
	// ------------------------------------

	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	e.GET("/api/search", searchHandler)
	e.GET("/api/config", getConfigHandler)
	e.POST("/api/config", updateConfigHandler)

	// Sửa đường dẫn static (do đã mount vào /app)
	e.Static("/", "/app/go-backend/static")

	log.Println("Starting TeaserSearch Backend on :8080...")
	e.Logger.Fatal(e.Start(":8080"))
}

// searchHandler: Xử lý logic tìm kiếm bằng Meilisearch
func searchHandler(c echo.Context) error {
	query := c.QueryParam("q")
	if query == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "query parameter 'q' is required"})
	}

	// Cấu hình yêu cầu tìm kiếm
	searchRequest := &meilisearch.SearchRequest{
		Limit:                20,
		AttributesToRetrieve: []string{"url", "title", "meta_description", "body_text"}, // Lấy cả body_text để snippet
		AttributesToHighlight: []string{"*"}, // Highlight tất cả các thuộc tính
		HighlightPreTag:      "<b>",
		HighlightPostTag:     "</b>",
		AttributesToCrop:     []string{"body_text"},
		CropLength:           15,
		CropMarker:           "...",
	}

	// Thực hiện tìm kiếm
	searchRes, err := meiliClient.Index("pages").Search(query, searchRequest)
	if err != nil {
		log.Printf("Meilisearch query failed: %v\n", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "search failed"})
	}

	// Chuyển đổi kết quả
	results := []SearchResult{}
	for _, hit := range searchRes.Hits {
		// Convert meilisearch.Hit (map[string]json.RawMessage) to map[string]interface{}
		var hitMap map[string]interface{}
		hitJSON, err := json.Marshal(hit)
		if err != nil {
			log.Printf("Error marshalling hit: %v", err)
			continue
		}
		if err := json.Unmarshal(hitJSON, &hitMap); err != nil {
			log.Printf("Error unmarshalling hit: %v", err)
			continue
		}

		res := SearchResult{}

		// Lấy các trường thông thường
		if url, ok := hitMap["url"].(string); ok {
			res.URL = url
		}
		if title, ok := hitMap["title"].(string); ok {
			res.Title = title
		}
		if meta, ok := hitMap["meta_description"].(string); ok {
			res.MetaDesc = meta
		}

		// Xử lý Snippet và Highlight
		var finalSnippet string
		if formatted, ok := hitMap["_formatted"].(map[string]interface{}); ok {
			// Ưu tiên highlight title nếu có
			if hTitle, ok := formatted["title"].(string); ok {
				res.Title = hTitle
			}
			// Ưu tiên snippet từ Meilisearch
			if hSnippet, ok := formatted["body_text"].(string); ok {
				finalSnippet = hSnippet
			}
		}

		// Nếu không có highlight, thử lấy snippet
		if finalSnippet == "" {
			if snippet, ok := hitMap["_snippet"].(map[string]interface{}); ok {
				if sBody, ok := snippet["body_text"].(string); ok {
					finalSnippet = sBody
				}
			}
		}

		// Nếu vẫn không có, dùng meta description
		if finalSnippet == "" {
			finalSnippet = res.MetaDesc
		}


		res.Snippet = finalSnippet
		results = append(results, res)
	}


	return c.JSON(http.StatusOK, results)
}

// getConfigHandler: Đọc cấu hình hiện tại từ file config.json
func getConfigHandler(c echo.Context) error {
	configMutex.Lock()
	defer configMutex.Unlock()

	data, err := os.ReadFile(configPath)
	if err != nil {
		log.Printf("Error reading config file: %v", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Could not read crawler config file"})
	}

	var currentConfig map[string]interface{}
	if err := json.Unmarshal(data, &currentConfig); err != nil {
		log.Printf("Error unmarshalling config JSON: %v", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Invalid config file format"})
	}

	// === SỬA LỖI: THÊM ANTI-CACHE HEADERS ===
	// Ra lệnh cho trình duyệt KHÔNG BAO GIỜ cache API này
	c.Response().Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate")
	c.Response().Header().Set("Pragma", "no-cache")
	c.Response().Header().Set("Expires", "0")
	// ======================================

	return c.JSON(http.StatusOK, currentConfig)
}

// updateConfigHandler: Cập nhật cấu hình vào file config.json
func updateConfigHandler(c echo.Context) error {
	configMutex.Lock()
	defer configMutex.Unlock()

	// 1. Đọc cấu hình hiện tại
	currentData, err := os.ReadFile(configPath)
	if err != nil {
		log.Printf("Error reading current config: %v", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Could not read current config"})
	}

	var currentConfig map[string]interface{}
	if err := json.Unmarshal(currentData, &currentConfig); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Invalid current config format"})
	}

	// 2. Phân tích cú pháp cấu hình cập nhật từ Body
	var updates map[string]interface{}
	if err := c.Bind(&updates); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid JSON payload"})
	}

	// 3. Hợp nhất (Merge) cấu hình
	for key, value := range updates {
		// Xử lý đặc biệt cho minio_storage (nested object)
		if key == "minio_storage" {
			if minioUpdates, ok := value.(map[string]interface{}); ok {
				if currentMinio, ok := currentConfig["minio_storage"].(map[string]interface{}); ok {
					for minioKey, minioValue := range minioUpdates {
						currentMinio[minioKey] = minioValue
					}
					currentConfig["minio_storage"] = currentMinio
				} else {
					currentConfig["minio_storage"] = minioUpdates
				}
			}
		} else {
			currentConfig[key] = value
		}
	}


	// 4. Ghi lại cấu hình đã hợp nhất vào file
	updatedData, err := json.MarshalIndent(currentConfig, "", "  ")
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Error marshalling updated config"})
	}
	
	if err := os.WriteFile(configPath, updatedData, 0644); err != nil {
		log.Printf("Error writing updated config file: %v", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Could not write updated config file"})
	}

	log.Println("Crawler config updated successfully.")
	return c.JSON(http.StatusOK, currentConfig)
}