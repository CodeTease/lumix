package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// --- Cấu Hình Lith Rank ---
const (
	DampingFactor        = 0.85 // Xác suất người dùng tiếp tục click
	DefaultMaxIterations = 20   // Số vòng lặp để tính điểm
	DefaultInterval      = 60   // Phút (Mặc định tính lại mỗi tiếng)
)

// Page đại diện cho một trang web trong đồ thị
type Page struct {
	URL        string
	LinksOut   int                 // Số lượng link đi ra
	Score      float64             // Điểm Lith hiện tại
	NewScore   float64             // Điểm Lith tạm thời trong vòng lặp
	LinksInURL map[string]struct{} // Danh sách các trang trỏ đến trang này
}

func main() {
	log.Println("Lith Ranker: Starting up...")

	// 1. Lấy cấu hình từ biến môi trường
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("FATAL: DATABASE_URL is required.")
	}

	intervalMinutes := DefaultInterval
	if val := os.Getenv("LITH_INTERVAL_MINUTES"); val != "" {
		if i, err := strconv.Atoi(val); err == nil && i > 0 {
			intervalMinutes = i
		}
	}

	maxIter := DefaultMaxIterations
	if val := os.Getenv("LITH_MAX_ITERATIONS"); val != "" {
		if i, err := strconv.Atoi(val); err == nil && i > 0 {
			maxIter = i
		}
	}

	// 2. Kết nối Database
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("FATAL: Cannot connect to DB: %v", err)
	}
	defer pool.Close()
	log.Println("Lith Ranker: Connected to PostgreSQL.")

	// 3. Chờ một chút để Crawler chạy trước (nếu khởi động cùng lúc)
	log.Println("Lith Ranker: Warming up (30s)...")
	time.Sleep(30 * time.Second)

	// 4. Vòng lặp chính
	ticker := time.NewTicker(time.Duration(intervalMinutes) * time.Minute)
	defer ticker.Stop()

	// Chạy ngay lần đầu tiên
	runLithCycle(pool, maxIter)

	for range ticker.C {
		runLithCycle(pool, maxIter)
	}
}

func runLithCycle(pool *pgxpool.Pool, maxIter int) {
	log.Println("--- Starting Lith Rank Calculation Cycle ---")
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	if err := calculateLithRank(ctx, pool, maxIter); err != nil {
		log.Printf("ERROR: Lith calculation failed: %v", err)
	} else {
		log.Printf("SUCCESS: Lith Rank updated in %v", time.Since(start))
	}
	log.Println("--- Cycle Finished ---")
}

// calculateLithRank thực hiện thuật toán PageRank
func calculateLithRank(ctx context.Context, pool *pgxpool.Pool, maxIter int) error {
	// B1: Tải đồ thị từ DB vào RAM
	pages, err := loadGraph(ctx, pool)
	if err != nil {
		return err
	}

	count := len(pages)
	if count == 0 {
		log.Println("Graph is empty. Nothing to rank.")
		return nil
	}
	log.Printf("Graph loaded: %d pages.", count)

	// B2: Khởi tạo điểm số ban đầu (1.0 / N)
	initialScore := 1.0 / float64(count)
	for _, p := range pages {
		p.Score = initialScore
	}

	// B3: Lặp để tính toán (The core algorithm)
	// Công thức: PR(A) = (1-d)/N + d * Sum(PR(B)/L(B))
	baseScore := (1.0 - DampingFactor) / float64(count)

	for i := 0; i < maxIter; i++ {
		for _, p := range pages {
			incomingScore := 0.0
			for sourceURL := range p.LinksInURL {
				if sourcePage, ok := pages[sourceURL]; ok && sourcePage.LinksOut > 0 {
					incomingScore += sourcePage.Score / float64(sourcePage.LinksOut)
				}
			}
			p.NewScore = baseScore + (DampingFactor * incomingScore)
		}

		// Cập nhật điểm cho vòng lặp sau
		for _, p := range pages {
			p.Score = p.NewScore
		}
	}

	// B4: Lưu kết quả xuống DB
	return saveScores(ctx, pool, pages)
}

func loadGraph(ctx context.Context, pool *pgxpool.Pool) (map[string]*Page, error) {
	pages := make(map[string]*Page)

	// Lấy danh sách các trang (Nodes)
	rows, err := pool.Query(ctx, "SELECT url FROM crawled_pages")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var url string
		if err := rows.Scan(&url); err == nil {
			pages[url] = &Page{URL: url, LinksInURL: make(map[string]struct{})}
		}
	}

	// Lấy danh sách liên kết (Edges)
	linkRows, err := pool.Query(ctx, "SELECT source_url, target_url FROM page_links")
	if err != nil {
		return nil, err
	}
	defer linkRows.Close()

	for linkRows.Next() {
		var source, target string
		if err := linkRows.Scan(&source, &target); err == nil {
			// Nếu cả 2 trang đều tồn tại trong tập dữ liệu crawl
			if srcPage, ok := pages[source]; ok {
				if tgtPage, ok := pages[target]; ok {
					srcPage.LinksOut++
					tgtPage.LinksInURL[source] = struct{}{}
				}
			}
		}
	}

	return pages, nil
}

func saveScores(ctx context.Context, pool *pgxpool.Pool, pages map[string]*Page) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	batch := &pgx.Batch{}
	for _, p := range pages {
		// Queue câu lệnh update
		batch.Queue("UPDATE crawled_pages SET lith_score = $1 WHERE url = $2", p.Score, p.URL)
	}

	br := tx.SendBatch(ctx, batch)
	defer br.Close()

	if _, err := br.Exec(); err != nil {
		return err
	}

	return tx.Commit(ctx)
}