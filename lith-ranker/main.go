package main

import (
	"context"
	"fmt"
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
// Optimized: Uses int64 ID instead of string URL to save RAM
type Page struct {
	ID       int64
	URL      string // Still keep URL for reference if needed, but not as map key
	LinksOut int    // Số lượng link đi ra
	Score    float64
	NewScore float64
	LinksIn  []int64 // List of IDs pointing to this page (replacing map[string]struct{})
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

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute) // Increased timeout for heavy tasks
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
	// B1: Tải đồ thị từ DB vào RAM (Optimized)
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
			for _, sourceID := range p.LinksIn {
				if sourcePage, ok := pages[sourceID]; ok && sourcePage.LinksOut > 0 {
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

	// B4: Lưu kết quả xuống DB (Optimized with Temp Table)
	return saveScores(ctx, pool, pages)
}

func loadGraph(ctx context.Context, pool *pgxpool.Pool) (map[int64]*Page, error) {
	pages := make(map[int64]*Page)
	urlToID := make(map[string]int64)

	// Lấy danh sách các trang (Nodes)
	// Fetch ID as well
	rows, err := pool.Query(ctx, "SELECT id, url FROM crawled_pages")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var url string
		if err := rows.Scan(&id, &url); err == nil {
			pages[id] = &Page{
				ID:      id,
				URL:     url,
				LinksIn: []int64{},
			}
			urlToID[url] = id
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
			// Resolve URLs to IDs
			srcID, srcOk := urlToID[source]
			tgtID, tgtOk := urlToID[target]

			if srcOk && tgtOk {
				if srcPage, ok := pages[srcID]; ok {
					srcPage.LinksOut++
				}
				if tgtPage, ok := pages[tgtID]; ok {
					tgtPage.LinksIn = append(tgtPage.LinksIn, srcID)
				}
			}
		}
	}

	// urlToID map will be garbage collected after this function returns
	return pages, nil
}

func saveScores(ctx context.Context, pool *pgxpool.Pool, pages map[int64]*Page) error {
	// Must use a transaction to ensure the temporary table persists across commands
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Create a temporary table
	_, err = tx.Exec(ctx, "CREATE TEMP TABLE lith_scores_temp (id BIGINT, score FLOAT) ON COMMIT DROP")
	if err != nil {
		return fmt.Errorf("creating temp table: %w", err)
	}

	// Prepare data for COPY
	rows := [][]interface{}{}
	for _, p := range pages {
		rows = append(rows, []interface{}{p.ID, p.Score})
	}

	// Bulk Insert into Temp Table using COPY
	count, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"lith_scores_temp"},
		[]string{"id", "score"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("copying to temp table: %w", err)
	}
	log.Printf("Copied %d rows to temp table.", count)

	// Single UPDATE from Temp Table
	// Using FROM clause to update efficiently
	updateQuery := `
		UPDATE crawled_pages
		SET lith_score = t.score
		FROM lith_scores_temp t
		WHERE crawled_pages.id = t.id
	`
	cmdTag, err := tx.Exec(ctx, updateQuery)
	if err != nil {
		return fmt.Errorf("updating main table: %w", err)
	}

	log.Printf("Updated %d rows in crawled_pages.", cmdTag.RowsAffected())
	return tx.Commit(ctx)
}
