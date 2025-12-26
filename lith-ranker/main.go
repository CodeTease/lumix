package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// --- Lith Rank Config ---
const (
	DampingFactor        = 0.85 // Probability of user continuing to click
	DefaultMaxIterations = 20   // Number of iterations for score calculation
	DefaultInterval      = 60   // Minutes (Default recalculation every hour)
)

func main() {
	log.Println("Lith Ranker: Starting up...")

	// 1. Get config from environment variables
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

	// 2. Connect to Database
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("FATAL: Cannot connect to DB: %v", err)
	}
	defer pool.Close()
	log.Println("Lith Ranker: Connected to PostgreSQL.")

	// 3. Wait a bit for Crawler to start (if started simultaneously)
	log.Println("Lith Ranker: Warming up (30s)...")
	time.Sleep(30 * time.Second)

	// 4. Main Loop
	ticker := time.NewTicker(time.Duration(intervalMinutes) * time.Minute)
	defer ticker.Stop()

	// Run immediately first time
	runLithCycle(pool, maxIter)

	for range ticker.C {
		runLithCycle(pool, maxIter)
	}
}

func runLithCycle(pool *pgxpool.Pool, maxIter int) {
	log.Println("--- Starting Lith Rank Calculation Cycle ---")
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	if err := calculateLithRank(ctx, pool, maxIter); err != nil {
		log.Printf("ERROR: Lith calculation failed: %v", err)
	} else {
		log.Printf("SUCCESS: Lith Rank updated in %v", time.Since(start))
	}
	log.Println("--- Cycle Finished ---")
}

// calculateLithRank executes the PageRank algorithm using SQL to avoid OOM
func calculateLithRank(ctx context.Context, pool *pgxpool.Pool, maxIter int) error {
	// Start transaction
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// 1. Count pages to determine graph size
	var count int64
	err = tx.QueryRow(ctx, "SELECT COUNT(*) FROM crawled_pages").Scan(&count)
	if err != nil {
		return fmt.Errorf("counting pages: %w", err)
	}
	if count == 0 {
		log.Println("Graph is empty. Nothing to rank.")
		return nil
	}
	log.Printf("Graph size: %d pages.", count)

	// 2. Initialize Ranking Temp Table
	// We store ID, current Score, and outgoing link count
	log.Println("Initializing ranking temporary table...")
	_, err = tx.Exec(ctx, `
		CREATE TEMP TABLE pr_nodes (
			id BIGINT PRIMARY KEY,
			score FLOAT8,
			links_out INT
		) ON COMMIT DROP
	`)
	if err != nil {
		return fmt.Errorf("creating pr_nodes: %w", err)
	}

	initialScore := 1.0 / float64(count)

	// Populate pr_nodes
	// We calculate links_out once.
	_, err = tx.Exec(ctx, `
		INSERT INTO pr_nodes (id, score, links_out)
		SELECT
			cp.id,
			$1,
			(SELECT count(*) FROM page_links WHERE source_id = cp.id)
		FROM crawled_pages cp
	`, initialScore)
	if err != nil {
		return fmt.Errorf("populating pr_nodes: %w", err)
	}

	// 3. Iteration (Power Method)
	baseScore := (1.0 - DampingFactor) / float64(count)

	log.Printf("Starting %d iterations...", maxIter)
	for i := 0; i < maxIter; i++ {
		if i%5 == 0 {
			log.Printf("Iteration %d/%d", i+1, maxIter)
		}

		// Calculate new scores into a temporary structure
		// PR(target) = baseScore + d * SUM(source.score / source.links_out)

		_, err = tx.Exec(ctx, `
			CREATE TEMP TABLE pr_next_scores (
				id BIGINT PRIMARY KEY,
				score FLOAT8
			) ON COMMIT DROP
		`)
		if err != nil {
			return fmt.Errorf("creating pr_next_scores: %w", err)
		}

		// Calculate inbound scores and insert combined result
		_, err = tx.Exec(ctx, `
			WITH inbound AS (
				SELECT
					l.target_id,
					SUM(s.score / GREATEST(s.links_out, 1)) as val
				FROM page_links l
				JOIN pr_nodes s ON l.source_id = s.id
				GROUP BY l.target_id
			)
			INSERT INTO pr_next_scores (id, score)
			SELECT
				n.id,
				$1 + $2 * COALESCE(i.val, 0)
			FROM pr_nodes n
			LEFT JOIN inbound i ON n.id = i.target_id
		`, baseScore, DampingFactor)
		if err != nil {
			return fmt.Errorf("iteration %d calculation: %w", i, err)
		}

		// Update pr_nodes with the new scores
		_, err = tx.Exec(ctx, `
			UPDATE pr_nodes n
			SET score = next.score
			FROM pr_next_scores next
			WHERE n.id = next.id
		`)
		if err != nil {
			return fmt.Errorf("iteration %d update: %w", i, err)
		}

		// Cleanup next_scores for next iteration
		_, err = tx.Exec(ctx, "DROP TABLE pr_next_scores")
		if err != nil {
			return fmt.Errorf("dropping pr_next_scores: %w", err)
		}
	}

	// 4. Final Calculation & Update Main Table
	log.Println("Calculating final composite scores and updating crawled_pages...")

	// Update crawled_pages with calculated metrics
	// Freshness: max(0, 1 - (AgeInDays / 365))
	// Quality: log10(text_length) + penalty for bad readability (implied in usage)
	// Domain Authority: Bonus for .gov, .edu
	// Readability: from DB
	// Lith Score: Weighted sum
	
	// Note: We use the second query block which seems to be the one active/correct (the first one in original code looked like dead code or failed attempt).
	// We will update metrics calculation.

	_, err = tx.Exec(ctx, `
		WITH metrics AS (
			SELECT
				n.id,
				GREATEST(0, 1.0 - (EXTRACT(EPOCH FROM (NOW() - COALESCE(cp.crawled_at, NOW()))) / (86400.0 * 365.0))) as freshness,
				CASE WHEN cp.text_length > 0 THEN LOG(cp.text_length) ELSE 0 END as quality,
				COALESCE(cp.readability_score, 0) / 100.0 as readability_norm,
				CASE 
					WHEN cp.url LIKE '%.gov%' OR cp.url LIKE '%.gov.%' THEN 1.0
					WHEN cp.url LIKE '%.edu%' OR cp.url LIKE '%.edu.%' THEN 0.8
					WHEN cp.url LIKE '%.org%' THEN 0.5
					ELSE 0.0
				END as domain_auth,
				n.score as raw_pr
			FROM pr_nodes n
			JOIN crawled_pages cp ON n.id = cp.id
		)
		UPDATE crawled_pages cp
		SET
			lith_score = (m.raw_pr * 0.7) + (m.freshness * 0.1) + (m.quality * 0.1) + (m.readability_norm * 0.05) + (m.domain_auth * 0.05),
			freshness_score = m.freshness,
			quality_score = m.quality,
			domain_score = m.domain_auth
		FROM metrics m
		WHERE cp.id = m.id
	`)
	if err != nil {
		return fmt.Errorf("final update: %w", err)
	}

	return tx.Commit(ctx)
}
