CREATE TABLE IF NOT EXISTS crawled_pages (
    
    id BIGSERIAL PRIMARY KEY,
    url VARCHAR(2048) UNIQUE NOT NULL,

    title VARCHAR(512),
    meta_description TEXT, 

    domain VARCHAR(255) NOT NULL,
    depth INTEGER NOT NULL,
    crawled_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- Truncated body text for quick previews and searches
    body_text TEXT, 
    
    -- Path to the full raw HTML stored in MinIO/TeaserCDN
    raw_html_path VARCHAR(1024),

    -- Detected language code (e.g., 'en', 'vi')
    language VARCHAR(10),

    -- Meilisearch sync timestamp
    indexed_at TIMESTAMPTZ,

    -- Full-text search vector
    tsv_document TSVECTOR
);

-- Index for full-text search
CREATE INDEX IF NOT EXISTS tsv_idx ON crawled_pages USING GIN (tsv_document);

-- Unique index for URL to prevent duplicates
CREATE UNIQUE INDEX IF NOT EXISTS url_idx ON crawled_pages (url);

-- Index for domain-based queries
CREATE INDEX IF NOT EXISTS domain_idx ON crawled_pages (domain);

-- Index for language-based queries
CREATE INDEX IF NOT EXISTS lang_idx ON crawled_pages (language);

