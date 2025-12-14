CREATE TABLE IF NOT EXISTS crawler_config (
    id SERIAL PRIMARY KEY,
    key VARCHAR(255) UNIQUE NOT NULL,
    value JSONB NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS crawled_pages (
    id BIGSERIAL PRIMARY KEY,
    url TEXT UNIQUE NOT NULL,
    title TEXT,
    meta_description TEXT,
    raw_html_path TEXT,
    domain TEXT,
    language TEXT,
    crawled_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    indexed_at TIMESTAMP WITH TIME ZONE,
    text_length INT DEFAULT 0,
    lith_score FLOAT DEFAULT 1.0,
    freshness_score FLOAT DEFAULT 0.0,
    quality_score FLOAT DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS page_links (
    source_id BIGINT REFERENCES crawled_pages(id) ON DELETE CASCADE,
    target_id BIGINT REFERENCES crawled_pages(id) ON DELETE CASCADE,
    PRIMARY KEY (source_id, target_id)
);
