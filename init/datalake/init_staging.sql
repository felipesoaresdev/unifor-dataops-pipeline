CREATE SCHEMA IF NOT EXISTS staging_tabnews;

CREATE TABLE IF NOT EXISTS staging_tabnews.contents (
    id UUID PRIMARY KEY,
    slug TEXT,
    title TEXT,
    owner_username TEXT,
    created_at TIMESTAMPTZ,
    tabcoins INTEGER,
    children_deep_count INTEGER,
    _ingested_at TIMESTAMPTZ
);
