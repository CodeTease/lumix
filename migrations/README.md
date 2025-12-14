# Database Migrations

This directory contains database migration files. 
We recommend using [golang-migrate](https://github.com/golang-migrate/migrate) to manage schema changes.

## Usage

1. Install `golang-migrate`.
2. Run migrations:
   ```bash
   migrate -path ./migrations -database "$DATABASE_URL" up
   ```

## Files
- `000001_init_schema.up.sql`: Initial schema snapshot.
