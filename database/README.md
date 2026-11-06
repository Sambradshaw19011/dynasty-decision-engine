# Database

Supabase/PostgreSQL is the planned central database for curated warehouse tables and query-serving analytics. The connection and `schema.sql` files are existing placeholders, not operational implementations.

No migrations or production tables exist yet. The canonical schema plan is [../docs/database_schema.md](../docs/database_schema.md); future schema changes belong in `migrations/`. Do not apply changes to a live Supabase project as part of this planning scaffold.
