# dbt Notes

## Examples

Examples:

1. Unit Testing - `models/dim/players_unit_test.yml` for Dict-only fixtures, `models/dim/teams_unit_test` for SQL File fixture
2. Meta Tags - `models/dim/players.yml`

## New Features

### Meta Tags

Meta Tags add additional metadata to your model documentation in the dbt Docs. If you don't use dbt Docs, then there probably isn't much value in adding these or using meta tags.

You can add things like Flags for PII Data, the owner of the dbt Model, and what state it's in (work in progress, for Client XYZ etc)

### Unit Testing

SQL File Fixtures split it up nicely but are a mfer to make.

- dict (default): Inline dictionary values.
- csv: Inline CSV values or a CSV file.
- sql: Inline SQL query or a SQL file. Note: For this format you must supply mock data for all rows.

```sql
SELECT 'Milwaukee Bucks' as team, 'MIL' as team_acronym
UNION ALL
SELECT 'Toronto Raptors', 'TOR', 'Eastern'
UNION ALL
SELECT 'Boston Celtics', 'BOS', 'Eastern'

```
