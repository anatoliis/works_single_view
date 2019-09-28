# Raw SQL is used to engage DB functionality that is not accessible using ORMs

from psycopg2 import sql

# This query copies all entries with non empty 'iswc' from temporary table into
# target one. Target table has unique constraint on 'iswc' column, so in case
# of conflict, contributors from both temporary and target table will be
# joined, deduplicated and stored in the appropriate row of target table.
MERGE_ROWS_BY_ISWC = sql.SQL(
    """\
INSERT INTO {target_table} AS target (title, contributors, iswc)
SELECT title, contributors, iswc FROM {tmp_table} AS source
WHERE source.iswc IS NOT NULL
ON CONFLICT (iswc) DO UPDATE SET
contributors=(
  SELECT array(
   SELECT DISTINCT *
     FROM unnest(target.contributors::TEXT[] || EXCLUDED.contributors::TEXT[]) AS c
   ORDER BY c
  )
)"""
)

# This query operates on entries from temporary table, that don't have 'iswc' value set.
# It will check whether target table contains a row with the same 'title' and at least
# one contributor in common. In case of success, target table row's contributors array
# will be updated with contributors from corresponding source table's row.
MERGE_ROWS_BY_TITLE_AND_ONE_CONTRIBUTOR = sql.SQL(
    """\
UPDATE {target_table} AS target
SET contributors = (
  SELECT array(
   SELECT DISTINCT *
     FROM unnest(target.contributors::TEXT[] || source.contributors::TEXT[]) AS c
   ORDER BY c
  )
)
FROM {tmp_table} AS source
WHERE source.iswc IS NULL
  AND target.title = source.title
  AND array_length(
    (
      SELECT array(
        SELECT unnest(source.contributors)
        INTERSECT
        SELECT unnest(target.contributors)
      )
    ),
    1
  ) > 0"""
)

COPY_ROWS_WITHOUT_ISWC = sql.SQL(
    """\
SELECT
source.title,
(
  SELECT array(
   SELECT DISTINCT *
     FROM unnest(target.contributors::TEXT[] || source.contributors::TEXT[]) AS c
   ORDER BY c
  )
) AS contributors,
target.iswc
FROM {tmp_table} as source
INNER JOIN {target_table} as target
ON source.title = target.title
WHERE source.iswc IS NULL
AND array_length(
  (
    SELECT array(
      SELECT unnest(source.contributors)
      INTERSECT
      SELECT unnest(target.contributors)
    )
  ),
  1
) > 0"""
)

COPY_ROWS_WITHOUT_ISWC = sql.SQL(
    """\
INSERT INTO {target_table} AS target (title, contributors, iswc)
SELECT title, contributors, iswc FROM {tmp_table} AS source
WHERE source.iswc IS NOT NULL
  AND (
    target.title = source.title
    AND array_length(
      (
        SELECT array(
          SELECT unnest(source.contributors)
          INTERSECT
          SELECT unnest(target.contributors)
        )
      ),
      1
    ) > 0
  ) IS FALSE"""
)

COPY_FROM_QUERY = sql.SQL(
    "COPY {tmp_table} (title, contributors, iswc) FROM STDIN WITH CSV HEADER"
)

CREATE_TEMP_TABLE_QUERY = sql.SQL(
    """\
CREATE TABLE IF NOT EXISTS {tmp_table} (
  id SERIAL PRIMARY KEY,
  title TEXT,
  contributors TEXT [],
  iswc TEXT
)
"""
)

DROP_TEMP_TABLE_QUERY = sql.SQL("DROP TABLE IF EXISTS {tmp_table} CASCADE")

CLEAR_TEMP_TABLE_QUERY = sql.SQL("DELETE FROM {tmp_table}")
