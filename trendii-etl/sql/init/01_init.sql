CREATE DATABASE trendii;

CREATE OR REPLACE VIEW raw.events AS
SELECT
  (line->>'event_name')::text AS event_name,
  COALESCE(
    (line->>'event_created_at')::timestamptz,
    (line->'event_context'->>'ingested_at')::timestamptz
  )                           AS event_created_at,
  line->'event_data'          AS event_data,
  line->'event_context'       AS event_context
FROM raw.events_jsonl;
