def query():
    return f"""
INSERT INTO {target} ()
SELECT
FROM {source}
GROUP BY 1, 2
ORDER BY 1, 2
ON CONFLICT () DO UPDATE
  SET
"""
