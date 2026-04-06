CREATE TABLE IF NOT EXISTS spark_apps_metadata (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline VARCHAR (100),
    genesis DATE,
    current DATE,
    layer VARCHAR (100)
);

INSERT INTO spark_apps_metadata (pipeline, genesis, current, layer) VALUES
('gaia_main_source', '2026-04-02', NULL, 'silver');
