CREATE TABLE IF NOT EXISTS association_rules (
    id SERIAL PRIMARY KEY,
    antecedent INTEGER,
    antecedent_product_names TEXT,
    consequent INTEGER,
    consequent_product_names TEXT,
    confidence DOUBLE PRECISION,
    lift DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
