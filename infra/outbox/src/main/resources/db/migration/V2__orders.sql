-- Both applications share one migration history for the same database.
CREATE TABLE orders (
    id UUID PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
