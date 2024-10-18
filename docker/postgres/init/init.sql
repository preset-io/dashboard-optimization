-- Create the table if it doesn't exist
CREATE TABLE IF NOT EXISTS daily_order_summary (
    order_date DATE NOT NULL,
    mktsegment VARCHAR(20) NOT NULL,
    nation_name VARCHAR(25) NOT NULL,
    order_count INTEGER NOT NULL,
    total_price DECIMAL(15, 2) NOT NULL,
    PRIMARY KEY (order_date, mktsegment, nation_name)
);

-- Create an index if it doesn't exist
CREATE INDEX IF NOT EXISTS idx_daily_order_summary_date ON daily_order_summary (order_date);
