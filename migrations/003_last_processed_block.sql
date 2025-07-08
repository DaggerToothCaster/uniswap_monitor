-- Create table to track last processed block for each chain
CREATE TABLE IF NOT EXISTS last_processed_blocks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain_id INTEGER UNIQUE NOT NULL,
    last_block_number BIGINT NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create index for fast lookups
CREATE INDEX IF NOT EXISTS idx_last_processed_blocks_chain_id ON last_processed_blocks(chain_id);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_last_processed_blocks_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger for last_processed_blocks
CREATE TRIGGER update_last_processed_blocks_updated_at 
    BEFORE UPDATE ON last_processed_blocks 
    FOR EACH ROW 
    EXECUTE FUNCTION update_last_processed_blocks_updated_at();

-- Insert initial records for supported chains if they don't exist
INSERT INTO last_processed_blocks (chain_id, last_block_number) 
VALUES 
    (1, 0),     -- Ethereum
    (56, 0),    -- BSC
    (137, 0),   -- Polygon
    (42161, 0)  -- Arbitrum
ON CONFLICT (chain_id) DO NOTHING;
