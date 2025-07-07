-- Create token_metadata table
CREATE TABLE IF NOT EXISTS token_metadata (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain_id INTEGER NOT NULL,
    address VARCHAR(42) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    name VARCHAR(100) NOT NULL,
    decimals INTEGER NOT NULL,
    description TEXT,
    website_url VARCHAR(500),
    logo_url VARCHAR(500),
    twitter_url VARCHAR(500),
    telegram_url VARCHAR(500),
    discord_url VARCHAR(500),
    github_url VARCHAR(500),
    explorer_url VARCHAR(500),
    coingecko_id VARCHAR(100),
    coinmarketcap_id VARCHAR(100),
    total_supply DECIMAL,
    max_supply DECIMAL,
    is_verified BOOLEAN DEFAULT FALSE,
    tags JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(chain_id, address)
);

-- Create indexes for token_metadata
CREATE INDEX IF NOT EXISTS idx_token_metadata_chain_address ON token_metadata(chain_id, address);
CREATE INDEX IF NOT EXISTS idx_token_metadata_symbol ON token_metadata(symbol);
CREATE INDEX IF NOT EXISTS idx_token_metadata_verified ON token_metadata(is_verified);
CREATE INDEX IF NOT EXISTS idx_token_metadata_tags ON token_metadata USING GIN(tags);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger for token_metadata
CREATE TRIGGER update_token_metadata_updated_at 
    BEFORE UPDATE ON token_metadata 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();
