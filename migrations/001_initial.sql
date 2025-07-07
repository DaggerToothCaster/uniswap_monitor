-- Create trading pairs table
CREATE TABLE IF NOT EXISTS trading_pairs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain_id INTEGER NOT NULL,
    address VARCHAR(42) NOT NULL,
    token0 VARCHAR(42) NOT NULL,
    token1 VARCHAR(42) NOT NULL,
    token0_symbol VARCHAR(20),
    token1_symbol VARCHAR(20),
    token0_decimals INTEGER,
    token1_decimals INTEGER,
    token0_name VARCHAR(100),
    token1_name VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    block_number BIGINT NOT NULL,
    transaction_hash VARCHAR(66) NOT NULL,
    UNIQUE(chain_id, address)
);

-- Create swap events table
-- UNIQUE 唯一约束，防止重复
CREATE TABLE IF NOT EXISTS swap_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain_id INTEGER NOT NULL,
    pair_address VARCHAR(42) NOT NULL,
    sender VARCHAR(42) NOT NULL,
    amount0_in DECIMAL NOT NULL,
    amount1_in DECIMAL NOT NULL,
    amount0_out DECIMAL NOT NULL,
    amount1_out DECIMAL NOT NULL,
    to_address VARCHAR(42) NOT NULL,
    block_number BIGINT NOT NULL,
    transaction_hash VARCHAR(66) NOT NULL,
    log_index INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    UNIQUE(chain_id, transaction_hash, log_index)
);

-- Create mint events table
CREATE TABLE IF NOT EXISTS mint_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain_id INTEGER NOT NULL,
    pair_address VARCHAR(42) NOT NULL,
    sender VARCHAR(42) NOT NULL,
    amount0 DECIMAL NOT NULL,
    amount1 DECIMAL NOT NULL,
    block_number BIGINT NOT NULL,
    transaction_hash VARCHAR(66) NOT NULL,
    log_index INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    UNIQUE(chain_id, transaction_hash, log_index)
);

-- Create burn events table
CREATE TABLE IF NOT EXISTS burn_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain_id INTEGER NOT NULL,
    pair_address VARCHAR(42) NOT NULL,
    sender VARCHAR(42) NOT NULL,
    amount0 DECIMAL NOT NULL,
    amount1 DECIMAL NOT NULL,
    to_address VARCHAR(42) NOT NULL,
    block_number BIGINT NOT NULL,
    transaction_hash VARCHAR(66) NOT NULL,
    log_index INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    UNIQUE(chain_id, transaction_hash, log_index)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_trading_pairs_chain_address ON trading_pairs(chain_id, address);
CREATE INDEX IF NOT EXISTS idx_swap_events_pair ON swap_events(pair_address);
CREATE INDEX IF NOT EXISTS idx_swap_events_chain_pair ON swap_events(chain_id, pair_address);
CREATE INDEX IF NOT EXISTS idx_swap_events_timestamp ON swap_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_mint_events_chain_pair ON mint_events(chain_id, pair_address);
CREATE INDEX IF NOT EXISTS idx_burn_events_chain_pair ON burn_events(chain_id, pair_address);
