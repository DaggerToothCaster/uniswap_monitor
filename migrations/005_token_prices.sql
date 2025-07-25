-- 创建代币价格表
CREATE TABLE IF NOT EXISTS token_prices (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain_id INTEGER NOT NULL,
    token_address VARCHAR(42) NOT NULL,
    token_symbol VARCHAR(20) NOT NULL,
    price_usd DECIMAL(36, 18) NOT NULL,
    source VARCHAR(50) NOT NULL, -- 价格来源，如 'bidacoin', 'coingecko' 等
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_token_prices_token ON token_prices(chain_id, token_address);
CREATE INDEX IF NOT EXISTS idx_token_prices_symbol ON token_prices(token_symbol);

-- 创建复合索引用于查询最新价格
CREATE INDEX IF NOT EXISTS idx_token_prices_latest ON token_prices(chain_id, token_address, timestamp DESC);

-- 添加注释
COMMENT ON TABLE token_prices IS '代币价格历史记录表';
COMMENT ON COLUMN token_prices.chain_id IS '区块链ID';
COMMENT ON COLUMN token_prices.token_address IS '代币合约地址';
COMMENT ON COLUMN token_prices.token_symbol IS '代币符号';
COMMENT ON COLUMN token_prices.price_usd IS '美元价格';
COMMENT ON COLUMN token_prices.source IS '价格数据来源';
COMMENT ON COLUMN token_prices.timestamp IS '价格时间戳';
