-- 修改 last_processed_blocks 表，添加事件类型字段
ALTER TABLE last_processed_blocks ADD COLUMN IF NOT EXISTS event_type VARCHAR(20) DEFAULT 'unified';

-- 删除原有的唯一约束
ALTER TABLE last_processed_blocks DROP CONSTRAINT IF EXISTS last_processed_blocks_chain_id_key;

-- 添加新的复合唯一约束
ALTER TABLE last_processed_blocks ADD CONSTRAINT last_processed_blocks_chain_event_unique 
    UNIQUE(chain_id, event_type);

-- 为现有记录设置事件类型
UPDATE last_processed_blocks SET event_type = 'unified' WHERE event_type IS NULL;

-- 为每个链创建工厂和交换事件的独立记录
INSERT INTO last_processed_blocks (chain_id, event_type, last_block_number, created_at, updated_at)
SELECT 
    chain_id,
    'factory' as event_type,
    last_block_number,
    NOW(),
    NOW()
FROM last_processed_blocks 
WHERE event_type = 'unified'
ON CONFLICT (chain_id, event_type) DO NOTHING;

INSERT INTO last_processed_blocks (chain_id, event_type, last_block_number, created_at, updated_at)
SELECT 
    chain_id,
    'swap' as event_type,
    last_block_number,
    NOW(),
    NOW()
FROM last_processed_blocks 
WHERE event_type = 'unified'
ON CONFLICT (chain_id, event_type) DO NOTHING;

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_last_processed_blocks_chain_event ON last_processed_blocks(chain_id, event_type);

-- 创建视图，用于查看整体处理进度
CREATE OR REPLACE VIEW processing_status AS
SELECT 
    chain_id,
    CASE 
        WHEN chain_id = 1 THEN 'Ethereum'
        WHEN chain_id = 56 THEN 'BSC'
        WHEN chain_id = 137 THEN 'Polygon'
        WHEN chain_id = 42161 THEN 'Arbitrum'
        WHEN chain_id = 2643 THEN 'NOS'
        WHEN chain_id = 2559 THEN 'KTO'
        ELSE 'Unknown'
    END as chain_name,
    MAX(CASE WHEN event_type = 'factory' THEN last_block_number END) as factory_block,
    MAX(CASE WHEN event_type = 'swap' THEN last_block_number END) as swap_block,
    MIN(CASE WHEN event_type IN ('factory', 'swap') THEN last_block_number END) as min_processed_block,
    MAX(CASE WHEN event_type IN ('factory', 'swap') THEN last_block_number END) as max_processed_block,
    MAX(CASE WHEN event_type = 'factory' THEN updated_at END) as factory_updated_at,
    MAX(CASE WHEN event_type = 'swap' THEN updated_at END) as swap_updated_at
FROM last_processed_blocks 
WHERE event_type IN ('factory', 'swap')
GROUP BY chain_id
ORDER BY chain_id;
