use reqwest;
use serde_json;
use std::fs;

#[tokio::main]
/// 该程序用于批量管理和上传代币元数据。
///
/// 主要流程：
/// 1. 从本地 JSON 文件（"token_metadata_examples.json"）读取代币元数据列表。
/// 2. 解析 JSON 内容为 `serde_json::Value` 类型的向量。
/// 3. 遍历每个代币元数据，向本地服务（`http://localhost:3000/api/metadata/tokens`）
///    发送 POST 请求以创建代币元数据。
/// 4. 根据响应状态输出每个代币的创建结果（成功或失败）。
/// 5. 所有代币处理完成后，输出管理完成提示。
///
/// 依赖库：
/// - `serde_json` 用于解析和处理 JSON 数据。
/// - `reqwest` 用于异步 HTTP 请求。
/// - `tokio` 作为异步运行时。
///
/// 用法：
/// 确保 `token_metadata_examples.json` 文件存在于当前目录，
/// 并且本地服务已在指定端口运行。
/// cargo run --bin manage_token_metadata
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load token metadata from JSON file
    let json_content = fs::read_to_string("token_metadata_examples.json")?;
    let tokens: Vec<serde_json::Value> = serde_json::from_str(&json_content)?;

    let client = reqwest::Client::new();
    let base_url = "http://localhost:3000/api/metadata/tokens";

    for token in tokens {
        println!("Creating metadata for token: {}", token["symbol"]);

        let response = client.post(base_url).json(&token).send().await?;

        if response.status().is_success() {
            println!("✅ Successfully created metadata for {}", token["symbol"]);
        } else {
            println!(
                "❌ Failed to create metadata for {}: {}",
                token["symbol"],
                response.status()
            );
        }
    }

    println!("Token metadata management completed!");
    Ok(())
}
