use serde_json;
use std::fs;
use reqwest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load token metadata from JSON file
    let json_content = fs::read_to_string("token_metadata_examples.json")?;
    let tokens: Vec<serde_json::Value> = serde_json::from_str(&json_content)?;

    let client = reqwest::Client::new();
    let base_url = "http://localhost:3000/api/metadata/tokens";

    for token in tokens {
        println!("Creating metadata for token: {}", token["symbol"]);
        
        let response = client
            .post(base_url)
            .json(&token)
            .send()
            .await?;

        if response.status().is_success() {
            println!("✅ Successfully created metadata for {}", token["symbol"]);
        } else {
            println!("❌ Failed to create metadata for {}: {}", token["symbol"], response.status());
        }
    }

    println!("Token metadata management completed!");
    Ok(())
}
