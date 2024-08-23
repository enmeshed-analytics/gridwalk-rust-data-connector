use aws_config;
use aws_config::BehaviorVersion;
use aws_sdk_sts::config::ProvideCredentials;
use deltalake::{open_table_with_storage_options, DeltaTable, DeltaTableError};
use std::collections::HashMap;
use std::time::Duration;

// Load AWS Creds into a hashmap for use with delta lake reader
pub async fn get_aws_config() -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .retry_config(aws_config::retry::RetryConfig::standard().with_max_attempts(5))
        .timeout_config(
            aws_config::timeout::TimeoutConfig::builder()
                .operation_timeout(Duration::from_secs(30))
                .build(),
        )
        .load()
        .await;

    let mut aws_info = HashMap::new();

    // Add credentials to HashMap if available
    if let Some(creds_provider) = config.credentials_provider() {
        match creds_provider.provide_credentials().await {
            Ok(creds) => {
                aws_info.insert(
                    "AWS_ACCESS_KEY_ID".to_string(),
                    creds.access_key_id().to_string(),
                );
                aws_info.insert(
                    "AWS_SECRET_ACCESS_KEY".to_string(),
                    creds.secret_access_key().to_string(),
                );
                if let Some(session_token) = creds.session_token() {
                    aws_info.insert("AWS_SESSION_TOKEN".to_string(), session_token.to_string());
                }
            }
            Err(e) => return Err(format!("Failed to retrieve credentials: {}", e).into()),
        }
    } else {
        return Err("No credentials provider found in the configuration".into());
    }

    // Add success message
    println!("AWS configuration loaded successfully and added to HashMap.");

    Ok(aws_info)
}

// Read basic info about delta lake stored in S3
pub async fn load_remote_delta_lake_table_info(
    s3_uri: &str,
    credential_hash_map: HashMap<String, String>,
) -> Result<DeltaTable, DeltaTableError> {
    let storage_options: HashMap<String, String> = credential_hash_map;
    deltalake_aws::register_handlers(None);
    let remote_delta_lake_table = open_table_with_storage_options(s3_uri, storage_options).await?;
    println!("version: {}", remote_delta_lake_table.version());
    println!("metadata: {:?}", remote_delta_lake_table.metadata());
    Ok(remote_delta_lake_table)
}
