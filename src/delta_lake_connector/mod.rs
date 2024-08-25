//! This module contains baisc functions to fetch aws credentials and read a remote DeltaLake

use aws_config;
use aws_config::BehaviorVersion;
use aws_sdk_sts::config::ProvideCredentials;
use deltalake::{open_table_with_storage_options, ObjectStore};
use std::collections::HashMap;
use std::time::Duration;
use datafusion::prelude::*;
use std::sync::Arc;
use url::Url;
use deltalake::storage::object_store::aws::AmazonS3;


/// This function fetches the AWS credentials
///
/// # Arguments
/// None
///
/// # Returns
///
/// HashMap with AWS credentials
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

    // Create a mutable HashMap
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

/// This function reads a remote delta lake stored in S3
///
/// # Arguments
///
/// * String reference - should be a S3 URI
/// * HashMap containing AWS ACCESS ID etc etc
///
/// # Returns
///
/// DeltaTable
pub async fn load_delta_lake_file_to_datafusion(
    s3_uri: &str,
    credential_hash_map: HashMap<String, String>,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    // Register AWS handlers
    deltalake_aws::register_handlers(None);

    // Parse the S3 URI
    let url = Url::parse(s3_uri)?;
    let bucket = url.host_str().ok_or("Invalid S3 URI: missing bucket name")?;

    // Create the AmazonS3 object store using deltalake's implementation
    let s3: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3::new()
            .with_bucket_name(bucket)
            .with_access_key_id(credential_hash_map.get("AWS_ACCESS_KEY_ID").unwrap())
            .with_secret_access_key(credential_hash_map.get("AWS_SECRET_ACCESS_KEY").unwrap())
            .with_region("eu-west-2")
            .build()?
    );

    // Create storage options - should have AWS access ID etc.
    let mut storage_options: HashMap<String, String> = credential_hash_map.clone();
    storage_options.insert("bucket".to_string(), bucket.to_string());
    storage_options.insert("region".to_string(), "eu-west-2".to_string());

    // Open the table with storage options
    let loaded_table = open_table_with_storage_options(s3_uri, storage_options).await?;

    // Get the first file from the table
    let mut files_iter = loaded_table.get_file_uris()?;
    let first_file = files_iter.next().ok_or("No files found in the table")?;
    println!("First file: {}", first_file);

    // Create a new DataFusion context
    let ctx = SessionContext::new();
    ctx.runtime_env().register_object_store(&url, s3);

    // Read the Parquet file into a DataFrame
    let df = ctx.read_parquet(first_file, ParquetReadOptions::default()).await?;

    Ok(df)
}

