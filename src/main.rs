use grid_walk_rust_data_loader::delta_lake_connector::{
    get_aws_config, load_remote_delta_lake_table_info,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match get_aws_config().await {
        Ok(config) => {
            // S3 URI
            let s3_uri = "s3://ADD_REST_HERE";

            match load_remote_delta_lake_table_info(s3_uri, config).await {
                Ok(_table) => {
                    println!("Successfully loaded the Delta table");
                    Ok(())
                }
                Err(e) => {
                    eprintln!("Error loading the Delta table: {}", e);
                    Err(e.into())
                }
            }
        }
        Err(e) => {
            eprintln!("Error getting AWS configuration: {}", e);
            Err(e.into())
        }
    }
}
