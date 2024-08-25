use grid_walk_rust_data_loader::delta_lake_connector::{
    get_aws_config, load_delta_lake_file_to_datafusion,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match get_aws_config().await {
        Ok(aws_creds) => {
            // Specify S3 URI
            let s3_uri = "s3://datastackprod-silverdatabucket04c06b24-mrfdumn6njwe/national_charge_point_data_silver/";

            match load_delta_lake_file_to_datafusion(s3_uri, aws_creds).await {
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
