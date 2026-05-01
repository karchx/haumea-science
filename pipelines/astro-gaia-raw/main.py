import os
import time
import logging
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from astroquery.gaia import Gaia
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

# Basic configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
S3_ENDPOINT = os.environ.get('S3_ENDPOINT', 'http://minio.platform:9000')
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'gaia-source')

S3_PREFIX="raw"
s3_client = boto3.client(
    's3', 
    endpoint_url=S3_ENDPOINT, 
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)
MAX_WORKERS = 4  # Gaia restricts concurrent connections, 4 is a safe limit.
CHUNK_SIZE_CSV = 100000  # Rows loaded into RAM simultaneously per worker
TOTAL_RECORDS = 89_946_381
PARTITIONS = 10 # Split the 89M into 10 async jobs of ~8.9M each

def execute_and_download_chunk(partition_id, start_idx, end_idx):
    """
    Launches an async job, downloads it to disk, and incrementally converts it to Parquet.
    """
    temp_csv = f"temp_gaia_part_{partition_id}.csv"
    temp_parquet = f"temp_gaia_part_{partition_id}.parquet"
    s3_key = f"{S3_PREFIX}/part_{partition_id:04d}.parquet"
    
    # Your base ADQL query, injecting the random_index limits
    # Adjust columns according to your needs to reduce I/O.
    query = f"""
    SELECT * FROM gaiadr3.gaia_source
    WHERE random_index >= {start_idx} AND random_index < {end_idx}
    AND parallax_over_error > 10 AND ruwe < 1.2
    """
    
    start_time = time.time()
    
    # Retry management
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logging.info(f"[Part {partition_id}] Starting Async Job (Attempt {attempt+1})")
            
            # 1. Launch async job and download directly to disk
            job = Gaia.launch_job_async(
                query=query,
                dump_to_file=True,
                output_file=temp_csv,
                output_format="csv" # CSV is crucial for easy iteration in pandas
            )
            
            logging.info(f"[Part {partition_id}] Download completed. Converting to Parquet...")
            
            # 2. Read CSV in streaming mode and write to Parquet
            writer = None
            total_rows = 0
            
            # Iterate over the downloaded CSV without loading it entirely into RAM
            for chunk in pl.scan_csv(temp_csv).collect_batches(chunk_size=CHUNK_SIZE_CSV):
                table = pa.table(chunk)
                
                # Initialize the Parquet writer with the schema from the first chunk
                if writer is None:
                    writer = pq.ParquetWriter(temp_parquet, table.schema, compression='snappy')
                
                writer.write_table(table)
                total_rows += len(chunk)
            
            if writer:
                writer.close()

            logging.info(f"upload {partition_id} to minio")
            s3_client.upload_file(temp_parquet, BUCKET_NAME, s3_key)
            
            if os.path.exists(temp_csv): os.remove(temp_csv)
            if os.path.exists(temp_parquet): os.remove(temp_parquet)
                
            elapsed = time.time() - start_time
            throughput = total_rows / elapsed if elapsed > 0 else 0
            
            logging.info(f"[Part {partition_id}] SUCCESS: {total_rows} rows written in {elapsed:.2f}s "
                         f"({throughput:.0f} rows/s).")
            return total_rows
            
        except Exception as e:
            logging.error(f"[Part {partition_id}] Error: {str(e)}")
            if attempt == max_retries - 1:
                logging.error(f"[Part {partition_id}] Permanent failure after {max_retries} attempts.")
                # Ensure temp file cleanup in case of critical error
                if os.path.exists(temp_csv): os.remove(temp_csv)
                raise
            time.sleep(2 ** attempt) # Exponential backoff fallback

def main():
    # Assuming we want to filter a subset, map the random_index range
    # For 89M records (approx 5% of the 1.8 billion Gaia catalog)
    # Adjust the indices according to your actual WHERE filter.
    index_step = TOTAL_RECORDS // PARTITIONS
    tasks = []
    
    # Generate boundaries for each partition
    for i in range(PARTITIONS):
        start = i * index_step
        end = start + index_step if i < PARTITIONS - 1 else TOTAL_RECORDS
        tasks.append((i, start, end))
        
    total_processed = 0
    global_start = time.time()
    
    # Execute in parallel (I/O bound)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Map futures to their IDs for better traceability
        future_to_part = {executor.submit(execute_and_download_chunk, t[0], t[1], t[2]): t[0] for t in tasks}
        
        for future in as_completed(future_to_part):
            part_id = future_to_part[future]
            try:
                rows = future.result()
                total_processed += rows
            except Exception as exc:
                logging.error(f"Partition {part_id} generated an exception: {exc}")

    global_elapsed = time.time() - global_start
    logging.info("="*50)
    logging.info("EXTRACTION COMPLETED")
    logging.info(f"Total records: {total_processed}")
    logging.info(f"Total time: {global_elapsed / 60:.2f} minutes")
    logging.info(f"Global throughput: {total_processed / global_elapsed:.0f} rows/second")

if __name__ == "__main__":
    main()
