# Init-Load Pipeline (S3 to Bronze)

This Go-based service handles the initial data ingestion process. It extracts raw data from S3-compatible storage (AWS S3 or Minio), processes it through PyArrow over the network for schema enforcement, and saves the final output as Parquet files in the **Bronze** layer.

## Directory Structure

```text
pipelines/init-load
├── config.json    # Connection parameters and paths
├── go.mod         # Go module definition
├── go.sum         # Dependency checksums
└── main.go        # Main pipeline logic
```

## Data Flow

1. **Extraction:** Connects to the source bucket (S3/Minio) to read raw objects.
2. **Processing:** Data is sent over the network to be handled by PyArrow, ensuring efficient serialization and strict type mapping.
3. **Loading:** The processed data is persisted into the **Bronze** layer in Parquet format, optimized for analytical workloads.


## Configuration

Modify `config.json` to define your environment settings:

```json
[
    {
      "source": "raw/nss_non_linear_spectro.csv",
      "dest": "bronze",
      "format": "csv",
      "table_name": "nss_non_linear_spectro",
      "partitions": [
        {
            "partition_key": "year",
            "partition_value": "current_year"
        },
        {
            "partition_key": "month",
            "partition_value": "current_month"
        },
        {
            "partition_key": "day",
            "partition_value": "current_day"
        }
      ]
    }
]
```

## Getting Started

1. **Install dependencies:**
   ```bash
   go mod tidy
   ```

2. **Run the pipeline:**
   ```bash
   go run main.go
   ```

```
+----------------+      +-------------------+ 
| NASA Exoplanet |      | Gaia Source DR3   | 
| API (JSON/CSV) |      | Archive (VOTable) | 
+-------+--------+      +---------+---------+ 
        |                         |           
        v                         v           
+---------------------------------------------------------------------+
|                     StellaDag (Orchestration local)                 |
+---------------------------------------------------------------------+
        |                         |
        | Batch Pull              | Batch Pull
        v                         v
+---------------------------------------------------------------------+
|                          DATA LAKE                                  |
|                                                                     |
|  +--------------+       +--------------+       +--------------+     |
|  |   BRONZE     | ----> |   SILVER     | ----> |    GOLD      |     |
|  | (Raw format) | Spark | (Iceberg)    | Spark | (Iceberg)    |     |
|  +--------------+       +--------------+       +--------------+     |
+---------------------------------------------------------------------+
                                                          |
                                                          | PyArrow/DuckDB
                                                          v
+-----------------------+      +--------------------------------------+
|  MODEL SERVING        |      |      JUPYTERLAB / MODEL TRAINING     |
|  Batch Inference      | <--- | XGBoost, scikit-learn, SHAP          |
|  Delta Table Output   |      | Model Registry (MLflow)              |
+-----------------------+      +--------------------------------------+
```
