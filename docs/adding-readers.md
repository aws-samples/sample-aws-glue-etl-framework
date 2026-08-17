# Adding Custom Readers

This guide explains how to extend the ETL Framework with custom source readers.

## Overview

The framework uses a plugin registry system. To add a new data source:

1. Create a reader class implementing `ReaderInterface`
2. Register it with `ReaderRegistry`
3. Reference the new type in your job configuration JSON

## Step 1: Create Your Reader Class

```python
# my_readers/shopify_reader.py

import traceback
from typing import Optional, Tuple

from etl_framework.glue.generic.models.job_config_models import JobContext, SourceDef
from etl_framework.glue.generic.readers.reader_interface import ReaderInterface


class ShopifyReader(ReaderInterface):
    """Reads order data from Shopify Admin API."""

    def __init__(self, job_context: JobContext):
        super().__init__(job_context)

    def read(self, source_def: SourceDef) -> Tuple[bool, Optional[str]]:
        """
        Read from Shopify API and create a temp view.

        Expected source_def fields:
        - source_api_url: Shopify store URL (e.g., "https://my-store.myshopify.com")
        - source_api_resource_path: API endpoint (e.g., "/admin/api/2024-01/orders.json")
        - source_api_secrets_arn: Secrets Manager ARN with API token
        """
        try:
            import json
            import requests
            import boto3

            # Get API credentials
            sm = boto3.client("secretsmanager", region_name=self.job_context.aws_region)
            secret = json.loads(
                sm.get_secret_value(SecretId=source_def.source_api_secrets_arn)["SecretString"]
            )
            access_token = secret["access_token"]

            # Build request
            base_url = source_def.source_api_url.rstrip("/")
            endpoint = source_def.source_api_resource_path
            url = f"{base_url}{endpoint}"

            headers = {
                "X-Shopify-Access-Token": access_token,
                "Content-Type": "application/json",
            }

            # Fetch data (with pagination)
            all_records = []
            while url:
                response = requests.get(url, headers=headers, timeout=60)
                response.raise_for_status()
                data = response.json()

                # Extract orders (or whatever resource)
                records = data.get("orders", data.get("products", []))
                all_records.extend(records)

                # Handle pagination via Link header
                url = None
                link_header = response.headers.get("Link", "")
                if 'rel="next"' in link_header:
                    for part in link_header.split(","):
                        if 'rel="next"' in part:
                            url = part.split("<")[1].split(">")[0]

            if not all_records:
                self.logger.info("ShopifyReader: No records returned")
                return False, None

            # Convert to DataFrame
            df = self.spark.createDataFrame(all_records)

            # Apply filters
            if source_def.source_select_columns:
                df = df.select(*source_def.source_select_columns)
            if source_def.source_filter:
                df = df.filter(source_def.source_filter)

            # Create temp view
            temp_view_name = self._create_temp_view(df, source_def)
            return False, temp_view_name

        except Exception as e:
            self.logger.error(f"ShopifyReader: Failed: {str(e)}")
            self.logger.error(traceback.format_exc())
            return True, None
```

## Step 2: Register Your Reader

There are several ways to register:

### Option A: In a separate module (recommended for packages)

```python
# my_readers/__init__.py
from etl_framework.glue.generic.registry import ReaderRegistry
from my_readers.shopify_reader import ShopifyReader

ReaderRegistry.register("SHOPIFY", ShopifyReader)
```

### Option B: In the Glue job script

```python
# Before SourceReader is instantiated in your custom Glue job:
from etl_framework.glue.generic.registry import ReaderRegistry
from my_readers.shopify_reader import ShopifyReader

ReaderRegistry.register("SHOPIFY", ShopifyReader)
```

### Option C: Self-registering module pattern

```python
# shopify_reader.py (at module level, outside the class)
from etl_framework.glue.generic.registry import ReaderRegistry

class ShopifyReader(ReaderInterface):
    ...

# Auto-register when module is imported
ReaderRegistry.register("SHOPIFY", ShopifyReader)
```

## Step 3: Use in Job Configuration

```json
{
  "config_key": "SHOPIFY_ORDERS_JOB",
  "source_list": [
    {
      "source_key": "shopify_orders",
      "source_type": "SHOPIFY",
      "source_api_url": "https://my-store.myshopify.com",
      "source_api_resource_path": "/admin/api/2024-01/orders.json",
      "source_api_secrets_arn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:shopify-api-key",
      "create_temp_view": true,
      "source_select_columns": ["id", "email", "total_price", "created_at"]
    }
  ],
  "target_list": [
    {
      "target_type": "S3",
      "source_config_key": "shopify_orders",
      "database_name": "ecommerce",
      "table_name": "orders",
      "target_s3_path": "s3://<YOUR-BUCKET-NAME>/raw/shopify/orders/",
      "target_format": "parquet",
      "write_mode": "append"
    }
  ]
}
```

## ReaderInterface Contract

Your reader **must**:
- Inherit from `ReaderInterface`
- Implement `read(self, source_def: SourceDef) -> Tuple[bool, Optional[str]]`
- Return `(False, temp_view_name)` on success
- Return `(True, None)` on failure
- Return `(False, None)` when there is no new data (e.g., watermark says no updates)

Your reader **gets**:
- `self.job_context` - Full job context (logger, Spark, region, buckets, etc.)
- `self.logger` - Pre-configured logger
- `self.spark` - SparkSession
- `self._create_temp_view(df, source_def)` - Helper to create and name temp views

## Packaging Tips

- Include your custom reader in the ETL Framework wheel (add to `src/`)
- Or package it as a separate wheel and include via `--extra-py-files`
- For Glue jobs, all dependencies must be available on the job runtime
