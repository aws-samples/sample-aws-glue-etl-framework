# Custom Reader Example

This example demonstrates how to extend the ETL Framework with a custom reader.

## Steps

1. Create your reader class implementing `ReaderInterface`
2. Register it with the `ReaderRegistry`
3. Use the custom storage type in your job configuration

## Example: Custom CSV-from-SFTP Reader

```python
# my_sftp_reader.py
from etl_framework.glue.generic.readers.reader_interface import ReaderInterface
from etl_framework.glue.generic.registry import ReaderRegistry
from etl_framework.glue.generic.models.job_config_models import JobContext, SourceDef


class SFTPReader(ReaderInterface):
    """Reads CSV files from an SFTP server."""

    def __init__(self, job_context: JobContext):
        super().__init__(job_context)

    def read(self, source_def: SourceDef):
        """
        Download file from SFTP and create a temp view.

        Expected source_def fields:
        - source_api_url: SFTP host
        - source_api_resource_path: Remote file path
        - source_api_secrets_arn: Secrets Manager ARN with SFTP credentials
        """
        import paramiko
        import boto3
        import json
        import tempfile

        # Get SFTP credentials from Secrets Manager
        sm = boto3.client("secretsmanager", region_name=self.job_context.aws_region)
        secret = json.loads(
            sm.get_secret_value(SecretId=source_def.source_api_secrets_arn)["SecretString"]
        )

        # Connect to SFTP and download file
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            source_def.source_api_url,
            username=secret["username"],
            password=secret["password"],
        )
        sftp = ssh.open_sftp()

        # Download to temp file
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            sftp.get(source_def.source_api_resource_path, tmp.name)
            local_path = tmp.name

        sftp.close()
        ssh.close()

        # Read into Spark DataFrame
        df = self.spark.read.option("header", "true").csv(local_path)

        # Create temp view
        temp_view_name = self._create_temp_view(df, source_def)
        return False, temp_view_name


# Register the custom reader
ReaderRegistry.register("SFTP", SFTPReader)
```

## Job Configuration

```json
{
  "config_key": "SFTP_IMPORT_JOB",
  "source_list": [
    {
      "source_key": "sftp_orders",
      "source_type": "SFTP",
      "source_api_url": "sftp.partner.com",
      "source_api_resource_path": "/exports/daily_orders.csv",
      "source_api_secrets_arn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:sftp-creds",
      "create_temp_view": true
    }
  ],
  "target_list": [
    {
      "target_type": "S3",
      "source_config_key": "sftp_orders",
      "database_name": "raw",
      "table_name": "partner_orders",
      "target_s3_path": "s3://<YOUR-BUCKET-NAME>/raw/partner_orders/",
      "target_format": "parquet",
      "write_mode": "append",
      "partition_by": ["order_date"]
    }
  ]
}
```

## Integration

To use your custom reader in a Glue job, import it before the job runs:

```python
# In your custom Glue job script or an __init__.py loaded by the wheel
import my_sftp_reader  # This triggers ReaderRegistry.register()
```

Or add the registration in the `generic_glue_job.py` entry point before
the `SourceReader` is instantiated.
