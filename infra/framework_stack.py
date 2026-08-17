"""
ETL Framework CDK Stack.

Provisions all AWS resources required for the ETL Framework:
- KMS encryption key
- S3 buckets (Glue scripts, ingestion, logs)
- DynamoDB tables (job configs, watermarks)
- IAM roles (Glue execution, Lambda)
- Glue jobs (from job config definitions)
- Lambda custom resource (loads JSON configs into DynamoDB)
- Glue connections (Redshift JDBC, network)
- Optional CloudWatch alarms
"""

import json
import glob
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Optional

import aws_cdk as cdk
from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_dynamodb as dynamodb,
    aws_glue as glue,
    aws_iam as iam,
    aws_kms as kms,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_s3_deployment as s3_deployment,
    custom_resources as cr,
)
from constructs import Construct


@dataclass
class JobDefinition:
    """Represents a Glue job to create."""
    name: str
    config_file_name: str
    worker_type: str = "G.1X"
    number_of_workers: int = 2
    description: str = ""
    schedule: Optional[str] = None
    timeout: int = 90
    max_retries: int = 0


class FrameworkStack(Stack):
    """
    Main ETL Framework infrastructure stack.

    Creates all AWS resources needed for running config-driven Glue ETL jobs.
    """

    def __init__(
        self, scope: Construct, construct_id: str, config: dict, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.config = config
        self.prefix = config["prefix"]
        self.env_name = config["env"]
        self.domain = config.get("domain", "default")
        self.region = config["region"]
        self.account_id = config["account_id"]

        # Create resources
        self.encryption_key = self._create_kms_key()
        self.glue_bucket = self._create_glue_bucket()
        self.ingestion_bucket = self._create_ingestion_bucket()
        self.log_bucket = self._create_log_bucket()
        self.config_table = self._create_config_table()
        self.watermark_table = self._create_watermark_table()
        self.glue_role = self._create_glue_role()

        # Create Glue connection if configured
        if config.get("create_connection", False):
            self._create_glue_connection()

        # Deploy ETL wheel and scripts to S3
        self._deploy_assets()

        # Create Glue jobs from config definitions
        self._create_glue_jobs()

        # Load job configs into DynamoDB via custom resource
        self._create_config_loader()

    def _create_kms_key(self) -> kms.Key:
        """Create KMS key for encrypting all resources."""
        key = kms.Key(
            self,
            "EncryptionKey",
            alias=f"alias/{self.prefix}-{self.env_name}-encryption",
            description=f"ETL Framework encryption key ({self.env_name})",
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # Grant key usage to relevant services
        key.grant_encrypt_decrypt(
            iam.ServicePrincipal("glue.amazonaws.com")
        )
        key.grant_encrypt_decrypt(
            iam.ServicePrincipal("lambda.amazonaws.com")
        )

        return key

    def _create_glue_bucket(self) -> s3.Bucket:
        """Create S3 bucket for Glue job scripts and artifacts."""
        bucket = s3.Bucket(
            self,
            "GlueBucket",
            bucket_name=f"{self.prefix}-{self.env_name}-{self.account_id}-{self.region}-glue",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.encryption_key,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
        )
        return bucket

    def _create_ingestion_bucket(self) -> s3.Bucket:
        """Create S3 bucket for data ingestion."""
        bucket = s3.Bucket(
            self,
            "IngestionBucket",
            bucket_name=f"{self.prefix}-{self.env_name}-{self.account_id}-{self.region}-ingestion",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.encryption_key,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
        )
        return bucket

    def _create_log_bucket(self) -> s3.Bucket:
        """Create S3 bucket for Glue job logs."""
        bucket = s3.Bucket(
            self,
            "LogBucket",
            bucket_name=f"{self.prefix}-{self.env_name}-{self.account_id}-{self.region}-glue-logs",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.encryption_key,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    expiration=Duration.days(90),
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=Duration.days(30),
                        )
                    ],
                )
            ],
            removal_policy=RemovalPolicy.DESTROY,
        )
        return bucket

    def _create_config_table(self) -> dynamodb.Table:
        """Create DynamoDB table for job configurations."""
        table = dynamodb.Table(
            self,
            "ConfigTable",
            table_name=f"{self.prefix}-{self.env_name}-etl-configs",
            partition_key=dynamodb.Attribute(
                name="config_key", type=dynamodb.AttributeType.STRING
            ),
            encryption=dynamodb.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=self.encryption_key,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery=True,
            deletion_protection=self.config.get("dynamodb", {}).get(
                "deletion_protection", self.env_name == "prd"
            ),
            removal_policy=RemovalPolicy.RETAIN,
        )
        return table

    def _create_watermark_table(self) -> dynamodb.Table:
        """Create DynamoDB table for watermark tracking."""
        table = dynamodb.Table(
            self,
            "WatermarkTable",
            table_name=f"{self.prefix}-{self.env_name}-etl-watermark",
            partition_key=dynamodb.Attribute(
                name="watermark_key", type=dynamodb.AttributeType.STRING
            ),
            encryption=dynamodb.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=self.encryption_key,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery=True,
            deletion_protection=self.config.get("dynamodb", {}).get(
                "deletion_protection", self.env_name == "prd"
            ),
            removal_policy=RemovalPolicy.RETAIN,
        )
        return table

    def _create_glue_role(self) -> iam.Role:
        """Create IAM role for Glue job execution."""
        role = iam.Role(
            self,
            "GlueRole",
            role_name=f"{self.prefix}-{self.env_name}-glue-{self.region}",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
            ],
        )

        # Grant access to S3 buckets
        self.glue_bucket.grant_read_write(role)
        self.ingestion_bucket.grant_read_write(role)
        self.log_bucket.grant_write(role)

        # Grant access to DynamoDB tables
        self.config_table.grant_read_data(role)
        self.watermark_table.grant_read_write_data(role)

        # Grant KMS usage
        self.encryption_key.grant_encrypt_decrypt(role)

        # Grant CloudWatch metrics
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
                conditions={
                    "StringEquals": {
                        "cloudwatch:namespace": "ETLFramework/Glue"
                    }
                },
            )
        )

        # Grant Secrets Manager access
        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "secretsmanager:GetSecretValue",
                    "secretsmanager:DescribeSecret",
                ],
                resources=[
                    f"arn:aws:secretsmanager:{self.region}:{self.account_id}:secret:etl-framework/*"
                ],
            )
        )

        # Grant access to external S3 buckets if configured
        external_buckets = self.config.get("external_s3_buckets", [])
        for bucket_name in external_buckets:
            role.add_to_policy(
                iam.PolicyStatement(
                    actions=["s3:GetObject", "s3:ListBucket", "s3:PutObject"],
                    resources=[
                        f"arn:aws:s3:::{bucket_name}",
                        f"arn:aws:s3:::{bucket_name}/*",
                    ],
                )
            )

        # Grant cross-account role assumption if configured
        cross_account_roles = self.config.get("cross_account_role_arns", [])
        if cross_account_roles:
            role.add_to_policy(
                iam.PolicyStatement(
                    actions=["sts:AssumeRole"],
                    resources=cross_account_roles,
                )
            )

        return role

    def _create_glue_connection(self) -> None:
        """Create Glue JDBC or network connection if configured."""
        connection_config = self.config.get("glue_connection", {})
        connection_type = connection_config.get("type", "JDBC")

        if connection_type == "JDBC":
            # Use Secrets Manager ARN for credential retrieval instead of
            # hardcoded username/password to avoid exposing credentials in
            # CloudFormation templates (CWE-798)
            secrets_arn = connection_config.get("secrets_arn", "")
            if not secrets_arn:
                raise ValueError(
                    "glue_connection.secrets_arn is required for JDBC connections. "
                    "Store JDBC credentials (username, password, jdbc_url) in "
                    "AWS Secrets Manager and provide the secret ARN."
                )

            # Resolve the secret to get JDBC credentials at deploy time
            secret = cdk.SecretValue.secrets_manager(secrets_arn)

            glue.CfnConnection(
                self,
                "GlueConnection",
                catalog_id=self.account_id,
                connection_input=glue.CfnConnection.ConnectionInputProperty(
                    name=f"{self.prefix}-{self.env_name}-connection",
                    connection_type="JDBC",
                    physical_connection_requirements=glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                        subnet_id=connection_config.get("subnet_id", ""),
                        security_group_id_list=connection_config.get(
                            "security_group_ids", []
                        ),
                        availability_zone=connection_config.get("availability_zone", ""),
                    ),
                    connection_properties={
                        "JDBC_CONNECTION_URL": connection_config.get("jdbc_url", ""),
                        "SECRET_ID": secrets_arn,
                    },
                ),
            )
        elif connection_type == "NETWORK":
            glue.CfnConnection(
                self,
                "GlueNetworkConnection",
                catalog_id=self.account_id,
                connection_input=glue.CfnConnection.ConnectionInputProperty(
                    name=f"{self.prefix}-{self.env_name}-network-connection",
                    connection_type="NETWORK",
                    physical_connection_requirements=glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                        subnet_id=connection_config.get("subnet_id", ""),
                        security_group_id_list=connection_config.get(
                            "security_group_ids", []
                        ),
                        availability_zone=connection_config.get("availability_zone", ""),
                    ),
                ),
            )

    def _deploy_assets(self) -> None:
        """Deploy ETL wheel and Glue scripts to S3."""
        # Deploy wheel
        wheel_path = Path(__file__).parent / "assets" / "wheels"
        if wheel_path.exists() and list(wheel_path.glob("*.whl")):
            s3_deployment.BucketDeployment(
                self,
                "WheelDeployment",
                sources=[s3_deployment.Source.asset(str(wheel_path))],
                destination_bucket=self.glue_bucket,
                destination_key_prefix="wheels/",
            )

        # Deploy Glue scripts
        scripts_path = Path(__file__).parent / "src" / "glue" / "scripts"
        if scripts_path.exists():
            s3_deployment.BucketDeployment(
                self,
                "ScriptsDeployment",
                sources=[s3_deployment.Source.asset(str(scripts_path))],
                destination_bucket=self.glue_bucket,
                destination_key_prefix="scripts/",
            )

    def _create_glue_jobs(self) -> None:
        """Create Glue jobs from job configuration definitions."""
        # Load job definitions from config.py in the config directory
        job_configs_path = (
            Path(__file__).parent
            / "config"
            / self.domain
            / self.env_name
            / "job_configs"
            / self.region
        )

        if not job_configs_path.exists():
            return

        # Try to load config.py for job definitions
        config_py = job_configs_path / "config.py"
        if not config_py.exists():
            return

        # Import job definitions dynamically
        import importlib.util
        spec = importlib.util.spec_from_file_location("job_config", config_py)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        jobs = getattr(module, "JOBS", {})
        glue_config = self.config.get("glue", {})
        glue_version = glue_config.get("glue_version", "4.0")
        max_retries = glue_config.get("max_retries", 0)
        timeout = glue_config.get("execution_timeout", 90)

        wheel_filename = self.config.get("etl_wheel_filename")

        for job_key, job_def in jobs.items():
            job_name = f"{self.prefix}-{self.env_name}-{job_def.name}"

            default_arguments = {
                "--job_name": job_def.name,
                "--environment": self.env_name,
                "--glue_s3_bucket": self.glue_bucket.bucket_name,
                "--ingestion_s3_bucket": self.ingestion_bucket.bucket_name,
                "--config_key": job_key,
                "--aws_region": self.region,
                "--aws_accountid": self.account_id,
                "--chunk_size": str(getattr(job_def, "chunk_size", 5000)),
                "--etl_configs_table_name": self.config_table.table_name,
                "--etl_watermark_table_name": self.watermark_table.table_name,
                "--additional-python-modules": "requests",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
            }

            # Add wheel as extra Python files if available
            if wheel_filename:
                default_arguments["--extra-py-files"] = (
                    f"s3://{self.glue_bucket.bucket_name}/wheels/{wheel_filename}"
                )

            glue.CfnJob(
                self,
                f"GlueJob-{job_def.name}",
                name=job_name,
                role=self.glue_role.role_arn,
                command=glue.CfnJob.JobCommandProperty(
                    name="glueetl",
                    python_version="3",
                    script_location=f"s3://{self.glue_bucket.bucket_name}/scripts/generic_glue_job.py",
                ),
                default_arguments=default_arguments,
                glue_version=glue_version,
                max_retries=max_retries,
                timeout=getattr(job_def, "timeout", timeout),
                number_of_workers=job_def.number_of_workers,
                worker_type=job_def.worker_type,
                description=job_def.description or f"ETL job: {job_def.name}",
            )

            # Create schedule trigger if configured
            schedule = getattr(job_def, "schedule", None)
            skip_schedule = getattr(job_def, "skip_schedule", False)
            if schedule and not skip_schedule:
                glue.CfnTrigger(
                    self,
                    f"Trigger-{job_def.name}",
                    name=f"{job_name}-trigger",
                    type="SCHEDULED",
                    schedule=schedule,
                    actions=[
                        glue.CfnTrigger.ActionProperty(job_name=job_name)
                    ],
                    start_on_creation=True,
                )

    def _create_config_loader(self) -> None:
        """
        Create a Lambda-backed custom resource that loads JSON config files
        from the config directory into the DynamoDB configs table.
        """
        job_configs_path = (
            Path(__file__).parent
            / "config"
            / self.domain
            / self.env_name
            / "job_configs"
            / self.region
        )

        if not job_configs_path.exists():
            return

        # Find all JSON config files
        json_files = list(job_configs_path.glob("*.json"))
        if not json_files:
            return

        # Create Lambda function for loading configs
        loader_role = iam.Role(
            self,
            "ConfigLoaderRole",
            role_name=f"{self.prefix}-{self.env_name}-config-loader-{self.region}",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
            ],
        )
        self.config_table.grant_write_data(loader_role)

        loader_function = _lambda.Function(
            self,
            "ConfigLoader",
            function_name=f"{self.prefix}-{self.env_name}-config-loader",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="index.handler",
            role=loader_role,
            timeout=Duration.minutes(5),
            code=_lambda.Code.from_inline(self._get_config_loader_code()),
            environment={
                "TABLE_NAME": self.config_table.table_name,
            },
        )

        # Load all JSON configs
        configs_to_load = {}
        for json_file in json_files:
            with open(json_file) as f:
                config_data = json.load(f)
                config_key = config_data.get("config_key", json_file.stem.upper())
                configs_to_load[config_key] = config_data

        # Create custom resource to load configs
        provider = cr.Provider(
            self, "ConfigLoaderProvider", on_event_handler=loader_function
        )

        cdk.CustomResource(
            self,
            "ConfigLoaderResource",
            service_token=provider.service_token,
            properties={
                "configs": json.dumps(configs_to_load),
                "table_name": self.config_table.table_name,
                # Force update on content change
                "hash": str(hash(json.dumps(configs_to_load, sort_keys=True))),
            },
        )

    def _get_config_loader_code(self) -> str:
        """Return inline Lambda code for loading configs into DynamoDB."""
        return '''
import json
import os
import boto3
from decimal import Decimal

dynamodb = boto3.resource("dynamodb")

def convert_floats(obj):
    """Convert float values to Decimal for DynamoDB."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats(i) for i in obj]
    return obj

def handler(event, context):
    table_name = os.environ["TABLE_NAME"]
    table = dynamodb.Table(table_name)

    request_type = event.get("RequestType", "Create")

    if request_type in ("Create", "Update"):
        configs = json.loads(event["ResourceProperties"]["configs"])
        for config_key, config_data in configs.items():
            config_data["config_key"] = config_key
            item = convert_floats(config_data)
            table.put_item(Item=item)
            print(f"Loaded config: {config_key}")

    return {
        "PhysicalResourceId": f"config-loader-{table_name}",
        "Data": {"Status": "SUCCESS"},
    }
'''
