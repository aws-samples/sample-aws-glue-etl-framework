"""
API Reader - Reads data from generic REST APIs.

Provides a base implementation for reading from HTTP/REST APIs
with support for authentication via Secrets Manager, pagination,
and response parsing.
"""

import json
import traceback
from typing import Any, Dict, List, Optional, Tuple

import boto3
import requests

from etl_framework.glue.generic.models.job_config_models import JobContext, SourceDef
from etl_framework.glue.generic.readers.reader_interface import ReaderInterface
from etl_framework.glue.generic.utils.sql_validation import (
    SQLValidationError,
    validate_source_filter,
)


class APIReader(ReaderInterface):
    """
    Reads data from REST APIs.

    Supports:
    - GET/POST methods
    - Authentication via AWS Secrets Manager
    - Configurable pagination (offset, cursor, link-based)
    - JSON response parsing
    - Conversion to Spark DataFrame
    """

    def __init__(self, job_context: JobContext):
        super().__init__(job_context)

    def read(self, source_def: SourceDef) -> Tuple[bool, Optional[str]]:
        """
        Read data from a REST API and create a temporary view.

        Args:
            source_def: Source definition with API URL, method, headers,
                       and optional pagination configuration

        Returns:
            Tuple of (has_error, temp_view_name)
        """
        try:
            # Validate required fields
            api_url = source_def.source_api_url
            if not api_url:
                self.logger.error("APIReader: source_api_url is required")
                return True, None

            method = (source_def.source_api_method or "GET").upper()
            resource_path = source_def.source_api_resource_path or ""
            full_url = f"{api_url.rstrip('/')}/{resource_path.lstrip('/')}" if resource_path else api_url

            self.logger.info(f"APIReader: {method} {full_url}")

            # Get authentication credentials if specified
            headers = dict(source_def.source_api_headers or {})
            if source_def.source_api_secrets_arn:
                auth_headers = self._get_auth_headers(source_def.source_api_secrets_arn)
                headers.update(auth_headers)

            # Fetch data with pagination support
            all_records = self._fetch_with_pagination(
                url=full_url,
                method=method,
                headers=headers,
                params=source_def.source_api_params,
                pagination_config=source_def.source_api_pagination,
            )

            if not all_records:
                self.logger.warning("APIReader: No records returned from API")
                # Return None temp view (no data available)
                return False, None

            self.logger.info(f"APIReader: Fetched {len(all_records)} records")

            # Convert to Spark DataFrame
            df = self.spark.createDataFrame(all_records)

            # Apply column selection if specified
            if source_def.source_select_columns:
                df = self._validate_and_select_columns(
                    df, source_def.source_select_columns
                )

            # Apply filter if specified
            if source_def.source_filter:
                validate_source_filter(source_def.source_filter, "source_filter")
                df = df.filter(source_def.source_filter)

            # Create temp view
            temp_view_name = self._create_temp_view(df, source_def)
            return False, temp_view_name

        except SQLValidationError as e:
            self.logger.error(f"APIReader: Validation failed: {str(e)}")
            return True, None
        except Exception as e:
            self.logger.error(f"APIReader: Failed to read: {str(e)}")
            self.logger.error(traceback.format_exc())
            return True, None

    def _get_auth_headers(self, secrets_arn: str) -> Dict[str, str]:
        """
        Retrieve authentication headers from Secrets Manager.

        Expected secret format:
        {
            "api_key": "...",
            "header_name": "Authorization",
            "header_prefix": "Bearer"
        }
        OR
        {
            "username": "...",
            "password": "..."
        }

        Returns:
            Dictionary of HTTP headers for authentication
        """
        try:
            client = boto3.client(
                "secretsmanager", region_name=self.job_context.aws_region
            )
            response = client.get_secret_value(SecretId=secrets_arn)
            secret = json.loads(response["SecretString"])

            # API key based auth
            if "api_key" in secret:
                header_name = secret.get("header_name", "Authorization")
                prefix = secret.get("header_prefix", "Bearer")
                return {header_name: f"{prefix} {secret['api_key']}"}

            # Basic auth
            if "username" in secret and "password" in secret:
                import base64
                credentials = base64.b64encode(
                    f"{secret['username']}:{secret['password']}".encode()
                ).decode()
                return {"Authorization": f"Basic {credentials}"}

            # Return raw headers if secret contains header key-value pairs
            return {k: v for k, v in secret.items() if isinstance(v, str)}

        except Exception as e:
            self.logger.error(f"APIReader: Failed to get auth: {str(e)}")
            return {}

    def _fetch_with_pagination(
        self,
        url: str,
        method: str,
        headers: Dict[str, str],
        params: Optional[Dict[str, str]] = None,
        pagination_config: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch data from API with optional pagination.

        Pagination config:
        {
            "type": "offset" | "cursor" | "none",
            "page_size": 100,
            "data_path": "results",          # JSON path to data array
            "next_cursor_path": "next_cursor", # For cursor-based
            "max_pages": 100                   # Safety limit
        }
        """
        all_records: List[Dict[str, Any]] = []
        params = dict(params or {})
        pagination = pagination_config or {"type": "none"}
        pagination_type = pagination.get("type", "none")
        page_size = pagination.get("page_size", 100)
        data_path = pagination.get("data_path", None)
        max_pages = pagination.get("max_pages", 100)

        page = 0
        while page < max_pages:
            self.logger.info(f"APIReader: Fetching page {page + 1}")

            try:
                if method == "GET":
                    response = requests.get(url, headers=headers, params=params, timeout=120)
                else:
                    response = requests.post(url, headers=headers, json=params, timeout=120)
            except requests.exceptions.Timeout:
                self.logger.error(
                    f"APIReader: Request timed out on page {page + 1}"
                )
                raise RuntimeError(
                    f"API request timed out after 120 seconds on page {page + 1}"
                )
            except requests.exceptions.ConnectionError:
                self.logger.error(
                    f"APIReader: Connection failed on page {page + 1}"
                )
                raise RuntimeError(
                    f"API connection failed on page {page + 1}. "
                    "Verify the URL and network connectivity."
                )
            except requests.exceptions.RequestException as e:
                # Log a sanitized message without exposing headers/credentials
                self.logger.error(
                    f"APIReader: Request failed on page {page + 1}: {type(e).__name__}"
                )
                raise RuntimeError(
                    f"API request failed on page {page + 1}: {type(e).__name__}"
                ) from None

            # Handle HTTP error responses without exposing credentials in traces
            if not response.ok:
                self.logger.error(
                    f"APIReader: HTTP {response.status_code} on page {page + 1}"
                )
                raise RuntimeError(
                    f"API returned HTTP {response.status_code} on page {page + 1}. "
                    f"Reason: {response.reason}"
                )

            try:
                response_data = response.json()
            except ValueError:
                self.logger.error(
                    f"APIReader: Invalid JSON response on page {page + 1}"
                )
                raise RuntimeError(
                    f"API returned non-JSON response on page {page + 1}"
                )

            # Extract records from response
            if data_path:
                records = response_data
                for key in data_path.split("."):
                    records = records.get(key, []) if isinstance(records, dict) else records
            else:
                records = response_data if isinstance(response_data, list) else [response_data]

            if not records:
                break

            all_records.extend(records)
            page += 1

            # Handle pagination
            if pagination_type == "none":
                break
            elif pagination_type == "offset":
                if len(records) < page_size:
                    break
                params["offset"] = str(int(params.get("offset", "0")) + page_size)
                params["limit"] = str(page_size)
            elif pagination_type == "cursor":
                next_cursor_path = pagination.get("next_cursor_path", "next_cursor")
                next_cursor = response_data
                for key in next_cursor_path.split("."):
                    next_cursor = next_cursor.get(key) if isinstance(next_cursor, dict) else None
                if not next_cursor:
                    break
                params["cursor"] = next_cursor
            else:
                break

        return all_records
