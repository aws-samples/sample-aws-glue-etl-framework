#!/usr/bin/env python3
"""
CDK Application Entry Point.

Deploys the ETL Framework infrastructure based on YAML configuration
for the specified environment, domain, and region.
"""

import glob
from pathlib import Path

import aws_cdk as cdk
import yaml

from framework_stack import FrameworkStack


def load_config(env: str, domain: str, region: str) -> dict:
    """Load configuration for the specified environment, domain, and region."""
    config_path = Path(__file__).parent / "config" / domain / env / f"{region}.yaml"

    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration not found: {config_path}\n"
            f"Create a config file at: config/{domain}/{env}/{region}.yaml"
        )

    with open(config_path) as f:
        return yaml.safe_load(f)


def main():
    """Main entry point for CDK deployment."""
    app = cdk.App()

    # Get context parameters
    env = app.node.try_get_context("env")
    domain = app.node.try_get_context("domain")
    account = app.node.try_get_context("account")
    region = app.node.try_get_context("region")

    if not all([env, domain, account, region]):
        raise ValueError(
            "Missing required context parameters. Usage:\n"
            "cdk deploy --context env=dev --context domain=myproject "
            "--context account=123456789012 --context region=us-west-2"
        )

    # Load configuration
    config = load_config(env, domain, region)
    config["domain"] = domain

    # Get wheel filename from assets directory
    wheel_files = glob.glob("assets/wheels/*.whl")
    config["etl_wheel_filename"] = Path(wheel_files[0]).name if wheel_files else None

    cdk_env = cdk.Environment(account=account, region=region)

    # Apply tags from configuration (fully user-configurable)
    default_tags = {
        "application": f"etl-framework-{domain}",
        "environment": env,
        "managed-by": "cdk",
    }
    # Merge with user-provided tags (user tags take precedence)
    all_tags = {**default_tags, **config.get("tags", {})}
    for key, value in all_tags.items():
        cdk.Tags.of(app).add(key, str(value))

    # Deploy Framework Stack
    if config.get("deploy_generic_etl", True):
        FrameworkStack(
            app,
            f"{config['prefix']}-{env}-etl-framework",
            config,
            env=cdk_env,
        )

    app.synth()


if __name__ == "__main__":
    main()
