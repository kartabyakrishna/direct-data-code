# Project Setup & Deployment Guide

This repository uses **GitHub Actions** for CI/CD and **AWS CloudFormation** for infrastructure.
Configuration is managed dynamically via **AWS Secrets Manager**, which is populated during deployment using GitHub Repository Secrets.

## 1. Prerequisites
*   AWS Account with permissions to deploy CloudFormation, Glue, S3, Redshift, and IAM roles.
*   GitHub Repository with "Actions" enabled.

## 2. GitHub Configuration
You need to configure **Secrets** in your GitHub Repository to enable automated deployment.
Go to: `Settings` -> `Secrets and variables` -> `Actions` -> `New repository secret`.

### Required Secrets (All Environments)

| Secret Name | Description | Example Value |
| :--- | :--- | :--- |
| `AWS_ACCESS_KEY_ID` | AWS Access Key for deployment user | `AKIA...` |
| `AWS_SECRET_ACCESS_KEY` | AWS Secret Key for deployment user | `wJalr...` |
| `AWS_REGION` | Target AWS Region | `us-east-1` |
| `VEEVA_LANDING_BUCKET` | S3 Bucket name for scripts & artifacts | `my-veeva-landing-bucket-dev` |
| `VPC_ID` | VPC ID where Redshift resides | `vpc-01234567` |
| `SUBNET_ID` | Private Subnet ID for Glue Job | `subnet-01234567` |
| `REDSHIFT_SG_ID` | Security Group ID of the Redshift Cluster | `sg-01234567` |

### Application Configuration Secrets

| Secret Name | Description | Example Value |
| :--- | :--- | :--- |
| `DIRECT_DATA_START` | Default Start Time (ISO 8601) | `2000-01-01T00:00Z` |
| `DIRECT_DATA_STOP` | Default Stop Time (ISO 8601) | `2025-04-09T00:00Z` |
| `DIRECT_DATA_ROLE_ARN` | IAM Role for S3 access (Veeva) | `arn:aws:iam::123:role/DirectDataRole` |
| `REDSHIFT_HOST` | Redshift Endpoint URL | `redshift-cluster-1...amazonaws.com` |
| `REDSHIFT_USER` | Redshift Database User | `admin` |
| `REDSHIFT_PASSWORD` | Redshift Database Password | `SuperSecret123!` |
| `REDSHIFT_DB` | Redshift Database Name | `dev` |
| `REDSHIFT_S3_IAM_ROLE` | IAM Role for Redshift COPY command | `arn:aws:iam::123:role/RedshiftS3Read` |
| `VAULT_USERNAME` | Veeva Vault Username | `integration.user@example.com` |
| `VAULT_PASSWORD` | Veeva Vault Password | `VaultPassword123` |
| `VAULT_DNS` | Veeva Vault DNS | `myvault.veevavault.com` |
| `VAULT_CLIENT_ID` | Generic Client ID | `My-Vault-Client` |

## 3. Deployment workflow
The pipeline supports multi-environment deployment based on Git branches.

| Branch | Environment | CloudFormation Stack | AWS Secret Name |
| :--- | :--- | :--- | :--- |
| `develop` | **DEV** | `VeevaRedshiftPipeline-dev` | `VeevaRedshiftConfig-dev` |
| `main` | **PROD** | `VeevaRedshiftPipeline-prod` | `VeevaRedshiftConfig-prod` |

### How to Deploy
1.  **Dev**: Push code to `develop`.
    *   Runs Tests + Linting.
    *   Deploys Stack to Dev.
    *   Updates Dev Secrets.
2.  **Prod**: Open a Pull Request from `develop` to `main`.
    *   Merging the PR triggers deployment to Prod.

## 4. Local Development
To run tests locally:
```bash
pip install -r requirements.txt
python tests/test_pipeline_mock.py
```
*Note: Local `cfn-lint` execution may require specific network access. CI pipeline handles this automatically.*
