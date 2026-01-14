terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

# ECR Repository
resource "aws_ecr_repository" "veeva_connector" {
  name = "veeva-redshift-connector"
}

# IAM Roles
resource "aws_iam_role" "batch_execution_role" {
  name = "veeva_batch_execution_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "batch_execution_role_policy" {
  role       = aws_iam_role.batch_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "batch_job_role" {
  name = "veeva_batch_job_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
    }]
  })
}

# Batch Compute Environment (Fargate)
resource "aws_batch_compute_environment" "veeva_batch_environment" {
  compute_environment_name = "veeva-batch-environment"
  type                     = "MANAGED"
  state                    = "ENABLED"
  
  compute_resources {
    type      = "FARGATE"
    max_vcpus = 16
    
    subnets = ["subnet-xxxxxxxx", "subnet-yyyyyyyy"] # REPLACE with your subnets
    security_group_ids = ["sg-xxxxxxxx"] # REPLACE with your security group
  }

  service_role = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/service-role/AWSBatchServiceRole"
}

# Batch Job Queue
resource "aws_batch_job_queue" "veeva_job_queue" {
  name                 = "veeva-job-queue"
  state                = "ENABLED"
  priority             = 1
  compute_environments = [aws_batch_compute_environment.veeva_batch_environment.arn]
}

# Batch Job Definition
resource "aws_batch_job_definition" "veeva_job_definition" {
  name                 = "veeva-connector-job"
  type                 = "container"
  platform_capabilities = ["FARGATE"]
  
  container_properties = jsonencode({
    image      = "${aws_ecr_repository.veeva_connector.repository_url}:latest"
    fargatePlatformConfiguration = {
      platformVersion = "LATEST"
    }
    resourceRequirements = [
      {
        type  = "VCPU"
        value = "1.0"
      },
      {
        type  = "MEMORY"
        value = "2048"
      }
    ]
    executionRoleArn = aws_iam_role.batch_execution_role.arn
    jobRoleArn       = aws_iam_role.batch_job_role.arn
    logConfiguration = {
      logDriver = "awslogs"
    }
    environment = [
      { name = "CONNECTOR_CONFIG_PATH", value = "/app/config/connector_config.json" },
      { name = "VAPIL_SETTINGS_PATH", value = "/app/config/vapil_settings.json" }
    ]
  })
}

# EventBridge Rule (Schedule)
resource "aws_cloudwatch_event_rule" "veeva_daily_schedule" {
  name                = "veeva-connector-daily"
  description         = "Trigger Veeva Connector Daily"
  schedule_expression = "cron(0 1 * * ? *)" # Run at 1 AM UTC
}

resource "aws_cloudwatch_event_target" "batch_target" {
  rule      = aws_cloudwatch_event_rule.veeva_daily_schedule.name
  target_id = "TriggerBatchJob"
  arn       = aws_batch_job_queue.veeva_job_queue.arn
  role_arn  = aws_iam_role.event_bridge_role.arn # Need to define this role

  batch_target {
    job_definition_arn = aws_batch_job_definition.veeva_job_definition.arn
    job_name           = "veeva-daily-sync"
  }
}

data "aws_caller_identity" "current" {}

resource "aws_iam_role" "event_bridge_role" {
  name = "veeva_event_bridge_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "events.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "event_bridge_policy" {
  name = "veeva_event_bridge_policy"
  role = aws_iam_role.event_bridge_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "batch:SubmitJob"
      Resource = "*"
    }]
  })
}
