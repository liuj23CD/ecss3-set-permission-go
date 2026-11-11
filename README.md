# ECS S3 Permission Manager (Go Version)

A Go implementation for setting permissions on Dell ObjectScale S3 bucket and objects.

This tool grants read-only permissions (READ and READ_ACP) to a target user for:
- The S3 bucket itself
- All objects within the bucket

## Features

- Grant bucket-level read permissions
- Grant object-level read permissions for all objects
- Parallel processing with configurable concurrency
- Checkpoint support for resume on failure
- Progress tracking and summary

## Environment Variables

```bash
export ECS_S3_ACCESS_KEY="your_access_key"
export ECS_S3_SECRET_KEY="your_secret_key"
export ECS_S3_ENDPOINT="http://your-objectscale-endpoint:port"
export ECS_S3_BUCKET="your_bucket_name"
export ECS_S3_ACCESS_TARGET_USER="target_user_id"
```

## Installation

```bash
go build -o ecssetacl-go
```

## Usage

```bash
# Set environment variables
export ECS_S3_ACCESS_KEY="object_admin1"
export ECS_S3_SECRET_KEY="your_secret_key"
export ECS_S3_ENDPOINT="http://192.168.31.200:9020"
export ECS_S3_BUCKET="test1"
export ECS_S3_ACCESS_TARGET_USER="object_user1"

# Run the program
./ecssetacl-go
```

## License

MIT
