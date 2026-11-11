# ECS S3 Permission Manager (Go Version)

A Go implementation for setting permissions on Dell ObjectScale S3 bucket and objects.

## Usage

```bash
# Set environment variables
export ECS_S3_ACCESS_KEY="object_admin1"
export ECS_S3_SECRET_KEY="your_secret_key"
export ECS_S3_ENDPOINT="http://192.168.31.200:9020"
export ECS_S3_BUCKET="test1"
export ECS_S3_ACCESS_TARGET_USER="object_user1"

# Build and run
./ecssetacl-go
```

## Features

1. Grant bucket-level READ and READ_ACP permissions to target user
2. Grant object-level READ and READ_ACP permissions to all objects
3. Parallel processing (10 concurrent workers)
4. Checkpoint support for resume on failure
5. Progress tracking every 100,000 objects
6. Detailed execution logs and summary

## License

MIT
