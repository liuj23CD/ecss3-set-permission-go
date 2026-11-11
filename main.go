package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3PermissionManager manages S3 bucket and object permissions
type S3PermissionManager struct {
	client       *s3.Client
	bucketName   string
	targetUser   string
	endpoint     string
	region       string
}

// NewS3PermissionManager creates a new S3 permission manager
func NewS3PermissionManager(accessKey, secretKey, endpoint, bucketName, targetUser string) (*S3PermissionManager, error) {
	// Load AWS config with custom endpoint
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(aws.NewCredentialsCache(
			aws.NewCredentialsCache(
				aws.NewStaticCredentialsProvider(accessKey, secretKey),
			),
		)),
		config.WithRegion("us-east-1"), // ObjectScale不依赖具体区域
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client with custom endpoint
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	return &S3PermissionManager{
		client:     client,
		bucketName: bucketName,
		targetUser: targetUser,
		endpoint:   endpoint,
		region:     "us-east-1",
	}, nil
}

// GrantBucketReadOnlyPermission grants read permission to the target user for the bucket
func (pm *S3PermissionManager) GrantBucketReadOnlyPermission() error {
	fmt.Printf("Granting bucket read permission to user: %s\n", pm.targetUser)

	// Get current bucket ACL
	getACLResult, err := pm.client.GetBucketAcl(context.TODO(), &s3.GetBucketAclInput{
		Bucket: aws.String(pm.bucketName),
	})
	if err != nil {
		return fmt.Errorf("failed to get bucket ACL: %w", err)
	}

	// Check if already granted
	for _, grant := range getACLResult.Grants {
		if grant.Grantee != nil && grant.Grantee.ID != nil && *grant.Grantee.ID == pm.targetUser {
			fmt.Printf("  - Permission already granted to %s\n", pm.targetUser)
			return nil
		}
	}

	// Build new ACL with grants
	var newGrants []types.Grant
	for _, g := range getACLResult.Grants {
		newGrants = append(newGrants, g)
	}

	// Add target user's permissions
	newGrants = append(newGrants, types.Grant{
		Grantee: &types.Grantee{
			ID:   aws.String(pm.targetUser),
			Type: types.TypeCanonicalUser,
		},
		Permission: types.PermissionRead,
	})
	newGrants = append(newGrants, types.Grant{
		Grantee: &types.Grantee{
			ID:   aws.String(pm.targetUser),
			Type: types.TypeCanonicalUser,
		},
		Permission: types.PermissionReadAcp,
	})

	// Put new ACL
	putInput := &s3.PutBucketAclInput{
		Bucket: aws.String(pm.bucketName),
		AccessControlPolicy: &types.AccessControlPolicy{
			Grants: newGrants,
			Owner:  getACLResult.Owner,
		},
	}

	_, err = pm.client.PutBucketAcl(context.TODO(), putInput)
	if err != nil {
		return fmt.Errorf("failed to set bucket ACL: %w", err)
	}

	fmt.Printf("  - Successfully granted bucket read permission to %s\n", pm.targetUser)
	return nil
}

// GrantObjectReadOnlyPermission grants read permission to all objects in the bucket
func (pm *S3PermissionManager) GrantObjectReadOnlyPermission() error {
	fmt.Printf("\nGranting object read permissions to user: %s\n", pm.targetUser)

	// Checkpoint file path
	checkpointFile := "/tmp/ecs_permission_checkpoint.txt"

	// Load checkpoint
	processedObjects := loadCheckpoint(checkpointFile)
	fmt.Printf("  - Loaded checkpoint: %d objects processed\n", len(processedObjects))

	// Counters
	var totalObjects, granted, skipped, failed int64

	// Create a wait group for concurrent processing
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 10) // 10 concurrent workers

	// List all objects
	err := pm.listObjectsAndProcess(&wg, semaphore, &processedObjects, &totalObjects, &granted, &skipped, &failed, checkpointFile)
	if err != nil {
		return err
	}

	// Wait for all workers to complete
	wg.Wait()

	// Save final checkpoint
	saveCheckpoint(checkpointFile, processedObjects)

	// Print summary
	fmt.Printf("\n=== Permission Grant Summary ===\n")
	fmt.Printf("Total objects processed: %d\n", totalObjects)
	fmt.Printf("Successfully granted: %d\n", granted)
	fmt.Printf("Skipped (already granted): %d\n", skipped)
	fmt.Printf("Failed: %d\n", failed)

	// Clear checkpoint
	clearCheckpoint(checkpointFile)

	return nil
}

// listObjectsAndProcess lists objects and processes them with concurrency
func (pm *S3PermissionManager) listObjectsAndProcess(wg *sync.WaitGroup, semaphore chan struct{}, processedObjects *map[string]bool, totalObjects, granted, skipped, failed *int64, checkpointFile string) error {
	continuationToken := ""

	for {
		// List objects
		input := &s3.ListObjectsV2Input{
			Bucket: aws.String(pm.bucketName),
		}
		if continuationToken != "" {
			input.ContinuationToken = aws.String(continuationToken)
		}

		result, err := pm.client.ListObjectsV2(context.TODO(), input)
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		// Process objects in batch
		for _, obj := range result.Contents {
			wg.Add(1)
			go func(key string) {
				defer wg.Done()
				semaphore <- struct{}{} // Acquire semaphore
				defer func() { <-semaphore }() // Release semaphore

				// Check checkpoint
				if (*processedObjects)[key] {
					atomic.AddInt64(skipped, 1)
					return
				}

				atomic.AddInt64(totalObjects, 1)

				// Grant permission
				err := pm.grantObjectReadPermission(key, processedObjects, granted, skipped, failed)
				if err != nil {
					log.Printf("  - [FAILED] %s - Error: %v", key, err)
				}

				// Save checkpoint every 100000 objects
				if *totalObjects%100000 == 0 {
					saveCheckpoint(checkpointFile, *processedObjects)
					fmt.Printf("  - Progress: processed %d objects, %d granted, %d skipped, %d failed\n",
						*totalObjects, *granted, *skipped, *failed)
				}
			}(aws.ToString(obj.Key))
		}

		// Check if more objects to list
		if result.IsTruncated {
			continuationToken = aws.ToString(result.NextContinuationToken)
		} else {
			break
		}
	}

	return nil
}

// grantObjectReadPermission grants read permission to a single object
func (pm *S3PermissionManager) grantObjectReadPermission(objectKey string, processedObjects *map[string]bool, granted, skipped, failed *int64) error {
	// Get current object ACL
	getACLResult, err := pm.client.GetObjectAcl(context.TODO(), &s3.GetObjectAclInput{
		Bucket: aws.String(pm.bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		atomic.AddInt64(failed, 1)
		return fmt.Errorf("failed to get object ACL: %w", err)
	}

	// Check if already granted
	alreadyGranted := true
	for _, grant := range getACLResult.Grants {
		if grant.Grantee != nil && grant.Grantee.ID != nil && *grant.Grantee.ID == pm.targetUser {
			if aws.ToString(grant.Permission) == string(types.PermissionRead) {
				// Has READ permission
			} else {
				alreadyGranted = false
			}
			if aws.ToString(grant.Permission) == string(types.PermissionReadAcp) {
				// Has READ_ACP permission
			} else {
				alreadyGranted = false
			}
		} else {
			alreadyGranted = false
		}
	}

	if alreadyGranted {
		fmt.Printf("  - [SKIPPED - Already granted] %s\n", objectKey)
		(*processedObjects)[objectKey] = true
		atomic.AddInt64(skipped, 1)
		return nil
	}

	// Build new ACL
	var newGrants []types.Grant
	for _, g := range getACLResult.Grants {
		newGrants = append(newGrants, g)
	}

	// Add target user's permissions
	newGrants = append(newGrants, types.Grant{
		Grantee: &types.Grantee{
			ID:   aws.String(pm.targetUser),
			Type: types.TypeCanonicalUser,
		},
		Permission: types.PermissionRead,
	})
	newGrants = append(newGrants, types.Grant{
		Grantee: &types.Grantee{
			ID:   aws.String(pm.targetUser),
			Type: types.TypeCanonicalUser,
		},
		Permission: types.PermissionReadAcp,
	})

	// Put new ACL
	putInput := &s3.PutObjectAclInput{
		Bucket: aws.String(pm.bucketName),
		Key:    aws.String(objectKey),
		AccessControlPolicy: &types.AccessControlPolicy{
			Grants: newGrants,
			Owner:  getACLResult.Owner,
		},
	}

	_, err = pm.client.PutObjectAcl(context.TODO(), putInput)
	if err != nil {
		atomic.AddInt64(failed, 1)
		return fmt.Errorf("failed to set object ACL: %w", err)
	}

	fmt.Printf("  - [SUCCESS] %s\n", objectKey)
	(*processedObjects)[objectKey] = true
	atomic.AddInt64(granted, 1)
	return nil
}

// loadCheckpoint loads processed objects from checkpoint file
func loadCheckpoint(filePath string) map[string]bool {
	processed := make(map[string]bool)
	
	file, err := os.Open(filePath)
	if err != nil {
		return processed
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			processed[parts[0]] = parts[1] == "true"
		}
	}

	return processed
}

// saveCheckpoint saves processed objects to checkpoint file
func saveCheckpoint(filePath string, processed map[string]bool) {
	file, err := os.Create(filePath)
	if err != nil {
		log.Printf("Warning: Failed to save checkpoint: %v", err)
		return
	}
	defer file.Close()

	for key, success := range processed {
		fmt.Fprintf(file, "%s:%t\n", key, success)
	}
}

// clearCheckpoint removes the checkpoint file
func clearCheckpoint(filePath string) {
	os.Remove(filePath)
}

func main() {
	fmt.Println("=== ECS S3 Permission Manager (Go Version) ===")

	// Get configuration from environment variables
	accessKey := os.Getenv("ECS_S3_ACCESS_KEY")
	secretKey := os.Getenv("ECS_S3_SECRET_KEY")
	endpoint := os.Getenv("ECS_S3_ENDPOINT")
	bucketName := os.Getenv("ECS_S3_BUCKET")
	targetUser := os.Getenv("ECS_S3_ACCESS_TARGET_USER")

	// Validate configuration
	if accessKey == "" {
		log.Fatal("ECS_S3_ACCESS_KEY is required")
	}
	if secretKey == "" {
		log.Fatal("ECS_S3_SECRET_KEY is required")
	}
	if endpoint == "" {
		log.Fatal("ECS_S3_ENDPOINT is required")
	}
	if bucketName == "" {
		log.Fatal("ECS_S3_BUCKET is required")
	}
	if targetUser == "" {
		log.Fatal("ECS_S3_ACCESS_TARGET_USER is required")
	}

	fmt.Printf("Bucket: %s\n", bucketName)
	fmt.Printf("Target User: %s\n", targetUser)
	fmt.Printf("Endpoint: %s\n", endpoint)
	fmt.Println()

	// Create permission manager
	manager, err := NewS3PermissionManager(accessKey, secretKey, endpoint, bucketName, targetUser)
	if err != nil {
		log.Fatalf("Failed to create permission manager: %v", err)
	}

	// Start time
	startTime := time.Now()

	// 1. Grant bucket-level read permission
	err = manager.GrantBucketReadOnlyPermission()
	if err != nil {
		log.Fatalf("Failed to grant bucket permission: %v", err)
	}

	// 2. Grant object-level read permission
	err = manager.GrantObjectReadOnlyPermission()
	if err != nil {
		log.Fatalf("Failed to grant object permissions: %v", err)
	}

	// End time
	elapsed := time.Since(startTime)

	fmt.Printf("\n=== Permission grant completed successfully ===\n")
	fmt.Printf("Total time: %v\n", elapsed)
	fmt.Printf("\n=== Final Summary ===\n")
	fmt.Printf("Bucket: %s\n", bucketName)
	fmt.Printf("Target User: %s\n", targetUser)
	fmt.Printf("Bucket permissions: READ, READ_ACP\n")
	fmt.Printf("Object permissions: READ, READ_ACP (subject to object ownership)\n")
}
