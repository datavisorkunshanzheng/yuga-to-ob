#!/bin/bash

set -e

# Define colors for console output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Parse command line arguments
SKIP_UPLOAD=false
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --skip-upload)
            SKIP_UPLOAD=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

echo -e "${GREEN}Starting build and upload script${NC}"

# Get the directory of the script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Read S3 path from config.json
S3_JAR_PATH=$(grep -o '"s3_jar_path":[^,]*' config.json | cut -d'"' -f4 | sed 's/s3a:/s3:/')

echo -e "${GREEN}Building JAR file in export_yuga directory${NC}"
cd ../export_yuga

# Clean and package the jar with Maven
mvn clean package -DskipTests

# Check if build was successful
if [ $? -ne 0 ]; then
    echo -e "${RED}Maven build failed${NC}"
    exit 1
fi

# Get the path of the newly created JAR file
JAR_FILE=$(find target -name "*.jar" | grep -v "original")

if [ -z "$JAR_FILE" ]; then
    echo -e "${RED}Could not find built JAR file${NC}"
    exit 1
fi

echo -e "${GREEN}JAR file built successfully: $JAR_FILE${NC}"

if [ "$SKIP_UPLOAD" = true ]; then
    echo -e "${YELLOW}Skipping S3 upload as requested${NC}"
    echo -e "${GREEN}JAR file is available at: $(pwd)/$JAR_FILE${NC}"
    echo -e "${GREEN}Target S3 path: $S3_JAR_PATH${NC}"
    echo -e "${GREEN}You can upload it manually with: aws s3 cp $(pwd)/$JAR_FILE $S3_JAR_PATH${NC}"
    echo -e "${GREEN}Build completed successfully!${NC}"
    exit 0
fi

# Check AWS credentials
echo -e "${GREEN}Checking AWS credentials...${NC}"
if ! aws sts get-caller-identity &>/dev/null; then
    echo -e "${RED}AWS credentials not configured or invalid${NC}"
    echo -e "${RED}Please configure AWS credentials with the following commands:${NC}"
    echo -e "aws configure set aws_access_key_id YOUR_ACCESS_KEY"
    echo -e "aws configure set aws_secret_access_key YOUR_SECRET_KEY"
    echo -e "aws configure set region us-west-2"
    echo -e "\nAlternatively, you can copy the JAR manually to S3 using the AWS console or other tools.\n"
    echo -e "${GREEN}JAR file is available at: $(pwd)/$JAR_FILE${NC}"
    echo -e "${GREEN}Target S3 path: $S3_JAR_PATH${NC}"
    echo -e "${GREEN}You can upload it manually with: aws s3 cp $(pwd)/$JAR_FILE $S3_JAR_PATH${NC}"
    echo -e "${YELLOW}You can also run this script with --skip-upload to skip the upload step.${NC}"
    exit 1
fi

# Upload to S3
echo -e "${GREEN}Uploading JAR file to $S3_JAR_PATH${NC}"
aws s3 cp $JAR_FILE $S3_JAR_PATH

if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to upload JAR file to S3${NC}"
    echo -e "${RED}Possible reasons:${NC}"
    echo -e "  - Insufficient permissions to write to the S3 bucket"
    echo -e "  - The S3 bucket does not exist"
    echo -e "  - Network connectivity issues"
    echo -e "\nYou can copy the JAR manually to S3 using the AWS console or other tools.\n"
    echo -e "${GREEN}JAR file is available at: $(pwd)/$JAR_FILE${NC}"
    echo -e "${GREEN}Target S3 path: $S3_JAR_PATH${NC}"
    echo -e "${GREEN}You can upload it manually with: aws s3 cp $(pwd)/$JAR_FILE $S3_JAR_PATH${NC}"
    echo -e "${YELLOW}You can also run this script with --skip-upload to skip the upload step.${NC}"
    exit 1
fi

echo -e "${GREEN}JAR file successfully uploaded to S3${NC}"
echo -e "${GREEN}Build and upload completed successfully!${NC}"
