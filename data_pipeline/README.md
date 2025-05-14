# CV Assistant Data Pipeline

A modern data processing pipeline for job description analysis and CV assistance using a data lake architecture.

## Overview

This project implements a comprehensive data pipeline for processing, analyzing, and storing job descriptions. The system extracts raw job data, parses it using advanced NLP techniques, normalizes fields like tech stack requirements, soft skills, and education majors, and stores the processed data in a structured format suitable for downstream applications like CV assistant tools.

## Architecture

The pipeline follows a multi-layer data lake architecture:

- **Bronze Layer**: Raw job descriptions extracted from sources
- **Parser Layer**: Processes raw text into structured information
- **Silver Layer**: Normalizes and cleanses data (tech stacks, soft skills, etc.)
- **Gold Layer**: Analytical views and aggregated data
- **Warehouse Layer**: Final structured data for application consumption

### Technologies

- **Dagster**: Orchestration platform for data pipelines
- **MongoDB**: Document database for storing raw and parsed job descriptions
- **MinIO**: S3-compatible object storage used as a data lake
- **PostgreSQL**: Relational database for structured data
- **Docker**: Containerization for deployment and scaling
- **Polars**: Fast DataFrame library for data processing

## Project Structure

```
.
├── dagster_home/          # Dagster configuration files
├── docker_images/         # Docker image definitions
├── mongodb_data_backup/   # Backup data for MongoDB
├── pipeline/              # Main pipeline code
│   ├── pipeline/          # Pipeline implementation
│   │   ├── assets/        # Dagster assets
│   │   └── resources/     # Pipeline resources (parsers, models, etc.)
│   ├── setup.py           # Package configuration
│   └── README.md          # Pipeline-specific documentation
├── storage/               # Storage service configurations
│   ├── minio/             # MinIO data lake storage
│   ├── mongo_terraform/   # MongoDB Atlas setup with Terraform
│   ├── mongodb/           # MongoDB local storage
│   └── postgresql/        # PostgreSQL data storage
├── test/                  # Test files and utilities
└── docker-compose.yaml    # Docker Compose configuration
```

## Setup & Installation

### Prerequisites

- Docker and Docker Compose
- Python 3.12+
- Terraform (optional, for MongoDB Atlas setup)

### Environment Configuration

1. Create a `.env` file based on the template provided:
   ```bash
   cp .env.template .env
   ```

2. Configure environment variables in the `.env` file:
   - MongoDB credentials
   - PostgreSQL credentials
   - MinIO access keys
   - Other configuration parameters

### Using Docker Compose

The provided Makefile simplifies Docker operations:

```bash
# Build Docker images
make build

# Start the services
make up

# Stop all services
make down

# Rebuild and restart
make rebuild
```

## Running the Pipeline

Once the environment is set up:

1. Access the Dagster UI at http://localhost:3001

2. Run the pipeline assets through Dagster:
   - `parse_jds_to_mongodb`: Parses job descriptions and saves to MongoDB
   - `bronze_job_descriptions`: Extracts data from MongoDB to the data lake
   - Silver layer assets: Normalize and cleanse data
   - Gold layer assets: Create analytical views
   - Warehouse assets: Prepare data for application consumption

## Data Model

### Job Description Structure

The pipeline processes job descriptions through several transformations:

1. **Raw Job Data**: Original text and metadata
2. **Parsed Job Data**: Structured information extracted from raw text
3. **Normalized Data**:
   - Technical skills (programming languages, tools, frameworks)
   - Soft skills (communication, leadership, etc.)
   - Education requirements (degrees, majors, etc.)
   - Experience levels
   - Job categories and roles

## Development

### Adding New Pipeline Components

1. Create new asset definitions in the appropriate layer folder
2. Define resources in the resources directory
3. Update Dagster configurations as needed

### Testing

Test utilities are available in the `test/` directory:
```bash
python -m pytest test/
```

## MongoDB Atlas Setup (Optional)

For production deployments, MongoDB Atlas can be set up using Terraform:

1. Navigate to the MongoDB Terraform directory:
   ```bash
   cd storage/mongo_terraform
   ```

2. Configure variables in `variables.tf`

3. Apply the Terraform configuration:
   ```bash
   terraform init
   terraform apply
   ```

## Contributing

Guidelines for contributing to the project:

1. Fork the repository
2. Create a feature branch
3. Submit a pull request with comprehensive documentation

## License

[Your license information here]