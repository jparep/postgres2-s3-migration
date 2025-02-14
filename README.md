health-data-migration/
│── terraform/
│   │── dms_cdc.tf                 # Terraform script to provision AWS DMS
│   │── sns_alerts.tf               # Terraform script for SNS alerting
│   │── ci_cd_pipeline.tf           # Terraform script for CI/CD automation
│   └── variables.tf                 # Terraform variables
│
│── lambda/
│   │── lambda_anomaly.py           # Lambda function for real-time anomaly detection
│   └── requirements.txt             # Required Python dependencies
│
│── glue/
│   └── glue_transform.py           # AWS Glue script for ETL processing
│
│── scripts/
│   │── trigger_glue.py             # Script to manually trigger AWS Glue
│   └── validate_data.py             # Script for data validation checks
│
│── notebooks/
│   └── PostgreSQL_to_S3.ipynb       # Jupyter Notebook for local testing
│
│── sql/
│   │── create_athena_table.sql      # Athena table creation script
│   └── partition_data.sql           # Athena partitioning script
│
└── README.md                        # Documentation
