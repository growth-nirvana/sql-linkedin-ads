# LinkedIn Ads Data Transformations

This repository contains SQL transformations for LinkedIn Ads data, converting raw Singer tap data into two types of tables:

1. **SCD (Slowly Changing Dimension) Tables**
   - Type 2 SCD tables that track historical changes
   - Includes metadata columns like `_gn_start`, `_gn_end`, `_gn_active`
   - Uses hash-based change detection with `_gn_id`

2. **Reporting Tables**
   - Daily snapshot tables for metrics and KPIs
   - Partitioned by date for efficient querying
   - Includes run_id tracking for data lineage

## Repository Structure

```
.
├── models/
│   ├── scd/           # SCD Type 2 transformations
│   └── reporting/     # Daily reporting tables
├── macros/           # Reusable SQL macros
└── tests/            # SQL tests
```

## Table Patterns

### SCD Type 2 Tables
- Track historical changes with start/end timestamps
- Use hash-based change detection
- Include metadata columns for tracking

### Reporting Tables
- Daily snapshots of metrics
- Partitioned by date
- Include run_id for data lineage
- Support batch-based updates

## Usage

1. Configure your target and source datasets in the variables
2. Run the transformations in order:
   - SCD tables first
   - Reporting tables second (they may depend on SCD tables)

## Variables

Required variables:
- `target_dataset_id`: Target BigQuery dataset
- `source_dataset_id`: Source BigQuery dataset 