# Loan Processing ETL Demo

A comprehensive ETL (Extract, Transform, Load) pipeline built with Ballerina for processing vehicle loan applications and generating business intelligence reports.

## Project Overview

This project demonstrates a complete ETL workflow that processes vehicle loan application data from CSV files, performs various data transformations, and loads the results into an H2 database. The pipeline generates actionable business insights including branch performance metrics, vehicle financing trends, officer performance analytics, and data quality assessments.

## Business Context

The system processes vehicle loan applications from multiple branches, analyzing:
- **Loan Applications**: Customer requests for vehicle financing
- **Approval Records**: Decisions made by loan officers
- **Financial Metrics**: Risk assessments and performance indicators
- **Business Intelligence**: Branch and officer performance analytics

## ETL Pipeline Architecture

### 1. Extract Phase
- Reads loan application data from `resources/request_applications.csv`
- Reads approval decisions from `resources/approved_applications.csv`
- Parses CSV data into structured Ballerina records

### 2. Transform Phase
The transformation layer performs seven key operations:

#### T1: Data Enrichment (JOIN)
- Enriches application data with approval information
- Creates detailed records combining customer, vehicle, and approval data

#### T2: Financial Calculations
- **Loan-to-Value (LTV) Ratio**: `(requested_amount / total_price) * 100`
- **Debt-to-Income (DTI) Ratio**: `(monthly_payment / monthly_income) * 100`
- **Age Calculation**: Customer age at application time

#### T3: Categorization
- **Age Groups**: Young (18-30), Mid (31-50), Senior (51+)
- **Income Brackets**: Low (<200K), Medium (200K-300K), High (>300K)
- **Credit Score Bands**: Poor (<650), Fair (650-699), Good (700-749), Excellent (750+)
- **Loan Amount Tiers**: Small (<5M), Medium (5M-10M), Large (>10M)
- **Risk Categories**: Low, Medium, High (based on credit, DTI, LTV)

#### T4: Branch Performance Aggregation
- Total applications per branch
- Approval/rejection counts and rates
- Average and total approved amounts
- Branch-level performance metrics

#### T5: Vehicle Analysis Aggregation
- Application counts by vehicle brand and fuel type
- Approval rates by vehicle characteristics
- Market trend analysis

#### T6: Officer Performance Aggregation
- Individual officer approval statistics
- Total and average approval amounts
- Performance benchmarking

#### T7: Data Quality Validation
- **Missing Approvals**: Applications marked approved without approval records
- **Amount Anomalies**: Approved amounts exceeding vehicle prices
- **High DTI Flags**: Debt-to-income ratios above 50%

### 3. Load Phase
Stores processed data in H2 database tables:
- `branch_summaries`: Branch performance metrics
- `vehicle_summaries`: Vehicle financing trends
- `officer_performances`: Loan officer analytics
- `data_quality_flags`: Data integrity issues
- `categorized_applications`: Risk and demographic segments

## Data Model

### Input Data Structures
- **Application**: Customer loan requests with vehicle and financial details
- **ApprovedApplication**: Officer decisions with approval amounts and dates

### Output Data Structures
- **FinalReport**: Consolidated business intelligence report
- **BranchSummary**: Branch-level performance metrics
- **VehicleSummary**: Vehicle category analysis
- **OfficerPerformance**: Individual officer statistics
- **DataQualityFlag**: Data integrity alerts
- **CategorizedApplication**: Risk and demographic segmentation

## Technology Stack

- **Language**: Ballerina
- **Database**: H2 (embedded SQL database)
- **Data Format**: CSV input files
- **Architecture**: ETL pipeline with modular transformations

## Project Structure

```
├── main.bal              # Main ETL orchestration logic
├── types.bal             # Data type definitions and enums
├── utils.bal             # Utility functions for calculations
├── database.bal          # Database operations and schema
├── resources/
│   ├── request_applications.csv    # Input: Loan applications
│   └── approved_applications.csv   # Input: Approval decisions
├── database/
│   └── loandatabase.mv.db         # H2 database file
├── tests/
│   ├── tests.bal         # Unit tests
│   └── final_report.json # Expected test output
├── Ballerina.toml        # Project configuration
└── Dependencies.toml     # Dependency management
```

## Getting Started

### Prerequisites
- Install Ballerina 

### Running the ETL Pipeline
```bash
bal run
```

### Running Tests
```bash
bal test
```

## Implementation Status

The project includes:
- ✅ Complete ETL pipeline structure
- ✅ Database schema and operations
- ✅ Type definitions and data model
- ✅ Transformation logic framework
- ⚠️ CSV reading implementation (TODO)
- ⚠️ Some aggregation functions (TODO)
- ⚠️ Utility function implementations (TODO)
