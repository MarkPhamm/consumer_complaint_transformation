# Consumer Complaint Data Transformation

A comprehensive dbt project that transforms raw consumer complaint data into a well-structured star schema for analytical reporting and business intelligence. This project uses **dbt Fusion** for enhanced development experience and performance.

## Project Overview

This project processes consumer complaint data from the Consumer Financial Protection Bureau (CFPB) and transforms it into a dimensional model optimized for analytics. The star schema design enables efficient querying and reporting across multiple business dimensions. Built with dbt Fusion for faster development cycles and improved performance.

## Data Source

- **Source**: Consumer Complaints Database
- **Table**: `CONSUMER_COMPLAINTS_DB.RAW.RAW__CONSUMER_COMPLAINTS`
- **Records**: Consumer complaint submissions with detailed information about products, issues, companies, and responses
- **Data Ingestion**: Raw data is extracted and loaded via the [Consumer Complaint Pipeline](https://github.com/MarkPhamm/consumer_complaint_pipeline) repository, which handles automated data extraction from the Consumer Financial Protection Bureau (CFPB) API and loads it into Snowflake

## Semantic Layers with MetricFlow

<img width="1066" height="736" alt="image" src="https://github.com/user-attachments/assets/e08b6e73-c527-4196-9c73-23ae80d182cb" />

### What are Semantic Layers?

A **semantic layer** is an abstraction layer that provides a business-friendly view of your data warehouse. It translates technical database schemas into business concepts that users can understand and query directly. Key components include:

- **Semantic Models**: Define how raw data maps to business entities
- **Metrics**: Business KPIs and calculations
- **Dimensions**: Attributes for slicing and dicing data
- **Entities**: Business objects that metrics can be grouped by
- **Measures**: Quantifiable values that can be aggregated

### What is MetricFlow?

**MetricFlow** is a semantic layer that sits on top of your dbt models, providing a standardized way to define, query, and analyze business metrics. It acts as a translation layer between business questions and SQL queries, enabling consistent metric definitions across different tools and users.

### Key Benefits of MetricFlow

- **Consistent Metrics**: Single source of truth for metric definitions
- **Self-Service Analytics**: Business users can query metrics without writing SQL
- **Time Intelligence**: Built-in support for time-based analysis and comparisons
- **Flexible Querying**: Support for complex analytical queries with automatic SQL generation
- **Tool Agnostic**: Works with various BI tools and can be accessed via API

### MetricFlow Implementation in This Project

Our consumer complaint transformation project implements MetricFlow through several key components:

#### 1. Semantic Model Configuration (`semantic_models.yml`)

```yaml
semantic_models:
  - name: complaints
    description: Consumer complaints semantic model
    model: ref('fct_complaints')
    defaults:
      agg_time_dimension: date_received
    
    entities:
      - name: complaint
        type: primary
        expr: complaint_id
      - name: company
        type: foreign
        expr: company_key
      - name: product
        type: foreign
        expr: product_key
      - name: issue
        type: foreign
        expr: issue_key
      - name: location
        type: foreign
        expr: location_key
    
    dimensions:
      - name: date_received
        type: time
        type_params:
          time_granularity: day
        expr: date_received
      - name: consumer_consent_provided_flag
        type: categorical
        expr: consumer_consent_provided_flag
      # ... additional dimensions
    
    measures:
      - name: total_complaints
        description: Total number of complaints
        agg: count
        expr: complaint_id
      - name: avg_days_to_send
        description: Average days to send complaint to company
        agg: average
        expr: days_to_send_to_company
```

#### 2. Metrics Definition (`metrics.yml`)

```yaml
metrics:
  - name: total_complaints
    description: Total number of consumer complaints
    type: simple
    label: Total Complaints
    type_params:
      measure: total_complaints
  
  - name: avg_response_time_days
    description: Average days to send complaint to company
    type: simple
    label: Average Response Time (Days)
    type_params:
      measure: avg_days_to_send
```

#### 3. Time Spine Configuration (`metricflow_time_spine.yml`)

```yaml
models:
  - name: metricflow_time_spine
    description: Date spine for MetricFlow metrics
    columns:
      - name: date_day
        description: Date column for time spine
```

### Available Metrics and Dimensions

Based on our MetricFlow configuration, the following metrics and dimensions are available:

#### Metrics

- **`total_complaints`**: Total number of consumer complaints
- **`avg_response_time_days`**: Average days to send complaint to company

#### Dimensions

- **`complaint__date_received`**: Date when complaint was received (time dimension)
- **`complaint__date_sent_to_company`**: Date when complaint was sent to company (time dimension)
- **`complaint__consumer_consent_provided_flag`**: Whether consumer provided consent
- **`complaint__timely_response_flag`**: Whether company provided timely response
- **`complaint__consumer_disputed_flag`**: Whether consumer disputed the response
- **`complaint__submitted_via`**: Method used to submit complaint
- **`metric_time`**: Default time dimension for metric queries

### Querying Metrics with MetricFlow

#### Command Line Interface

```bash
# List available metrics
mf list metrics

# List dimensions for a specific metric
mf list dimensions --metrics total_complaints

# Query metrics with dimensions
mf query --metrics total_complaints --group-by complaint__date_received__day --order complaint__date_received__day --limit 10

# Query multiple metrics
mf query --metrics total_complaints avg_response_time_days --group-by complaint__submitted_via

# Time-based analysis
mf query --metrics total_complaints --group-by metric_time__month --order metric_time__month
```

#### Example Query Results

```
COMPLAINT__DATE_RECEIVED__DAY      TOTAL_COMPLAINTS
-------------------------------  ------------------
2011-12-01T00:00:00                              38
2011-12-02T00:00:00                              37
2011-12-03T00:00:00                               7
2011-12-04T00:00:00                               3
2011-12-05T00:00:00                              59
```

### Integration with BI Tools

MetricFlow can be integrated with various business intelligence tools:

- **dbt Semantic Layer**: Native integration for dbt Cloud users
- **REST API**: Programmatic access to metrics
- **SQL Interface**: Direct SQL querying capabilities
- **Custom Applications**: Embed metrics in custom dashboards

### Validation and Testing

```bash
# Validate MetricFlow configuration
mf validate-configs

# Test semantic model definitions
mf validate-semantic-model complaints

# Check for configuration errors
mf validate-configs --show-all
```

### Benefits for Business Users

1. **Self-Service Analytics**: Business users can query metrics without technical SQL knowledge
2. **Consistent Definitions**: All users access the same metric calculations
3. **Flexible Analysis**: Easy slicing and dicing across multiple dimensions
4. **Time Intelligence**: Built-in support for period-over-period comparisons
5. **Governance**: Centralized control over metric definitions and business logic

### Future Enhancements

- **Derived Metrics**: Complex calculations combining multiple base metrics
- **Metric Filters**: Pre-defined filters for common business scenarios
- **Custom Aggregations**: Advanced aggregation methods beyond standard SQL functions
- **Metric Lineage**: Track dependencies and impact analysis for metric changes

## Architecture

### Data Flow

```
CFPB API → [Consumer Complaint Pipeline](https://github.com/MarkPhamm/consumer_complaint_pipeline) → Snowflake Raw → Staging Layer → Marts Layer (Star Schema)
```

### Complete Data Pipeline Architecture

This transformation project is part of a larger data pipeline ecosystem:

1. **Data Ingestion**: [Consumer Complaint Pipeline](https://github.com/MarkPhamm/consumer_complaint_pipeline) - Automated extraction from CFPB API using Apache Airflow
2. **Data Transformation**: This repository - dbt Fusion-based star schema transformation
3. **Data Storage**: Snowflake data warehouse with structured schemas
4. **Data Analysis**: Ready for business intelligence and analytical reporting

### Project Structure

```
models/
├── staging/
│   ├── stg__consumer_complaints.sql          # Raw data staging
│   └── stg__consumer_complaints_source.yml   # Source configuration
└── marts/
    ├── dim_companies.sql                     # Company dimension
    ├── dim_products.sql                      # Product dimension
    ├── dim_issues.sql                        # Issue dimension
    ├── dim_locations.sql                     # Location dimension
    ├── dim_dates.sql                         # Date dimension
    ├── fct_complaints.sql                    # Complaints fact table
    └── marts_schema.yml                      # Marts documentation
```

## Star Schema Design

### Dimension Tables

#### `dim_companies`

Contains company information and response patterns:

- `company_key`: Surrogate key
- `company_name`: Company name
- `company_public_response`: Public response text
- `company_response_to_consumer`: Response to consumer
- `timely_response_flag`: Boolean for timely response
- `consumer_disputed_flag`: Boolean for consumer dispute

#### `dim_products`

Product and sub-product categorization:

- `product_key`: Surrogate key
- `product_name`: Main product category
- `sub_product_name`: Sub-product classification
- `product_full_name`: Combined product description

#### `dim_issues`

Issue and sub-issue classification:

- `issue_key`: Surrogate key
- `issue_name`: Main issue category
- `sub_issue_name`: Sub-issue classification
- `issue_full_name`: Combined issue description

#### `dim_locations`

Geographic information:

- `location_key`: Surrogate key
- `state_code`: Two-letter state code
- `zip_code`: ZIP code
- `location_description`: Combined location description

#### `dim_dates`

Rich date dimension with multiple attributes:

- `date_key`: Surrogate key
- `complaint_date`: Actual date
- `year`, `month`, `day`, `quarter`: Date components
- `day_of_week`, `day_of_year`: Additional date attributes
- `season`, `day_type`: Business-friendly attributes
- Various date truncations for reporting

### Fact Table

#### `fct_complaints`

Main fact table with foreign keys to all dimensions:

- `complaint_id`: Primary key
- Foreign keys to all dimension tables
- `days_to_send_to_company`: Calculated metric
- Boolean flags for consent, timely response, and disputes
- Text fields for descriptions and responses

## Key Features

### Data Quality

- Comprehensive data tests including uniqueness, not-null constraints, and referential integrity
- Surrogate keys using `dbt_utils.generate_surrogate_key()` for consistent dimension keys
- Data type standardization and null handling

### Performance Optimization

- Star schema design for efficient analytical queries
- Materialized tables for fast query performance
- Proper indexing through surrogate keys
- dbt Fusion caching for faster development cycles
- Optimized query execution with Fusion's enhanced engine

### Business Logic

- Calculated metrics like response time
- Boolean flag conversions for easier analysis
- Rich date dimension for flexible time-based reporting

## Getting Started

### Prerequisites

- dbt Fusion installed
- Access to Snowflake database
- Required dbt packages: `dbt_utils`, `dbt_expectations`, `dbt_date`

### dbt Fusion Setup

#### Installation

1. Install dbt Fusion:

   ```bash
   pip install dbt-fusion
   ```

2. Verify installation:

   ```bash
   dbtf --version
   ```

#### Configuration

1. Clone the repository
2. Install dependencies:

   ```bash
   dbtf deps
   ```

3. Configure your `profiles.yml` with database credentials
4. Initialize Fusion cache:

   ```bash
   dbtf cache
   ```

### Running the Project

#### Build Models

```bash
# Build all models
dbtf build

# Build specific models
dbtf build -s models/marts/

# Build with specific target
dbtf build --target prod
```

#### Run Tests

```bash
# Run all tests
dbtf test

# Run tests for specific models
dbtf test -s marts

# Run tests for a specific model
dbtf test -s fct_complaints
```

#### Development Workflow

```bash
# Parse and validate project
dbtf parse

# Run specific model in development
dbtf run -s stg__consumer_complaints

# Run with dependencies
dbtf run -s +fct_complaints

# Run downstream models
dbtf run -s fct_complaints+

# Compile models to view SQL
dbtf compile -s fct_complaints
```

### Fusion-Specific Commands

#### Cache Management

```bash
# Initialize cache
dbtf cache

# Clear cache
dbtf cache --clear

# Cache specific models
dbtf cache -s marts
```

#### Performance Monitoring

```bash
# Show execution plan
dbtf plan

# Run with performance insights
dbtf run --log-level debug
```

## Usage Examples

### Top Companies by Complaint Volume

```sql
SELECT 
    c.company_name,
    COUNT(f.complaint_id) as complaint_count,
    AVG(f.days_to_send_to_company) as avg_response_days
FROM MARTS.fct_complaints f
JOIN MARTS.dim_companies c ON f.company_key = c.company_key
GROUP BY c.company_name
ORDER BY complaint_count DESC;
```

### Complaints by Product and Issue

```sql
SELECT 
    p.product_name,
    i.issue_name,
    COUNT(f.complaint_id) as complaint_count,
    SUM(CASE WHEN f.consumer_disputed_flag THEN 1 ELSE 0 END) as disputed_count
FROM MARTS.fct_complaints f
JOIN MARTS.dim_products p ON f.product_key = p.product_key
JOIN MARTS.dim_issues i ON f.issue_key = i.issue_key
GROUP BY p.product_name, i.issue_name
ORDER BY complaint_count DESC;
```

### Monthly Complaint Trends

```sql
SELECT 
    d.year,
    d.month,
    d.month_start,
    COUNT(f.complaint_id) as complaint_count,
    COUNT(DISTINCT f.company_key) as unique_companies
FROM MARTS.fct_complaints f
JOIN MARTS.dim_dates d ON f.date_received_key = d.date_key
GROUP BY d.year, d.month, d.month_start
ORDER BY d.year, d.month;
```

## Data Quality Monitoring

The project includes comprehensive data quality tests:

- **Uniqueness**: Ensures primary keys and surrogate keys are unique
- **Not Null**: Validates required fields are populated
- **Referential Integrity**: Confirms foreign key relationships
- **Data Freshness**: Monitors data recency

## dbt Fusion Benefits

### Enhanced Development Experience

- **Faster Compilation**: Fusion's optimized engine reduces model compilation time
- **Intelligent Caching**: Automatic caching of compiled models and dependencies
- **Better Error Messages**: More descriptive error reporting and debugging
- **Performance Insights**: Built-in query performance monitoring

### Improved Workflow

- **Incremental Development**: Run only changed models and their dependencies
- **Smart Dependency Resolution**: Automatic detection of model relationships
- **Enhanced Testing**: Faster test execution with intelligent test selection
- **Real-time Feedback**: Immediate validation during development

## Maintenance

### Adding New Dimensions

1. Create new dimension model in `models/marts/`
2. Add surrogate key generation
3. Update fact table with new foreign key
4. Add tests to `marts_schema.yml`
5. Update Fusion cache: `dbtf cache -s new_model`

### Updating Business Logic

- Modify calculated fields in fact table
- Update dimension transformations as needed
- Ensure tests remain valid after changes
- Clear cache if needed: `dbtf cache --clear`

### Fusion-Specific Maintenance

```bash
# Update cache after schema changes
dbtf cache --refresh

# Validate project structure
dbtf parse

# Check for performance regressions
dbtf run --log-level debug
```

## Contributing

1. Follow dbt best practices for model organization
2. Include comprehensive tests for new models
3. Update documentation in schema.yml files
4. Test changes thoroughly before committing
5. Use Fusion commands for development: `dbtf run`, `dbtf test`, `dbtf compile`

## License

This project is licensed under the MIT License.
