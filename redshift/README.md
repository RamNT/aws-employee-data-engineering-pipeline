
# Amazon Redshift

Amazon Redshift Serverless is used as the curated data warehouse layer in the Employee Data Engineering Pipeline.

## 1. Schemas

### 1.1 Staging Schema

The `staging` schema contains the current incoming employee batch loaded from AWS Glue.

**Main table:** `staging.stg_employee`

**Responsibilities:**
- Store the current incoming employee batch.
- Provide a staging area for validation and downstream processing.
- Supply incoming records to the SCD Type 2 stored procedure.

### 1.2 Curated Schema

The `curated` schema contains the historical employee dimension maintained using Slowly Changing Dimension Type 2 (SCD Type 2).

**Main table:** `curated.dim_employee`

**Responsibilities:**
- Maintain employee history when tracked attributes change.
- Keep the previous version of a changed employee record.
- Maintain the current version of each employee record.
- Support historical reporting and analytics.

## 2. Stored Procedure

**Procedure:** `curated.sp_load_employee_scd2`

The stored procedure processes incoming records from the staging table and applies SCD Type 2 logic to the curated employee dimension.

## 3. SCD Type 2 Implementation

The incremental stored procedure classifies incoming employee records into three categories:

| Classification | Description | Action |
|---|---|---|
| `NEW` | Employee does not exist in the curated dimension. | Insert a new current record. |
| `CHANGED` | Employee exists, but one or more SCD-tracked attributes have changed. | Expire the previous current record and insert a new current version. |
| `UNCHANGED` | Employee exists and no tracked attributes have changed. | Keep the existing current record without creating a new version. |

### Change Detection

A business hash is used to identify changes in the SCD-tracked employee attributes.

- **New records:** Inserted into the curated dimension as current records.
- **Changed records:** The previous current version is expired, and a new current version is inserted.
- **Unchanged records:** No new version is created.

The business hash helps avoid comparing every tracked attribute individually during change detection.

## 4. End-to-End Data Flow

```text
AWS Glue ETL
     |
     v
staging.stg_employee
     |
     v
curated.sp_load_employee_scd2
     |
     v
curated.dim_employee
     |
     v
Historical Reporting and Analytics
```

## 5. Key Design Principles

- **Separation of concerns:** Staging and curated data are maintained in separate schemas.
- **Historical tracking:** SCD Type 2 preserves previous employee versions when tracked attributes change.
- **Change detection:** Business hashing identifies changes to tracked attributes.
- **Incremental processing:** Incoming batches are processed without unnecessarily creating new versions for unchanged records.
- **Analytics-ready data:** The curated dimension supports reporting on current and historical employee attributes.
