-- ============================================================
-- Employee Pipeline
-- Curated Employee Dimension
--
-- Purpose:
--   Stores the employee dimension using Slowly Changing
--   Dimension Type 2 history.
--
-- Business key:
--   emp_id
--
-- Version key:
--   employee_sk
-- ============================================================
CREATE TABLE IF NOT EXISTS curated.dim_employee
(
    -- Unique identifier for each employee version.
    employee_sk          BIGINT IDENTITY(1,1),
    -- Source-system business key.
    emp_id               INTEGER,
    -- Employee business attributes.
    emp_name             VARCHAR(200),
    age                  INTEGER,
    gender               VARCHAR(50),
    department_name      VARCHAR(100),
    position             VARCHAR(150),
    salary               DECIMAL(12,2),
    joining_date         DATE,
    experience_years     DECIMAL(5,2),
    -- Source audit timestamps.
    created_at           TIMESTAMP,
    updated_at           TIMESTAMP,
    -- Derived analytical attributes.
    age_band             VARCHAR(50),
    experience_band      VARCHAR(50),
    -- Target copy of the incoming SHA-256 business hash.
    tgt_business_hash    VARCHAR(64),
    -- SCD Type 2 lifecycle fields.
    effective_from       TIMESTAMP NOT NULL,
    effective_to         TIMESTAMP,
    is_current           CHAR(1) NOT NULL
);
