-- ============================================================
-- Employee Pipeline
-- SCD Type 2 Stored Procedure
--
-- Supports:
--   HISTORICAL
--   INCREMENTAL
--
-- Historical:
--   Rebuilds the initial curated SCD2 baseline from staging.
--
-- Incremental:
--   Classifies incoming employees as:
--     NEW
--     CHANGED
--     UNCHANGED
--
--   NEW       -> insert
--   CHANGED   -> expire previous version + insert new version
--   UNCHANGED -> no action
-- ============================================================
CREATE OR REPLACE PROCEDURE curated.sp_load_employee_scd2
(
    p_run_type VARCHAR(20),
    p_batch_run_timestamp TIMESTAMP
)
LANGUAGE plpgsql
AS
$$
BEGIN
    -- --------------------------------------------------------
    -- Validate pipeline execution mode.
    -- --------------------------------------------------------
    IF UPPER(p_run_type) NOT IN ('HISTORICAL', 'INCREMENTAL') THEN
        RAISE EXCEPTION
            'Invalid run_type. Expected HISTORICAL or INCREMENTAL.';
    END IF;
    -- ========================================================
    -- HISTORICAL
    --
    -- Build the initial SCD Type 2 baseline.
    -- ========================================================
    IF UPPER(p_run_type) = 'HISTORICAL' THEN
        -- Controlled historical rebuild.
        --
        -- DELETE is used instead of TRUNCATE because the
        -- pipeline execution identity has DELETE privilege on
        -- the curated table.
        DELETE FROM curated.dim_employee;
        INSERT INTO curated.dim_employee
        (
            emp_id,
            emp_name,
            age,
            gender,
            department_name,
            position,
            salary,
            joining_date,
            experience_years,
            created_at,
            updated_at,
            age_band,
            experience_band,
            tgt_business_hash,
            effective_from,
            effective_to,
            is_current
        )
        SELECT
            emp_id,
            emp_name,
            age,
            gender,
            department_name,
            position,
            salary,
            joining_date,
            experience_years,
            created_at,
            updated_at,
            age_band,
            experience_band,
            -- Hash calculated during Silver -> staging.
            stg_business_hash,
            -- Initial SCD2 version begins at batch time.
            p_batch_run_timestamp,
            -- NULL means the version has not expired.
            NULL,
            -- Every employee is initially current.
            'Y'
        FROM staging.stg_employee;
    -- ========================================================
    -- INCREMENTAL
    -- ========================================================
    ELSEIF UPPER(p_run_type) = 'INCREMENTAL' THEN
        -- ----------------------------------------------------
        -- Build a stable classification before modifying Gold.
        --
        -- This is important because CHANGED current records
        -- will later be expired. The classification must be
        -- captured before that update occurs.
        -- ----------------------------------------------------
        DROP TABLE IF EXISTS tmp_employee_changes;
        CREATE TEMP TABLE tmp_employee_changes AS
        SELECT
            s.*,
            CASE
                -- Employee does not currently exist in Curated.
                WHEN g.emp_id IS NULL
                    THEN 'NEW'
                -- Employee exists, but one or more tracked
                -- business attributes changed.
                WHEN s.stg_business_hash <>
                     g.tgt_business_hash
                    THEN 'CHANGED'
                -- Same employee and same business-state hash.
                ELSE 'UNCHANGED'
            END AS change_type
        FROM staging.stg_employee AS s
        LEFT JOIN curated.dim_employee AS g
            ON s.emp_id = g.emp_id
           AND g.is_current = 'Y';
        -- ----------------------------------------------------
        -- Expire the previous current version for employees
        -- whose tracked business attributes changed.
        -- ----------------------------------------------------
        UPDATE curated.dim_employee
        SET
            effective_to = p_batch_run_timestamp,
            is_current = 'N'
        FROM tmp_employee_changes AS c
        WHERE curated.dim_employee.emp_id = c.emp_id
          AND curated.dim_employee.is_current = 'Y'
          AND c.change_type = 'CHANGED';
        -- ----------------------------------------------------
        -- Insert:
        --
        -- NEW employees
        -- +
        -- new versions of CHANGED employees
        --
        -- UNCHANGED employees create no new version.
        -- ----------------------------------------------------
        INSERT INTO curated.dim_employee
        (
            emp_id,
            emp_name,
            age,
            gender,
            department_name,
            position,
            salary,
            joining_date,
            experience_years,
            created_at,
            updated_at,
            age_band,
            experience_band,
            tgt_business_hash,
            effective_from,
            effective_to,
            is_current
        )
        SELECT
            emp_id,
            emp_name,
            age,
            gender,
            department_name,
            position,
            salary,
            joining_date,
            experience_years,
            created_at,
            updated_at,
            age_band,
            experience_band,
            -- Incoming staging hash becomes target hash.
            stg_business_hash,
            -- New SCD2 version starts at incremental batch time.
            p_batch_run_timestamp,
            NULL,
            'Y'
        FROM tmp_employee_changes
        WHERE change_type IN ('NEW', 'CHANGED');
    END IF;
END;
$$;
