-- ============================================================
-- Incremental SCD Type 2 Validation
-- ============================================================
-- 1. Current vs historical versions after incremental run.
SELECT
    is_current,
    COUNT(*) AS record_count
FROM curated.dim_employee
GROUP BY is_current
ORDER BY is_current;
-- 2. Verify that no employee has multiple current versions.
--
-- Expected result:
-- zero rows.
SELECT
    emp_id,
    COUNT(*) AS current_version_count
FROM curated.dim_employee
WHERE is_current = 'Y'
GROUP BY emp_id
HAVING COUNT(*) > 1;
-- 3. Inspect employees with multiple SCD2 versions.
--
-- These are candidates for employees whose tracked
-- business attributes changed.
SELECT
    emp_id,
    COUNT(*) AS version_count
FROM curated.dim_employee
GROUP BY emp_id
HAVING COUNT(*) > 1
ORDER BY version_count DESC, emp_id;
-- 4. Inspect the complete history of one employee.
--
-- Replace <EMPLOYEE_ID> before running.
SELECT
    employee_sk,
    emp_id,
    emp_name,
    department_name,
    position,
    salary,
    tgt_business_hash,
    effective_from,
    effective_to,
    is_current
FROM curated.dim_employee
WHERE emp_id = <EMPLOYEE_ID>
ORDER BY effective_from;
-- 5. Expired records should normally have effective_to.
SELECT COUNT(*) AS invalid_expired_records
FROM curated.dim_employee
WHERE is_current = 'N'
  AND effective_to IS NULL;
-- 6. Current records should have NULL effective_to.
SELECT COUNT(*) AS invalid_current_records
FROM curated.dim_employee
WHERE is_current = 'Y'
  AND effective_to IS NOT NULL;
-- 7. Validate allowed is_current values.
SELECT
    is_current,
    COUNT(*) AS record_count
FROM curated.dim_employee
GROUP BY is_current;
-- 8. Compare current staging hashes with current curated hashes.
--
-- MATCH      = business attributes unchanged
-- DIFFERENT  = business attributes differ
SELECT
    s.emp_id,
    s.stg_business_hash,
    g.tgt_business_hash,
    CASE
        WHEN g.emp_id IS NULL
            THEN 'NEW'
        WHEN s.stg_business_hash <> g.tgt_business_hash
            THEN 'DIFFERENT'
        ELSE 'MATCH'
    END AS hash_comparison
FROM staging.stg_employee AS s
LEFT JOIN curated.dim_employee AS g
    ON s.emp_id = g.emp_id
   AND g.is_current = 'Y'
ORDER BY s.emp_id;
