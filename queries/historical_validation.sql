-- ============================================================
-- Historical Pipeline Validation
-- ============================================================
-- 1. Total curated employee versions
SELECT COUNT(*) AS total_curated_records
FROM curated.dim_employee;
-- 2. Historical initialization should initially contain
-- current employee versions.
SELECT
    is_current,
    COUNT(*) AS record_count
FROM curated.dim_employee
GROUP BY is_current
ORDER BY is_current;
-- 3. Business key must never be NULL.
SELECT COUNT(*) AS null_emp_id_count
FROM curated.dim_employee
WHERE emp_id IS NULL;
-- 4. A valid SCD2 dimension must not have more than
-- one current version for the same employee.
SELECT
    emp_id,
    COUNT(*) AS current_version_count
FROM curated.dim_employee
WHERE is_current = 'Y'
GROUP BY emp_id
HAVING COUNT(*) > 1;
-- 5. Current records should not have an effective_to value.
SELECT COUNT(*) AS invalid_current_effective_to_count
FROM curated.dim_employee
WHERE is_current = 'Y'
  AND effective_to IS NOT NULL;
-- 6. Inspect sample current records.
SELECT
    employee_sk,
    emp_id,
    emp_name,
    department_name,
    position,
    salary,
    effective_from,
    effective_to,
    is_current,
    tgt_business_hash
FROM curated.dim_employee
WHERE is_current = 'Y'
ORDER BY emp_id
LIMIT 20;
