
SELECT
    CASE
        WHEN hrr_adjustment_factor IS NULL THEN '4. No financial data'
        WHEN hrr_adjustment_factor >= 1.0 THEN '1. No penalty (factor >= 1.0)'
        WHEN hrr_adjustment_factor >= 0.99 THEN '2. Light penalty (0.99-1.0)'
        WHEN hrr_adjustment_factor >= 0.97 THEN '3. Moderate penalty (0.97-0.99)'
        ELSE '4. Heavy penalty (<0.97)'
    END AS penalty_band,
    COUNT(*) AS hospital_count,
    ROUND(AVG(hrr_adjustment_factor), 4) AS avg_adjustment_factor
FROM workspace.hrrp_marts.dim_providers
GROUP BY penalty_band
ORDER BY penalty_band

--The hrr_adjustment_factor is the Medicare payment multiplier under HRRP.
-- Values <1.0 mean the hospital receives reduced reimbursement.