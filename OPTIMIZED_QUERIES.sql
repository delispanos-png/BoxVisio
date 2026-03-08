-- Optimized dashboard KPI query patterns (aggregate-only)

-- 1) Sales summary
SELECT count(id), coalesce(sum(qty), 0), coalesce(sum(net_value), 0), coalesce(sum(gross_value), 0)
FROM agg_sales_daily
WHERE doc_date BETWEEN :from_date AND :to_date
  AND (:branches_is_empty OR branch_ext_id = ANY(:branches))
  AND (:warehouses_is_empty OR warehouse_ext_id = ANY(:warehouses))
  AND (:brands_is_empty OR brand_ext_id = ANY(:brands))
  AND (:categories_is_empty OR category_ext_id = ANY(:categories))
  AND (:groups_is_empty OR group_ext_id = ANY(:groups));

-- 2) Sales by branch
SELECT
  a.branch_ext_id,
  coalesce(max(b.name), a.branch_ext_id) AS branch_name,
  coalesce(sum(a.net_value), 0) AS net_value,
  coalesce(sum(a.gross_value), 0) AS gross_value
FROM agg_sales_daily a
LEFT JOIN dim_branches b ON b.external_id = a.branch_ext_id
WHERE a.doc_date BETWEEN :from_date AND :to_date
  AND (:branches_is_empty OR a.branch_ext_id = ANY(:branches))
  AND (:warehouses_is_empty OR a.warehouse_ext_id = ANY(:warehouses))
  AND (:brands_is_empty OR a.brand_ext_id = ANY(:brands))
  AND (:categories_is_empty OR a.category_ext_id = ANY(:categories))
  AND (:groups_is_empty OR a.group_ext_id = ANY(:groups))
GROUP BY a.branch_ext_id
ORDER BY sum(a.net_value) DESC;

-- 3) Sales monthly trend (from monthly aggregate)
SELECT
  month_start,
  coalesce(sum(net_value), 0) AS net_value,
  coalesce(sum(gross_value), 0) AS gross_value,
  coalesce(sum(qty), 0) AS qty
FROM agg_sales_monthly
WHERE month_start BETWEEN :from_month AND :to_month
  AND (:branches_is_empty OR branch_ext_id = ANY(:branches))
  AND (:warehouses_is_empty OR warehouse_ext_id = ANY(:warehouses))
  AND (:brands_is_empty OR brand_ext_id = ANY(:brands))
  AND (:categories_is_empty OR category_ext_id = ANY(:categories))
  AND (:groups_is_empty OR group_ext_id = ANY(:groups))
GROUP BY month_start
ORDER BY month_start;

-- 4) Purchases summary
SELECT count(id), coalesce(sum(qty), 0), coalesce(sum(net_value), 0), coalesce(sum(cost_amount), 0)
FROM agg_purchases_daily
WHERE doc_date BETWEEN :from_date AND :to_date
  AND (:branches_is_empty OR branch_ext_id = ANY(:branches))
  AND (:warehouses_is_empty OR warehouse_ext_id = ANY(:warehouses))
  AND (:brands_is_empty OR brand_ext_id = ANY(:brands))
  AND (:categories_is_empty OR category_ext_id = ANY(:categories))
  AND (:groups_is_empty OR group_ext_id = ANY(:groups));
