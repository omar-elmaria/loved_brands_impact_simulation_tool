-- Step 1: Get the DF tiers of each ASA and join the vendors per ASA to that table
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.vendors_and_fees_per_asa_order_loved_brands_pairwise_simulation` AS
WITH join_vendors_and_fees AS (
  SELECT 
    a.* EXCEPT(fee, asa_name, asa_id),
    b.is_asa_clustered,
    b.vendor_count_caught_by_asa,
    b.vendor_code,
    a.fee,
    MIN(a.fee) OVER (PARTITION BY a.entity_id, a.country_code, a.master_asa_id) AS min_tt_fee_master_asa_level,
  FROM `dh-logistics-product-ops.staging.df_tiers_per_asa_loved_brands_scaled_code` a -- This table contains the DF tiers of each ASA (staging dataset because that's where the production tables are stored)
  LEFT JOIN `dh-logistics-product-ops.staging.vendor_ids_per_asa_loved_brands_scaled_code` b -- This table contains the vendor IDs per ASA (staging dataset because that's where the production tables are stored)
    ON TRUE
    AND a.entity_id = b.entity_id
    AND a.country_code = b.country_code
    AND a.master_asa_id = b.master_asa_id
),

-- Step 2: Get the active entities
entities AS (
    SELECT
        ent.region,
        p.entity_id,
        ent.country_iso,
        ent.country_name,
FROM `fulfillment-dwh-production.cl.entities` ent
LEFT JOIN UNNEST(platforms) p
INNER JOIN (SELECT DISTINCT entity_id FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`) dps ON p.entity_id = dps.entity_id 
WHERE TRUE
    AND p.entity_id NOT LIKE 'ODR%' -- Eliminate entities starting with DN_ as they are not part of DPS
    AND p.entity_id NOT LIKE 'DN_%' -- Eliminate entities starting with ODR (on-demand riders)
    AND p.entity_id NOT IN ('FP_DE', 'FP_JP') -- Eliminate JP and DE because they are not DH markets any more
    AND p.entity_id != 'TB_SA' -- Eliminate this incorrect entity_id for Saudi
    AND p.entity_id != 'HS_BH' -- Eliminate this incorrect entity_id for Bahrain
),

-- Step 3: Pull the orders data
orders_table AS (
  SELECT 
    -- Identifiers and supplementary fields     
    -- Date and time
    a.created_date AS created_date_utc,

    -- Location of order
    a.entity_id,
    a.country_code,

    -- Order/customer identifiers and session data
    a.platform_order_code,
    
    -- Vendor data and information on the delivery
    a.vendor_id,
    a.exchange_rate,

    -- Business KPIs (These are the components of profit)
    dps_travel_time_fee_local,
    a.commission_local,
    a.joker_vendor_fee_local,
    COALESCE(a.service_fee_local, 0) AS service_fee_local,
    dwh.value.mov_customer_fee_local AS sof_local_cdwh,
    IF(a.gfv_local - a.dps_minimum_order_value_local >= 0, 0, COALESCE(dwh.value.mov_customer_fee_local, (a.dps_minimum_order_value_local - a.gfv_local))) AS sof_local,
    a.delivery_costs_local,
    CASE
        WHEN ent.region IN ('Europe', 'Asia') THEN COALESCE( -- Get the delivery fee data of Pandora countries from Pandata tables
            pd.delivery_fee_local, 
            -- In 99 pct of cases, we won't need to use that fallback logic as pd.delivery_fee_local is reliable
            IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.delivery_fee_local)
        )
        -- If the order comes from a non-Pandora country, use delivery_fee_local
        WHEN ent.region NOT IN ('Europe', 'Asia') THEN (CASE WHEN a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE THEN 0 ELSE a.delivery_fee_local END)
    END AS actual_df_paid_by_customer,
    a.gmv_local
  FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` a
  LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` dwh 
    ON TRUE 
      AND a.entity_id = dwh.global_entity_id
      AND a.platform_order_code = dwh.order_id -- There is no country_code field in this table
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pd -- Contains info on the orders in Pandora countries
    ON TRUE 
      AND a.entity_id = pd.global_entity_id
      AND a.platform_order_code = pd.code 
      AND a.created_date = pd.created_date_utc -- There is no country_code field in this table
  INNER JOIN entities ent ON a.entity_id = ent.entity_id -- Get the region associated with every entity_id
  WHERE TRUE
    AND created_date BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AND LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) -- Last completed month of data
    AND delivery_status = "completed" AND a.is_sent -- Completed AND Successful order
)

-- Step 4: Join the orders data to the ASA data
SELECT 
  a.*,
  c.is_lb_lm,
  COALESCE(COUNT(DISTINCT platform_order_code), 0) AS vendor_order_count,
  COALESCE(SUM(actual_df_paid_by_customer), 0) AS df_revenue_local,
  COALESCE(SUM(actual_df_paid_by_customer / exchange_rate), 0) AS df_revenue_eur,
  COALESCE(SUM(gmv_local), 0) AS vendor_gmv_local,
  COALESCE(SUM(gmv_local / exchange_rate), 0) AS vendor_gmv_eur,
  COALESCE(SUM(actual_df_paid_by_customer + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local)), 0) AS tot_revenue_local,
  COALESCE(SUM((actual_df_paid_by_customer + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local)) / exchange_rate), 0) AS tot_revenue_eur,
  COALESCE(SUM(actual_df_paid_by_customer + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local) - delivery_costs_local), 0) AS gp_local,
  COALESCE(SUM((actual_df_paid_by_customer + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local) - delivery_costs_local) / exchange_rate), 0) AS gp_eur,
FROM join_vendors_and_fees a
LEFT JOIN orders_table b
  ON TRUE
    AND a.entity_id = b.entity_id
    AND a.country_code = b.country_code
    AND a.vendor_code = b.vendor_id
    AND a.fee = b.dps_travel_time_fee_local
LEFT JOIN `dh-logistics-product-ops.staging.final_vendor_list_all_data_temp_loved_brands_scaled_code` c -- The final containing the LBs (staging dataset because that's where the production tables are stored)
  ON TRUE
    AND a.entity_id = c.entity_id
    AND a.country_code = c.country_code
    AND a.vendor_code = c.vendor_code
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
ORDER BY a.entity_id, a.master_asa_id, a.vendor_code, a.fee;

-- Step 5: Calculate the elasticity on the ASA level

-- Step 5.1: Get the ASA level data
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.asa_level_data_loved_brands_pairwise_simulation` AS
WITH asa_level_data AS (
  SELECT 
    region,
    entity_id,
    country_code,
    master_asa_id,
    asa_common_name,
    is_asa_clustered,
    vendor_count_caught_by_asa,
    fee,
    is_lb_lm,
    AVG(min_tt_fee_master_asa_level) AS min_tt_fee_master_asa_level,
    SUM(vendor_order_count) AS order_count_vendor_cluster_tt_fee_level,
    ROUND(SUM(df_revenue_local), 2) AS df_revenue_local_vendor_cluster_tt_fee_level,
    ROUND(SUM(df_revenue_eur), 2) AS df_revenue_eur_vendor_cluster_tt_fee_level,
    ROUND(SUM(vendor_gmv_local), 2) AS vendor_gmv_local_vendor_cluster_tt_fee_level,
    ROUND(SUM(vendor_gmv_eur), 2) AS vendor_gmv_eur_vendor_cluster_tt_fee_level,
    ROUND(SUM(tot_revenue_local), 2) AS tot_revenue_local_vendor_cluster_tt_fee_level,
    ROUND(SUM(tot_revenue_eur), 2) AS tot_revenue_eur_vendor_cluster_tt_fee_level,
    ROUND(SUM(gp_local), 2) AS gp_local_vendor_cluster_tt_fee_level,
    ROUND(SUM(gp_eur), 2) AS gp_eur_vendor_cluster_tt_fee_level
  FROM `dh-logistics-product-ops.pricing.vendors_and_fees_per_asa_order_loved_brands_pairwise_simulation`
  GROUP BY 1,2,3,4,5,6,7,8,9
),

add_min_order_count AS (
  SELECT 
    a.*,
    SUM(a.order_count_vendor_cluster_tt_fee_level) OVER (PARTITION BY a.entity_id, a.master_asa_id, a.is_lb_lm) AS order_count_vendor_cluster_level,
    SUM(a.order_count_vendor_cluster_tt_fee_level) OVER (PARTITION BY a.entity_id, a.master_asa_id) AS order_count_master_asa_level,
    SUM(a.order_count_vendor_cluster_tt_fee_level) OVER (PARTITION BY a.entity_id) AS order_count_entity_level,
    ROUND(SUM(a.order_count_vendor_cluster_tt_fee_level) OVER (PARTITION BY a.entity_id, a.master_asa_id, a.is_lb_lm) / NULLIF(SUM(a.order_count_vendor_cluster_tt_fee_level) OVER (PARTITION BY a.entity_id), 0), 4) AS vendor_cluster_order_share_of_entity,
    ROUND(SUM(a.order_count_vendor_cluster_tt_fee_level) OVER (PARTITION BY a.entity_id, a.master_asa_id) / NULLIF(SUM(a.order_count_vendor_cluster_tt_fee_level) OVER (PARTITION BY a.entity_id), 0), 4) AS master_asa_order_share_of_entity,
    SUM(a.df_revenue_local_vendor_cluster_tt_fee_level) OVER (PARTITION BY a.entity_id, a.master_asa_id, a.is_lb_lm) AS df_revenue_local_vendor_cluster_level,
    SUM(a.df_revenue_eur_vendor_cluster_tt_fee_level) OVER (PARTITION BY a.entity_id, a.master_asa_id, a.is_lb_lm) AS df_revenue_eur_vendor_cluster_level,
    SUM(a.vendor_gmv_local_vendor_cluster_tt_fee_level) OVER (PARTITION BY a.entity_id, a.master_asa_id, a.is_lb_lm) AS gmv_local_vendor_cluster_level,
    SUM(a.vendor_gmv_eur_vendor_cluster_tt_fee_level) OVER (PARTITION BY a.entity_id, a.master_asa_id, a.is_lb_lm) AS gmv_eur_vendor_cluster_level,
    SUM(a.tot_revenue_local_vendor_cluster_tt_fee_level) OVER (PARTITION BY a.entity_id, a.master_asa_id, a.is_lb_lm) AS tot_revenue_local_vendor_cluster_level,
    SUM(a.tot_revenue_eur_vendor_cluster_tt_fee_level) OVER (PARTITION BY a.entity_id, a.master_asa_id, a.is_lb_lm) AS tot_revenue_eur_vendor_cluster_level,
    SUM(a.gp_local_vendor_cluster_tt_fee_level) OVER (PARTITION BY a.entity_id, a.master_asa_id, a.is_lb_lm) AS gp_local_vendor_cluster_level,
    SUM(a.gp_eur_vendor_cluster_tt_fee_level) OVER (PARTITION BY a.entity_id, a.master_asa_id, a.is_lb_lm) AS gp_eur_vendor_cluster_level,
    LAG(a.fee) OVER (PARTITION BY a.entity_id, a.master_asa_id, a.is_lb_lm ORDER BY a.fee) AS previous_fee_vendor_cluster_tt_fee_level,
    LAG(a.order_count_vendor_cluster_tt_fee_level) OVER (PARTITION BY a.entity_id, a.master_asa_id, a.is_lb_lm ORDER BY a.fee) AS previous_order_count_vendor_cluster_tt_fee_level
  FROM asa_level_data a
),

add_tier_rank AS (
    SELECT
        region,
        entity_id,
        country_code,
        master_asa_id,
        asa_common_name,
        is_asa_clustered,
        vendor_count_caught_by_asa,
        fee,
        is_lb_lm,
        ROW_NUMBER() OVER (PARTITION BY entity_id, country_code, master_asa_id, is_lb_lm ORDER BY fee) AS tier_rank_master_asa,
        * EXCEPT(region, entity_id, country_code, master_asa_id, asa_common_name, is_asa_clustered, vendor_count_caught_by_asa, fee, is_lb_lm)
    FROM add_min_order_count
),

add_num_tiers AS (
    SELECT
        region,
        entity_id,
        country_code,
        master_asa_id,
        asa_common_name,
        is_asa_clustered,
        vendor_count_caught_by_asa,
        fee,
        tier_rank_master_asa,
        is_lb_lm,
        MAX(tier_rank_master_asa) OVER (PARTITION BY entity_id, country_code, master_asa_id, is_lb_lm) AS num_tiers_master_asa,
        * EXCEPT(region, entity_id, country_code, master_asa_id, asa_common_name, is_asa_clustered, vendor_count_caught_by_asa, fee, tier_rank_master_asa, is_lb_lm)
    FROM add_tier_rank
)

SELECT
  *,
  -- Case 1: Calculating elasticity w.r.t 0 DF would produce a value of infinity
  -- Case 2: Calculating elasticity with 1 TT tier is not possible
  -- Case 3: Calculating elasticity for the first travel time tier is not possible
  -- Case 4: Calculating elasticity for a TT tier where the previous order count is 0 is not possible
  CASE 
    WHEN previous_fee_vendor_cluster_tt_fee_level = 0 OR num_tiers_master_asa = 1 OR tier_rank_master_asa = 1 OR previous_order_count_vendor_cluster_tt_fee_level = 0 THEN NULL
    ELSE (order_count_vendor_cluster_tt_fee_level / previous_order_count_vendor_cluster_tt_fee_level - 1) / (fee / previous_fee_vendor_cluster_tt_fee_level - 1)
  END AS tier_elasticity_vendor_cluster_level,

  -- Calculate the % difference between one TT fee tier and the next
  CASE 
    WHEN previous_fee_vendor_cluster_tt_fee_level = 0 OR num_tiers_master_asa = 1 OR tier_rank_master_asa = 1 THEN NULL
    ELSE (fee / previous_fee_vendor_cluster_tt_fee_level - 1)
  END AS fee_pct_diff_vendor_cluster_level,
FROM add_num_tiers
ORDER BY entity_id, master_asa_id, is_lb_lm, tier_rank_master_asa;
