SELECT DISTINCT fips, ssa
FROM anbc-hcb-prod.msa_share_mcr_hcb_prod.mpdr_cms_enrolled_pbpcounty
UNION
SELECT '48261' AS fips, '45732' AS ssa
UNION
SELECT '15005' AS fips, '12030' AS ssa
UNION
SELECT '48269' AS fips, '45741' AS ssa
UNION
SELECT '48301' AS fips, '45762' AS ssa;

SELECT DISTINCT CAST(SUBSTR(aep_period, 4, 4) AS INT) AS year,
                fips_cd_std AS fips,
                st_cd_src AS state,
                mkt_cd_src AS market,
                submkt_cd_src AS submarket,
                cnty_nm_src AS county,
                ma_status_desc,
                dsnp_status_desc
FROM anbc-hcb-prod.msa_share_mgap_hcb_prod.mgap_fips_lookup;

select 2025 as year,
                a.contract_plan,
                cy2025_plan_type, 
                cy2025_snp_type,
                cy2025_snp_detail,
                cy2025_dual_integration_status,
                cy2025_part_c_part_d_coverage,
                cy2025_plan_name,
                parent_name,
                case 
                when b.contract_plan is null then 'Yes'
                else 'No' end as new_plan_flag,
                cy2025_vbidufssbci_indicator,
                
                ssa_code,
                
                cy2025_overthecounter_drug_card,
                cy2025_overthecounter_drug_card_period,
                
                cy2025_vbidufssbci_group_1_additional_services_aggregate_limit,
                cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period,
                cy2025_vbidufssbci_group_1_additional_services_condition,
                cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,
                
                cy2025_vbidufssbci_group_2_additional_services_aggregate_limit,
                cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period,
                cy2025_vbidufssbci_group_2_additional_services_condition,
                cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,
                
                cy2025_vbidufssbci_group_3_additional_services_aggregate_limit,
                cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period,
                cy2025_vbidufssbci_group_3_additional_services_condition,
                cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,
                
                cy2025_vbidufssbci_group_4_additional_services_aggregate_limit,
                cy2025_vbidufssbci_group_4_additional_services_aggregate_limit_period,
                cy2025_vbidufssbci_group_4_additional_services_condition,
                cy2025_vbidufssbci_group_4_additional_services_nonmedicare_covered_benefits,
                
                cy2025_vbidufssbci_group_5_additional_services_aggregate_limit,
                cy2025_vbidufssbci_group_5_additional_services_aggregate_limit_period,
                cy2025_vbidufssbci_group_5_additional_services_condition,
                cy2025_vbidufssbci_group_5_additional_services_nonmedicare_covered_benefits
                
          from anbc-hcb-prod.msa_external_de_hcb_prod.milliman_2025_macvat_2025_benefits as a
          left outer join (select distinct contract_plan from anbc-hcb-prod.msa_external_de_hcb_prod.milliman_2024_macvat_2024_benefits_v2) as b
          on a.contract_plan=b.contract_plan
          
SELECT DISTINCT year,
                TRIM(contract_plan) AS contract_plan,
                SUBSTR(TRIM(contract_plan), 1, 5) AS contract_id,
                SUBSTR(TRIM(contract_plan), 7, 9) AS PBP,
                SUBSTR(TRIM(contract_plan), 11, 13) AS segment,
                cy2025_plan_type,
                cy2025_snp_type,
                cy2025_snp_detail,
                cy2025_dual_integration_status,
                cy2025_part_c_part_d_coverage,
                cy2025_plan_name,
                parent_name,
                new_plan_flag,
                cy2025_vbidufssbci_indicator,
                LPAD(ssa_code, 5, '0') AS ssa,
                cy2025_overthecounter_drug_card,
                cy2025_overthecounter_drug_card_period,
                cy2025_vbidufssbci_group_1_additional_services_aggregate_limit,
            cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period,
            cy2025_vbidufssbci_group_1_additional_services_condition,
            cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,
            
            cy2025_vbidufssbci_group_2_additional_services_aggregate_limit,
            cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period,
            cy2025_vbidufssbci_group_2_additional_services_condition,
            cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,
            
            cy2025_vbidufssbci_group_3_additional_services_aggregate_limit,
            cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period,
            cy2025_vbidufssbci_group_3_additional_services_condition,
            cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,
            
            cy2025_vbidufssbci_group_4_additional_services_aggregate_limit,
            cy2025_vbidufssbci_group_4_additional_services_aggregate_limit_period,
            cy2025_vbidufssbci_group_4_additional_services_condition,
            cy2025_vbidufssbci_group_4_additional_services_nonmedicare_covered_benefits,
            
            cy2025_vbidufssbci_group_5_additional_services_aggregate_limit,
            cy2025_vbidufssbci_group_5_additional_services_aggregate_limit_period,
            cy2025_vbidufssbci_group_5_additional_services_condition,
            cy2025_vbidufssbci_group_5_additional_services_nonmedicare_covered_benefits
            FROM macvat_2025_benefit;
            
SELECT m.*, f.ssa, a.state, a.county
FROM macvat_2025_benefit m
LEFT JOIN fips_ssa f ON m.ssa = f.ssa
INNER JOIN (SELECT fips, state, county
            FROM aep_footprint_history
            WHERE year = 2025) a ON m.fips = a.fips;
            
SELECT *,
       REGEXP_EXTRACT(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits, r'(Food and Produce \(.*?\))|(\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\))') AS food_and_produce_string_1,
       REGEXP_EXTRACT(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits, r'(Food and Produce \(.*?\))|(\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\))') AS food_and_produce_string_2,
       REGEXP_EXTRACT(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits, r'(Food and Produce \(.*?\))|(\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\))') AS food_and_produce_string_3,
       REGEXP_EXTRACT(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits, r'\(Healthy Food.*?\)|\(Living Expense Support.*?\)') AS healthy_food_string_1,
       REGEXP_EXTRACT(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits, r'\(Healthy Food.*?\)|\(Living Expense Support.*?\)') AS healthy_food_string_2,
       REGEXP_EXTRACT(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits, r'\(Healthy Food.*?\)|\(Living Expense Support.*?\)') AS healthy_food_string_3,
       REGEXP_EXTRACT(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits, r'General Supports for Living \(.*?\)') AS general_supports_for_living_string_1,
       REGEXP_EXTRACT(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits, r'\(Utilities\), \$[0-9]+.*') AS general_supports_for_living_string_1_alter,
       REGEXP_EXTRACT(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits, r'General Supports for Living \(.*?\)') AS general_supports_for_living_string_2,
       REGEXP_EXTRACT(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits, r'\(Utilities\), \$[0-9]+.*') AS general_supports_for_living_string_2_alter,
       REGEXP_EXTRACT(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits, r'General Supports for Living \(.*?\)') AS general_supports_for_living_string_3,
       REGEXP_EXTRACT(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits, r'\(Utilities\), \$[0-9]+.*') AS general_supports_for_living_string_3_alter,
       
       COALESCE(CAST(REGEXP_EXTRACT(food_and_produce_string_1, r'[0-9]+') AS INT), 0) *
           CASE
               WHEN LOWER(food_and_produce_string_1) LIKE '%every month%' THEN 1
               WHEN LOWER(food_and_produce_string_1) LIKE '%every three months%' THEN 1/3
               ELSE 1/12
           END AS food_and_produce_allowance_1,

       COALESCE(CAST(REGEXP_EXTRACT(food_and_produce_string_2, r'[0-9]+') AS INT), 0) *
           CASE
               WHEN LOWER(food_and_produce_string_2) LIKE '%every month%' THEN 1
               WHEN LOWER(food_and_produce_string_2) LIKE '%every three months%' THEN 1/3
               ELSE 1/12
           END AS food_and_produce_allowance_2,

       COALESCE(CAST(REGEXP_EXTRACT(food_and_produce_string_3, r'[0-9]+') AS INT), 0) *
           CASE
               WHEN LOWER(food_and_produce_string_3) LIKE '%every month%' THEN 1
               WHEN LOWER(food_and_produce_string_3) LIKE '%every three months%' THEN 1/3
               ELSE 1/12
           END AS food_and_produce_allowance_3,

       COALESCE(CAST(REGEXP_EXTRACT(healthy_food_string_1, r'[0-9]+') AS INT), 0) *
           CASE
               WHEN LOWER(healthy_food_string_1) LIKE '%every month%' THEN 1
               WHEN LOWER(healthy_food_string_1) LIKE '%every three months%' THEN 1/3
               ELSE 1/12
           END AS healthy_food_allowance_1,

       COALESCE(CAST(REGEXP_EXTRACT(healthy_food_string_2, r'[0-9]+') AS INT), 0) *
           CASE
               WHEN LOWER(healthy_food_string_2) LIKE '%every month%' THEN 1
               WHEN LOWER(healthy_food_string_2) LIKE '%every three months%' THEN 1/3
               ELSE 1/12
           END AS healthy_food_allowance_2,

       COALESCE(CAST(REGEXP_EXTRACT(healthy_food_string_3, r'[0-9]+') AS INT), 0) *
           CASE
               WHEN LOWER(healthy_food_string_3) LIKE '%every month%' THEN 1
               WHEN LOWER(healthy_food_string_3) LIKE '%every three months%' THEN 1/3
               ELSE 1/12
           END AS healthy_food_allowance_3,

       COALESCE(CAST(REGEXP_EXTRACT(general_supports_for_living_string_alter, r'[0-9]+') AS INT), 
                CAST(REGEXP_EXTRACT(general_supports_for_living_string_alter, r'[0-9]+') AS INT), 
                0) *
           CASE
               WHEN LOWER(general_supports_for_living_string_alter) LIKE '%every month%' THEN 1
               WHEN LOWER(general_supports_for_living_string_alter) LIKE '%every three months%' THEN 1/3
               ELSE 1/12
           END AS general_supports_for_living_allowance_alter;
           
SELECT year,
       contract_plan,
       contract_id,
       PBP,
       segment,
       cy2025_plan_type,
       cy2025_snp_type,
       cy2025_snp_detail,
       cy2025_dual_integration_status,
       cy2025_part_c_part_d_coverage,
       cy2025_plan_name,
       parent_name,
       new_plan_flag,
       cy2025_vbidufssbci_indicator,
       state,
       county,
       fips,
       ssa,
       cy2025_overthecounter_drug_card,
       cy2025_overthecounter_drug_card_period,
       
       -- Calculate monthly_allowance_1
       COALESCE(
           CASE
               WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period) IS NULL THEN 1
               WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period) = 'every month' THEN 1
               WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period) = 'every three months' THEN 1/3
               WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period) = 'every year' THEN 1/12
               ELSE 1
           END *
           COALESCE(
               CAST(REGEXP_REPLACE(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit, r'[\$,]', '') AS FLOAT),
               food_and_produce_allowance_1 + healthy_food_allowance_1 + general_supports_for_living_allowance_1
           ), 0
       ) AS monthly_allowance_1,

       -- Calculate food_allowance_1
       food_and_produce_allowance_1 + healthy_food_allowance_1 AS food_allowance_1,
       general_supports_for_living_allowance_1,

       -- Calculate eligibility_1
       CASE
           WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_condition) LIKE '%vbid%' THEN 'VBID'
           WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_condition) LIKE '%ssbci%' THEN 'SSBCI'
           WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_condition) LIKE '%uf%' THEN 'UF'
           ELSE ''
       END AS eligibility_1,

       -- Calculate food_and_produce_1
       CASE
           WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%13j%' OR
                LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%food and produce%' OR
                LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%healthy food%' THEN 'Yes'
           ELSE 'No'
       END AS food_and_produce_1,

       -- Calculate general_supports_for_living_1
       CASE
           WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%general supports for living%' THEN 'Yes'
           ELSE 'No'
       END AS general_supports_for_living_1,

       -- Calculate transportation_1
       CASE
           WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%10b%' OR
                LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%transportation%' THEN 'Yes'
           ELSE 'No'
       END AS transportation_1,

       -- Calculate otc_1
       CASE
           WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%13b%' OR
                LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%otc%' THEN 'Yes'
           ELSE 'No'
       END AS otc_1,

       -- Calculate meals_1
       CASE
           WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%13c%' OR
                LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%meal%' THEN 'Yes'
           ELSE 'No'
       END AS meals_1,

       -- Calculate Others_1
       CASE
           WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%16b%' OR
                LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%16c%' OR
                LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%home%' OR
                LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%pet%' OR
                LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%pest%' OR
                LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%social needs%' OR
                LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%other%' OR
                LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%indoor air%' OR
                LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%emergency%' THEN 'Yes'
           ELSE 'No'
       END AS Others_1,

       -- Calculate monthly_allowance_2
       COALESCE(
           CASE
               WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period) IS NULL THEN 1
               WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period) = 'every month' THEN 1
               WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period) = 'every three months' THEN 1/3
               WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period) = 'every year' THEN 1/12
               ELSE 1
           END *
           COALESCE(
               CAST(REGEXP_REPLACE(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit, r'[\$,]', '') AS FLOAT),
               food_and_produce_allowance_2 + healthy_food_allowance_2 + general_supports_for_living_allowance_2
           ), 0
       ) AS monthly_allowance_2,

       -- Calculate food_allowance_2
       food_and_produce_allowance_2 + healthy_food_allowance_2 AS food_allowance_2,
       general_supports_for_living_allowance_2
FROM macvat_2025_benefit_with_geo_formatted;

-- Calculate eligibility_2
CASE
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_condition) LIKE '%vbid%' THEN 'VBID'
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_condition) LIKE '%ssbci%' THEN 'SSBCI'
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_condition) LIKE '%uf%' THEN 'UF'
    ELSE ''
END AS eligibility_2,

-- Calculate food_and_produce_2
CASE
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%13j%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%food and produce%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%healthy food%' THEN 'Yes'
    ELSE 'No'
END AS food_and_produce_2,

-- Calculate general_supports_for_living_2
CASE
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%general supports for living%' THEN 'Yes'
    ELSE 'No'
END AS general_supports_for_living_2,

-- Calculate transportation_2
CASE
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%10b%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%transportation%' THEN 'Yes'
    ELSE 'No'
END AS transportation_2,

-- Calculate otc_2
CASE
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%13b%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%otc%' THEN 'Yes'
    ELSE 'No'
END AS otc_2,

-- Calculate meals_2
CASE
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%13c%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%meal%' THEN 'Yes'
    ELSE 'No'
END AS meals_2,

-- Calculate Others_2
CASE
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%16b%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%16c%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%home%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%pet%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%pest%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%social needs%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%other%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%indoor air%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%emergency%' THEN 'Yes'
    ELSE 'No'
END AS Others_2,

-- Calculate monthly_allowance_3
COALESCE(
    CASE
        WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period) IS NULL THEN 1
        WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period) = 'every month' THEN 1
        WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period) = 'every three months' THEN 1/3
        WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period) = 'every year' THEN 1/12
        ELSE 1
    END *
    COALESCE(
        CAST(REGEXP_REPLACE(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit, r'[\$,]', '') AS FLOAT),
        food_and_produce_allowance_3 + healthy_food_allowance_3 + general_supports_for_living_allowance_3
    ), 0
) AS monthly_allowance_3,

-- Calculate food_allowance_3
food_and_produce_allowance_3 + healthy_food_allowance_3 AS food_allowance_3,
general_supports_for_living_allowance_3,

-- Calculate eligibility_3
CASE
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_condition) LIKE '%vbid%' THEN 'VBID'
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_condition) LIKE '%ssbci%' THEN 'SSBCI'
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_condition) LIKE '%uf%' THEN 'UF'
    ELSE ''
END AS eligibility_3,

-- Calculate food_and_produce_3
CASE
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%13j%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%food and produce%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%healthy food%' THEN 'Yes'
    ELSE 'No'
END AS food_and_produce_3,

-- Calculate general_supports_for_living_3
CASE
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%general supports for living%' THEN 'Yes'
    ELSE 'No'
END AS general_supports_for_living_3,

-- Calculate transportation_3
CASE
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%10b%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%transportation%' THEN 'Yes'
    ELSE 'No'
END AS transportation_3,

-- Calculate otc_3
CASE
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%13b%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%otc%' THEN 'Yes'
    ELSE 'No'
END AS otc_3,

-- Calculate meals_3
CASE
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%13c%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%meal%' THEN 'Yes'
    ELSE 'No'
END AS meals_3,

-- Calculate Others_3
CASE
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%16b%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%16c%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%home%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%pet%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%pest%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%social needs%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%other%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)
ELSE 'No'
END AS Others_3,

SELECT year,
       contract_plan,
       contract_id,
       PBP,
       segment,
       cy2025_plan_type,
       cy2025_snp_type,
       cy2025_snp_detail,
       cy2025_dual_integration_status,
       cy2025_part_c_part_d_coverage,
       cy2025_plan_name,
       parent_name,
       new_plan_flag,
       cy2025_vbidufssbci_indicator,
       state,
       county,
       fips,
       ssa,
       cy2025_overthecounter_drug_card,
       cy2025_overthecounter_drug_card_period,
       
       -- Calculate monthly_allowance_primary
       CASE
           WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN monthly_allowance_1
           WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN monthly_allowance_2
           ELSE monthly_allowance_3
       END AS monthly_allowance_primary,

       -- Calculate food_and_produce_allowance_primary
       CASE
           WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN food_allowance_1
           WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN food_allowance_2
           ELSE food_allowance_3
       END AS food_and_produce_allowance_primary,

       -- Calculate general_supports_for_living_allowance_primary
       CASE
           WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN general_supports_for_living_allowance_1
           WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN general_supports_for_living_allowance_2
           ELSE general_supports_for_living_allowance_3
       END AS general_supports_for_living_allowance_primary,

       -- Calculate eligibility_primary
       CASE
           WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN eligibility_1
           WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN eligibility_2
           ELSE eligibility_3
       END AS eligibility_primary,

       -- Calculate food_and_produce_primary
       CASE
           WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN food_and_produce_1
           WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN food_and_produce_2
           ELSE food_and_produce_3
       END AS food_and_produce_primary,

       -- Calculate general_supports_for_living_primary
       CASE
           WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN general_supports_for_living_1
           WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN general_supports_for_living_2
           ELSE general_supports_for_living_3
       END AS general_supports_for_living_primary,

       -- Calculate transportation_primary
       CASE
           WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN transportation_1
           WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN transportation_2
           ELSE transportation_3
       END AS transportation_primary,

       -- Calculate otc_primary
       CASE
           WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN otc_1
           WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN otc_2
           ELSE otc_3
       END AS otc_primary,

       -- Calculate meals_primary
       CASE
           WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN meals_1
           WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN meals_2
           ELSE meals_3
       END AS meals_primary,

       -- Calculate Others_primary
       CASE
           WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN Others_1
           WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN Others_2
           ELSE Others_3
       END AS Others_primary,

       -- Calculate Conditions_primary
       CASE
           WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN cy2025_vbidufssbci_group_1_additional_services_condition
           WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN cy2025_vbidufssbci_group_2_additional_services_condition
           ELSE cy2025_vbidufssbci_group_3_additional_services_condition
       END AS Conditions_primary,

       -- Calculate monthly_allowance_secondary
       CASE
           WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN
               CASE
                   WHEN monthly_allowance_2 > monthly_allowance_3 THEN monthly_allowance_2
                   ELSE monthly_allowance_3
               END
           WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN
               CASE
                   WHEN monthly_allowance_1 > monthly_allowance_3 THEN monthly_allowance_1
                   ELSE monthly_allowance_3
               END
           ELSE
               CASE
                   WHEN monthly_allowance_1 > monthly_allowance_2 THEN monthly_allowance_1
                   ELSE monthly_allowance_2
               END
       END AS monthly_allowance_secondary,

       -- Calculate food_and_produce_allowance_secondary
       CASE
           WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN
               CASE
                   WHEN monthly_allowance_2 > monthly_allowance_3 THEN food_allowance_2
                   ELSE food_allowance_3
               END
           WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN
               CASE
                   WHEN monthly_allowance_1 > monthly_allowance_3 THEN food_allowance_1
                   ELSE food_allowance_3
               END
           ELSE
               CASE
                   WHEN monthly_allowance_1 > monthly_allowance_2 THEN food_allowance_1
                   ELSE food_allowance_2
               END
       END AS food_and_produce_allowance_secondary,

       -- Calculate general_supports_for_living_allowance_secondary
       CASE
           WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN
               CASE
                   WHEN monthly_allowance_2 > monthly_allowance_3 THEN general_supports_for_living_allowance_2
                   ELSE general_supports_for_living_allowance_3
               END
           WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN
               CASE
                   WHEN monthly_allowance_1 > monthly_allowance_3 THEN general_supports_for_living_allowance_1
                   ELSE general_supports_for_living_allowance_3
               END
           ELSE
               CASE
                   WHEN monthly_allowance_1 > monthly_allowance_2 THEN general_supports_for_living_allowance_1
                   ELSE general_supports_for_living_allowance_2
               END
       END AS general_supports_for_living_allowance_secondary

-- Calculate eligibility_secondary
CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_2 > monthly_allowance_3 THEN eligibility_2
            ELSE eligibility_3
        END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_3 THEN eligibility_1
            ELSE eligibility_3
        END
    ELSE
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_2 THEN eligibility_1
            ELSE eligibility_2
        END
END AS eligibility_secondary,

-- Calculate food_and_produce_secondary
CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_2 > monthly_allowance_3 THEN food_and_produce_2
            ELSE food_and_produce_3
        END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_3 THEN food_and_produce_1
            ELSE food_and_produce_3
        END
    ELSE
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_2 THEN food_and_produce_1
            ELSE food_and_produce_2
        END
END AS food_and_produce_secondary,

-- Calculate general_supports_for_living_secondary
CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_2 > monthly_allowance_3 THEN general_supports_for_living_2
            ELSE general_supports_for_living_3
        END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_3 THEN general_supports_for_living_1
            ELSE general_supports_for_living_3
        END
    ELSE
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_2 THEN general_supports_for_living_1
            ELSE general_supports_for_living_2
        END
END AS general_supports_for_living_secondary,

-- Calculate transportation_secondary
CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_2 > monthly_allowance_3 THEN transportation_2
            ELSE transportation_3
        END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_3 THEN transportation_1
            ELSE transportation_3
        END
    ELSE
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_2 THEN transportation_1
            ELSE transportation_2
        END
END AS transportation_secondary,

-- Calculate otc_secondary
CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_2 > monthly_allowance_3 THEN otc_2
            ELSE otc_3
        END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_3 THEN otc_1
            ELSE otc_3
        END
    ELSE
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_2 THEN otc_1
            ELSE otc_2
        END
END AS otc_secondary,

-- Calculate meals_secondary
CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_2 > monthly_allowance_3 THEN meals_2
            ELSE meals_3
        END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_3 THEN meals_1
            ELSE meals_3
        END
    ELSE
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_2 THEN meals_1
            ELSE meals_2
        END
END AS meals_secondary,

-- Calculate Others_secondary
CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_2 > monthly_allowance_3 THEN Others_2
            ELSE Others_3
        END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_3 THEN Others_1
            ELSE Others_3
        END
    ELSE
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_2 THEN Others_1
            ELSE Others_2
        END
END AS Others_secondary,

-- Calculate Conditions_secondary
CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_2 > monthly_allowance_3 THEN cy2025_vbidufssbci_group_2_additional_services_condition
            ELSE cy2025_vbidufssbci_group_3_additional_services_condition
        END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_3 THEN cy2025_vbidufssbci_group_1_additional_services_condition
            ELSE cy2025_vbidufssbci_group_3_additional_services_condition
        END
    ELSE
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_2 THEN cy2025_vbidufssbci_group_1_additional_services_condition
            ELSE cy2025_vbidufssbci_group_2_additional_services_condition
        END
END AS Conditions_secondary,

-- Calculate monthly_allowance_third
CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_2 > monthly_allowance_3 THEN monthly_allowance_3
            ELSE monthly_allowance_2
        END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_3 THEN monthly_allowance_3
            ELSE monthly_allowance_1
        END
    ELSE
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_2 THEN monthly_allowance_2
            ELSE monthly_allowance_1
        END
END AS monthly_allowance_third,

-- Calculate food_and_produce_allowance_third
CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_2 > monthly_allowance_3 THEN food_allowance_3
            ELSE food_allowance_2
        END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_3 THEN food_allowance_3
            ELSE food_allowance_1
        END
    ELSE
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_2 THEN food_allowance_2
            ELSE food_allowance_1
        END
END AS food_and_produce_allowance_third,

-- Calculate general_supports_for_living_allowance_third
CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_2 > monthly_allowance_3 THEN general_supports_for_living_allowance_3
            ELSE general_supports_for_living_allowance_2
        END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_3 THEN general_supports_for_living_allowance_3
            ELSE general_supports_for_living_allowance_1
        END
    ELSE
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_2 THEN general_supports_for_living_allowance_2
            ELSE general_supports_for_living_allowance_1
        END
END AS general_supports_for_living_allowance_third

-- Calculate eligibility_third
CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_2 > monthly_allowance_3 THEN eligibility_3
            ELSE eligibility_2
        END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_3 THEN eligibility_3
            ELSE eligibility_1
        END
    ELSE
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_2 THEN eligibility_2
            ELSE eligibility_1
        END
END AS eligibility_third,

-- Calculate food_and_produce_third
CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_2 > monthly_allowance_3 THEN food_and_produce_3
            ELSE food_and_produce_2
        END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_3 THEN food_and_produce_3
            ELSE food_and_produce_1
        END
    ELSE
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_2 THEN food_and_produce_2
            ELSE food_and_produce_1
        END
END AS food_and_produce_third,

-- Calculate general_supports_for_living_third
CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_2 > monthly_allowance_3 THEN general_supports_for_living_3
            ELSE general_supports_for_living_2
        END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_3 THEN general_supports_for_living_3
            ELSE general_supports_for_living_1
        END
    ELSE
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_2 THEN general_supports_for_living_2
            ELSE general_supports_for_living_1
        END
END AS general_supports_for_living_third,

-- Calculate transportation_third
CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_2 > monthly_allowance_3 THEN transportation_3
            ELSE transportation_2
        END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_3 THEN transportation_3
            ELSE transportation_1
        END
    ELSE
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_2 THEN transportation_2
            ELSE transportation_1
        END
END AS transportation_third,

-- Calculate otc_third
CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_2 > monthly_allowance_3 THEN otc_3
            ELSE otc_2
        END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_3 THEN otc_3
            ELSE otc_1
        END
    ELSE
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_2 THEN otc_2
            ELSE otc_1
        END
END AS otc_third,

-- Calculate meals_third
CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_2 > monthly_allowance_3 THEN meals_3
            ELSE meals_2
        END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_3 THEN meals_3
            ELSE meals_1
        END
    ELSE
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_2 THEN meals_2
            ELSE meals_1
        END
END AS meals_third,

-- Calculate Others_third
CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_2 > monthly_allowance_3 THEN Others_3
            ELSE Others_2
        END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_3 THEN Others_3
            ELSE Others_1
        END
    ELSE
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_2 THEN Others_2
            ELSE Others_1
        END
END AS Others_third,

-- Calculate Conditions_third
CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_2 > monthly_allowance_3 THEN cy2025_vbidufssbci_group_3_additional_services_condition
            ELSE cy2025_vbidufssbci_group_2_additional_services_condition
        END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_3 THEN cy2025_vbidufssbci_group_3_additional_services_condition
            ELSE cy2025_vbidufssbci_group_1_additional_services_condition
        END
    ELSE
        CASE
            WHEN monthly_allowance_1 > monthly_allowance_2 THEN cy2025_vbidufssbci_group_2_additional_services_condition
            ELSE cy2025_vbidufssbci_group_1_additional_services_condition
        END
END AS Conditions_third


SELECT year,
       contract_plan,
       contract_id,
       PBP,
       segment,
       cy2025_plan_type,
       cy2025_snp_type,
       cy2025_snp_detail,
       cy2025_dual_integration_status,
       cy2025_part_c_part_d_coverage,
       cy2025_plan_name,
       parent_name,
       new_plan_flag,
       cy2025_vbidufssbci_indicator,
       state,
       county,
       fips,
       ssa,
       -- cy2025_overthecounter_drug_card,
       -- cy2025_overthecounter_drug_card_period,
       monthly_allowance_primary,
       food_and_produce_allowance_primary,
       general_supports_for_living_allowance_primary,
       eligibility_primary,
       food_and_produce_primary,
       general_supports_for_living_primary,
       transportation_primary,
       otc_primary,
       meals_primary,
       Others_primary,
       monthly_allowance_secondary,
       food_and_produce_allowance_secondary,
       general_supports_for_living_allowance_secondary,
       eligibility_secondary,
       food_and_produce_secondary,
       general_supports_for_living_secondary,
       transportation_secondary,
       otc_secondary,
       meals_secondary,
       Others_secondary,
       -- monthly_allowance_third,
       -- food_and_produce_allowance_third,
       -- general_supports_for_living_allowance_third,
       -- eligibility_third,
       -- food_and_produce_third,
       -- general_supports_for_living_third,
       -- transportation_third,
       -- otc_third,
       -- meals_third,
       -- Others_third,
       CASE WHEN LOWER(Conditions_primary) LIKE '%anemia%' THEN 'Y' ELSE 'N' END AS anemia,
       CASE WHEN LOWER(Conditions_primary) LIKE '%asthma%' THEN 'Y' ELSE 'N' END AS asthma,
       CASE WHEN LOWER(Conditions_primary) LIKE '%autoimmune disorder%' THEN 'Y' ELSE 'N' END AS autoimmune_disorder,
       CASE WHEN LOWER(Conditions_primary) LIKE '%arthritis%' THEN 'Y' ELSE 'N' END AS arthritis,
       CASE WHEN LOWER(Conditions_primary) LIKE '%cardiovascular disorder%' THEN 'Y' ELSE 'N' END AS cardiovascular_disorder,
       CASE WHEN LOWER(Conditions_primary) LIKE '%cellulitis%' THEN 'Y' ELSE 'N' END AS cellulitis,
       CASE WHEN LOWER(Conditions_primary) LIKE '%circulatory disease%' THEN 'Y' ELSE 'N' END AS circulatory_disease,
       CASE WHEN LOWER(Conditions_primary) LIKE '%chronic alcohol and other drug dependence%' THEN 'Y' ELSE 'N' END AS chronic_alcohol_and_other_drug_dependence,
       CASE WHEN LOWER(Conditions_primary) LIKE '%chronic cognitive impairment%' THEN 'Y' ELSE 'N' END AS chronic_cognitive_impairment,
       CASE WHEN LOWER(Conditions_primary) LIKE '%chronic heart failure%' THEN 'Y' ELSE 'N' END AS chronic_heart_failure,
       CASE WHEN LOWER(Conditions_primary) LIKE '%chronic gastrointestinal disorder%' OR LOWER(Conditions_primary) LIKE '%chronic gi disorder%' THEN 'Y' ELSE 'N' END AS chronic_gastrointestinal_disorder,
       CASE WHEN LOWER(Conditions_primary) LIKE '%malnutrition%' THEN 'Y' ELSE 'N' END AS malnutrition,
       CASE WHEN LOWER(Conditions_primary) LIKE '%chronic infectious disorder%' THEN 'Y' ELSE 'N' END AS chronic_infectious_disorder,
       CASE WHEN LOWER(Conditions_primary) LIKE '%chronic otolaryngological disorder%' THEN 'Y' ELSE 'N' END AS chronic_otolaryngological_disorder,
       CASE WHEN LOWER(Conditions_primary) LIKE '%chronic pain%' THEN 'Y' ELSE 'N' END AS chronic_pain,
       CASE WHEN LOWER(Conditions_primary) LIKE '%cancer%' THEN 'Y' ELSE 'N' END AS cancer,
       CASE WHEN LOWER(Conditions_primary) LIKE '%chronic lung disorder%' THEN 'Y' ELSE 'N' END AS chronic_lung_disorder,
       CASE WHEN LOWER(Conditions_primary) LIKE '%chronic liver%' OR LOWER(Conditions_primary) LIKE '%chronic kidney/liver%' THEN 'Y' ELSE 'N' END AS chronic_liver_disease,
       CASE WHEN LOWER(Conditions_primary) LIKE '%chronic and disabling mental health condition%' THEN 'Y' ELSE 'N' END AS chronic_and_disabling_mental_health_condition,
       CASE WHEN LOWER(Conditions_primary) LIKE '%chronic kidney disease%' OR LOWER(Conditions_primary) LIKE '%chronic liver/kidney%' THEN 'Y' ELSE 'N' END AS chronic_kidney_disease,
       CASE WHEN LOWER(Conditions_primary) LIKE '%chronic non-alcohol%' THEN 'Y' ELSE 'N' END AS chronic_non_alcoholic_fatty_liver_disease,
       CASE WHEN LOWER(Conditions_primary) LIKE '%copd%' OR LOWER(Conditions_primary) LIKE '%chronic obstructive pulmonary disease%' THEN 'Y' ELSE 'N' END AS copd,
       CASE WHEN LOWER(Conditions_primary) LIKE '%congestive heart failure%' THEN 'Y' ELSE 'N' END AS congestive_heart_failure,
       CASE WHEN LOWER(Conditions_primary) LIKE '%coronary artery disease%' THEN 'Y' ELSE 'N' END AS coronary_artery_disease,
       CASE WHEN LOWER(Conditions_primary) LIKE '%cystic fibrosis%' THEN 'Y' ELSE 'N' END AS cystic_fibrosis,
       CASE WHEN LOWER(Conditions_primary) LIKE '%down syndrome%' THEN 'Y' ELSE 'N' END AS down_syndrome,
       CASE WHEN LOWER(Conditions_primary) LIKE '%diabete%' THEN 'Y' ELSE 'N' END AS diabete,
       CASE WHEN LOWER(Conditions_primary) LIKE '%dementia%' THEN 'Y' ELSE 'N' END AS dementia,
       CASE WHEN LOWER(Conditions_primary) LIKE '%dyslipidemia%' THEN 'Y' ELSE 'N' END AS dyslipidemia,
       CASE WHEN LOWER(Conditions_primary) LIKE '%eating disorder%' THEN 'Y' ELSE 'N' END AS eating_disorder,
       CASE WHEN LOWER(Conditions_primary) LIKE '%end-stage liver disease%' THEN 'Y' ELSE 'N' END AS end_stage_liver_disease,
       CASE WHEN LOWER(Conditions_primary) LIKE '%end-stage renal disease%' THEN 'Y' ELSE 'N' END AS end_stage_renal_disease,
       CASE WHEN LOWER(Conditions_primary) LIKE '%endometriosis%' THEN 'Y' ELSE 'N' END AS endometriosis,
       CASE WHEN LOWER(Conditions_primary) LIKE '%endocrine%' THEN 'Y' ELSE 'N' END AS endocrine,
       CASE WHEN LOWER(Conditions_primary) LIKE '%gastrointestinal%' THEN 'Y' ELSE 'N' END AS gastrointestinal,
       CASE WHEN LOWER(Conditions_primary) LIKE '%glaucoma%' THEN 'Y' ELSE 'N' END AS glaucoma,
       CASE WHEN LOWER(Conditions_primary) LIKE '%hiv%' THEN 'Y' ELSE 'N' END AS hiv,
       CASE WHEN LOWER(Conditions_primary) LIKE '%hepatitis%' THEN 'Y' ELSE 'N' END AS hepatitis,
       CASE WHEN LOWER(Conditions_primary) LIKE '%hypertension%' THEN 'Y' ELSE 'N' END AS hypertension,
       CASE WHEN LOWER(Conditions_primary) LIKE '%hyperlipidemia%' OR LOWER(Conditions_primary) LIKE '%chronic lipid%' THEN 'Y' ELSE 'N' END AS hyperlipidemia,
       CASE WHEN LOWER(Conditions_primary) LIKE '%hypercholesterolemia%' THEN 'Y' ELSE 'N' END AS hypercholesterolemia,
       CASE WHEN LOWER(Conditions_primary) LIKE '%inflammatory bowel disease%' THEN 'Y' ELSE 'N' END AS inflammatory_bowel_disease,
       CASE WHEN LOWER(Conditions_primary) LIKE '%joints%' THEN 'Y' ELSE 'N' END AS joints_and_spine,
       CASE WHEN LOWER(Conditions_primary) LIKE '%loss of limb%' THEN 'Y' ELSE 'N' END AS loss_of_limb,
       CASE WHEN LOWER(Conditions_primary) LIKE '%low back pain%' THEN 'Y' ELSE 'N' END AS low_back_pain,
       CASE WHEN LOWER(Conditions_primary) LIKE '%metabolic syndrome%' THEN 'Y' ELSE 'N' END AS metabolic_syndrome,
       CASE WHEN LOWER(Conditions_primary) LIKE '%muscular dystrophy%' THEN 'Y' ELSE 'N' END AS muscular_dystrophy,
       CASE WHEN LOWER(Conditions_primary) LIKE '%musculoskeletal disorder%' THEN 'Y' ELSE 'N' END AS musculoskeletal_disorder,
       CASE WHEN LOWER(Conditions_primary) LIKE '%neurologic disorder%' THEN 'Y' ELSE 'N' END AS neurologic_disorder,
       CASE WHEN LOWER(Conditions_primary) LIKE '%osteoporosis%' THEN 'Y' ELSE 'N' END AS osteoporosis,
       CASE WHEN LOWER(Conditions_primary) LIKE '%obesity%' OR LOWER(Conditions_primary) LIKE '%obese%' THEN 'Y' ELSE 'N' END AS obesity,
       CASE WHEN LOWER(Conditions_primary) LIKE '%pneumonia%' THEN 'Y' ELSE 'N' END AS pneumonia,
       CASE WHEN LOWER(Conditions_primary) LIKE '%pregnancy%' THEN 'Y' ELSE 'N' END AS pregnancy,
       CASE WHEN LOWER(Conditions_primary) LIKE '%rsd%' THEN 'Y' ELSE 'N' END AS rsd,
       CASE WHEN LOWER(Conditions_primary) LIKE '%sjogren%' THEN 'Y' ELSE 'N' END AS sjogren,
       CASE WHEN LOWER(Conditions_primary) LIKE '%severe hematologic disorder%' THEN 'Y' ELSE
       CASE WHEN LOWER(Conditions_primary) LIKE '%stroke%' THEN 'Y' ELSE 'N' END AS stroke,
CASE WHEN LOWER(Conditions_primary) LIKE '%urinary tract infection%' THEN 'Y' ELSE 'N' END AS urinary_tract_infection,
CASE WHEN LOWER(Conditions_primary) LIKE '%urinary incontinance%' THEN 'Y' ELSE 'N' END AS urinary_incontinance,
CASE WHEN LOWER(Conditions_primary) LIKE '%vascular disease%' THEN 'Y' ELSE 'N' END AS vascular_disease;


CREATE TABLE macvat_2025_benefit_with_geo_formatted_arranged_plan AS
SELECT DISTINCT
  year, contract_plan, contract_id, PBP, segment, cy2025_plan_type, cy2025_snp_type, cy2025_snp_detail,
  cy2025_dual_integration_status, cy2025_part_c_part_d_coverage, cy2025_plan_name, parent_name,
  new_plan_flag, cy2025_vbidufssbci_indicator
FROM macvat_2025_benefit_with_geo_formatted_arranged;

-- Create a table with selected columns and remove duplicates
CREATE TABLE macvat_2025_benefit_with_geo_formatted_arranged_plan_fips AS
SELECT DISTINCT
  year, contract_plan, contract_id, PBP, segment, cy2025_plan_type, cy2025_snp_type, cy2025_snp_detail,
  cy2025_dual_integration_status, cy2025_part_c_part_d_coverage, cy2025_plan_name, parent_name,
  new_plan_flag, cy2025_vbidufssbci_indicator, state, county, fips, ssa
FROM macvat_2025_benefit_with_geo_formatted_arranged;

-- Update the main table with data from the Excel file
WITH excel_data AS (
  SELECT
    TRIM(`2025 Contract - Plan - Segment`) AS contract_plan,
    CAST(REGEXP_SUBSTR(`Food/Utilities 2025`, '[0-9]+') AS NUMERIC) AS section_19_dollars
  FROM read_xlsx('UHC Plan Data Grid.xlsx')
  WHERE `2025 Contract - Plan - Segment` IS NOT NULL
)
UPDATE macvat_2025_benefit_with_geo_formatted_arranged_plan
SET
  monthly_allowance_primary = COALESCE(excel_data.section_19_dollars, monthly_allowance_primary),
  otc_primary = CASE WHEN excel_data.section_19_dollars IS NOT NULL THEN 'Yes' ELSE otc_primary END
FROM excel_data
WHERE macvat_2025_benefit_with_geo_formatted_arranged_plan.contract_plan = excel_data.contract_plan;

-- Remove the section_19_dollars column
ALTER TABLE macvat_2025_benefit_with_geo_formatted_arranged_plan
DROP COLUMN section_19_dollars;

select 2024 as year,
                a.contract_plan,
                cy2024_plan_type as cy2025_plan_type, 
                cy2024_snp_type as cy2025_snp_type,
                cy2024_snp_detail as cy2025_snp_detail,
                cy2024_dual_integration_status as cy2025_dual_integration_status,
                cy2024_part_c_part_d_coverage as cy2025_part_c_part_d_coverage,
                cy2024_plan_name as cy2025_plan_name,
                parent_name,
                case 
                when b.contract_plan is null then 'Yes'
                else 'No' end as new_plan_flag,
                cy2024_vbid_uf_ssbci_indicator as cy2025_vbidufssbci_indicator,
                
                ssa_code,
                
                cy2024_over_the_counter_drug_card as cy2025_overthecounter_drug_card,
                cy2024_over_the_counter_drug_card_period as cy2025_overthecounter_drug_card_period,
                
                cy2024_vbid_uf_ssbci_group_1_additional_services_aggregate_limit as cy2025_vbidufssbci_group_1_additional_services_aggregate_limit,
                cy2024_vbid_uf_ssbci_group_1_additional_services_aggregate_limit_period as cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period,
                cy2024_vbid_uf_ssbci_group_1_additional_services_condition as cy2025_vbidufssbci_group_1_additional_services_condition,
                cy2024_vbid_uf_ssbci_group_1_additional_services_non_medicare_covered_benefits as cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,
                
                cy2024_vbid_uf_ssbci_group_2_additional_services_aggregate_limit as cy2025_vbidufssbci_group_2_additional_services_aggregate_limit,
                cy2024_vbid_uf_ssbci_group_2_additional_services_aggregate_limit_period as cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period,
                cy2024_vbid_uf_ssbci_group_2_additional_services_condition as cy2025_vbidufssbci_group_2_additional_services_condition,
                cy2024_vbid_uf_ssbci_group_2_additional_services_non_medicare_covered_benefits as cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,
                
                cy2024_vbid_uf_ssbci_group_3_additional_services_aggregate_limit as cy2025_vbidufssbci_group_3_additional_services_aggregate_limit,
                cy2024_vbid_uf_ssbci_group_3_additional_services_aggregate_limit_period as cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period,
                cy2024_vbid_uf_ssbci_group_3_additional_services_condition as cy2025_vbidufssbci_group_3_additional_services_condition,
                cy2024_vbid_uf_ssbci_group_3_additional_services_non_medicare_covered_benefits as cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,
                
                cy2024_vbid_uf_ssbci_group_4_additional_services_aggregate_limit as cy2025_vbidufssbci_group_4_additional_services_aggregate_limit,
                cy2024_vbid_uf_ssbci_group_4_additional_services_aggregate_limit_period as cy2025_vbidufssbci_group_4_additional_services_aggregate_limit_period,
                cy2024_vbid_uf_ssbci_group_4_additional_services_condition as cy2025_vbidufssbci_group_4_additional_services_condition,
                cy2024_vbid_uf_ssbci_group_4_additional_services_non_medicare_covered_benefits as cy2025_vbidufssbci_group_4_additional_services_nonmedicare_covered_benefits,
                
                cy2024_vbid_uf_ssbci_group_5_additional_services_aggregate_limit as cy2025_vbidufssbci_group_5_additional_services_aggregate_limit,
                cy2024_vbid_uf_ssbci_group_5_additional_services_aggregate_limit_period as cy2025_vbidufssbci_group_5_additional_services_aggregate_limit_period,
                cy2024_vbid_uf_ssbci_group_5_additional_services_condition as cy2025_vbidufssbci_group_5_additional_services_condition,
                cy2024_vbid_uf_ssbci_group_5_additional_services_non_medicare_covered_benefits as cy2025_vbidufssbci_group_5_additional_services_nonmedicare_covered_benefits
                
          from anbc-hcb-prod.msa_external_de_hcb_prod.milliman_2024_macvat_2024_benefits_v2 as a
          left outer join (select distinct contract_plan from anbc-hcb-dev.growth_anlyt_hcb_dev.milliman_2023_macvat_2023_benefits_v3) as b
          on a.contract_plan=b.contract_plan
          
          -- Remove spaces from contract_plan and create a new table with the required columns
CREATE TABLE macvat_2025_benefit_with_geo_formatted_arranged_plan AS
SELECT DISTINCT
  year,
  REPLACE(contract_plan, ' ', '') AS contract_plan,
  SUBSTRING(REPLACE(contract_plan, ' ', ''), 1, 5) AS contract_id,
  SUBSTRING(REPLACE(contract_plan, ' ', ''), 7, 9) AS PBP,
  SUBSTRING(REPLACE(contract_plan, ' ', ''), 11, 13) AS segment,
  cy2025_plan_type,
  cy2025_snp_type,
  cy2025_snp_detail,
  cy2025_dual_integration_status,
  cy2025_part_c_part_d_coverage,
  cy2025_plan_name,
  parent_name,
  new_plan_flag,
  cy2025_vbidufssbci_indicator,
  LPAD(CAST(ssa_code AS VARCHAR), 5, '0') AS ssa,
  cy2025_overthecounter_drug_card,
  cy2025_overthecounter_drug_card_period,
  cy2025_vbidufssbci_group_1_additional_services_aggregate_limit,
  cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period,
  cy2025_vbidufssbci_group_1_additional_services_condition,
  cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,
  cy2025_vbidufssbci_group_2_additional_services_aggregate_limit,
  cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period,
  cy2025_vbidufssbci_group_2_additional_services_condition,
  cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,
  cy2025_vbidufssbci_group_3_additional_services_aggregate_limit,
  cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period,
  cy2025_vbidufssbci_group_3_additional_services_condition,
  cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,
  cy2025_vbidufssbci_group_4_additional_services_aggregate_limit,
  cy2025_vbidufssbci_group_4_additional_services_aggregate_limit_period,
  cy2025_vbidufssbci_group_4_additional_services_condition,
  cy2025_vbidufssbci_group_4_additional_services_nonmedicare_covered_benefits,
  cy2025_vbidufssbci_group_5_additional_services_aggregate_limit,
  cy2025_vbidufssbci_group_5_additional_services_aggregate_limit_period,
  cy2025_vbidufssbci_group_5_additional_services_condition,
  cy2025_vbidufssbci_group_5_additional_services_nonmedicare_covered_benefits
FROM macvat_2025_benefit_with_geo_formatted_arranged;

-- Join macvat_2024_benefit with fips_ssa and aep_footprint_history for the year 2025
CREATE TABLE macvat_2024_benefit_with_geo AS
SELECT DISTINCT
  b.*,
  f.state,
  f.county
FROM macvat_2024_benefit b
LEFT JOIN fips_ssa f ON b.ssa = f.ssa
INNER JOIN (
  SELECT fips, state, county
  FROM aep_footprint_history
  WHERE year = 2025
) a ON f.fips = a.fips;

WITH extracted_data AS (
  SELECT *,
    REGEXP_SUBSTR(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits, '(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))') AS food_and_produce_string_1,
    REGEXP_SUBSTR(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits, '(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))') AS food_and_produce_string_2,
    REGEXP_SUBSTR(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits, '(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))') AS food_and_produce_string_3,
    REGEXP_SUBSTR(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits, '\\(Healthy Food.*?\\)|\\(Living Expense Support.*?\\)') AS healthy_food_string_1,
    REGEXP_SUBSTR(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits, '\\(Healthy Food.*?\\)|\\(Living Expense Support.*?\\)') AS healthy_food_string_2,
    REGEXP_SUBSTR(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits, '\\(Healthy Food.*?\\)|\\(Living Expense Support.*?\\)') AS healthy_food_string_3,
    REGEXP_SUBSTR(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits, 'General Supports for Living \\(.*?\\)') AS general_supports_for_living_string_1,
    REGEXP_SUBSTR(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits, '\\(Utilities\\), \\$[0-9]+.*') AS general_supports_for_living_string_1_alter,
    REGEXP_SUBSTR(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits, 'General Supports for Living \\(.*?\\)') AS general_supports_for_living_string_2,
    REGEXP_SUBSTR(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits, '\\(Utilities\\), \\$[0-9]+.*') AS general_supports_for_living_string_2_alter,
    REGEXP_SUBSTR(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits, 'General Supports for Living \\(.*?\\)') AS general_supports_for_living_string_3,
    REGEXP_SUBSTR(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits, '\\(Utilities\\), \\$[0-9]+.*') AS general_supports_for_living_string_3_alter
  FROM macvat_2024_benefit_with_geo
),
calculated_data AS (
  SELECT *,
    CAST(REGEXP_SUBSTR(food_and_produce_string_1, '[0-9]+') AS NUMERIC) *
      CASE
        WHEN LOWER(food_and_produce_string_1) LIKE '%every month%' THEN 1
        WHEN LOWER(food_and_produce_string_1) LIKE '%every three months%' THEN 1/3
        ELSE 1/12
      END AS food_and_produce_allowance_1,
    CAST(REGEXP_SUBSTR(food_and_produce_string_2, '[0-9]+') AS NUMERIC) *
      CASE
        WHEN LOWER(food_and_produce_string_2) LIKE '%every month%' THEN 1
        WHEN LOWER(food_and_produce_string_2) LIKE '%every three months%' THEN 1/3
        ELSE 1/12
      END AS food_and_produce_allowance_2,
    CAST(REGEXP_SUBSTR(food_and_produce_string_3, '[0-9]+') AS NUMERIC) *
      CASE
        WHEN LOWER(food_and_produce_string_3) LIKE '%every month%' THEN 1
        WHEN LOWER(food_and_produce_string_3) LIKE '%every three months%' THEN 1/3
        ELSE 1/12
      END AS food_and_produce_allowance_3,
    CAST(REGEXP_SUBSTR(healthy_food_string_1, '[0-9]+') AS NUMERIC) *
      CASE
        WHEN LOWER(healthy_food_string_1) LIKE '%every month%' THEN 1
        WHEN LOWER(healthy_food_string_1) LIKE '%every three months%' THEN 1/3
        ELSE 1/12
      END AS healthy_food_allowance_1,
    CAST(REGEXP_SUBSTR(healthy_food_string_2, '[0-9]+') AS NUMERIC) *
      CASE
        WHEN LOWER(healthy_food_string_2) LIKE '%every month%' THEN 1
        WHEN LOWER(healthy_food_string_2) LIKE '%every three months%' THEN 1/3
        ELSE 1/12
      END AS healthy_food_allowance_2,
    CAST(REGEXP_SUBSTR(healthy_food_string_3, '[0-9]+') AS NUMERIC) *
      CASE
        WHEN LOWER(healthy_food_string_3) LIKE '%every month%' THEN 1
        WHEN LOWER(healthy_food_string_3) LIKE '%every three months%' THEN 1/3
        ELSE 1/12
      END AS healthy_food_allowance_3,
    COALESCE(
      CAST(REGEXP_SUBSTR(general_supports_for_living_string_1, '[0-9]+') AS NUMERIC) *
      CASE
        WHEN LOWER(general_supports_for_living_string_1) LIKE '%every month%' THEN 1
        WHEN LOWER(general_supports_for_living_string_1) LIKE '%every three months%' THEN 1/3
        ELSE 1/12
      END,
      CAST(REGEXP_SUBSTR(general_supports_for_living_string_1_alter, '[0-9]+') AS NUMERIC) *
      CASE
        WHEN LOWER(general_supports_for_living_string_1_alter) LIKE '%every month%' THEN 1
        WHEN LOWER(general_supports_for_living_string_1_alter) LIKE '%every three months%' THEN 1/3
        ELSE 1/12
      END,
      0
    ) AS general_supports_for_living_allowance_1,
    COALESCE(
      CAST(REGEXP_SUBSTR(general_supports_for_living_string_2, '[0-9]+') AS NUMERIC) *
      CASE
        WHEN LOWER(general_supports_for_living_string_2) LIKE '%every month%' THEN 1
        WHEN LOWER(general_supports_for_living_string_2) LIKE '%every three months%' THEN 1/3
        ELSE 1/12
      END,
      CAST(REGEXP_SUBSTR(general_supports_for_living_string_2_alter, '[0-9]+') AS NUMERIC) *
      CASE
        WHEN LOWER(general_supports_for_living_string_2_alter) LIKE '%every month%' THEN 1
        WHEN LOWER(general_supports_for_living_string_2_alter) LIKE '%every three months%' THEN 1/3
        ELSE 1/12
      END,
      0
    ) AS general_supports_for_living_allowance_2,
    COALESCE(
      CAST(REGEXP_SUBSTR(general_supports_for_living_string_3, '[0-9]+') AS NUMERIC) *
      CASE
        WHEN LOWER(general_supports_for_living_string_3) LIKE '%every month%' THEN 1
        WHEN LOWER(general_supports_for_living_string_3) LIKE '%every three months%' THEN 1/3
        ELSE 1/12
      END,
      CAST(REGEXP_SUBSTR(general_supports_for_living_string_3_alter, '[0-9]+') AS NUMERIC) *
      CASE
        WHEN LOWER(general_supports_for_living_string_3_alter) LIKE '%every month%' THEN 1
        WHEN LOWER(general_supports_for_living_string_3_alter) LIKE '%every three months%' THEN 1/3
        ELSE 1/12
      END,
      0
    ) AS general_supports_for_living_allowance_3
  FROM extracted_data
)
SELECT *,
  COALESCE(food_and_produce_allowance_1, 0) AS food_and_produce_allowance_1,
  COALESCE(food_and_produce_allowance_2, 0) AS food_and_produce_allowance_2,
  COALESCE(food_and_produce_allowance_3, 0) AS food_and_produce_allowance_3,
  COALESCE(healthy_food_allowance_1, 0) AS healthy_food_allowance_1,
  COALESCE(healthy_food_allowance_2, 0) AS healthy_food_allowance_2,
  COALESCE(healthy_food_allowance_3, 0) AS healthy_food_allow;
  
  
SELECT 
  year,
  contract_plan,
  contract_id,
  PBP,
  segment,
  cy2025_plan_type,
  cy2025_snp_type,
  cy2025_snp_detail,
  cy2025_dual_integration_status,
  cy2025_part_c_part_d_coverage,
  cy2025_plan_name,
  parent_name,
  new_plan_flag,
  cy2025_vbidufssbci_indicator,
  state,
  county,
  fips,
  ssa,
  cy2025_overthecounter_drug_card,
  cy2025_overthecounter_drug_card_period,
  
  COALESCE(
    CASE
      WHEN cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period IS NULL THEN 1
      WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period) = 'every month' THEN 1
      WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period) = 'every three months' THEN 1/3
      WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period) = 'every year' THEN 1/12
      ELSE 1
    END *
    COALESCE(
      CAST(REPLACE(REPLACE(REPLACE(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit, '$', ''), ',', ''), ' ', '') AS NUMERIC),
      food_and_produce_allowance_1 + healthy_food_allowance_1 + general_supports_for_living_allowance_1
    ),
    0
  ) AS monthly_allowance_1,
  
  food_and_produce_allowance_1 + healthy_food_allowance_1 AS food_allowance_1,
  general_supports_for_living_allowance_1,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_condition) LIKE '%vbid%' THEN 'VBID'
    WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_condition) LIKE '%ssbci%' THEN 'SSBCI'
    WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_condition) LIKE '%uf%' THEN 'UF'
    ELSE ''
  END AS eligibility_1,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%13j%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%food and produce%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%healthy food%' THEN 'Yes'
    ELSE 'No'
  END AS food_and_produce_1,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%general supports for living%' THEN 'Yes'
    ELSE 'No'
  END AS general_supports_for_living_1,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%10b%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%transportation%' THEN 'Yes'
    ELSE 'No'
  END AS transportation_1,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%13b%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%otc%' THEN 'Yes'
    ELSE 'No'
  END AS otc_1,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%13c%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%meal%' THEN 'Yes'
    ELSE 'No'
  END AS meals_1,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%16b%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%16c%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%home%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%pet%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%pest%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%social needs%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%other%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%indoor air%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%emergency%' THEN 'Yes'
    ELSE 'No'
  END AS Others_1
  
  CASE
      WHEN cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period IS NULL THEN 1
      WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period) = 'every month' THEN 1
      WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period) = 'every three months' THEN 1/3
      WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period) = 'every year' THEN 1/12
      ELSE 1
    END *
    COALESCE(
      CAST(REPLACE(REPLACE(REPLACE(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit, '$', ''), ',', ''), ' ', '') AS NUMERIC),
      food_and_produce_allowance_2 + healthy_food_allowance_2 + general_supports_for_living_allowance_2
    ),
    0
  ) AS monthly_allowance_2,
  
  food_and_produce_allowance_2 + healthy_food_allowance_2 AS food_allowance_2,
  general_supports_for_living_allowance_2,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_condition) LIKE '%vbid%' THEN 'VBID'
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_condition) LIKE '%ssbci%' THEN 'SSBCI'
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_condition) LIKE '%uf%' THEN 'UF'
    ELSE ''
  END AS eligibility_2,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%13j%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%food and produce%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%healthy food%' THEN 'Yes'
    ELSE 'No'
  END AS food_and_produce_2,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%general supports for living%' THEN 'Yes'
    ELSE 'No'
  END AS general_supports_for_living_2,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%10b%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%transportation%' THEN 'Yes'
    ELSE 'No'
  END AS transportation_2,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%13b%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%otc%' THEN 'Yes'
    ELSE 'No'
  END AS otc_2,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%13c%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%meal%' THEN 'Yes'
    ELSE 'No'
  END AS meals_2,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%16b%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%16c%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%home%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%pet%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%pest%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%social needs%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%other%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%indoor air%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%emergency%' THEN 'Yes'
    ELSE 'No'
  END AS Others_2
  
  CASE
      WHEN cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period IS NULL THEN 1
      WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period) = 'every month' THEN 1
      WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period) = 'every three months' THEN 1/3
      WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period) = 'every year' THEN 1/12
      ELSE 1
    END *
    COALESCE(
      CAST(REPLACE(REPLACE(REPLACE(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit, '$', ''), ',', ''), ' ', '') AS NUMERIC),
      food_and_produce_allowance_3 + healthy_food_allowance_3 + general_supports_for_living_allowance_3
    ),
    0
  ) AS monthly_allowance_3,
  
  food_and_produce_allowance_3 + healthy_food_allowance_3 AS food_allowance_3,
  general_supports_for_living_allowance_3,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_condition) LIKE '%vbid%' THEN 'VBID'
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_condition) LIKE '%ssbci%' THEN 'SSBCI'
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_condition) LIKE '%uf%' THEN 'UF'
    ELSE ''
  END AS eligibility_3,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%13j%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%food and produce%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%healthy food%' THEN 'Yes'
    ELSE 'No'
  END AS food_and_produce_3,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%general supports for living%' THEN 'Yes'
    ELSE 'No'
  END AS general_supports_for_living_3,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%10b%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%transportation%' THEN 'Yes'
    ELSE 'No'
  END AS transportation_3,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%13b%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%otc%' THEN 'Yes'
    ELSE 'No'
  END AS otc_3,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%13c%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%meal%' THEN 'Yes'
    ELSE 'No'
  END AS meals_3,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%16b%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%16c%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%home%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%pet%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%pest%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%social needs%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%other%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%indoor air%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%emergency%' THEN 'Yes'
    ELSE 'No'
  END AS Others_3,
  
  cy2025_vbidufssbci_group_1_additional_services_aggregate_limit,
  cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period,
  cy2025_vbidufssbci_group_1_additional_services_condition,
  cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,
  
  cy2025_vbidufssbci_group_2_additional_services_aggregate_limit,
  cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period,
  cy2025_vbidufssbci_group_2_additional_services_condition,
  cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,
  
  cy2025_vbidufssbci_group_3_additional_services_aggregate_limit,
  cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period,
  cy2025_vbidufssbci_group_3_additional_services_condition,
  cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits
FROM macvat_2024_benefit_with_geo;

SELECT 
  year,
  contract_plan,
  contract_id,
  PBP,
  segment,
  cy2025_plan_type,
  cy2025_snp_type,
  cy2025_snp_detail,
  cy2025_dual_integration_status,
  cy2025_part_c_part_d_coverage,
  cy2025_plan_name,
  parent_name,
  new_plan_flag,
  cy2025_vbidufssbci_indicator,
  state,
  county,
  fips,
  ssa,
  cy2025_overthecounter_drug_card,
  cy2025_overthecounter_drug_card_period,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN monthly_allowance_1
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN monthly_allowance_2
    ELSE monthly_allowance_3
  END AS monthly_allowance_primary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN food_allowance_1
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN food_allowance_2
    ELSE food_allowance_3
  END AS food_and_produce_allowance_primary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN general_supports_for_living_allowance_1
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN general_supports_for_living_allowance_2
    ELSE general_supports_for_living_allowance_3
  END AS general_supports_for_living_allowance_primary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN eligibility_1
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN eligibility_2
    ELSE eligibility_3
  END AS eligibility_primary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN food_and_produce_1
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN food_and_produce_2
    ELSE food_and_produce_3
  END AS food_and_produce_primary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN general_supports_for_living_1
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN general_supports_for_living_2
    ELSE general_supports_for_living_3
  END AS general_supports_for_living_primary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN transportation_1
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN transportation_2
    ELSE transportation_3
  END AS transportation_primary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN otc_1
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN otc_2
    ELSE otc_3
  END AS otc_primary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN meals_1
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN meals_2
    ELSE meals_3
  END AS meals_primary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN Others_1
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN Others_2
    ELSE Others_3
  END AS Others_primary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN cy2025_vbidufssbci_group_1_additional_services_condition
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN cy2025_vbidufssbci_group_2_additional_services_condition
    ELSE cy2025_vbidufssbci_group_3_additional_services_condition
  END AS Conditions_primary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN monthly_allowance_2
        ELSE monthly_allowance_3
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN monthly_allowance_1
        ELSE monthly_allowance_3
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN monthly_allowance_1
        ELSE monthly_allowance_2
      END
  END AS monthly_allowance_secondary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN food_allowance_2
        ELSE food_allowance_3
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN food_allowance_1
        ELSE food_allowance_3
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN food_allowance_1
        ELSE food_allowance_2
      END
  END AS food_and_produce_allowance_secondary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN general_supports_for_living_allowance_2
        ELSE general_supports_for_living_allowance_3
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN general_supports_for_living_allowance_1
        ELSE general_supports_for_living_allowance_3
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN general_supports_for_living_allowance_1
        ELSE general_supports_for_living_allowance_2
      END
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN eligibility_2
        ELSE eligibility_3
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN eligibility_1
        ELSE eligibility_3
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN eligibility_1
        ELSE eligibility_2
      END
  END AS eligibility_secondary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN food_and_produce_2
        ELSE food_and_produce_3
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN food_and_produce_1
        ELSE food_and_produce_3
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN food_and_produce_1
        ELSE food_and_produce_2
      END
  END AS food_and_produce_secondary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN general_supports_for_living_2
        ELSE general_supports_for_living_3
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN general_supports_for_living_1
        ELSE general_supports_for_living_3
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN general_supports_for_living_1
        ELSE general_supports_for_living_2
      END
  END AS general_supports_for_living_secondary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN transportation_2
        ELSE transportation_3
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN transportation_1
        ELSE transportation_3
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN transportation_1
        ELSE transportation_2
      END
  END AS transportation_secondary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN otc_2
        ELSE otc_3
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN otc_1
        ELSE otc_3
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN otc_1
        ELSE otc_2
      END
  END AS otc_secondary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN meals_2
        ELSE meals_3
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN meals_1
        ELSE meals_3
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN meals_1
        ELSE meals_2
      END
  END AS meals_secondary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN Others_2
        ELSE Others_3
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN Others_1
        ELSE Others_3
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN Others_1
        ELSE Others_2
      END
  END AS Others_secondary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN cy2025_vbidufssbci_group_2_additional_services_condition
        ELSE cy2025_vbidufssbci_group_3_additional_services_condition
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN cy2025_vbidufssbci_group_1_additional_services_condition
        ELSE cy2025_vbidufssbci_group_3_additional_services_condition
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN cy2025_vbidufssbci_group_1_additional_services_condition
        ELSE cy2025_vbidufssbci_group_2_additional_services_condition
      END
  END AS Conditions_secondary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN monthly_allowance_3
        ELSE monthly_allowance_2
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN monthly_allowance_3
        ELSE monthly_allowance_1
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN monthly_allowance_2
        ELSE monthly_allowance_1
      END
  END AS monthly_allowance_third,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN food_allowance_3
        ELSE food_allowance_2
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN food_allowance_3
        ELSE food_allowance_1
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN food_allowance_2
        ELSE food_allowance_1
      END
  END AS food_and_produce_allowance_third,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN general_supports_for_living_allowance_3
        ELSE general_supports_for_living_allowance_2
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN general_supports_for_living_allowance_3
        ELSE general_supports_for_living_allowance_1
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN general_supports_for_living_allowance_2
        ELSE general_supports_for_living_allowance_1
      END
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN eligibility_3
        ELSE eligibility_2
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN eligibility_3
        ELSE eligibility_1
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN eligibility_2
        ELSE eligibility_1
      END
  END AS eligibility_third,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN food_and_produce_3
        ELSE food_and_produce_2
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN food_and_produce_3
        ELSE food_and_produce_1
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN food_and_produce_2
        ELSE food_and_produce_1
      END
  END AS food_and_produce_third,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN general_supports_for_living_3
        ELSE general_supports_for_living_2
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN general_supports_for_living_3
        ELSE general_supports_for_living_1
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN general_supports_for_living_2
        ELSE general_supports_for_living_1
      END
  END AS general_supports_for_living_third,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN transportation_3
        ELSE transportation_2
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN transportation_3
        ELSE transportation_1
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN transportation_2
        ELSE transportation_1
      END
  END AS transportation_third,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN otc_3
        ELSE otc_2
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN otc_3
        ELSE otc_1
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN otc_2
        ELSE otc_1
      END
  END AS otc_third,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN meals_3
        ELSE meals_2
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN meals_3
        ELSE meals_1
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN meals_2
        ELSE meals_1
      END
  END AS meals_third,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN Others_3
        ELSE Others_2
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN Others_3
        ELSE Others_1
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN Others_2
        ELSE Others_1
      END
  END AS Others_third,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN cy2025_vbidufssbci_group_3_additional_services_condition
        ELSE cy2025_vbidufssbci_group_2_additional_services_condition
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN cy2025_vbidufssbci_group_3_additional_services_condition
        ELSE cy2025_vbidufssbci_group_1_additional_services_condition
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN cy2025_vbidufssbci_group_2_additional_services_condition
        ELSE cy2025_vbidufssbci_group_1_additional_services_condition
      END
  END AS Conditions_third
FROM macvat_2024_benefit_with_geo_formatted;

SELECT 
  year,
  contract_plan,
  contract_id,
  PBP,
  segment,
  cy2025_plan_type,
  cy2025_snp_type,
  cy2025_snp_detail,
  cy2025_dual_integration_status,
  cy2025_part_c_part_d_coverage,
  cy2025_plan_name,
  parent_name,
  new_plan_flag,
  cy2025_vbidufssbci_indicator,
  state,
  county,
  fips,
  ssa,
  monthly_allowance_primary,
  food_and_produce_allowance_primary,
  general_supports_for_living_allowance_primary,
  eligibility_primary,
  food_and_produce_primary,
  general_supports_for_living_primary,
  transportation_primary,
  otc_primary,
  meals_primary,
  Others_primary,
  monthly_allowance_secondary,
  food_and_produce_allowance_secondary,
  general_supports_for_living_allowance_secondary,
  eligibility_secondary,
  food_and_produce_secondary,
  general_supports_for_living_secondary,
  transportation_secondary,
  otc_secondary,
  meals_secondary,
  Others_secondary,
  CASE WHEN LOWER(Conditions_primary) LIKE '%anemia%' THEN 'Y' ELSE 'N' END AS anemia,
  CASE WHEN LOWER(Conditions_primary) LIKE '%asthma%' THEN 'Y' ELSE 'N' END AS asthma,
  CASE WHEN LOWER(Conditions_primary) LIKE '%autoimmune disorder%' THEN 'Y' ELSE 'N' END AS autoimmune_disorder,
  CASE WHEN LOWER(Conditions_primary) LIKE '%arthritis%' THEN 'Y' ELSE 'N' END AS arthritis,
  CASE WHEN LOWER(Conditions_primary) LIKE '%cardiovascular disorder%' THEN 'Y' ELSE 'N' END AS cardiovascular_disorder,
  CASE WHEN LOWER(Conditions_primary) LIKE '%cellulitis%' THEN 'Y' ELSE 'N' END AS cellulitis,
  CASE WHEN LOWER(Conditions_primary) LIKE '%circulatory disease%' THEN 'Y' ELSE 'N' END AS circulatory_disease,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic alcohol and other drug dependence%' THEN 'Y' ELSE 'N' END AS chronic_alcohol_and_other_drug_dependence,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic cognitive impairment%' THEN 'Y' ELSE 'N' END AS chronic_cognitive_impairment,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic heart failure%' THEN 'Y' ELSE 'N' END AS chronic_heart_failure,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic gastrointestinal disorder%' OR LOWER(Conditions_primary) LIKE '%chronic gi disorder%' THEN 'Y' ELSE 'N' END AS chronic_gastrointestinal_disorder,
  CASE WHEN LOWER(Conditions_primary) LIKE '%malnutrition%' THEN 'Y' ELSE 'N' END AS malnutrition,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic infectious disorder%' THEN 'Y' ELSE 'N' END AS chronic_infectious_disorder,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic otolaryngological disorder%' THEN 'Y' ELSE 'N' END AS chronic_otolaryngological_disorder,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic pain%' THEN 'Y' ELSE 'N' END AS chronic_pain,
  CASE WHEN LOWER(Conditions_primary) LIKE '%cancer%' THEN 'Y' ELSE 'N' END AS cancer,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic lung disorder%' THEN 'Y' ELSE 'N' END AS chronic_lung_disorder,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic liver%' OR LOWER(Conditions_primary) LIKE '%chronic kidney/liver%' THEN 'Y' ELSE 'N' END AS chronic_liver_disease,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic and disabling mental health condition%' THEN 'Y' ELSE 'N' END AS chronic_and_disabling_mental_health_condition,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic kidney disease%' OR LOWER(Conditions_primary) LIKE '%chronic liver/kidney%' THEN 'Y' ELSE 'N' END AS chronic_kidney_disease,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic non-alcohol%' THEN 'Y' ELSE 'N' END AS chronic_non_alcoholic_fatty_liver_disease,
  CASE WHEN LOWER(Conditions_primary) LIKE '%copd%' OR LOWER(Conditions_primary) LIKE '%chronic obstructive pulmonary disease%' THEN 'Y' ELSE 'N' END AS copd,
  CASE WHEN LOWER(Conditions_primary) LIKE '%congestive heart failure%' THEN 'Y' ELSE 'N' END AS congestive_heart_failure,
  CASE WHEN LOWER(Conditions_primary) LIKE '%coronary artery disease%' THEN 'Y' ELSE 'N' END AS coronary_artery_disease,
  CASE WHEN LOWER(Conditions_primary) LIKE '%cystic fibrosis%' THEN 'Y' ELSE 'N' END AS cystic_fibrosis,
  CASE WHEN LOWER(Conditions_primary) LIKE '%down syndrome%' THEN 'Y' ELSE 'N' END AS down_syndrome,
  CASE WHEN LOWER(Conditions_primary) LIKE '%diabete%' THEN 'Y' ELSE 'N' END AS diabete,
  CASE WHEN LOWER(Conditions_primary) LIKE '%dementia%' THEN 'Y' ELSE 'N' END AS dementia,
  CASE WHEN LOWER(Conditions_primary) LIKE '%dyslipidemia%' THEN 'Y' ELSE 'N' END AS dyslipidemia,
  CASE WHEN LOWER(Conditions_primary) LIKE '%eating disorder%' THEN 'Y' ELSE 'N' END AS eating_disorder,
  CASE WHEN LOWER(Conditions_primary) LIKE '%end-stage liver disease%' THEN 'Y' ELSE 'N' END AS end_stage_liver_disease,
  CASE WHEN LOWER(Conditions_primary) LIKE '%end-stage renal disease%' THEN 'Y' ELSE 'N' END AS end_stage_renal_disease,
  CASE WHEN LOWER(Conditions_primary) LIKE '%endometriosis%' THEN 'Y' ELSE 'N' END AS endometriosis,
  CASE WHEN LOWER(Conditions_primary) LIKE '%endocrine%' THEN 'Y' ELSE 'N' END AS endocrine,
  CASE WHEN LOWER(Conditions_primary) LIKE '%gastrointestinal%' THEN 'Y' ELSE 'N' END AS gastrointestinal,
  CASE WHEN LOWER(Conditions_primary) LIKE '%glaucoma%' THEN 'Y' ELSE 'N' END AS glaucoma,
  CASE WHEN LOWER(Conditions_primary) LIKE '%hiv%' THEN 'Y' ELSE 'N' END AS hiv,
  CASE WHEN LOWER(Conditions_primary) LIKE '%hepatitis%' THEN 'Y' ELSE 'N' END AS hepatitis,
  CASE WHEN LOWER(Conditions_primary) LIKE '%hypertension%' THEN 'Y' ELSE 'N' END AS hypertension,
  CASE WHEN LOWER(Conditions_primary) LIKE '%hyperlipidemia%' OR LOWER(Conditions_primary) LIKE '%chronic lipid%' THEN 'Y' ELSE 'N' END AS hyperlipidemia,
  CASE WHEN LOWER(Conditions_primary) LIKE '%hypercholesterolemia%' THEN 'Y' ELSE 'N' END AS hypercholesterolemia,
  CASE WHEN LOWER(Conditions_primary) LIKE '%inflammatory bowel disease%' THEN 'Y' ELSE 'N' END AS inflammatory_bowel_disease,
  CASE WHEN LOWER(Conditions_primary) LIKE '%joints%' THEN 'Y' ELSE 'N' END AS joints_and_spine,
  CASE WHEN LOWER(Conditions_primary) LIKE '%loss of limb%' THEN 'Y' ELSE 'N' END AS loss_of_limb,
  CASE WHEN LOWER(Conditions_primary) LIKE '%low back pain%' THEN 'Y' ELSE 'N' END AS low_back_pain,
  CASE WHEN LOWER(Conditions_primary) LIKE '%metabolic syndrome%' THEN 'Y' ELSE 'N' END AS metabolic_syndrome,
  CASE WHEN LOWER(Conditions_primary) LIKE '%muscular dystrophy%' THEN 'Y' ELSE 'N' END AS muscular_dystrophy,
  CASE WHEN LOWER(Conditions_primary) LIKE '%musculoskeletal disorder%' THEN 'Y' ELSE 'N' END AS musculoskeletal_disorder,
  CASE WHEN LOWER(Conditions_primary) LIKE '%neurologic disorder%' THEN 'Y' ELSE 'N' END AS neurologic_disorder,
  CASE WHEN LOWER(Conditions_primary) LIKE '%osteoporosis%' THEN 'Y' ELSE 'N' END AS osteoporosis,
  CASE WHEN LOWER(Conditions_primary) LIKE '%obesity%' OR LOWER(Conditions_primary) LIKE '%obese%' THEN 'Y' ELSE 'N' END AS obesity,
  CASE WHEN LOWER(Conditions_primary) LIKE '%pneumonia%' THEN 'Y' ELSE 'N' END AS pneumonia,
  CASE WHEN LOWER(Conditions_primary) LIKE '%pregnancy%' THEN 'Y' ELSE 'N' END AS pregnancy,
  CASE WHEN LOWER(Conditions_primary) LIKE '%rsd%' THEN 'Y' ELSE 'N' END AS rsd,
  CASE WHEN LOWER(Conditions_primary) LIKE '%sjogren%' THEN 'Y' ELSE 'N' END AS sjogren,
  CASE WHEN LOWER(Conditions_primary) LIKE '%severe hematologic disorder%' THEN 'Y' ELSE 'N' END AS severe_hematologic_disorder,
  CASE WHEN LOWER(Conditions_primary) LIKE '%stroke%' THEN 'Y' ELSE 'N' END AS stroke,
  CASE WHEN LOWER(Conditions_primary) LIKE '%urinary tract infection%' THEN 'Y' ELSE 'N' END AS urinary_tract_infection,
  CASE WHEN LOWER(Conditions_primary) LIKE '%urinary incontinance%' THEN 'Y' ELSE 'N' END AS urinary_incontinance,
  CASE WHEN LOWER(Conditions_primary) LIKE '%vascular disease%' THEN 'Y' ELSE 'N' END AS vascular_disease;
  

CREATE TABLE macvat_2024_benefit_with_geo_formatted_arranged_plan AS
SELECT DISTINCT
  year,
  contract_plan,
  contract_id,
  PBP,
  segment,
  cy2025_plan_type,
  cy2025_snp_type,
  cy2025_snp_detail,
  cy2025_dual_integration_status,
  cy2025_part_c_part_d_coverage,
  cy2025_plan_name,
  parent_name,
  new_plan_flag,
  cy2025_vbidufssbci_indicator,
  monthly_allowance_primary,
  food_and_produce_allowance_primary,
  general_supports_for_living_allowance_primary,
  eligibility_primary,
  food_and_produce_primary,
  general_supports_for_living_primary,
  transportation_primary,
  otc_primary,
  meals_primary,
  Others_primary,
  monthly_allowance_secondary,
  food_and_produce_allowance_secondary,
  general_supports_for_living_allowance_secondary,
  eligibility_secondary,
  food_and_produce_secondary,
  general_supports_for_living_secondary,
  transportation_secondary,
  otc_secondary,
  meals_secondary,
  Others_secondary
FROM macvat_2024_benefit_with_geo_formatted_arranged;

-- Create the second table with selected columns
CREATE TABLE macvat_2024_benefit_with_geo_formatted_arranged_plan_fips AS
SELECT DISTINCT
  year,
  contract_plan,
  contract_id,
  PBP,
  segment,
  cy2025_plan_type,
  cy2025_snp_type,
  cy2025_snp_detail,
  cy2025_dual_integration_status,
  cy2025_part_c_part_d_coverage,
  cy2025_plan_name,
  parent_name,
  new_plan_flag,
  cy2025_vbidufssbci_indicator,
  state,
  county,
  fips,
  ssa
FROM macvat_2024_benefit_with_geo_formatted_arranged;

-- Create the third table with data from 2023
CREATE TABLE macvat_2023_benefit AS
SELECT 
  2023 AS year,
  a.contract_plan,
  cy2023_plan_type AS cy2025_plan_type,
  cy2023_snp_type AS cy2025_snp_type,
  cy2023_snp_detail AS cy2025_snp_detail,
  cy2023_dual_integration_status AS cy2025_dual_integration_status,
  cy2023_part_c_part_d_coverage AS cy2025_part_c_part_d_coverage,
  cy2023_plan_name AS cy2025_plan_name,
  parent_name,
  CASE 
    WHEN b.contract_plan IS NULL THEN 'Yes'
    ELSE 'No'
  END AS new_plan_flag,
  cy2023_vbid_uf_ssbci_indicator AS cy2025_vbidufssbci_indicator,
  ssa_code,
  cy2023_over_the_counter_drug_card_in_network AS cy2025_overthecounter_drug_card,
  cy2023_over_the_counter_drug_card_period_in_network AS cy2025_overthecounter_drug_card_period,
  cy2023_vbid_uf_ssbci_group_1_additional_services_aggregate_limit_in_network AS cy2025_vbidufssbci_group_1_additional_services_aggregate_limit,
  cy2023_vbid_uf_ssbci_group_1_additional_services_aggregate_limit_period_in_network AS cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period,
  cy2023_vbid_uf_ssbci_group_1_additional_services_condition_in_network AS cy2025_vbidufssbci_group_1_additional_services_condition,
  cy2023_vbid_uf_ssbci_group_1_additional_services_non_medicare_covered_benefits_in_network AS cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,
  cy2023_vbid_uf_ssbci_group_2_additional_services_aggregate_limit_in_network AS cy2025_vbidufssbci_group_2_additional_services_aggregate_limit,
  cy2023_vbid_uf_ssbci_group_2_additional_services_aggregate_limit_period_in_network AS cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period,
  cy2023_vbid_uf_ssbci_group_2_additional_services_condition_in_network AS cy2025_vbidufssbci_group_2_additional_services_condition,
  cy2023_vbid_uf_ssbci_group_2_additional_services_non_medicare_covered_benefits_in_network AS cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,
  cy2023_vbid_uf_ssbci_group_3_additional_services_aggregate_limit_in_network AS cy2025_vbidufssbci_group_3_additional_services_aggregate_limit,
  cy2023_vbid_uf_ssbci_group_3_additional_services_aggregate_limit_period_in_network AS cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period,
  cy2023_vbid_uf_ssbci_group_3_additional_services_condition_in_network AS cy2025_vbidufssbci_group_3_additional_services_condition,
  cy2023_vbid_uf_ssbci_group_3_additional_services_non_medicare_covered_benefits_in_network AS cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,
  cy2023_vbid_uf_ssbci_group_4_additional_services_aggregate_limit_in_network AS cy2025_vbidufssbci_group_4_additional_services_aggregate_limit,
  cy2023_vbid_uf_ssbci_group_4_additional_services_aggregate_limit_period_in_network AS cy2025_vbidufssbci_group_4_additional_services_aggregate_limit_period,
  cy2023_vbid_uf_ssbci_group_4_additional_services_condition_in_network AS cy2025_vbidufssbci_group_4_additional_services_condition,
  cy2023_vbid_uf_ssbci_group_4_additional_services_non_medicare_covered_benefits_in_network AS cy2025_vbidufssbci_group_4_additional_services_nonmedicare_covered_benefits,
  cy2023_vbid_uf_ssbci_group_5_additional_services_aggregate_limit_in_network AS cy2025_vbidufssbci_group_5_additional_services_aggregate_limit,
  cy2023_vbid_uf_ssbci_group_5_additional_services_aggregate_limit_period_in_network AS cy2025_vbidufssbci_group_5_additional_services_aggregate_limit_period,
  cy2023_vbid_uf_ssbci_group_5_additional_services_condition_in_network AS cy2025_vbidufssbci_group_5_additional_services_condition,
  cy2023_vbid_uf_ssbci_group_5_additional_services_non_medicare_covered_benefits_in_network AS cy2025_vbidufssbci_group_5_additional_services_nonmedicare_covered_benefits
FROM anbc_hcb_dev.growth_anlyt_hcb_dev.milliman_2023_macvat_2023_benefits_v3 AS a
LEFT OUTER JOIN (
  SELECT DISTINCT contract_plan 
  FROM anbc_hcb_dev.growth_anlyt_hcb_dev.milliman_2022_macvat_2022_benefits_v3
) AS b
ON a.contract_plan = b.contract_plan;

CREATE TABLE macvat_2023_benefit_with_geo_formatted AS
SELECT DISTINCT
  year,
  REPLACE(contract_plan, ' ', '') AS contract_plan,
  SUBSTRING(REPLACE(contract_plan, ' ', ''), 1, 5) AS contract_id,
  SUBSTRING(REPLACE(contract_plan, ' ', ''), 7, 3) AS PBP,
  SUBSTRING(REPLACE(contract_plan, ' ', ''), 11, 3) AS segment,
  cy2025_plan_type,
  cy2025_snp_type,
  cy2025_snp_detail,
  cy2025_dual_integration_status,
  cy2025_part_c_part_d_coverage,
  cy2025_plan_name,
  parent_name,
  new_plan_flag,
  cy2025_vbidufssbci_indicator,
  LPAD(CAST(ssa_code AS VARCHAR), 5, '0') AS ssa,
  cy2025_overthecounter_drug_card,
  cy2025_overthecounter_drug_card_period,
  cy2025_vbidufssbci_group_1_additional_services_aggregate_limit,
  cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period,
  cy2025_vbidufssbci_group_1_additional_services_condition,
  cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,
  cy2025_vbidufssbci_group_2_additional_services_aggregate_limit,
  cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period,
  cy2025_vbidufssbci_group_2_additional_services_condition,
  cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,
  cy2025_vbidufssbci_group_3_additional_services_aggregate_limit,
  cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period,
  cy2025_vbidufssbci_group_3_additional_services_condition,
  cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,
  cy2025_vbidufssbci_group_4_additional_services_aggregate_limit,
  cy2025_vbidufssbci_group_4_additional_services_aggregate_limit_period,
  cy2025_vbidufssbci_group_4_additional_services_condition,
  cy2025_vbidufssbci_group_4_additional_services_nonmedicare_covered_benefits,
  cy2025_vbidufssbci_group_5_additional_services_aggregate_limit,
  cy2025_vbidufssbci_group_5_additional_services_aggregate_limit_period,
  cy2025_vbidufssbci_group_5_additional_services_condition,
  cy2025_vbidufssbci_group_5_additional_services_nonmedicare_covered_benefits
FROM macvat_2023_benefit;

-- Create the second table by joining with fips_ssa and aep_footprint_history
CREATE TABLE macvat_2023_benefit_with_geo AS
SELECT 
  a.*,
  b.fips,
  b.state,
  b.county
FROM macvat_2023_benefit_with_geo_formatted a
LEFT JOIN fips_ssa b ON a.ssa = b.ssa
INNER JOIN (
  SELECT fips, state, county
  FROM aep_footprint_history
  WHERE year = 2025
) c ON b.fips = c.fips;

SELECT *,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits, '(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))') AS food_and_produce_string_1,
  CASE 
    WHEN cy2025_snp_type = 'Dual Eligible' 
      AND parent_name = 'UnitedHealth Group, Inc.' 
      AND (
        cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits LIKE '%VBID Food Allowance and Utilities Combined Benefit%' 
        OR cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits LIKE '%VBID Food Allowance and Utilities Combined Benefit%' 
        OR cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits LIKE '%VBID Food Allowance and Utilities Combined Benefit%'
      ) 
    THEN CONCAT(cy2025_overthecounter_drug_card, ' ', cy2025_overthecounter_drug_card_period)
    ELSE REGEXP_SUBSTR(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits, '(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))')
  END AS food_and_produce_string_1,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits, '(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))') AS food_and_produce_string_2,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits, '(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))') AS food_and_produce_string_3,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits, '\\(Healthy Food.*?\\)|\\(Living Expense Support.*?\\)') AS healthy_food_string_1,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits, '\\(Healthy Food.*?\\)|\\(Living Expense Support.*?\\)') AS healthy_food_string_2,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits, '\\(Healthy Food.*?\\)|\\(Living Expense Support.*?\\)') AS healthy_food_string_3,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits, 'General Supports for Living \\(.*?\\)') AS general_supports_for_living_string_1,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits, '\\(Utilities\\), \\$[0-9]+.*') AS general_supports_for_living_string_1_alter,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits, 'General Supports for Living \\(.*?\\)') AS general_supports_for_living_string_2,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits, '\\(Utilities\\), \\$[0-9]+.*') AS general_supports_for_living_string_2_alter,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits, 'General Supports for Living \\(.*?\\)') AS general_supports_for_living_string_3,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits, '\\(Utilities\\), \\$[0-9]+.*') AS general_supports_for_living_string_3_alter,

  COALESCE(
    CAST(REGEXP_SUBSTR(food_and_produce_string_1, '[0-9]+') AS NUMERIC) *
    CASE
      WHEN LOWER(food_and_produce_string_1) LIKE '%every month%' THEN 1
      WHEN LOWER(food_and_produce_string_1) LIKE '%every three months%' THEN 1/3
      ELSE 1/12
    END,
    0
  ) AS food_and_produce_allowance_1,

  COALESCE(
    CAST(REGEXP_SUBSTR(food_and_produce_string_2, '[0-9]+') AS NUMERIC) *
    CASE
      WHEN LOWER(food_and_produce_string_2) LIKE '%every month%' THEN 1
      WHEN LOWER(food_and_produce_string_2) LIKE '%every three months%' THEN 1/3
      ELSE 1/12
    END,
    0
  ) AS food_and_produce_allowance_2,

  COALESCE(
    CAST(REGEXP_SUBSTR(food_and_produce_string_3, '[0-9]+') AS NUMERIC) *
    CASE
      WHEN LOWER(food_and_produce_string_3) LIKE '%every month%' THEN 1
      WHEN LOWER(food_and_produce_string_3) LIKE '%every three months%' THEN 1/3
      ELSE 1/12
    END,
    0
  ) AS food_and_produce_allowance_3,

  COALESCE(
    CAST(REGEXP_SUBSTR(healthy_food_string_1, '[0-9]+') AS NUMERIC) *
    CASE
      WHEN LOWER(healthy_food_string_1) LIKE '%every month%' THEN 1
      WHEN LOWER(healthy_food_string_1) LIKE '%every three months%' THEN 1/3
      ELSE 1/12
    END,
    0
  ) AS healthy_food_allowance_1,

  COALESCE(
    CAST(REGEXP_SUBSTR(healthy_food_string_2, '[0-9]+') AS NUMERIC) *
    CASE
      WHEN LOWER(healthy_food_string_2) LIKE '%every month%' THEN 1
      WHEN LOWER(healthy_food_string_2) LIKE '%every three months%' THEN 1/3
      ELSE 1/12
    END,
    0
  ) AS healthy_food_allowance_2,

  COALESCE(
    CAST(REGEXP_SUBSTR(healthy_food_string_3, '[0-9]+') AS NUMERIC) *
    CASE
      WHEN LOWER(healthy_food_string_3) LIKE '%every month%' THEN 1
      WHEN LOWER(healthy_food_string_3) LIKE '%every three months%' THEN 1/3
      ELSE 1/12
    END,
    0
  ) AS healthy_food_allowance,

COALESCE(
CAST(REGEXP_SUBSTR(general_supports_for_living_string _alter,'[0-9]+')AS NUMERIC)*CASE WHEN LOWER(general_supports_for_living _alter)'%every month%'THEN 
WHEN LOWER(general_supports_for_living _alter)'%every three months%'THEN 
ELSE 
END,general_supports_for_living _alter,general_supports_for_living _alter,general_supports_for_living _alter,general_supports_for_living _alter,general_supports_for_living _alter,general_supports_for_living _alter,general_supports_for_living _alter,general_supports_for_living _alter,general_supports_for_living _alter,general_supports_for_living _alter,general_supports_for_living _alter,general_supports_for_living _alter,general_supports_for_living _alter,general_supports_for_living _alter,general_supports_for_living _alter,general_supports_for_living _alter,general_supports_for_living _alter,general_supports_for_living _alter,general_supports_for_living _alter,general_supports_for_living _allowance_alter,'[0-9]+'))AS NUMERIC)*CASE WHEN LOWER(general supports for living string alter)'%every month%'THEN 
WHEN LOWER(general supports for living string alter)'%every three months%'THEN 
ELSE 
END,general supports for living string alter,'[0-9]+'))AS NUMERIC)*CASE WHEN LOWER(general supports for living string alter)'%every month%'THEN 
WHEN LOWER(general supports for living string alter)'%every three months%'THEN 
ELSE 
END,general supports for living string alter,'[0-9]+'))AS NUMERIC)*CASE WHEN LOWER(general supports for living string alter)'%every month%'THEN 
WHEN LOWER(general supports for living string alter)'%every three months%'THEN 
ELSE 
END,general supports for living string alter,'[0-9]+'))AS NUMERIC)*CASE WHEN LOWER(general supports for living string alter)'%every month%'THEN 
WHEN LOWER(general supports for living string alter)'%every three months'
)


SELECT 
  year,
  contract_plan,
  contract_id,
  PBP,
  segment,
  cy2025_plan_type,
  cy2025_snp_type,
  cy2025_snp_detail,
  cy2025_dual_integration_status,
  cy2025_part_c_part_d_coverage,
  cy2025_plan_name,
  parent_name,
  new_plan_flag,
  cy2025_vbidufssbci_indicator,
  state,
  county,
  fips,
  ssa,
  cy2025_overthecounter_drug_card,
  cy2025_overthecounter_drug_card_period,
  
  COALESCE(
    CASE
      WHEN cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period IS NULL THEN 1
      WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period) = 'every month' THEN 1
      WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period) = 'every three months' THEN 1/3
      WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period) = 'every year' THEN 1/12
      ELSE 1
    END *
    COALESCE(
      CAST(REPLACE(REPLACE(REPLACE(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit, '$', ''), ',', ''), ' ', '') AS NUMERIC),
      food_and_produce_allowance_1 + healthy_food_allowance_1 + general_supports_for_living_allowance_1
    ),
    0
  ) AS monthly_allowance_1,
  
  food_and_produce_allowance_1 + healthy_food_allowance_1 AS food_allowance_1,
  general_supports_for_living_allowance_1,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_condition) LIKE '%vbid%' THEN 'VBID'
    WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_condition) LIKE '%ssbci%' THEN 'SSBCI'
    WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_condition) LIKE '%uf%' THEN 'UF'
    ELSE ''
  END AS eligibility_1,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%13j%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%food and produce%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%healthy food%' THEN 'Yes'
    ELSE 'No'
  END AS food_and_produce_1,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%general supports for living%' THEN 'Yes'
    ELSE 'No'
  END AS general_supports_for_living_1,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%10b%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%transportation%' THEN 'Yes'
    ELSE 'No'
  END AS transportation_1,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%13b%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%otc%' THEN 'Yes'
    ELSE 'No'
  END AS otc_1,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%13c%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%meal%' THEN 'Yes'
    ELSE 'No'
  END AS meals_1,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%16b%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%16c%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%home%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%pet%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%pest%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%social needs%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%other%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%indoor air%' OR
         LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%emergency%' THEN 'Yes'
    ELSE 'No'
  END AS Others_1

    CASE
      WHEN cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period IS NULL THEN 1
      WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period) = 'every month' THEN 1
      WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period) = 'every three months' THEN 1/3
      WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period) = 'every year' THEN 1/12
      ELSE 1
    END *
    COALESCE(
      CAST(REPLACE(REPLACE(REPLACE(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit, '$', ''), ',', ''), ' ', '') AS NUMERIC),
      food_and_produce_allowance_2 + healthy_food_allowance_2 + general_supports_for_living_allowance_2
    ),
    0
  ) AS monthly_allowance_2,
  
  food_and_produce_allowance_2 + healthy_food_allowance_2 AS food_allowance_2,
  general_supports_for_living_allowance_2,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_condition) LIKE '%vbid%' THEN 'VBID'
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_condition) LIKE '%ssbci%' THEN 'SSBCI'
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_condition) LIKE '%uf%' THEN 'UF'
    ELSE ''
  END AS eligibility_2,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%13j%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%food and produce%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%healthy food%' THEN 'Yes'
    ELSE 'No'
  END AS food_and_produce_2,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%general supports for living%' THEN 'Yes'
    ELSE 'No'
  END AS general_supports_for_living_2,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%10b%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%transportation%' THEN 'Yes'
    ELSE 'No'
  END AS transportation_2,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%13b%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%otc%' THEN 'Yes'
    ELSE 'No'
  END AS otc_2,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%13c%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%meal%' THEN 'Yes'
    ELSE 'No'
  END AS meals_2,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%16b%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%16c%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%home%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%pet%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%pest%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%social needs%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%other%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%indoor air%' OR
         LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%emergency%' THEN 'Yes'
    ELSE 'No'
  END AS Others_2

    CASE
      WHEN cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period IS NULL THEN 1
      WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period) = 'every month' THEN 1
      WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period) = 'every three months' THEN 1/3
      WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period) = 'every year' THEN 1/12
      ELSE 1
    END *
    COALESCE(
      CAST(REPLACE(REPLACE(REPLACE(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit, '$', ''), ',', ''), ' ', '') AS NUMERIC),
      food_and_produce_allowance_3 + healthy_food_allowance_3 + general_supports_for_living_allowance_3
    ),
    0
  ) AS monthly_allowance_3,
  
  food_and_produce_allowance_3 + healthy_food_allowance_3 AS food_allowance_3,
  general_supports_for_living_allowance_3,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_condition) LIKE '%vbid%' THEN 'VBID'
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_condition) LIKE '%ssbci%' THEN 'SSBCI'
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_condition) LIKE '%uf%' THEN 'UF'
    ELSE ''
  END AS eligibility_3,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%13j%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%food and produce%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%healthy food%' THEN 'Yes'
    ELSE 'No'
  END AS food_and_produce_3,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%general supports for living%' THEN 'Yes'
    ELSE 'No'
  END AS general_supports_for_living_3,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%10b%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%transportation%' THEN 'Yes'
    ELSE 'No'
  END AS transportation_3,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%13b%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%otc%' THEN 'Yes'
    ELSE 'No'
  END AS otc_3,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%13c%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%meal%' THEN 'Yes'
    ELSE 'No'
  END AS meals_3,
  
  CASE
    WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%16b%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%16c%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%home%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%pet%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%pest%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%social needs%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%other%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%indoor air%' OR
         LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%emergency%' THEN 'Yes'
    ELSE 'No'
  END AS Others_3,
  
  cy2025_vbidufssbci_group_1_additional_services_aggregate_limit,
  cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period,
  cy2025_vbidufssbci_group_1_additional_services_condition,
  cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,
  
  cy2025_vbidufssbci_group_2_additional_services_aggregate_limit,
  cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period,
  cy2025_vbidufssbci_group_2_additional_services_condition,
  cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,
  
  cy2025_vbidufssbci_group_3_additional_services_aggregate_limit,
  cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period,
  cy2025_vbidufssbci_group_3_additional_services_condition,
  cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits
FROM macvat_2024_benefit_with_geo_formatted;


SELECT 
  year,
  contract_plan,
  contract_id,
  PBP,
  segment,
  cy2025_plan_type,
  cy2025_snp_type,
  cy2025_snp_detail,
  cy2025_dual_integration_status,
  cy2025_part_c_part_d_coverage,
  cy2025_plan_name,
  parent_name,
  new_plan_flag,
  cy2025_vbidufssbci_indicator,
  state,
  county,
  fips,
  ssa,
  cy2025_overthecounter_drug_card,
  cy2025_overthecounter_drug_card_period,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN monthly_allowance_1
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN monthly_allowance_2
    ELSE monthly_allowance_3
  END AS monthly_allowance_primary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN food_allowance_1
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN food_allowance_2
    ELSE food_allowance_3
  END AS food_and_produce_allowance_primary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN general_supports_for_living_allowance_1
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN general_supports_for_living_allowance_2
    ELSE general_supports_for_living_allowance_3
  END AS general_supports_for_living_allowance_primary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN eligibility_1
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN eligibility_2
    ELSE eligibility_3
  END AS eligibility_primary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN food_and_produce_1
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN food_and_produce_2
    ELSE food_and_produce_3
  END AS food_and_produce_primary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN general_supports_for_living_1
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN general_supports_for_living_2
    ELSE general_supports_for_living_3
  END AS general_supports_for_living_primary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN transportation_1
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN transportation_2
    ELSE transportation_3
  END AS transportation_primary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN otc_1
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN otc_2
    ELSE otc_3
  END AS otc_primary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN meals_1
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN meals_2
    ELSE meals_3
  END AS meals_primary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN Others_1
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN Others_2
    ELSE Others_3
  END AS Others_primary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN cy2025_vbidufssbci_group_1_additional_services_condition
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN cy2025_vbidufssbci_group_2_additional_services_condition
    ELSE cy2025_vbidufssbci_group_3_additional_services_condition
  END AS Conditions_primary

  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN monthly_allowance_2
        ELSE monthly_allowance_3
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN monthly_allowance_1
        ELSE monthly_allowance_3
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN monthly_allowance_1
        ELSE monthly_allowance_2
      END
  END AS monthly_allowance_secondary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN food_allowance_2
        ELSE food_allowance_3
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN food_allowance_1
        ELSE food_allowance_3
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN food_allowance_1
        ELSE food_allowance_2
      END
  END AS food_and_produce_allowance_secondary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN general_supports_for_living_allowance_2
        ELSE general_supports_for_living_allowance_3
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN general_supports_for_living_allowance_1
        ELSE general_supports_for_living_allowance_3
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN general_supports_for_living_allowance_1
        ELSE general_supports_for_living_allowance_2
      END
  END AS general_supports_for_living_allowance_secondary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN eligibility_2
        ELSE eligibility_3
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN eligibility_1
        ELSE eligibility_3
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN eligibility_1
        ELSE eligibility_2
      END
  END AS eligibility_secondary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN food_and_produce_2
        ELSE food_and_produce_3
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN food_and_produce_1
        ELSE food_and_produce_3
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN food_and_produce_1
        ELSE food_and_produce_2
      END
  END AS food_and_produce_secondary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN general_supports_for_living_2
        ELSE general_supports_for_living_3
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN general_supports_for_living_1
        ELSE general_supports_for_living_3
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN general_supports_for_living_1
        ELSE general_supports_for_living_2
      END
  END AS general_supports_for_living_secondary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN transportation_2
        ELSE transportation_3
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN transportation_1
        ELSE transportation_3
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN transportation_1
        ELSE transportation_2
      END
  END AS transportation_secondary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN otc_2
        ELSE otc_3
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN otc_1
        ELSE otc_3
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN otc_1
        ELSE otc_2
      END
  END AS otc_secondary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN meals_2
        ELSE meals_3
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN meals_1
        ELSE meals_3
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN meals_1
        ELSE meals_2
      END
  END AS meals_secondary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN Others_2
        ELSE Others_3
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN Others_1
        ELSE Others_3
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN Others_1
        ELSE Others_2
      END
  END AS Others_secondary,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN cy2025_vbidufssbci_group_2_additional_services_condition
        ELSE cy2025_vbidufssbci_group_3_additional_services_condition
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN cy2025_vbidufssbci_group_1_additional_services_condition
        ELSE cy2025_vbidufssbci_group_3_additional_services_condition
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN cy2025_vbidufssbci_group_1_additional_services_condition
        ELSE cy2025_vbidufssbci_group_2_additional_services_condition
      END
  END AS Conditions_secondary

  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN monthly_allowance_3
        ELSE monthly_allowance_2
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN monthly_allowance_3
        ELSE monthly_allowance_1
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN monthly_allowance_2
        ELSE monthly_allowance_1
      END
  END AS monthly_allowance_third,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN food_allowance_3
        ELSE food_allowance_2
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN food_allowance_3
        ELSE food_allowance_1
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN food_allowance_2
        ELSE food_allowance_1
      END
  END AS food_and_produce_allowance_third,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN general_supports_for_living_allowance_3
        ELSE general_supports_for_living_allowance_2
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN general_supports_for_living_allowance_3
        ELSE general_supports_for_living_allowance_1
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN general_supports_for_living_allowance_2
        ELSE general_supports_for_living_allowance_1
      END
  END AS general_supports_for_living_allowance_third,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN eligibility_3
        ELSE eligibility_2
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN eligibility_3
        ELSE eligibility_1
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN eligibility_2
        ELSE eligibility_1
      END
  END AS eligibility_third,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN food_and_produce_3
        ELSE food_and_produce_2
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN food_and_produce_3
        ELSE food_and_produce_1
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN food_and_produce_2
        ELSE food_and_produce_1
      END
  END AS food_and_produce_third,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN general_supports_for_living_3
        ELSE general_supports_for_living_2
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN general_supports_for_living_3
        ELSE general_supports_for_living_1
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN general_supports_for_living_2
        ELSE general_supports_for_living_1
      END
  END AS general_supports_for_living_third,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN transportation_3
        ELSE transportation_2
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN transportation_3
        ELSE transportation_1
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN transportation_2
        ELSE transportation_1
      END
  END AS transportation_third,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN otc_3
        ELSE otc_2
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN otc_3
        ELSE otc_1
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN otc_2
        ELSE otc_1
      END
  END AS otc_third,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN meals_3
        ELSE meals_2
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN meals_3
        ELSE meals_1
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN meals_2
        ELSE meals_1
      END
  END AS meals_third,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN Others_3
        ELSE Others_2
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN Others_3
        ELSE Others_1
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN Others_2
        ELSE Others_1
      END
  END AS Others_third,
  
  CASE
    WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_2 > monthly_allowance_3 THEN cy2025_vbidufssbci_group_3_additional_services_condition
        ELSE cy2025_vbidufssbci_group_2_additional_services_condition
      END
    WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_3 THEN cy2025_vbidufssbci_group_3_additional_services_condition
        ELSE cy2025_vbidufssbci_group_1_additional_services_condition
      END
    ELSE 
      CASE
        WHEN monthly_allowance_1 > monthly_allowance_2 THEN cy2025_vbidufssbci_group_2_additional_services_condition
        ELSE cy2025_vbidufssbci_group_1_additional_services_condition
      END
  END AS Conditions_third
FROM macvat_2023_benefit_with_geo_formatted_arranged;

SELECT 
  year,
  contract_plan,
  contract_id,
  PBP,
  segment,
  cy2025_plan_type,
  cy2025_snp_type,
  cy2025_snp_detail,
  cy2025_dual_integration_status,
  cy2025_part_c_part_d_coverage,
  cy2025_plan_name,
  parent_name,
  new_plan_flag,
  cy2025_vbidufssbci_indicator,
  state,
  county,
  fips,
  ssa,
  monthly_allowance_primary,
  food_and_produce_allowance_primary,
  general_supports_for_living_allowance_primary,
  eligibility_primary,
  food_and_produce_primary,
  general_supports_for_living_primary,
  transportation_primary,
  otc_primary,
  meals_primary,
  Others_primary,
  monthly_allowance_secondary,
  food_and_produce_allowance_secondary,
  general_supports_for_living_allowance_secondary,
  eligibility_secondary,
  food_and_produce_secondary,
  general_supports_for_living_secondary,
  transportation_secondary,
  otc_secondary,
  meals_secondary,
  Others_secondary,
  CASE WHEN LOWER(Conditions_primary) LIKE '%anemia%' THEN 'Y' ELSE 'N' END AS anemia,
  CASE WHEN LOWER(Conditions_primary) LIKE '%asthma%' THEN 'Y' ELSE 'N' END AS asthma,
  CASE WHEN LOWER(Conditions_primary) LIKE '%autoimmune disorder%' THEN 'Y' ELSE 'N' END AS autoimmune_disorder,
  CASE WHEN LOWER(Conditions_primary) LIKE '%arthritis%' THEN 'Y' ELSE 'N' END AS arthritis,
  CASE WHEN LOWER(Conditions_primary) LIKE '%cardiovascular disorder%' THEN 'Y' ELSE 'N' END AS cardiovascular_disorder,
  CASE WHEN LOWER(Conditions_primary) LIKE '%cellulitis%' THEN 'Y' ELSE 'N' END AS cellulitis,
  CASE WHEN LOWER(Conditions_primary) LIKE '%circulatory disease%' THEN 'Y' ELSE 'N' END AS circulatory_disease,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic alcohol and other drug dependence%' THEN 'Y' ELSE 'N' END AS chronic_alcohol_and_other_drug_dependence,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic cognitive impairment%' THEN 'Y' ELSE 'N' END AS chronic_cognitive_impairment,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic heart failure%' THEN 'Y' ELSE 'N' END AS chronic_heart_failure,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic gastrointestinal disorder%' OR LOWER(Conditions_primary) LIKE '%chronic gi disorder%' THEN 'Y' ELSE 'N' END AS chronic_gastrointestinal_disorder,
  CASE WHEN LOWER(Conditions_primary) LIKE '%malnutrition%' THEN 'Y' ELSE 'N' END AS malnutrition,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic infectious disorder%' THEN 'Y' ELSE 'N' END AS chronic_infectious_disorder,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic otolaryngological disorder%' THEN 'Y' ELSE 'N' END AS chronic_otolaryngological_disorder,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic pain%' THEN 'Y' ELSE 'N' END AS chronic_pain,
  CASE WHEN LOWER(Conditions_primary) LIKE '%cancer%' THEN 'Y' ELSE 'N' END AS cancer,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic lung disorder%' THEN 'Y' ELSE 'N' END AS chronic_lung_disorder,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic liver%' OR LOWER(Conditions_primary) LIKE '%chronic kidney/liver%' THEN 'Y' ELSE 'N' END AS chronic_liver_disease,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic and disabling mental health condition%' THEN 'Y' ELSE 'N' END AS chronic_and_disabling_mental_health_condition,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic kidney disease%' OR LOWER(Conditions_primary) LIKE '%chronic liver/kidney%' THEN 'Y' ELSE 'N' END AS chronic_kidney_disease,
  CASE WHEN LOWER(Conditions_primary) LIKE '%chronic non-alcohol%' THEN 'Y' ELSE 'N' END AS chronic_non_alcoholic_fatty_liver_disease,
  CASE WHEN LOWER(Conditions_primary) LIKE '%copd%' OR LOWER(Conditions_primary) LIKE '%chronic obstructive pulmonary disease%' THEN 'Y' ELSE 'N' END AS copd,
  CASE WHEN LOWER(Conditions_primary) LIKE '%congestive heart failure%' THEN 'Y' ELSE 'N' END AS congestive_heart_failure,
  CASE WHEN LOWER(Conditions_primary) LIKE '%coronary artery disease%' THEN 'Y' ELSE 'N' END AS coronary_artery_disease,
  CASE WHEN LOWER(Conditions_primary) LIKE '%cystic fibrosis%' THEN 'Y' ELSE 'N' END AS cystic_fibrosis,
  CASE WHEN LOWER(Conditions_primary) LIKE '%down syndrome%' THEN 'Y' ELSE 'N' END AS down_syndrome,
  CASE WHEN LOWER(Conditions_primary) LIKE '%diabete%' THEN 'Y' ELSE 'N' END AS diabete,
  CASE WHEN LOWER(Conditions_primary) LIKE '%dementia%' THEN 'Y' ELSE 'N' END AS dementia,
  CASE WHEN LOWER(Conditions_primary) LIKE '%dyslipidemia%' THEN 'Y' ELSE 'N' END AS dyslipidemia,
  CASE WHEN LOWER(Conditions_primary) LIKE '%eating disorder%' THEN 'Y' ELSE 'N' END AS eating_disorder,
  CASE WHEN LOWER(Conditions_primary) LIKE '%end-stage liver disease%' THEN 'Y' ELSE 'N' END AS end_stage_liver_disease,
  CASE WHEN LOWER(Conditions_primary) LIKE '%end-stage renal disease%' THEN 'Y' ELSE 'N' END AS end_stage_renal_disease,
  CASE WHEN LOWER(Conditions_primary) LIKE '%endometriosis%' THEN 'Y' ELSE 'N' END AS endometriosis,
  CASE WHEN LOWER(Conditions_primary) LIKE '%endocrine%' THEN 'Y' ELSE 'N' END AS endocrine,
  CASE WHEN LOWER(Conditions_primary) LIKE '%gastrointestinal%' THEN 'Y' ELSE 'N' END AS gastrointestinal,
  CASE WHEN LOWER(Conditions_primary) LIKE '%glaucoma%' THEN 'Y' ELSE 'N' END AS glaucoma,
  CASE WHEN LOWER(Conditions_primary) LIKE '%hiv%' THEN 'Y' ELSE 'N' END AS hiv,
  CASE WHEN LOWER(Conditions_primary) LIKE '%hepatitis%' THEN 'Y' ELSE 'N' END AS hepatitis,
  CASE WHEN LOWER(Conditions_primary) LIKE '%hypertension%' THEN 'Y' ELSE 'N' END AS hypertension,
  CASE WHEN LOWER(Conditions_primary) LIKE '%hyperlipidemia%' OR LOWER(Conditions_primary) LIKE '%chronic lipid%' THEN 'Y' ELSE 'N' END AS hyperlipidemia,
  CASE WHEN LOWER(Conditions_primary) LIKE '%hypercholesterolemia%' THEN 'Y' ELSE 'N' END AS hypercholesterolemia,
  CASE WHEN LOWER(Conditions_primary) LIKE '%inflammatory bowel disease%' THEN 'Y' ELSE 'N' END AS inflammatory_bowel_disease,
  CASE WHEN LOWER(Conditions_primary) LIKE '%joints%' THEN 'Y' ELSE 'N' END AS joints_and_spine,
  CASE WHEN LOWER(Conditions_primary) LIKE '%loss of limb%' THEN 'Y' ELSE 'N' END AS loss_of_limb,
  CASE WHEN LOWER(Conditions_primary) LIKE '%low back pain%' THEN 'Y' ELSE 'N' END AS low_back_pain,
  CASE WHEN LOWER(Conditions_primary) LIKE '%metabolic syndrome%' THEN 'Y' ELSE 'N' END AS metabolic_syndrome,
  CASE WHEN LOWER(Conditions_primary) LIKE '%muscular dystrophy%' THEN 'Y' ELSE 'N' END AS muscular_dystrophy,
  CASE WHEN LOWER(Conditions_primary) LIKE '%musculoskeletal disorder%' THEN 'Y' ELSE 'N' END AS musculoskeletal_disorder,
  CASE WHEN LOWER(Conditions_primary) LIKE '%neurologic disorder%' THEN 'Y' ELSE 'N' END AS neurologic_disorder,
  CASE WHEN LOWER(Conditions_primary) LIKE '%osteoporosis%' THEN 'Y' ELSE 'N' END AS osteoporosis,
  CASE WHEN LOWER(Conditions_primary) LIKE '%obesity%' OR LOWER(Conditions_primary) LIKE '%obese%' THEN 'Y' ELSE 'N' END AS obesity,
  CASE WHEN LOWER(Conditions_primary) LIKE '%pneumonia%' THEN 'Y' ELSE 'N' END AS pneumonia,
  CASE WHEN LOWER(Conditions_primary) LIKE '%pregnancy%' THEN 'Y' ELSE 'N' END AS pregnancy,
  CASE WHEN LOWER(Conditions_primary) LIKE '%rsd%' THEN 'Y' ELSE 'N' END AS rsd,
  CASE WHEN LOWER(Conditions_primary) LIKE '%sjogren%' THEN 'Y' ELSE 'N' END AS sjogren,
  CASE WHEN LOWER(Conditions_primary) LIKE '%severe hematologic disorder%' THEN 'Y' ELSE 'N' END AS severe_hematologic_disorder,
  CASE WHEN LOWER(Conditions_primary) LIKE '%stroke%' THEN 'Y' ELSE 'N' END AS stroke,
  CASE WHEN LOWER(Conditions_primary) LIKE '%urinary tract infection%' THEN 'Y' ELSE 'N' END AS urinary_tract_infection,
  CASE WHEN LOWER(Conditions_primary) LIKE '%urinary incontinance%' THEN 'Y' ELSE 'N' END AS urinary_incontinance,
  CASE WHEN LOWER(Conditions_primary) LIKE '%vascular disease%' THEN 'Y' ELSE 'N' END AS vascular_disease;
  
  CREATE TABLE macvat_2023_benefit_with_geo_formatted_arranged_plan AS
SELECT DISTINCT
  year,
  contract_plan,
  contract_id,
  PBP,
  segment,
  cy2025_plan_type,
  cy2025_snp_type,
  cy2025_snp_detail,
  cy2025_dual_integration_status,
  cy2025_part_c_part_d_coverage,
  cy2025_plan_name,
  parent_name,
  new_plan_flag,
  cy2025_vbidufssbci_indicator,
  monthly_allowance_primary,
  food_and_produce_allowance_primary,
  general_supports_for_living_allowance_primary,
  eligibility_primary,
  food_and_produce_primary,
  general_supports_for_living_primary,
  transportation_primary,
  otc_primary,
  meals_primary,
  Others_primary,
  monthly_allowance_secondary,
  food_and_produce_allowance_secondary,
  general_supports_for_living_allowance_secondary,
  eligibility_secondary,
  food_and_produce_secondary,
  general_supports_for_living_secondary,
  transportation_secondary,
  otc_secondary,
  meals_secondary,
  Others_secondary
FROM macvat_2023_benefit_with_geo_formatted_arranged;

-- Create the second table with selected columns
CREATE TABLE macvat_2023_benefit_with_geo_formatted_arranged_plan_fips AS
SELECT DISTINCT
  year,
  contract_plan,
  contract_id,
  PBP,
  segment,
  cy2025_plan_type,
  cy2025_snp_type,
  cy2025_snp_detail,
  cy2025_dual_integration_status,
  cy2025_part_c_part_d_coverage,
  cy2025_plan_name,
  parent_name,
  new_plan_flag,
  cy2025_vbidufssbci_indicator,
  state,
  county,
  fips,
  ssa
FROM macvat_2023_benefit_with_geo_formatted_arranged;

CREATE TABLE macvat_2022_benefit AS
SELECT 
  2022 AS year,
  a.contract_plan,
  cy2022_plan_type AS cy2025_plan_type,
  cy2022_snp_type AS cy2025_snp_type,
  cy2022_snp_detail AS cy2025_snp_detail,
  NULL AS cy2025_dual_integration_status,
  cy2022_part_c_part_d_coverage AS cy2025_part_c_part_d_coverage,
  cy2022_plan_name AS cy2025_plan_name,
  parent_name,
  CASE 
    WHEN b.contract_plan IS NULL THEN 'Yes'
    ELSE 'No'
  END AS new_plan_flag,
  cy2022_vbid_uf_ssbci_indicator AS cy2025_vbidufssbci_indicator,
  ssa_code,
  cy2022_over_the_counter_drug_card_in_network AS cy2025_overthecounter_drug_card,
  cy2022_over_the_counter_drug_card_period_in_network AS cy2025_overthecounter_drug_card_period,
  cy2022_vbid_uf_ssbci_group_1_additional_services_aggregate_limit_in_network AS cy2025_vbidufssbci_group_1_additional_services_aggregate_limit,
  'every year' AS cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period,
  cy2022_vbid_uf_ssbci_group_1_additional_services_condition_in_network AS cy2025_vbidufssbci_group_1_additional_services_condition,
  cy2022_vbid_uf_ssbci_group_1_additional_services_non_medicare_covered_benefits_in_network AS cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,
  cy2022_vbid_uf_ssbci_group_2_additional_services_aggregate_limit_in_network AS cy2025_vbidufssbci_group_2_additional_services_aggregate_limit,
  'every year' AS cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period,
  cy2022_vbid_uf_ssbci_group_2_additional_services_condition_in_network AS cy2025_vbidufssbci_group_2_additional_services_condition,
  cy2022_vbid_uf_ssbci_group_2_additional_services_non_medicare_covered_benefits_in_network AS cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,
  cy2022_vbid_uf_ssbci_group_3_additional_services_aggregate_limit_in_network AS cy2025_vbidufssbci_group_3_additional_services_aggregate_limit,
  'every year' AS cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period,
  cy2022_vbid_uf_ssbci_group_3_additional_services_condition_in_network AS cy2025_vbidufssbci_group_3_additional_services_condition,
  cy2022_vbid_uf_ssbci_group_3_additional_services_non_medicare_covered_benefits_in_network AS cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,
  cy2022_vbid_uf_ssbci_group_4_additional_services_aggregate_limit_in_network AS cy2025_vbidufssbci_group_4_additional_services_aggregate_limit,
  NULL AS cy2025_vbidufssbci_group_4_additional_services_aggregate_limit_period,
  cy2022_vbid_uf_ssbci_group_4_additional_services_condition_in_network AS cy2025_vbidufssbci_group_4_additional_services_condition,
  cy2022_vbid_uf_ssbci_group_4_additional_services_non_medicare_covered_benefits_in_network AS cy2025_vbidufssbci_group_4_additional_services_nonmedicare_covered_benefits,
  NULL AS cy2025_vbidufssbci_group_5_additional_services_aggregate_limit,
  NULL AS cy2025_vbidufssbci_group_5_additional_services_aggregate_limit_period,
  NULL AS cy2025_vbidufssbci_group_5_additional_services_condition,
  NULL AS cy2025_vbidufssbci_group_5_additional_services_nonmedicare_covered_benefits
FROM anbc_hcb_dev.growth_anlyt_hcb_dev.milliman_2022_macvat_2022_benefits_v3 AS a
LEFT OUTER JOIN (
  SELECT DISTINCT contract_plan 
  FROM anbc_hcb_prod.msa_share_mcr_hcb_prod.milliman_2021_macvat_2021_benefits
) AS b
ON a.contract_plan = b.contract_plan;

CREATE TABLE macvat_2022_benefit_with_geo_formatted AS
SELECT DISTINCT
  year,
  REPLACE(contract_plan, ' ', '') AS contract_plan,
  SUBSTRING(REPLACE(contract_plan, ' ', ''), 1, 5) AS contract_id,
  SUBSTRING(REPLACE(contract_plan, ' ', ''), 7, 3) AS PBP,
  SUBSTRING(REPLACE(contract_plan, ' ', ''), 11, 3) AS segment,
  cy2025_plan_type,
  cy2025_snp_type,
  cy2025_snp_detail,
  cy2025_dual_integration_status,
  cy2025_part_c_part_d_coverage,
  cy2025_plan_name,
  parent_name,
  new_plan_flag,
  cy2025_vbidufssbci_indicator,
  LPAD(CAST(ssa_code AS VARCHAR), 5, '0') AS ssa,
  cy2025_overthecounter_drug_card,
  cy2025_overthecounter_drug_card_period,
  cy2025_vbidufssbci_group_1_additional_services_aggregate_limit,
  cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period,
  cy2025_vbidufssbci_group_1_additional_services_condition,
  cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,
  cy2025_vbidufssbci_group_2_additional_services_aggregate_limit,
  cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period,
  cy2025_vbidufssbci_group_2_additional_services_condition,
  cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,
  cy2025_vbidufssbci_group_3_additional_services_aggregate_limit,
  cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period,
  cy2025_vbidufssbci_group_3_additional_services_condition,
  cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,
  cy2025_vbidufssbci_group_4_additional_services_aggregate_limit,
  cy2025_vbidufssbci_group_4_additional_services_aggregate_limit_period,
  cy2025_vbidufssbci_group_4_additional_services_condition,
  cy2025_vbidufssbci_group_4_additional_services_nonmedicare_covered_benefits,
  cy2025_vbidufssbci_group_5_additional_services_aggregate_limit,
  cy2025_vbidufssbci_group_5_additional_services_aggregate_limit_period,
  cy2025_vbidufssbci_group_5_additional_services_condition,
  cy2025_vbidufssbci_group_5_additional_services_nonmedicare_covered_benefits
FROM macvat_2022_benefit;

-- Create the second table by joining with fips_ssa and aep_footprint_history
CREATE TABLE macvat_2022_benefit_with_geo AS
SELECT 
  a.*,
  b.fips,
  b.state,
  b.county
FROM macvat_2022_benefit_with_geo_formatted a
LEFT JOIN fips_ssa b ON a.ssa = b.ssa
INNER JOIN (
  SELECT fips, state, county
  FROM aep_footprint_history
  WHERE year = 2025
) c ON b.fips = c.fips;

CREATE TABLE macvat_2022_benefit_with_geo_formatted AS
CREATE TABLE macvat_2022_benefit_with_geo_formatted AS
SELECT *,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits, '(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))') AS food_and_produce_string_1,
  CASE 
    WHEN cy2025_snp_type = 'Dual Eligible' 
      AND parent_name = 'UnitedHealth Group, Inc.' 
      AND (
        cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits LIKE '%OTC and VBID Food Allowance Combined Benefit%' 
        OR cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits LIKE '%OTC and VBID Food Allowance Combined Benefit%' 
        OR cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits LIKE '%OTC and VBID Food Allowance Combined Benefit%'
      ) 
    THEN CONCAT(cy2025_overthecounter_drug_card, ' ', cy2025_overthecounter_drug_card_period)
    ELSE REGEXP_SUBSTR(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits, '(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))')
  END AS food_and_produce_string_1,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits, '(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))') AS food_and_produce_string_2,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits, '(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))') AS food_and_produce_string_3,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits, '\\(Healthy Food.*?\\)|\\(Living Expense Support.*?\\)') AS healthy_food_string_1,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits, '\\(Healthy Food.*?\\)|\\(Living Expense Support.*?\\)') AS healthy_food_string_2,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits, '\\(Healthy Food.*?\\)|\\(Living Expense Support.*?\\)') AS healthy_food_string_3,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits, 'General Supports for Living \\(.*?\\)') AS general_supports_for_living_string_1,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits, '\\(Utilities\\), \\$[0-9]+.*') AS general_supports_for_living_string_1_alter,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits, 'General Supports for Living \\(.*?\\)') AS general_supports_for_living_string_2,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits, '\\(Utilities\\), \\$[0-9]+.*') AS general_supports_for_living_string_2_alter,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits, 'General Supports for Living \\(.*?\\)') AS general_supports_for_living_string_3,
  REGEXP_SUBSTR(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits, '\\(Utilities\\), \\$[0-9]+.*') AS general_supports_for_living_string_3_alter,

  COALESCE(
    CAST(REGEXP_SUBSTR(food_and_produce_string_1, '[0-9]+') AS NUMERIC) *
    CASE
      WHEN LOWER(food_and_produce_string_1) LIKE '%every month%' THEN 1
      WHEN LOWER(food_and_produce_string_1) LIKE '%every three months%' THEN 1/3
      ELSE 1/12
    END,
    0
  ) AS food_and_produce_allowance_1,

  COALESCE(
    CAST(REGEXP_SUBSTR(food_and_produce_string_2, '[0-9]+') AS NUMERIC) *
    CASE
      WHEN LOWER(food_and_produce_string_2) LIKE '%every month%' THEN 1
      WHEN LOWER(food_and_produce_string_2) LIKE '%every three months%' THEN 1/3
      ELSE 1/12
    END,
    0
  ) AS food_and_produce_allowance_2,

  COALESCE(
    CAST(REGEXP_SUBSTR(food_and_produce_string_3, '[0-9]+') AS NUMERIC) *
    CASE
      WHEN LOWER(food_and_produce_string_3) LIKE '%every month%' THEN 1
      WHEN LOWER(food_and_produce_string_3) LIKE '%every three months%' THEN 1/3
      ELSE 1/12
    END,
    0
  ) AS food_and_produce_allowance_3,

  COALESCE(
    CAST(REGEXP_SUBSTR(healthy_food_string_1, '[0-9]+') AS NUMERIC) *
    CASE
      WHEN LOWER(healthy_food_string_1) LIKE '%every month%' THEN 1
      WHEN LOWER(healthy_food_string_1) LIKE '%every three months%' THEN 1/3
      ELSE 1/12
    END,
    0
  ) AS healthy_food_allowance_1,

  COALESCE(
    CAST(REGEXP_SUBSTR(healthy_food_string_2, '[0-9]+') AS NUMERIC) *
    CASE
      WHEN LOWER(healthy_food_string_2) LIKE '%every month%' THEN 1
      WHEN LOWER(healthy_food_string_2) LIKE '%every three months%' THEN 1/3
      ELSE 1/12
    END,
    0
  ) AS healthy_food_allowance_2,

  COALESCE(
    CAST(REGEXP_SUBSTR(healthy_food_string_3, '[0-9]+') AS NUMERIC) *
    CASE
      WHEN LOWER(healthy_food_string_3) LIKE '%every month%' THEN 1
      WHEN LOWER(healthy_food_string_3) LIKE '%every three months%' THEN 1/3
      ELSE 1/12
    END,
    0
  ) AS healthy_food_allowance_3,

  COALESCE(
    CAST(REGEXP_SUBSTR(general_supports_for_living_string_1, '[0-9]+') AS NUMERIC) *
    CASE
      WHEN LOWER(general_supports_for_living_string_1) LIKE '%every month%' THEN 1
      WHEN LOWER(general_supports_for_living_string_1) LIKE '%every three months%' THEN 1/3
      ELSE 1/12
    END,
    CAST(REGEXP_SUBSTR(general_supports_for_living_string_1_alter, '[0-9]+') AS NUMERIC) *
    CASE
      WHEN LOWER(general_supports_for_living_string_1_alter) LIKE '%every month%' THEN 1
      WHEN LOWER(general_supports_for_living_string_1_alter) LIKE '%every three months%' THEN 1/3
      ELSE 1/12
    END,
    0
  ) AS general_supports_for_living_allowance_1,

  COALESCE(
    CAST(REGEXP_SUBSTR(general_supports_for_living_string_2, '[0-9]+') AS NUMERIC) *
    CASE
      WHEN LOWER(general_supports_for_living_string_2) LIKE '%every month%' THEN 1
      WHEN LOWER(general_supports_for_living_string_2) LIKE '%every three months%' THEN 1/3
      ELSE 1/12
    END,
    CAST(REGEXP_SUBSTR(general_supports_for_living_string_2_alter, '[0-9]+') AS NUMERIC) *
    CASE
      WHEN LOWER(general_supports_for_living_string_2_alter) LIKE '%every month%' THEN 1
      WHEN LOWER(general_supports_for_living_string_2_alter) LIKE '%every three months%' THEN 1/3
      ELSE 1/12
    END,
    0
  ) AS general_supports_for_living_allowance_2,

  COALESCE(
    CAST(REGEXP_SUBSTR(general_supports_for_living_string_3, '[0-9]+') AS NUMERIC) *
    CASE
    WHEN LOWER(general_supports_for_living_string_3) LIKE '%every month%' THEN 
       WHEN LOWER(general_supports_for_living_string_alter)'%every three months' THEN 
       ELSE 
     END,general supports for living string alter,'[0-9]+'))AS NUMERIC)*CASE WHEN LOWER(general supports for living string alter)'%every month%'THEN 
     WHEN LOWER(general supports for living string alter)'%every three months' THEN 
     ELSE 
     END,general supports for living string alter,'[0-9]+'))AS NUMERIC)*CASE WHEN LOWER(general supports for living string alter)'%every month%'THEN 
     WHEN LOWER(general supports for living string alter)'%every three months' THEN 
     ELSE 
     END,general supports for living string alter,'[0-9]+'))AS NUMERIC)*CASE WHEN LOWER(general supports for living string alter)'%every month%'THEN 
     WHEN LOWER(general supports for living string alter)'%every three months' THEN 
     ELSE 
     END,general supports for living string alter,'[0-9]+'))AS NUMERIC)*CASE WHEN LOWER(general supports for living string alter)'%every month%'THEN 
     WHEN LOWER(general supports for living string alter)'%every three months'

COALESCE(
CAST(REGEXP_SUBSTR(food_and_produce_allowance,'[0-9]+')AS NUMERIC)*CASE WHEN LOWER(food_and_produce_allowance)'%every month%'THEN 

COALESCE(
CAST(REGEXP_SUBSTR(food_and_produce_allowance,'[0-9]+')AS NUMERIC)*CASE WHEN LOWER(food_and_produce_allowance)'%every month%'THEN 

COALESCE(
CAST(REGEXP_SUBSTR(food_and_produce_allowance,'[0-9]+')AS NUMERIC)*CASE WHEN LOWER(food_and_produce_allowance)'%every month%'THEN 

COALESCE(
CAST(REGEXP_SUBSTR(food_and_produce_allowance,'[0-9]+')AS NUMERIC)*CASE WHEN LOWER(food_and_produce_allowance)'%every month%'THEN 

COALESCE(
CAST(REGEXP_SUBSTR(food_and_produce_allowance,'[0-9]+')AS NUMERIC)*CASE WHEN LOWER(food_and_produce_allowance)'%every month%'THEN 

COALESCE(
CAST(REGEXP_SUBSTR(food_and_produce_allowance,'[0-9]+')AS NUMERIC)*CASE WHEN LOWER(food_and_produce_allowance)'%every month%'THEN 

COALESCE(
CAST(REGEXP_SUBSTR(food_and_produce_allowance,'[0-9]+')AS NUMERIC)*CASE WHEN LOWER(food_and_produce_allowance)'%every month%'THEN 

COALESCE(
CAST(REGEXP_SUBSTR(food_and_produce_allowance,'[0-9]+')AS NUMERIC)*CASE WHEN LOWER(food_and_produce_allowance)'%every month%'


SELECT 
    year,
    contract_plan,
    contract_id,
    PBP,
    segment,
    cy2025_plan_type,
    cy2025_snp_type,
    cy2025_snp_detail,
    cy2025_dual_integration_status,
    cy2025_part_c_part_d_coverage,
    cy2025_plan_name,
    parent_name,
    new_plan_flag,
    cy2025_vbidufssbci_indicator,
    state,
    county,
    fips,
    ssa,
    cy2025_overthecounter_drug_card,
    cy2025_overthecounter_drug_card_period,
    
    CASE 
        WHEN cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period IS NULL THEN 1
        WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period) = 'every month' THEN 1
        WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period) = 'every three months' THEN 1/3
        WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period) = 'every year' THEN 1/12
        ELSE 1
    END * 
    CASE 
        WHEN cy2025_vbidufssbci_group_1_additional_services_aggregate_limit IS NULL THEN food_and_produce_allowance_1 + healthy_food_allowance_1 + general_supports_for_living_allowance_1
        ELSE CAST(REPLACE(REPLACE(REPLACE(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit, '$', ''), ',', ''), ' ', '') AS NUMERIC)
    END AS monthly_allowance_1,
    
    food_and_produce_allowance_1 + healthy_food_allowance_1 AS food_allowance_1,
    general_supports_for_living_allowance_1,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_condition) LIKE '%vbid%' THEN 'VBID'
        WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_condition) LIKE '%ssbci%' THEN 'SSBCI'
        WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_condition) LIKE '%uf%' THEN 'UF'
        ELSE ''
    END AS eligibility_1,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%13j%' OR 
             LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%food and produce%' OR 
             LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%healthy food%' THEN 'Yes'
        ELSE 'No'
    END AS food_and_produce_1,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%general supports for living%' THEN 'Yes'
        ELSE 'No'
    END AS general_supports_for_living_1,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%10b%' OR 
             LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%transportation%' THEN 'Yes'
        ELSE 'No'
    END AS transportation_1,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%13b%' OR 
             LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%otc%' THEN 'Yes'
        ELSE 'No'
    END AS otc_1,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%13c%' OR 
             LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%meal%' THEN 'Yes'
        ELSE 'No'
    END AS meals_1,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%16b%' OR 
             LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%16c%' OR 
             LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%home%' OR 
             LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%pet%' OR 
             LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%pest%' OR 
             LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%social needs%' OR 
             LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%other%' OR 
             LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%indoor air%' OR 
             LOWER(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) LIKE '%emergency%' THEN 'Yes'
        ELSE 'No'
    END AS Others_1
CASE 
        WHEN cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period IS NULL THEN 1
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period) = 'every month' THEN 1
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period) = 'every three months' THEN 1/3
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period) = 'every year' THEN 1/12
        ELSE 1
    END * 
    CASE 
        WHEN cy2025_vbidufssbci_group_2_additional_services_aggregate_limit IS NULL THEN food_and_produce_allowance_2 + healthy_food_allowance_2 + general_supports_for_living_allowance_2
        ELSE CAST(REPLACE(REPLACE(REPLACE(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit, '$', ''), ',', ''), ' ', '') AS NUMERIC)
    END AS monthly_allowance_2,
    
    food_and_produce_allowance_2 + healthy_food_allowance_2 AS food_allowance_2,
    general_supports_for_living_allowance_2,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_condition) LIKE '%vbid%' THEN 'VBID'
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_condition) LIKE '%ssbci%' THEN 'SSBCI'
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_condition) LIKE '%uf%' THEN 'UF'
        ELSE ''
    END AS eligibility_2,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%13j%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%food and produce%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%healthy food%' THEN 'Yes'
        ELSE 'No'
    END AS food_and_produce_2,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%general supports for living%' THEN 'Yes'
        ELSE 'No'
    END AS general_supports_for_living_2,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%10b%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%transportation%' THEN 'Yes'
        ELSE 'No'
    END AS transportation_2,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%13b%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%otc%' THEN 'Yes'
        ELSE 'No'
    END AS otc_2,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%13c%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%meal%' THEN 'Yes'
        ELSE 'No'
    END AS meals_2,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%16b%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%16c%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%home%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%pet%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%pest%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%social needs%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%other%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%indoor air%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%emergency%' THEN 'Yes'
        ELSE 'No'
    END AS Others_2
    CASE 
        WHEN cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period IS NULL THEN 1
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period) = 'every month' THEN 1
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period) = 'every three months' THEN 1/3
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period) = 'every year' THEN 1/12
        ELSE 1
    END * 
    CASE 
        WHEN cy2025_vbidufssbci_group_2_additional_services_aggregate_limit IS NULL THEN food_and_produce_allowance_2 + healthy_food_allowance_2 + general_supports_for_living_allowance_2
        ELSE CAST(REPLACE(REPLACE(REPLACE(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit, '$', ''), ',', ''), ' ', '') AS NUMERIC)
    END AS monthly_allowance_2,
    
    food_and_produce_allowance_2 + healthy_food_allowance_2 AS food_allowance_2,
    general_supports_for_living_allowance_2,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_condition) LIKE '%vbid%' THEN 'VBID'
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_condition) LIKE '%ssbci%' THEN 'SSBCI'
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_condition) LIKE '%uf%' THEN 'UF'
        ELSE ''
    END AS eligibility_2,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%13j%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%food and produce%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%healthy food%' THEN 'Yes'
        ELSE 'No'
    END AS food_and_produce_2,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%general supports for living%' THEN 'Yes'
        ELSE 'No'
    END AS general_supports_for_living_2,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%10b%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%transportation%' THEN 'Yes'
        ELSE 'No'
    END AS transportation_2,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%13b%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%otc%' THEN 'Yes'
        ELSE 'No'
    END AS otc_2,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%13c%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%meal%' THEN 'Yes'
        ELSE 'No'
    END AS meals_2,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%16b%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%16c%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%home%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%pet%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%pest%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%social needs%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%other%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%indoor air%' OR 
             LOWER(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) LIKE '%emergency%' THEN 'Yes'
        ELSE 'No'
    END AS Others_2
CASE 
        WHEN cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period IS NULL THEN 1
        WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period) = 'every month' THEN 1
        WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period) = 'every three months' THEN 1/3
        WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period) = 'every year' THEN 1/12
        ELSE 1
    END * 
    CASE 
        WHEN cy2025_vbidufssbci_group_3_additional_services_aggregate_limit IS NULL THEN food_and_produce_allowance_3 + healthy_food_allowance_3 + general_supports_for_living_allowance_3
        ELSE CAST(REPLACE(REPLACE(REPLACE(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit, '$', ''), ',', ''), ' ', '') AS NUMERIC)
    END AS monthly_allowance_3,
    
    food_and_produce_allowance_3 + healthy_food_allowance_3 AS food_allowance_3,
    general_supports_for_living_allowance_3,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_condition) LIKE '%vbid%' THEN 'VBID'
        WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_condition) LIKE '%ssbci%' THEN 'SSBCI'
        WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_condition) LIKE '%uf%' THEN 'UF'
        ELSE ''
    END AS eligibility_3,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%13j%' OR 
             LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%food and produce%' OR 
             LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%healthy food%' THEN 'Yes'
        ELSE 'No'
    END AS food_and_produce_3,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%general supports for living%' THEN 'Yes'
        ELSE 'No'
    END AS general_supports_for_living_3,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%10b%' OR 
             LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%transportation%' THEN 'Yes'
        ELSE 'No'
    END AS transportation_3,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%13b%' OR 
             LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%otc%' THEN 'Yes'
        ELSE 'No'
    END AS otc_3,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%13c%' OR 
             LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%meal%' THEN 'Yes'
        ELSE 'No'
    END AS meals_3,
    
    CASE 
        WHEN LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%16b%' OR 
             LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%16c%' OR 
             LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%home%' OR 
             LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%pet%' OR 
             LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%pest%' OR 
             LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%social needs%' OR 
             LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%other%' OR 
             LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%indoor air%' OR 
             LOWER(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits) LIKE '%emergency%' THEN 'Yes'
        ELSE 'No'
    END AS Others_3,
    
    cy2025_vbidufssbci_group_1_additional_services_aggregate_limit,
    cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period,
    cy2025_vbidufssbci_group_1_additional_services_condition,
    cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,
    
    cy2025_vbidufssbci_group_2_additional_services_aggregate_limit,
    cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period,
    cy2025_vbidufssbci_group_2_additional_services_condition,
    cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,
    
    cy2025_vbidufssbci_group_3_additional_services_aggregate_limit,
    cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period,
    cy2025_vbidufssbci_group_3_additional_services_condition,
    cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits
FROM your_table_name;

SELECT 
    year,
    contract_plan,
    contract_id,
    PBP,
    segment,
    cy2025_plan_type,
    cy2025_snp_type,
    cy2025_snp_detail,
    cy2025_dual_integration_status,
    cy2025_part_c_part_d_coverage,
    cy2025_plan_name,
    parent_name,
    new_plan_flag,
    cy2025_vbidufssbci_indicator,
    state,
    county,
    fips,
    ssa,
    cy2025_overthecounter_drug_card,
    cy2025_overthecounter_drug_card_period,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN monthly_allowance_1
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN monthly_allowance_2
        ELSE monthly_allowance_3
    END AS monthly_allowance_primary,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN food_allowance_1
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN food_allowance_2
        ELSE food_allowance_3
    END AS food_and_produce_allowance_primary,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN general_supports_for_living_allowance_1
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN general_supports_for_living_allowance_2
        ELSE general_supports_for_living_allowance_3
    END AS general_supports_for_living_allowance_primary,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN eligibility_1
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN eligibility_2
        ELSE eligibility_3
    END AS eligibility_primary,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN food_and_produce_1
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN food_and_produce_2
        ELSE food_and_produce_3
    END AS food_and_produce_primary,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN general_supports_for_living_1
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN general_supports_for_living_2
        ELSE general_supports_for_living_3
    END AS general_supports_for_living_primary,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN transportation_1
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN transportation_2
        ELSE transportation_3
    END AS transportation_primary,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN otc_1
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN otc_2
        ELSE otc_3
    END AS otc_primary,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN meals_1
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN meals_2
        ELSE meals_3
    END AS meals_primary,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN Others_1
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN Others_2
        ELSE Others_3
    END AS Others_primary,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN cy2025_vbidufssbci_group_1_additional_services_condition
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN cy2025_vbidufssbci_group_2_additional_services_condition
        ELSE cy2025_vbidufssbci_group_3_additional_services_condition
    END AS Conditions_primary,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_2 > monthly_allowance_3 THEN monthly_allowance_2
                ELSE monthly_allowance_3
            END
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_3 THEN monthly_allowance_1
                ELSE monthly_allowance_3
            END
        ELSE 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_2 THEN monthly_allowance_1
                ELSE monthly_allowance_2
            END
    END AS monthly_allowance_secondary,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_2 > monthly_allowance_3 THEN food_allowance_2
                ELSE food_allowance_3
            END
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_3 THEN food_allowance_1
                ELSE food_allowance_3
            END
        ELSE 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_2 THEN food_allowance_1
                ELSE food_allowance_2
            END
    END AS food_and_produce_allowance_secondary,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_2 > monthly_allowance_3 THEN general_supports_for_living_allowance_2
                ELSE general_supports_for_living_allowance_3
            END
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_3 THEN general_supports_for_living_allowance_1
                ELSE general_supports_for_living_allowance_3
            END
        ELSE 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_2 THEN general_supports_for_living_allowance_1
                ELSE general_supports_for_living_allowance_2
            END
    END AS general_supports_for_living_allowance_secondary
CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_2 > monthly_allowance_3 THEN eligibility_2
                ELSE eligibility_3
            END
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_3 THEN eligibility_1
                ELSE eligibility_3
            END
        ELSE 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_2 THEN eligibility_1
                ELSE eligibility_2
            END
    END AS eligibility_secondary,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_2 > monthly_allowance_3 THEN food_and_produce_2
                ELSE food_and_produce_3
            END
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_3 THEN food_and_produce_1
                ELSE food_and_produce_3
            END
        ELSE 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_2 THEN food_and_produce_1
                ELSE food_and_produce_2
            END
    END AS food_and_produce_secondary,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_2 > monthly_allowance_3 THEN general_supports_for_living_2
                ELSE general_supports_for_living_3
            END
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_3 THEN general_supports_for_living_1
                ELSE general_supports_for_living_3
            END
        ELSE 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_2 THEN general_supports_for_living_1
                ELSE general_supports_for_living_2
            END
    END AS general_supports_for_living_secondary,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_2 > monthly_allowance_3 THEN transportation_2
                ELSE transportation_3
            END
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_3 THEN transportation_1
                ELSE transportation_3
            END
        ELSE 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_2 THEN transportation_1
                ELSE transportation_2
            END
    END AS transportation_secondary,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_2 > monthly_allowance_3 THEN otc_2
                ELSE otc_3
            END
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_3 THEN otc_1
                ELSE otc_3
            END
        ELSE 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_2 THEN otc_1
                ELSE otc_2
            END
    END AS otc_secondary,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_2 > monthly_allowance_3 THEN meals_2
                ELSE meals_3
            END
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_3 THEN meals_1
                ELSE meals_3
            END
        ELSE 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_2 THEN meals_1
                ELSE meals_2
            END
    END AS meals_secondary,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_2 > monthly_allowance_3 THEN Others_2
                ELSE Others_3
            END
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_3 THEN Others_1
                ELSE Others_3
            END
        ELSE 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_2 THEN Others_1
                ELSE Others_2
            END
    END AS Others_secondary,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_2 > monthly_allowance_3 THEN cy2025_vbidufssbci_group_2_additional_services_condition
                ELSE cy2025_vbidufssbci_group_3_additional_services_condition
            END
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_3 THEN cy2025_vbidufssbci_group_1_additional_services_condition
                ELSE cy2025_vbidufssbci_group_3_additional_services_condition
            END
        ELSE 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_2 THEN cy2025_vbidufssbci_group_1_additional_services_condition
                ELSE cy2025_vbidufssbci_group_2_additional_services_condition
            END
    END AS Conditions_secondary,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_2 > monthly_allowance_3 THEN monthly_allowance_3
                ELSE monthly_allowance_2
            END
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_3 THEN monthly_allowance_3
                ELSE monthly_allowance_1
            END
        ELSE 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_2 THEN monthly_allowance_2
                ELSE monthly_allowance_1
            END
    END AS monthly_allowance_third,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_2 > monthly_allowance_3 THEN food_allowance_3
                ELSE food_allowance_2
            END
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_3 THEN food_allowance_3
                ELSE food_allowance_1
            END
        ELSE 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_2 THEN food_allowance_2
                ELSE food_allowance_1
            END
    END AS food_and_produce_allowance_third,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_2 > monthly_allowance_3 THEN general_supports_for_living_allowance_3
                ELSE general_supports_for_living_allowance_2
            END
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_3 THEN general_supports_for_living_allowance_3
                ELSE general_supports_for_living_allowance_1
            END
        ELSE 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_2 THEN general_supports_for_living_allowance_2
                ELSE general_supports_for_living_allowance_1
            END
    END AS general_supports_for_living_allowance_third
CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_2 > monthly_allowance_3 THEN eligibility_3
                ELSE eligibility_2
            END
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_3 THEN eligibility_3
                ELSE eligibility_1
            END
        ELSE 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_2 THEN eligibility_2
                ELSE eligibility_1
            END
    END AS eligibility_third,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_2 > monthly_allowance_3 THEN food_and_produce_3
                ELSE food_and_produce_2
            END
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_3 THEN food_and_produce_3
                ELSE food_and_produce_1
            END
        ELSE 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_2 THEN food_and_produce_2
                ELSE food_and_produce_1
            END
    END AS food_and_produce_third,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_2 > monthly_allowance_3 THEN general_supports_for_living_3
                ELSE general_supports_for_living_2
            END
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_3 THEN general_supports_for_living_3
                ELSE general_supports_for_living_1
            END
        ELSE 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_2 THEN general_supports_for_living_2
                ELSE general_supports_for_living_1
            END
    END AS general_supports_for_living_third,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_2 > monthly_allowance_3 THEN transportation_3
                ELSE transportation_2
            END
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_3 THEN transportation_3
                ELSE transportation_1
            END
        ELSE 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_2 THEN transportation_2
                ELSE transportation_1
            END
    END AS transportation_third,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_2 > monthly_allowance_3 THEN otc_3
                ELSE otc_2
            END
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_3 THEN otc_3
                ELSE otc_1
            END
        ELSE 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_2 THEN otc_2
                ELSE otc_1
            END
    END AS otc_third,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_2 > monthly_allowance_3 THEN meals_3
                ELSE meals_2
            END
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_3 THEN meals_3
                ELSE meals_1
            END
        ELSE 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_2 THEN meals_2
                ELSE meals_1
            END
    END AS meals_third,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_2 > monthly_allowance_3 THEN Others_3
                ELSE Others_2
            END
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_3 THEN Others_3
                ELSE Others_1
            END
        ELSE 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_2 THEN Others_2
                ELSE Others_1
            END
    END AS Others_third,
    
    CASE 
        WHEN monthly_allowance_1 >= monthly_allowance_2 AND monthly_allowance_1 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_2 > monthly_allowance_3 THEN cy2025_vbidufssbci_group_3_additional_services_condition
                ELSE cy2025_vbidufssbci_group_2_additional_services_condition
            END
        WHEN monthly_allowance_2 >= monthly_allowance_1 AND monthly_allowance_2 >= monthly_allowance_3 THEN 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_3 THEN cy2025_vbidufssbci_group_3_additional_services_condition
                ELSE cy2025_vbidufssbci_group_1_additional_services_condition
            END
        ELSE 
            CASE 
                WHEN monthly_allowance_1 > monthly_allowance_2 THEN cy2025_vbidufssbci_group_2_additional_services_condition
                ELSE cy2025_vbidufssbci_group_1_additional_services_condition
            END
    END AS Conditions_third
FROM macvat_2022_benefit_with_geo_formatted;


SELECT 
    year,
    contract_plan,
    contract_id,
    PBP,
    segment,
    cy2025_plan_type,
    cy2025_snp_type,
    cy2025_snp_detail,
    cy2025_dual_integration_status,
    cy2025_part_c_part_d_coverage,
    cy2025_plan_name,
    parent_name,
    new_plan_flag,
    cy2025_vbidufssbci_indicator,
    state,
    county,
    fips,
    ssa,
    
    -- cy2025_overthecounter_drug_card,
    -- cy2025_overthecounter_drug_card_period,
    
    monthly_allowance_primary,
    food_and_produce_allowance_primary,
    general_supports_for_living_allowance_primary,
    eligibility_primary,
    food_and_produce_primary,
    general_supports_for_living_primary,
    transportation_primary,
    otc_primary,
    meals_primary,
    Others_primary,
    
    monthly_allowance_secondary,
    food_and_produce_allowance_secondary,
    general_supports_for_living_allowance_secondary,
    eligibility_secondary,
    food_and_produce_secondary,
    general_supports_for_living_secondary,
    transportation_secondary,
    otc_secondary,
    meals_secondary,
    Others_secondary,
    
    -- monthly_allowance_third,
    -- food_and_produce_allowance_third,
    -- general_supports_for_living_allowance_third,
    -- eligibility_third,
    -- food_and_produce_third,
    -- general_supports_for_living_third,
    -- transportation_third,
    -- otc_third,
    -- meals_third,
    -- Others_third,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%anemia%' THEN 'Y'
        ELSE 'N'
    END AS anemia,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%asthma%' THEN 'Y'
        ELSE 'N'
    END AS asthma,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%autoimmune disorder%' THEN 'Y'
        ELSE 'N'
    END AS autoimmune_disorder,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%arthritis%' THEN 'Y'
        ELSE 'N'
    END AS arthritis,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%cardiovascular disorder%' THEN 'Y'
        ELSE 'N'
    END AS cardiovascular_disorder,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%cellulitis%' THEN 'Y'
        ELSE 'N'
    END AS cellulitis,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%circulatory disease%' THEN 'Y'
        ELSE 'N'
    END AS circulatory_disease,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%chronic alcohol and other drug dependence%' THEN 'Y'
        ELSE 'N'
    END AS chronic_alcohol_and_other_drug_dependence,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%chronic cognitive impairment%' THEN 'Y'
        ELSE 'N'
    END AS chronic_cognitive_impairment,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%chronic heart failure%' THEN 'Y'
        ELSE 'N'
    END AS chronic_heart_failure,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%chronic gastrointestinal disorder%' OR LOWER(Conditions_primary) LIKE '%chronic gi disorder%' THEN 'Y'
        ELSE 'N'
    END AS chronic_gastrointestinal_disorder,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%malnutrition%' THEN 'Y'
        ELSE 'N'
    END AS malnutrition,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%chronic infectious disorder%' THEN 'Y'
        ELSE 'N'
    END AS chronic_infectious_disorder,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%chronic otolaryngological disorder%' THEN 'Y'
        ELSE 'N'
    END AS chronic_otolaryngological_disorder,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%chronic pain%' THEN 'Y'
        ELSE 'N'
    END AS chronic_pain,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%cancer%' THEN 'Y'
        ELSE 'N'
    END AS cancer,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%chronic lung disorder%' THEN 'Y'
        ELSE 'N'
    END AS chronic_lung_disorder,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%chronic liver%' OR LOWER(Conditions_primary) LIKE '%chronic kidney/liver%' THEN 'Y'
        ELSE 'N'
    END AS chronic_liver_disease,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%chronic and disabling mental health condition%' THEN 'Y'
        ELSE 'N'
    END AS chronic_and_disabling_mental_health_condition,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%chronic kidney disease%' OR LOWER(Conditions_primary) LIKE '%chronic liver/kidney%' THEN 'Y'
        ELSE 'N'
    END AS chronic_kidney_disease,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%chronic non-alcohol%' THEN 'Y'
        ELSE 'N'
    END AS chronic_non_alcoholic_fatty_liver_disease,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%copd%' OR LOWER(Conditions_primary) LIKE '%chronic obstructive pulmonary disease%' THEN 'Y'
        ELSE 'N'
    END AS copd
CASE 
        WHEN LOWER(Conditions_primary) LIKE '%congestive heart failure%' THEN 'Y'
        ELSE 'N'
    END AS congestive_heart_failure,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%coronary artery disease%' THEN 'Y'
        ELSE 'N'
    END AS coronary_artery_disease,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%cystic fibrosis%' THEN 'Y'
        ELSE 'N'
    END AS cystic_fibrosis,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%down syndrome%' THEN 'Y'
        ELSE 'N'
    END AS down_syndrome,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%diabete%' THEN 'Y'
        ELSE 'N'
    END AS diabete,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%dementia%' THEN 'Y'
        ELSE 'N'
    END AS dementia,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%dyslipidemia%' THEN 'Y'
        ELSE 'N'
    END AS dyslipidemia,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%eating disorder%' THEN 'Y'
        ELSE 'N'
    END AS eating_disorder,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%end-stage liver disease%' THEN 'Y'
        ELSE 'N'
    END AS end_stage_liver_disease,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%end-stage renal disease%' THEN 'Y'
        ELSE 'N'
    END AS end_stage_renal_disease,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%endometriosis%' THEN 'Y'
        ELSE 'N'
    END AS endometriosis,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%endocrine%' THEN 'Y'
        ELSE 'N'
    END AS endocrine,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%gastrointestinal%' THEN 'Y'
        ELSE 'N'
    END AS gastrointestinal,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%glaucoma%' THEN 'Y'
        ELSE 'N'
    END AS glaucoma,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%hiv%' THEN 'Y'
        ELSE 'N'
    END AS hiv,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%hepatitis%' THEN 'Y'
        ELSE 'N'
    END AS hepatitis,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%hypertension%' THEN 'Y'
        ELSE 'N'
    END AS hypertension,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%hyperlipidemia%' OR LOWER(Conditions_primary) LIKE '%chronic lipid%' THEN 'Y'
        ELSE 'N'
    END AS hyperlipidemia,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%hypercholesterolemia%' THEN 'Y'
        ELSE 'N'
    END AS hypercholesterolemia,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%inflammatory bowel disease%' THEN 'Y'
        ELSE 'N'
    END AS inflammatory_bowel_disease,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%joints%' THEN 'Y'
        ELSE 'N'
    END AS joints_and_spine,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%loss of limb%' THEN 'Y'
        ELSE 'N'
    END AS loss_of_limb,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%low back pain%' THEN 'Y'
        ELSE 'N'
    END AS low_back_pain,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%metabolic syndrome%' THEN 'Y'
        ELSE 'N'
    END AS metabolic_syndrome,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%muscular dystrophy%' THEN 'Y'
        ELSE 'N'
    END AS muscular_dystrophy,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%musculoskeletal disorder%' THEN 'Y'
        ELSE 'N'
    END AS musculoskeletal_disorder,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%neurologic disorder%' THEN 'Y'
        ELSE 'N'
    END AS neurologic_disorder,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%osteoporosis%' THEN 'Y'
        ELSE 'N'
    END AS osteoporosis,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%obesity%' OR LOWER(Conditions_primary) LIKE '%obese%' THEN 'Y'
        ELSE 'N'
    END AS obesity,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%pneumonia%' THEN 'Y'
        ELSE 'N'
    END AS pneumonia,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%pregnancy%' THEN 'Y'
        ELSE 'N'
    END AS pregnancy,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%rsd%' THEN 'Y'
        ELSE 'N'
    END AS rsd,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%sjogren%' THEN 'Y'
        ELSE 'N'
    END AS sjogren,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%severe hematologic disorder%' THEN 'Y'
        ELSE 'N'
    END AS severe_hematologic_disorder,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%stroke%' THEN 'Y'
        ELSE 'N'
    END AS stroke,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%urinary tract infection%' THEN 'Y'
        ELSE 'N'
    END AS urinary_tract_infection,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%urinary incontinance%' THEN 'Y'
        ELSE 'N'
    END AS urinary_incontinance,
    
    CASE 
        WHEN LOWER(Conditions_primary) LIKE '%vascular disease%' THEN 'Y'
        ELSE 'N'
    END AS vascular_disease
FROM macvat_2022_benefit_with_geo_formatted;

CREATE TABLE macvat_2022_benefit_with_geo_formatted_arranged_plan AS
SELECT DISTINCT
    year,
    contract_plan,
    contract_id,
    PBP,
    segment,
    cy2025_plan_type,
    cy2025_snp_type,
    cy2025_snp_detail,
    cy2025_dual_integration_status,
    cy2025_part_c_part_d_coverage,
    cy2025_plan_name,
    parent_name,
    new_plan_flag,
    cy2025_vbidufssbci_indicator
FROM macvat_2022_benefit_with_geo_formatted_arranged;

CREATE TABLE macvat_2022_benefit_with_geo_formatted_arranged_plan_fips AS
SELECT DISTINCT
    year,
    contract_plan,
    contract_id,
    PBP,
    segment,
    cy2025_plan_type,
    cy2025_snp_type,
    cy2025_snp_detail,
    cy2025_dual_integration_status,
    cy2025_part_c_part_d_coverage,
    cy2025_plan_name,
    parent_name,
    new_plan_flag,
    cy2025_vbidufssbci_indicator,
    state,
    county,
    fips,
    ssa
FROM macvat_2022_benefit_with_geo_formatted_arranged;

-- Assuming you have a way to write SQL query results to an Excel file
-- This part is more about the logic rather than direct SQL translation
-- You would need to use a tool or script to export the combined data to Excel

CREATE TABLE combined_plan_level AS
SELECT * FROM macvat_2025_benefit_with_geo_formatted_arranged_plan
UNION ALL
SELECT * FROM macvat_2024_benefit_with_geo_formatted_arranged_plan
UNION ALL
SELECT * FROM macvat_2023_benefit_with_geo_formatted_arranged_plan
UNION ALL
SELECT * FROM macvat_2022_benefit_with_geo_formatted_arranged_plan;

-- Export combined_plan_level to "formatted_section_19_plan_level.xlsx"

CREATE TABLE section_19_SOT AS
SELECT * FROM macvat_2025_benefit_with_geo_formatted_arranged_plan
UNION ALL
SELECT * FROM macvat_2024_benefit_with_geo_formatted_arranged_plan
UNION ALL
SELECT * FROM macvat_2023_benefit_with_geo_formatted_arranged_plan
UNION ALL
SELECT * FROM macvat_2022_benefit_with_geo_formatted_arranged_plan;

-- This part involves using BigQuery's API or command-line tool
-- The following is a conceptual representation

-- Create the table in BigQuery
CREATE TABLE 'anbc-hcb-dev.growth_anlyt_hcb_dev.section_19_sot' (
    -- Define the schema based on section_19_SOT
);

-- Upload the data to BigQuery
-- This would typically be done using a BigQuery client or command-line tool

-- Assuming you have a way to write SQL query results to an Excel file
-- This part is more about the logic rather than direct SQL translation
-- You would need to use a tool or script to export the combined data to Excel

CREATE TABLE combined_plan_fips AS
SELECT * FROM macvat_2025_benefit_with_geo_formatted_arranged_plan_fips
UNION ALL
SELECT * FROM macvat_2024_benefit_with_geo_formatted_arranged_plan_fips
UNION ALL
SELECT * FROM macvat_2023_benefit_with_geo_formatted_arranged_plan_fips
UNION ALL
SELECT * FROM macvat_2022_benefit_with_geo_formatted_arranged_plan_fips;

-- Export combined_plan_fips to "formatted_section_19_plan_fips_combination.xlsx"

CREATE TABLE combined_benefit_with_geo_formatted AS
SELECT * FROM macvat_2025_benefit_with_geo_formatted
UNION ALL
SELECT * FROM macvat_2024_benefit_with_geo_formatted
UNION ALL
SELECT * FROM macvat_2023_benefit_with_geo_formatted
UNION ALL
SELECT * FROM macvat_2022_benefit_with_geo_formatted;

CREATE TABLE combined_benefit_with_geo_formatted_distinct AS
SELECT DISTINCT
    year,
    contract_plan,
    contract_id,
    PBP,
    segment,
    cy2025_plan_type,
    cy2025_snp_type,
    cy2025_snp_detail,
    cy2025_dual_integration_status,
    cy2025_part_c_part_d_coverage,
    cy2025_plan_name,
    parent_name,
    new_plan_flag,
    cy2025_vbidufssbci_indicator,
    -- other columns except ssa, fips, state, county
FROM combined_benefit_with_geo_formatted;

CREATE TABLE combined_plan_level AS
SELECT * FROM macvat_2025_benefit_with_geo_formatted_arranged_plan
UNION ALL
SELECT * FROM macvat_2024_benefit_with_geo_formatted_arranged_plan
UNION ALL
SELECT * FROM macvat_2023_benefit_with_geo_formatted_arranged_plan
UNION ALL
SELECT * FROM macvat_2022_benefit_with_geo_formatted_arranged_plan;

CREATE TABLE joined_benefit_with_geo_formatted AS
SELECT a.*, b.monthly_allowance_primary
FROM combined_benefit_with_geo_formatted_distinct a
LEFT JOIN combined_plan_level b
ON a.year = b.year AND a.contract_plan = b.contract_plan;

CREATE TABLE final_benefit_with_geo_formatted AS
SELECT *,
    CASE 
        WHEN (cy2025_vbidufssbci_group_1_additional_services_condition IS NULL OR TRIM(cy2025_vbidufssbci_group_1_additional_services_condition) = 'NA') AND
             (cy2025_vbidufssbci_group_2_additional_services_condition IS NULL OR TRIM(cy2025_vbidufssbci_group_2_additional_services_condition) = 'NA') AND
             (cy2025_vbidufssbci_group_3_additional_services_condition IS NULL OR TRIM(cy2025_vbidufssbci_group_3_additional_services_condition) = 'NA') THEN 'No'
        ELSE 'Yes'
    END AS covered_or_not,
    CASE 
        WHEN monthly_allowance_1 > 0 OR monthly_allowance_2 > 0 OR monthly_allowance_3 > 0 OR monthly_allowance_primary > 0 THEN 'Yes'
        ELSE 'No'
    END AS allowance_identified
FROM joined_benefit_with_geo_formatted;

CREATE TABLE filtered_benefit_with_geo_formatted AS
SELECT *
FROM final_benefit_with_geo_formatted
WHERE cy2025_plan_type NOT IN ('Cost', 'MSA', 'PFFS') AND cy2025_snp_type IN ('Not SNP', 'Dual Eligible');

CREATE TABLE benefit_coverage_summary AS
SELECT year,
       cy2025_snp_type,
       covered_or_not,
       allowance_identified,
       COUNT(*) AS n
FROM filtered_benefit_with_geo_formatted
GROUP BY year, cy2025_snp_type, covered_or_not, allowance_identified;

CREATE TABLE benefit_coverage_summary_final AS
SELECT year,
       cy2025_snp_type,
       SUM(n) AS num_of_plans,
       SUM(CASE WHEN covered_or_not = 'No' THEN n ELSE 0 END) AS num_of_not_covered,
       SUM(CASE WHEN covered_or_not = 'Yes' AND allowance_identified = 'No' THEN n ELSE 0 END) AS num_of_covered_not_identified,
       SUM(CASE WHEN covered_or_not = 'Yes' AND allowance_identified = 'Yes' THEN n ELSE 0 END) AS num_of_covered_identified,
       SUM(CASE WHEN covered_or_not = 'Yes' AND allowance_identified = 'Yes' THEN n ELSE 0 END) / 
       (SUM(CASE WHEN covered_or_not = 'Yes' AND allowance_identified = 'Yes' THEN n ELSE 0 END) + 
        SUM(CASE WHEN covered_or_not = 'Yes' AND allowance_identified = 'No' THEN n ELSE 0 END)) AS identified_rate
FROM benefit_coverage_summary
GROUP BY year, cy2025_snp_type;

-- Export benefit_coverage_summary_final to "section_19_benefits_coverage_summary.xlsx"

CREATE TABLE combined_benefit_with_geo_formatted AS
SELECT * FROM macvat_2025_benefit_with_geo_formatted
UNION ALL
SELECT * FROM macvat_2024_benefit_with_geo_formatted
UNION ALL
SELECT * FROM macvat_2023_benefit_with_geo_formatted
UNION ALL
SELECT * FROM macvat_2022_benefit_with_geo_formatted;

CREATE TABLE combined_benefit_with_geo_formatted_distinct AS
SELECT DISTINCT
    year,
    contract_plan,
    contract_id,
    PBP,
    segment,
    cy2025_plan_type,
    cy2025_snp_type,
    cy2025_snp_detail,
    cy2025_dual_integration_status,
    cy2025_part_c_part_d_coverage,
    cy2025_plan_name,
    parent_name,
    new_plan_flag,
    cy2025_vbidufssbci_indicator
FROM combined_benefit_with_geo_formatted;

CREATE TABLE combined_plan_level AS
SELECT * FROM macvat_2025_benefit_with_geo_formatted_arranged_plan
UNION ALL
SELECT * FROM macvat_2024_benefit_with_geo_formatted_arranged_plan
UNION ALL
SELECT * FROM macvat_2023_benefit_with_geo_formatted_arranged_plan
UNION ALL
SELECT * FROM macvat_2022_benefit_with_geo_formatted_arranged_plan;

CREATE TABLE joined_benefit_with_geo_formatted AS
SELECT a.*, b.monthly_allowance_primary
FROM combined_benefit_with_geo_formatted_distinct a
LEFT JOIN combined_plan_level b
ON a.year = b.year AND a.contract_plan = b.contract_plan;

CREATE TABLE final_benefit_with_geo_formatted AS
SELECT *,
    CASE 
        WHEN (cy2025_vbidufssbci_group_1_additional_services_condition IS NULL OR TRIM(cy2025_vbidufssbci_group_1_additional_services_condition) = 'NA') AND
             (cy2025_vbidufssbci_group_2_additional_services_condition IS NULL OR TRIM(cy2025_vbidufssbci_group_2_additional_services_condition) = 'NA') AND
             (cy2025_vbidufssbci_group_3_additional_services_condition IS NULL OR TRIM(cy2025_vbidufssbci_group_3_additional_services_condition) = 'NA') THEN 'No'
        ELSE 'Yes'
    END AS covered_or_not,
    CASE 
        WHEN monthly_allowance_1 > 0 OR monthly_allowance_2 > 0 OR monthly_allowance_3 > 0 OR monthly_allowance_primary > 0 THEN 'Yes'
        ELSE 'No'
    END AS allowance_identified
FROM joined_benefit_with_geo_formatted;

CREATE TABLE filtered_benefit_with_geo_formatted AS
SELECT *
FROM final_benefit_with_geo_formatted
WHERE cy2025_plan_type NOT IN ('Cost', 'MSA', 'PFFS') AND cy2025_snp_type IN ('Not SNP', 'Dual Eligible');

CREATE TABLE benefit_coverage_summary AS
SELECT year,
       parent_name,
       cy2025_snp_type,
       covered_or_not,
       allowance_identified,
       COUNT(*) AS n
FROM filtered_benefit_with_geo_formatted
GROUP BY year, parent_name, cy2025_snp_type, covered_or_not, allowance_identified;

CREATE TABLE benefit_coverage_summary_final AS
SELECT year,
       parent_name,
       cy2025_snp_type,
       SUM(n) AS num_of_plans,
       SUM(CASE WHEN covered_or_not = 'No' THEN n ELSE 0 END) AS num_of_not_covered,
       SUM(CASE WHEN covered_or_not = 'Yes' AND allowance_identified = 'No' THEN n ELSE 0 END) AS num_of_covered_not_identified,
       SUM(CASE WHEN covered_or_not = 'Yes' AND allowance_identified = 'Yes' THEN n ELSE 0 END) AS num_of_covered_identified,
       SUM(CASE WHEN covered_or_not = 'Yes' AND allowance_identified = 'Yes' THEN n ELSE 0 END) / 
       (SUM(CASE WHEN covered_or_not = 'Yes' AND allowance_identified = 'Yes' THEN n ELSE 0 END) + 
        SUM(CASE WHEN covered_or_not = 'Yes' AND allowance_identified = 'No' THEN n ELSE 0 END)) AS identified_rate
FROM benefit_coverage_summary
GROUP BY year, parent_name, cy2025_snp_type;

-- Export benefit_coverage_summary_final to "section_19_benefits_coverage_summary_carrier_ge_dsnp.xlsx"
