library(readxl)
library(writexl)
library(DBI)
library(bigrquery)
library(rpivotTable)
library(tidyverse)
library(readr)

con <- dbConnect(
  bigrquery::bigquery(),
  project = "anbc-hcb-dev", 
  billing = "anbc-hcb-dev"
)

fips_ssa <- dbGetQuery(con, "select distinct fips, ssa from anbc-hcb-prod.msa_share_mcr_hcb_prod.mpdr_cms_enrolled_pbpcounty") %>%
  rbind(tribble(~fips,~ssa,
                "48261","45732",
                "15005","12030",
                "48269","45741",
                "48301","45762")) %>%
  distinct()

aep_footprint_history <-
  dbGetQuery(con,"select distinct cast(substr(aep_period,4,4) as int) as year, 
                                  fips_cd_std as fips,
                                  st_cd_src as state,
                                  mkt_cd_src as market,
                                  submkt_cd_src as submarket,
                                  cnty_nm_src as county,
                                  ma_status_desc,
                                  dsnp_status_desc
                          from anbc-hcb-prod.msa_share_mgap_hcb_prod.mgap_fips_lookup")

macvat_2025_benefit  <- dbGetQuery(con, 
                                   "
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
          on a.contract_plan=b.contract_plan ") %>%
  
  mutate(contract_plan = gsub(" ", "", contract_plan)) %>%
  transmute(year,
            contract_plan,
            contract_id=substr(contract_plan,1,5),
            PBP=substr(contract_plan,7,9),
            segment=substr(contract_plan,11,13),
            cy2025_plan_type,
            cy2025_snp_type,
            cy2025_snp_detail,
            cy2025_dual_integration_status,
            cy2025_part_c_part_d_coverage,
            cy2025_plan_name,
            parent_name,
            new_plan_flag,
            cy2025_vbidufssbci_indicator,
            
            ssa=sprintf("%05d", as.numeric(ssa_code)),
            
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
            cy2025_vbidufssbci_group_5_additional_services_nonmedicare_covered_benefits) %>%
  distinct()

macvat_2025_benefit_with_geo <-
  macvat_2025_benefit %>%
  left_join(fips_ssa,by=c("ssa")) %>%
  inner_join(aep_footprint_history %>% filter(year==2025) %>% select(fips,state,county))


macvat_2025_benefit_with_geo_formatted <-
  macvat_2025_benefit_with_geo %>%
  mutate(food_and_produce_string_1=str_extract(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,"(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))"),
         food_and_produce_string_2=str_extract(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,"(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))"),
         food_and_produce_string_3=str_extract(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,"(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))"),
         healthy_food_string_1=str_extract(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,"\\(Healthy Food.*?\\)|\\(Living Expense Support.*?\\)"),
         healthy_food_string_2=str_extract(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,"\\(Healthy Food.*?\\)|\\(Living Expense Support.*?\\)"),
         healthy_food_string_3=str_extract(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,"\\(Healthy Food.*?\\)|\\(Living Expense Support.*?\\)"),
         
         general_supports_for_living_string_1=str_extract(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,"General Supports for Living \\(.*?\\)"),
         general_supports_for_living_string_1_alter=str_extract(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,"\\(Utilities\\), \\$[0-9]+.*"),
         general_supports_for_living_string_2=str_extract(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,"General Supports for Living \\(.*?\\)"),
         general_supports_for_living_string_2_alter=str_extract(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,"\\(Utilities\\), \\$[0-9]+.*"),
         general_supports_for_living_string_3=str_extract(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,"General Supports for Living \\(.*?\\)"),
         general_supports_for_living_string_3_alter=str_extract(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,"\\(Utilities\\), \\$[0-9]+.*"),
         food_and_produce_allowance_1=as.numeric(str_extract(food_and_produce_string_1,"[0-9]+")) *
           ifelse(grepl("every month",tolower(food_and_produce_string_1)),1,
                  ifelse(grepl("every three months",tolower(food_and_produce_string_1)),1/3,1/12)),
         food_and_produce_allowance_2=as.numeric(str_extract(food_and_produce_string_2,"[0-9]+")) *
           ifelse(grepl("every month",tolower(food_and_produce_string_2)),1,
                  ifelse(grepl("every three months",tolower(food_and_produce_string_2)),1/3,1/12)),
         food_and_produce_allowance_3=as.numeric(str_extract(food_and_produce_string_3,"[0-9]+")) *
           ifelse(grepl("every month",tolower(food_and_produce_string_3)),1,
                  ifelse(grepl("every three months",tolower(food_and_produce_string_3)),1/3,1/12)),
         healthy_food_allowance_1=as.numeric(str_extract(healthy_food_string_1,"[0-9]+")) *
           ifelse(grepl("every month",tolower(healthy_food_string_1)),1,
                  ifelse(grepl("every three months",tolower(healthy_food_string_1)),1/3,1/12)),
         healthy_food_allowance_2=as.numeric(str_extract(healthy_food_string_2,"[0-9]+")) *
           ifelse(grepl("every month",tolower(healthy_food_string_2)),1,
                  ifelse(grepl("every three months",tolower(healthy_food_string_2)),1/3,1/12)),
         healthy_food_allowance_3=as.numeric(str_extract(healthy_food_string_3,"[0-9]+")) *
           ifelse(grepl("every month",tolower(healthy_food_string_3)),1,
                  ifelse(grepl("every three months",tolower(healthy_food_string_3)),1/3,1/12)),
         general_supports_for_living_allowance_1=as.numeric(str_extract(general_supports_for_living_string_1,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_1)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_1)),1/3,1/12)),
         general_supports_for_living_allowance_1_alter=as.numeric(str_extract(general_supports_for_living_string_1_alter,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_1_alter)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_1_alter)),1/3,1/12)),
         general_supports_for_living_allowance_2=as.numeric(str_extract(general_supports_for_living_string_2,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_2)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_2)),1/3,1/12)),
         general_supports_for_living_allowance_2_alter=as.numeric(str_extract(general_supports_for_living_string_2_alter,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_2_alter)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_2_alter)),1/3,1/12)),
         general_supports_for_living_allowance_3=as.numeric(str_extract(general_supports_for_living_string_3,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_3)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_3)),1/3,1/12)),
         general_supports_for_living_allowance_3_alter=as.numeric(str_extract(general_supports_for_living_string_3_alter,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_3_alter)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_3_alter)),1/3,1/12)),
         food_and_produce_allowance_1=ifelse(is.na(food_and_produce_allowance_1),0,food_and_produce_allowance_1),
         food_and_produce_allowance_2=ifelse(is.na(food_and_produce_allowance_2),0,food_and_produce_allowance_2),
         food_and_produce_allowance_3=ifelse(is.na(food_and_produce_allowance_3),0,food_and_produce_allowance_3),
         healthy_food_allowance_1=ifelse(is.na(healthy_food_allowance_1),0,healthy_food_allowance_1),
         healthy_food_allowance_2=ifelse(is.na(healthy_food_allowance_2),0,healthy_food_allowance_2),
         healthy_food_allowance_3=ifelse(is.na(healthy_food_allowance_3),0,healthy_food_allowance_3),
         general_supports_for_living_allowance_1=ifelse(is.na(general_supports_for_living_allowance_1),general_supports_for_living_allowance_1_alter,general_supports_for_living_allowance_1),
         general_supports_for_living_allowance_1=ifelse(is.na(general_supports_for_living_allowance_1),0,general_supports_for_living_allowance_1),
         general_supports_for_living_allowance_2=ifelse(is.na(general_supports_for_living_allowance_2),general_supports_for_living_allowance_2_alter,general_supports_for_living_allowance_2),
         general_supports_for_living_allowance_2=ifelse(is.na(general_supports_for_living_allowance_2),0,general_supports_for_living_allowance_2),
         general_supports_for_living_allowance_3=ifelse(is.na(general_supports_for_living_allowance_3),general_supports_for_living_allowance_3_alter,general_supports_for_living_allowance_3),
         general_supports_for_living_allowance_3=ifelse(is.na(general_supports_for_living_allowance_3),0,general_supports_for_living_allowance_3)) %>%
  transmute(year,
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
            
            monthly_allowance_1=ifelse(is.na(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period),1,
                                       ifelse(tolower(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period)=="every month",1,
                                              ifelse(tolower(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period)=="every three months",1/3,
                                                     ifelse(tolower(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period)=="every year",1/12,1)))) *
              ifelse(is.na(as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_1_additional_services_aggregate_limit))),
                     food_and_produce_allowance_1+healthy_food_allowance_1+general_supports_for_living_allowance_1,as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_1_additional_services_aggregate_limit))),
            food_allowance_1=food_and_produce_allowance_1+healthy_food_allowance_1,
            general_supports_for_living_allowance_1,
            
            eligibility_1=ifelse(grepl("vbid",tolower(cy2025_vbidufssbci_group_1_additional_services_condition)),"VBID",
                                 ifelse(grepl("ssbci",tolower(cy2025_vbidufssbci_group_1_additional_services_condition)),"SSBCI",
                                        ifelse(grepl("uf",tolower(cy2025_vbidufssbci_group_1_additional_services_condition)),"UF",""))),
            food_and_produce_1=ifelse(grepl("13j",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("food and produce",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("healthy food",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            general_supports_for_living_1=ifelse(grepl("general supports for living",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            transportation_1=ifelse(grepl("10b",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                                      grepl("transportation",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            otc_1=ifelse(grepl("13b",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                           grepl("otc",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            meals_1=ifelse(grepl("13c",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                             grepl("meal",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            Others_1=ifelse(grepl("16b",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("16c",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("home",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pet",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pest",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("social needs",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("other",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("indoor air",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("emergency",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            
            monthly_allowance_2=ifelse(is.na(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period),1,
                                       ifelse(tolower(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period)=="every month",1,
                                              ifelse(tolower(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period)=="every three months",1/3,
                                                     ifelse(tolower(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period)=="every year",1/12,1)))) *
              ifelse(is.na(as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_2_additional_services_aggregate_limit))),
                     food_and_produce_allowance_2+healthy_food_allowance_2+general_supports_for_living_allowance_2,as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_2_additional_services_aggregate_limit))),
            food_allowance_2=food_and_produce_allowance_2+healthy_food_allowance_2,
            general_supports_for_living_allowance_2,
            
            eligibility_2=ifelse(grepl("vbid",tolower(cy2025_vbidufssbci_group_2_additional_services_condition)),"VBID",
                                 ifelse(grepl("ssbci",tolower(cy2025_vbidufssbci_group_2_additional_services_condition)),"SSBCI",
                                        ifelse(grepl("uf",tolower(cy2025_vbidufssbci_group_2_additional_services_condition)),"UF",""))),
            food_and_produce_2=ifelse(grepl("13j",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("food and produce",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("healthy food",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            general_supports_for_living_2=ifelse(grepl("general supports for living",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            transportation_2=ifelse(grepl("10b",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                                      grepl("transportation",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            otc_2=ifelse(grepl("13b",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                           grepl("otc",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            meals_2=ifelse(grepl("13c",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                             grepl("meal",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            Others_2=ifelse(grepl("16b",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("16c",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("home",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pet",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pest",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("social needs",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("other",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("indoor air",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("emergency",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            
            monthly_allowance_3=ifelse(is.na(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period),1,
                                       ifelse(tolower(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period)=="every month",1,
                                              ifelse(tolower(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period)=="every three months",1/3,
                                                     ifelse(tolower(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period)=="every year",1/12,1)))) *
              ifelse(is.na(as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_3_additional_services_aggregate_limit))),
                     food_and_produce_allowance_3+healthy_food_allowance_3+general_supports_for_living_allowance_3,as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_3_additional_services_aggregate_limit))),
            food_allowance_3=food_and_produce_allowance_3+healthy_food_allowance_3,
            general_supports_for_living_allowance_3,
            
            eligibility_3=ifelse(grepl("vbid",tolower(cy2025_vbidufssbci_group_3_additional_services_condition)),"VBID",
                                 ifelse(grepl("ssbci",tolower(cy2025_vbidufssbci_group_3_additional_services_condition)),"SSBCI",
                                        ifelse(grepl("uf",tolower(cy2025_vbidufssbci_group_3_additional_services_condition)),"UF",""))),
            food_and_produce_3=ifelse(grepl("13j",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("food and produce",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("healthy food",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            general_supports_for_living_3=ifelse(grepl("general supports for living",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            transportation_3=ifelse(grepl("10b",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                                      grepl("transportation",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            otc_3=ifelse(grepl("13b",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                           grepl("otc",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            meals_3=ifelse(grepl("13c",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                             grepl("meal",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            Others_3=ifelse(grepl("16b",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("16c",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("home",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pet",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pest",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("social needs",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("other",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("indoor air",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("emergency",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            
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
            cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)

macvat_2025_benefit_with_geo_formatted_arranged <-
  macvat_2025_benefit_with_geo_formatted %>%
  transmute(year,
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
            
            monthly_allowance_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                             monthly_allowance_1,
                                             ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, monthly_allowance_2, monthly_allowance_3)),
            
            food_and_produce_allowance_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                      food_allowance_1,
                                                      ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, food_allowance_2, food_allowance_3)),
            
            general_supports_for_living_allowance_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                                 general_supports_for_living_allowance_1,
                                                                 ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, general_supports_for_living_allowance_2, general_supports_for_living_allowance_3)),
            
            eligibility_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                       eligibility_1,
                                       ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, eligibility_2, eligibility_3)),
            
            food_and_produce_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                            food_and_produce_1,
                                            ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, food_and_produce_2, food_and_produce_3)),
            
            general_supports_for_living_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                       general_supports_for_living_1,
                                                       ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, general_supports_for_living_2, general_supports_for_living_3)),
            
            transportation_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                          transportation_1,
                                          ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, transportation_2, transportation_3)),
            
            otc_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                               otc_1,
                               ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, otc_2, otc_3)),
            
            meals_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                 meals_1,
                                 ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, meals_2, meals_3)),
            
            Others_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                  Others_1,
                                  ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, Others_2, Others_3)),
            
            Conditions_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                      cy2025_vbidufssbci_group_1_additional_services_condition,
                                  ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, cy2025_vbidufssbci_group_2_additional_services_condition, cy2025_vbidufssbci_group_3_additional_services_condition)),
            
            
            monthly_allowance_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                             ifelse(monthly_allowance_2>monthly_allowance_3,monthly_allowance_2,monthly_allowance_3),
                                             ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                    ifelse(monthly_allowance_1>monthly_allowance_3,monthly_allowance_1,monthly_allowance_3), 
                                                    ifelse(monthly_allowance_1>monthly_allowance_2,monthly_allowance_1,monthly_allowance_2))),
            
            food_and_produce_allowance_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                        ifelse(monthly_allowance_2>monthly_allowance_3,food_allowance_2,food_allowance_3),
                                                        ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                               ifelse(monthly_allowance_1>monthly_allowance_3,food_allowance_1,food_allowance_3), 
                                                               ifelse(monthly_allowance_1>monthly_allowance_2,food_allowance_1,food_allowance_2))),
            
            general_supports_for_living_allowance_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                                   ifelse(monthly_allowance_2>monthly_allowance_3,general_supports_for_living_allowance_2,general_supports_for_living_allowance_3),
                                                                   ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                                          ifelse(monthly_allowance_1>monthly_allowance_3,general_supports_for_living_allowance_1,general_supports_for_living_allowance_3), 
                                                                          ifelse(monthly_allowance_1>monthly_allowance_2,general_supports_for_living_allowance_1,general_supports_for_living_allowance_2))),
            
            eligibility_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                         ifelse(monthly_allowance_2>monthly_allowance_3,eligibility_2,eligibility_3),
                                         ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                ifelse(monthly_allowance_1>monthly_allowance_3,eligibility_1,eligibility_3), 
                                                ifelse(monthly_allowance_1>monthly_allowance_2,eligibility_1,eligibility_2))),
            
            food_and_produce_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                              ifelse(monthly_allowance_2>monthly_allowance_3,food_and_produce_2,food_and_produce_3),
                                              ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                     ifelse(monthly_allowance_1>monthly_allowance_3,food_and_produce_1,food_and_produce_3), 
                                                     ifelse(monthly_allowance_1>monthly_allowance_2,food_and_produce_1,food_and_produce_2))),
            
            general_supports_for_living_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                         ifelse(monthly_allowance_2>monthly_allowance_3,general_supports_for_living_2,general_supports_for_living_3),
                                                         ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                                ifelse(monthly_allowance_1>monthly_allowance_3,general_supports_for_living_1,general_supports_for_living_3), 
                                                                ifelse(monthly_allowance_1>monthly_allowance_2,general_supports_for_living_1,general_supports_for_living_2))),
            
            transportation_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                            ifelse(monthly_allowance_2>monthly_allowance_3,transportation_2,transportation_3),
                                            ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                   ifelse(monthly_allowance_1>monthly_allowance_3,transportation_1,transportation_3), 
                                                   ifelse(monthly_allowance_1>monthly_allowance_2,transportation_1,transportation_2))),
            
            otc_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                 ifelse(monthly_allowance_2>monthly_allowance_3,otc_2,otc_3),
                                 ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                        ifelse(monthly_allowance_1>monthly_allowance_3,otc_1,otc_3), 
                                        ifelse(monthly_allowance_1>monthly_allowance_2,otc_1,otc_2))),
            
            meals_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                   ifelse(monthly_allowance_2>monthly_allowance_3,meals_2,meals_3),
                                   ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                          ifelse(monthly_allowance_1>monthly_allowance_3,meals_1,meals_3), 
                                          ifelse(monthly_allowance_1>monthly_allowance_2,meals_1,meals_2))),
            
            Others_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                    ifelse(monthly_allowance_2>monthly_allowance_3,Others_2,Others_3),
                                    ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                           ifelse(monthly_allowance_1>monthly_allowance_3,Others_1,Others_3), 
                                           ifelse(monthly_allowance_1>monthly_allowance_2,Others_1,Others_2))),
            
            Conditions_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                    ifelse(monthly_allowance_2>monthly_allowance_3,cy2025_vbidufssbci_group_2_additional_services_condition,cy2025_vbidufssbci_group_3_additional_services_condition),
                                    ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                           ifelse(monthly_allowance_1>monthly_allowance_3,cy2025_vbidufssbci_group_1_additional_services_condition,cy2025_vbidufssbci_group_3_additional_services_condition), 
                                           ifelse(monthly_allowance_1>monthly_allowance_2,cy2025_vbidufssbci_group_1_additional_services_condition,cy2025_vbidufssbci_group_2_additional_services_condition))),
            
            
            monthly_allowance_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                               ifelse(monthly_allowance_2>monthly_allowance_3,monthly_allowance_3,monthly_allowance_2),
                                               ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                      ifelse(monthly_allowance_1>monthly_allowance_3,monthly_allowance_3,monthly_allowance_1), 
                                                      ifelse(monthly_allowance_1>monthly_allowance_2,monthly_allowance_2,monthly_allowance_1))),
            
            food_and_produce_allowance_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                        ifelse(monthly_allowance_2>monthly_allowance_3,food_allowance_3,food_allowance_2),
                                                        ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                               ifelse(monthly_allowance_1>monthly_allowance_3,food_allowance_3,food_allowance_1), 
                                                               ifelse(monthly_allowance_1>monthly_allowance_2,food_allowance_2,food_allowance_1))),
            
            general_supports_for_living_allowance_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                                   ifelse(monthly_allowance_2>monthly_allowance_3,general_supports_for_living_allowance_3,general_supports_for_living_allowance_2),
                                                                   ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                                          ifelse(monthly_allowance_1>monthly_allowance_3,general_supports_for_living_allowance_3,general_supports_for_living_allowance_1), 
                                                                          ifelse(monthly_allowance_1>monthly_allowance_2,general_supports_for_living_allowance_2,general_supports_for_living_allowance_1))),
            
            eligibility_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                         ifelse(monthly_allowance_2>monthly_allowance_3,eligibility_3,eligibility_2),
                                         ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                ifelse(monthly_allowance_1>monthly_allowance_3,eligibility_3,eligibility_1), 
                                                ifelse(monthly_allowance_1>monthly_allowance_2,eligibility_2,eligibility_1))),
            
            food_and_produce_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                              ifelse(monthly_allowance_2>monthly_allowance_3,food_and_produce_3,food_and_produce_2),
                                              ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                     ifelse(monthly_allowance_1>monthly_allowance_3,food_and_produce_3,food_and_produce_1), 
                                                     ifelse(monthly_allowance_1>monthly_allowance_2,food_and_produce_2,food_and_produce_1))),
            
            general_supports_for_living_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                         ifelse(monthly_allowance_2>monthly_allowance_3,general_supports_for_living_3,general_supports_for_living_2),
                                                         ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                                ifelse(monthly_allowance_1>monthly_allowance_3,general_supports_for_living_3,general_supports_for_living_1), 
                                                                ifelse(monthly_allowance_1>monthly_allowance_2,general_supports_for_living_2,general_supports_for_living_1))),
            
            transportation_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                            ifelse(monthly_allowance_2>monthly_allowance_3,transportation_3,transportation_2),
                                            ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                   ifelse(monthly_allowance_1>monthly_allowance_3,transportation_3,transportation_1), 
                                                   ifelse(monthly_allowance_1>monthly_allowance_2,transportation_2,transportation_1))),
            
            otc_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                 ifelse(monthly_allowance_2>monthly_allowance_3,otc_3,otc_2),
                                 ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                        ifelse(monthly_allowance_1>monthly_allowance_3,otc_3,otc_1), 
                                        ifelse(monthly_allowance_1>monthly_allowance_2,otc_2,otc_1))),
            
            meals_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                   ifelse(monthly_allowance_2>monthly_allowance_3,meals_3,meals_2),
                                   ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                          ifelse(monthly_allowance_1>monthly_allowance_3,meals_3,meals_1), 
                                          ifelse(monthly_allowance_1>monthly_allowance_2,meals_2,meals_1))),
            
            Others_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                    ifelse(monthly_allowance_2>monthly_allowance_3,Others_3,Others_2),
                                    ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                           ifelse(monthly_allowance_1>monthly_allowance_3,Others_3,Others_1), 
                                           ifelse(monthly_allowance_1>monthly_allowance_2,Others_2,Others_1))),
            
            Conditions_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                ifelse(monthly_allowance_2>monthly_allowance_3,cy2025_vbidufssbci_group_3_additional_services_condition,cy2025_vbidufssbci_group_2_additional_services_condition),
                                ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                       ifelse(monthly_allowance_1>monthly_allowance_3,cy2025_vbidufssbci_group_3_additional_services_condition,cy2025_vbidufssbci_group_1_additional_services_condition), 
                                       ifelse(monthly_allowance_1>monthly_allowance_2,cy2025_vbidufssbci_group_2_additional_services_condition,cy2025_vbidufssbci_group_1_additional_services_condition)))) %>%
  
  transmute(year,
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
            
            # cy2025_overthecounter_drug_card,
            # cy2025_overthecounter_drug_card_period,
            
            
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
            
            # monthly_allowance_third,
            # food_and_produce_allowance_third,
            # general_supports_for_living_allowance_third,
            # eligibility_third,
            # food_and_produce_third,
            # general_supports_for_living_third,
            # transportation_third,
            # otc_third,
            # meals_third,
            # Others_third,
            
            anemia=ifelse(grepl("anemia",tolower(Conditions_primary)),"Y","N"),
            asthma=ifelse(grepl("asthma",tolower(Conditions_primary)),"Y","N"),
            autoimmune_disorder=ifelse(grepl("autoimmune disorder",tolower(Conditions_primary)),"Y","N"),
            arthritis=ifelse(grepl("arthritis",tolower(Conditions_primary)),"Y","N"),
            cardiovascular_disorder=ifelse(grepl("cardiovascular disorder",tolower(Conditions_primary)),"Y","N"),
            cellulitis=ifelse(grepl("cellulitis",tolower(Conditions_primary)),"Y","N"),
            circulatory_disease=ifelse(grepl("circulatory disease",tolower(Conditions_primary)),"Y","N"),
            chronic_alcohol_and_other_drug_dependence=ifelse(grepl("chronic alcohol and other drug dependence",tolower(Conditions_primary)),"Y","N"),
            chronic_cognitive_impairment=ifelse(grepl("chronic cognitive impairment",tolower(Conditions_primary)),"Y","N"),
            chronic_heart_failure=ifelse(grepl("chronic heart failure",tolower(Conditions_primary)),"Y","N"),
            chronic_gastrointestinal_disorder=ifelse(grepl("chronic gastrointestinal disorder",tolower(Conditions_primary)) |
                                                       grepl("chronic gi disorder",tolower(Conditions_primary)),"Y","N"),
            malnutrition=ifelse(grepl("malnutrition",tolower(Conditions_primary)),"Y","N"),
            chronic_infectious_disorder=ifelse(grepl("chronic infectious disorder",tolower(Conditions_primary)),"Y","N"),
            chronic_otolaryngological_disorder=ifelse(grepl("chronic otolaryngological disorder",tolower(Conditions_primary)),"Y","N"),
            chronic_pain=ifelse(grepl("chronic pain",tolower(Conditions_primary)),"Y","N"),
            cancer=ifelse(grepl("cancer",tolower(Conditions_primary)),"Y","N"),
            chronic_lung_disorder=ifelse(grepl("chronic lung disorder",tolower(Conditions_primary)),"Y","N"),
            chronic_liver_disease=ifelse(grepl("chronic liver",tolower(Conditions_primary)) |
                                           grepl("chronic kidney/liver",tolower(Conditions_primary)),"Y","N"),
            chronic_and_disabling_mental_health_condition=ifelse(grepl("chronic and disabling mental health condition",tolower(Conditions_primary)),"Y","N"),
            chronic_kidney_disease=ifelse(grepl("chronic kidney disease",tolower(Conditions_primary)) |
                                            grepl("chronic liver/kidney",tolower(Conditions_primary)),"Y","N"),
            chronic_non_alcoholic_fatty_liver_disease=ifelse(grepl("chronic non-alcohol",tolower(Conditions_primary)),"Y","N"),
            copd=ifelse(grepl("copd",tolower(Conditions_primary)) |
                          grepl("chronic obstructive pulmonary disease",tolower(Conditions_primary)),"Y","N"),
            congestive_heart_failure=ifelse(grepl("congestive heart failure",tolower(Conditions_primary)),"Y","N"),
            coronary_artery_disease=ifelse(grepl("coronary artery disease",tolower(Conditions_primary)),"Y","N"),
            cystic_fibrosis=ifelse(grepl("cystic fibrosis",tolower(Conditions_primary)),"Y","N"),
            down_syndrome=ifelse(grepl("down syndrome",tolower(Conditions_primary)),"Y","N"),
            diabete=ifelse(grepl("diabete",tolower(Conditions_primary)),"Y","N"),
            dementia=ifelse(grepl("dementia",tolower(Conditions_primary)),"Y","N"),
            dyslipidemia=ifelse(grepl("dyslipidemia",tolower(Conditions_primary)),"Y","N"),
            eating_disorder=ifelse(grepl("eating disorder",tolower(Conditions_primary)),"Y","N"),
            end_stage_liver_disease=ifelse(grepl("end-stage liver disease",tolower(Conditions_primary)),"Y","N"),
            end_stage_renal_disease=ifelse(grepl("end-stage renal disease",tolower(Conditions_primary)),"Y","N"),
            endometriosis=ifelse(grepl("endometriosis",tolower(Conditions_primary)),"Y","N"),
            endocrine=ifelse(grepl("endocrine",tolower(Conditions_primary)),"Y","N"),
            gastrointestinal=ifelse(grepl("gastrointestinal",tolower(Conditions_primary)),"Y","N"),
            glaucoma=ifelse(grepl("glaucoma",tolower(Conditions_primary)),"Y","N"),
            hiv=ifelse(grepl("hiv",tolower(Conditions_primary)),"Y","N"),
            hepatitis=ifelse(grepl("hepatitis",tolower(Conditions_primary)),"Y","N"),
            hypertension=ifelse(grepl("hypertension",tolower(Conditions_primary)),"Y","N"),
            hyperlipidemia=ifelse(grepl("hyperlipidemia",tolower(Conditions_primary)) |
                                    grepl("chronic lipid",tolower(Conditions_primary)),"Y","N"),
            hypercholesterolemia=ifelse(grepl("hypercholesterolemia",tolower(Conditions_primary)),"Y","N"),
            inflammatory_bowel_disease=ifelse(grepl("inflammatory bowel disease",tolower(Conditions_primary)),"Y","N"),
            joints_and_spine=ifelse(grepl("joints",tolower(Conditions_primary)),"Y","N"),
            loss_of_limb=ifelse(grepl("loss of limb",tolower(Conditions_primary)),"Y","N"),
            low_back_pain=ifelse(grepl("low back pain",tolower(Conditions_primary)),"Y","N"),
            metabolic_syndrome=ifelse(grepl("metabolic syndrome",tolower(Conditions_primary)),"Y","N"),
            muscular_dystrophy=ifelse(grepl("muscular dystrophy",tolower(Conditions_primary)),"Y","N"),
            musculoskeletal_disorder=ifelse(grepl("musculoskeletal disorder",tolower(Conditions_primary)),"Y","N"),
            neurologic_disorder=ifelse(grepl("neurologic disorder",tolower(Conditions_primary)),"Y","N"),
            osteoporosis=ifelse(grepl("osteoporosis",tolower(Conditions_primary)),"Y","N"),
            obesity=ifelse(grepl("obesity",tolower(Conditions_primary)) |
                             grepl("obese",tolower(Conditions_primary)),"Y","N"),
            pneumonia=ifelse(grepl("pneumonia",tolower(Conditions_primary)),"Y","N"),
            pregnancy=ifelse(grepl("pregnancy",tolower(Conditions_primary)),"Y","N"),
            rsd=ifelse(grepl("rsd",tolower(Conditions_primary)),"Y","N"),
            sjogren=ifelse(grepl("sjogren",tolower(Conditions_primary)),"Y","N"),
            severe_hematologic_disorder=ifelse(grepl("severe hematologic disorder",tolower(Conditions_primary)),"Y","N"),
            stroke=ifelse(grepl("stroke",tolower(Conditions_primary)),"Y","N"),
            urinary_tract_infection=ifelse(grepl("urinary tract infection",tolower(Conditions_primary)),"Y","N"),
            urinary_incontinance=ifelse(grepl("urinary incontinance",tolower(Conditions_primary)),"Y","N"),
            vascular_disease=ifelse(grepl("vascular disease",tolower(Conditions_primary)),"Y","N")
            
            )


macvat_2025_benefit_with_geo_formatted_arranged_plan <-
  macvat_2025_benefit_with_geo_formatted_arranged %>% 
  select(-state,-county,-fips,-ssa) %>%
  distinct()


macvat_2025_benefit_with_geo_formatted_arranged_plan_fips <-
  macvat_2025_benefit_with_geo_formatted_arranged %>% 
  select(year,contract_plan,contract_id,PBP,segment,cy2025_plan_type,cy2025_snp_type,cy2025_snp_detail,cy2025_dual_integration_status,
         cy2025_part_c_part_d_coverage,cy2025_plan_name,parent_name,new_plan_flag,cy2025_vbidufssbci_indicator,state,county,fips,ssa) %>%
  distinct()

macvat_2025_benefit_with_geo_formatted_arranged_plan <-
macvat_2025_benefit_with_geo_formatted_arranged_plan %>%
  left_join(read_xlsx("UHC Plan Data Grid.xlsx") %>%
              select(`2025 Contract - Plan - Segment`,`Food/Utilities 2025`) %>%
              rename(contract_plan=`2025 Contract - Plan - Segment`,
                     section_19_benefits=`Food/Utilities 2025`) %>%
              mutate(contract_plan=trimws(contract_plan),
                     section_19_dollars=str_extract(section_19_benefits,"[0-9]+")) %>%
              mutate(section_19_dollars=ifelse(is.na(section_19_dollars),0,section_19_dollars)) %>%
              distinct() %>%
              select(contract_plan,section_19_dollars),by="contract_plan") %>%
  mutate(monthly_allowance_primary=ifelse(!is.na(section_19_dollars),section_19_dollars,monthly_allowance_primary),
         otc_primary=ifelse(!is.na(section_19_dollars),"Yes",otc_primary)) %>%
  select(-section_19_dollars)


macvat_2024_benefit  <- dbGetQuery(con, 
                                   "
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
          on a.contract_plan=b.contract_plan ") %>%
  
  mutate(contract_plan = gsub(" ", "", contract_plan)) %>%
  transmute(year,
            contract_plan,
            contract_id=substr(contract_plan,1,5),
            PBP=substr(contract_plan,7,9),
            segment=substr(contract_plan,11,13),
            cy2025_plan_type,
            cy2025_snp_type,
            cy2025_snp_detail,
            cy2025_dual_integration_status,
            cy2025_part_c_part_d_coverage,
            cy2025_plan_name,
            parent_name,
            new_plan_flag,
            cy2025_vbidufssbci_indicator,
            
            ssa=sprintf("%05d", as.numeric(ssa_code)),
            
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
            cy2025_vbidufssbci_group_5_additional_services_nonmedicare_covered_benefits) %>%
  distinct()

macvat_2024_benefit_with_geo <-
  macvat_2024_benefit %>%
  left_join(fips_ssa,by=c("ssa")) %>%
  inner_join(aep_footprint_history %>% filter(year==2025) %>% select(fips,state,county))


macvat_2024_benefit_with_geo_formatted <-
  macvat_2024_benefit_with_geo %>%
  mutate(food_and_produce_string_1=str_extract(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,"(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))"),
         food_and_produce_string_2=str_extract(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,"(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))"),
         food_and_produce_string_3=str_extract(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,"(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))"),
         healthy_food_string_1=str_extract(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,"\\(Healthy Food.*?\\)|\\(Living Expense Support.*?\\)"),
         healthy_food_string_2=str_extract(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,"\\(Healthy Food.*?\\)|\\(Living Expense Support.*?\\)"),
         healthy_food_string_3=str_extract(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,"\\(Healthy Food.*?\\)|\\(Living Expense Support.*?\\)"),
         
         general_supports_for_living_string_1=str_extract(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,"General Supports for Living \\(.*?\\)"),
         general_supports_for_living_string_1_alter=str_extract(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,"\\(Utilities\\), \\$[0-9]+.*"),
         general_supports_for_living_string_2=str_extract(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,"General Supports for Living \\(.*?\\)"),
         general_supports_for_living_string_2_alter=str_extract(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,"\\(Utilities\\), \\$[0-9]+.*"),
         general_supports_for_living_string_3=str_extract(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,"General Supports for Living \\(.*?\\)"),
         general_supports_for_living_string_3_alter=str_extract(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,"\\(Utilities\\), \\$[0-9]+.*"),
         food_and_produce_allowance_1=as.numeric(str_extract(food_and_produce_string_1,"[0-9]+")) *
           ifelse(grepl("every month",tolower(food_and_produce_string_1)),1,
                  ifelse(grepl("every three months",tolower(food_and_produce_string_1)),1/3,1/12)),
         food_and_produce_allowance_2=as.numeric(str_extract(food_and_produce_string_2,"[0-9]+")) *
           ifelse(grepl("every month",tolower(food_and_produce_string_2)),1,
                  ifelse(grepl("every three months",tolower(food_and_produce_string_2)),1/3,1/12)),
         food_and_produce_allowance_3=as.numeric(str_extract(food_and_produce_string_3,"[0-9]+")) *
           ifelse(grepl("every month",tolower(food_and_produce_string_3)),1,
                  ifelse(grepl("every three months",tolower(food_and_produce_string_3)),1/3,1/12)),
         healthy_food_allowance_1=as.numeric(str_extract(healthy_food_string_1,"[0-9]+")) *
           ifelse(grepl("every month",tolower(healthy_food_string_1)),1,
                  ifelse(grepl("every three months",tolower(healthy_food_string_1)),1/3,1/12)),
         healthy_food_allowance_2=as.numeric(str_extract(healthy_food_string_2,"[0-9]+")) *
           ifelse(grepl("every month",tolower(healthy_food_string_2)),1,
                  ifelse(grepl("every three months",tolower(healthy_food_string_2)),1/3,1/12)),
         healthy_food_allowance_3=as.numeric(str_extract(healthy_food_string_3,"[0-9]+")) *
           ifelse(grepl("every month",tolower(healthy_food_string_3)),1,
                  ifelse(grepl("every three months",tolower(healthy_food_string_3)),1/3,1/12)),
         general_supports_for_living_allowance_1=as.numeric(str_extract(general_supports_for_living_string_1,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_1)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_1)),1/3,1/12)),
         general_supports_for_living_allowance_1_alter=as.numeric(str_extract(general_supports_for_living_string_1_alter,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_1_alter)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_1_alter)),1/3,1/12)),
         general_supports_for_living_allowance_2=as.numeric(str_extract(general_supports_for_living_string_2,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_2)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_2)),1/3,1/12)),
         general_supports_for_living_allowance_2_alter=as.numeric(str_extract(general_supports_for_living_string_2_alter,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_2_alter)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_2_alter)),1/3,1/12)),
         general_supports_for_living_allowance_3=as.numeric(str_extract(general_supports_for_living_string_3,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_3)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_3)),1/3,1/12)),
         general_supports_for_living_allowance_3_alter=as.numeric(str_extract(general_supports_for_living_string_3_alter,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_3_alter)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_3_alter)),1/3,1/12)),
         food_and_produce_allowance_1=ifelse(is.na(food_and_produce_allowance_1),0,food_and_produce_allowance_1),
         food_and_produce_allowance_2=ifelse(is.na(food_and_produce_allowance_2),0,food_and_produce_allowance_2),
         food_and_produce_allowance_3=ifelse(is.na(food_and_produce_allowance_3),0,food_and_produce_allowance_3),
         healthy_food_allowance_1=ifelse(is.na(healthy_food_allowance_1),0,healthy_food_allowance_1),
         healthy_food_allowance_2=ifelse(is.na(healthy_food_allowance_2),0,healthy_food_allowance_2),
         healthy_food_allowance_3=ifelse(is.na(healthy_food_allowance_3),0,healthy_food_allowance_3),
         general_supports_for_living_allowance_1=ifelse(is.na(general_supports_for_living_allowance_1),general_supports_for_living_allowance_1_alter,general_supports_for_living_allowance_1),
         general_supports_for_living_allowance_1=ifelse(is.na(general_supports_for_living_allowance_1),0,general_supports_for_living_allowance_1),
         general_supports_for_living_allowance_2=ifelse(is.na(general_supports_for_living_allowance_2),general_supports_for_living_allowance_2_alter,general_supports_for_living_allowance_2),
         general_supports_for_living_allowance_2=ifelse(is.na(general_supports_for_living_allowance_2),0,general_supports_for_living_allowance_2),
         general_supports_for_living_allowance_3=ifelse(is.na(general_supports_for_living_allowance_3),general_supports_for_living_allowance_3_alter,general_supports_for_living_allowance_3),
         general_supports_for_living_allowance_3=ifelse(is.na(general_supports_for_living_allowance_3),0,general_supports_for_living_allowance_3)) %>%
  transmute(year,
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
            
            monthly_allowance_1=ifelse(is.na(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period),1,
                                       ifelse(tolower(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period)=="every month",1,
                                              ifelse(tolower(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period)=="every three months",1/3,
                                                     ifelse(tolower(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period)=="every year",1/12,1)))) *
              ifelse(is.na(as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_1_additional_services_aggregate_limit))),
                     food_and_produce_allowance_1+healthy_food_allowance_1+general_supports_for_living_allowance_1,as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_1_additional_services_aggregate_limit))),
            food_allowance_1=food_and_produce_allowance_1+healthy_food_allowance_1,
            general_supports_for_living_allowance_1,
            
            eligibility_1=ifelse(grepl("vbid",tolower(cy2025_vbidufssbci_group_1_additional_services_condition)),"VBID",
                                 ifelse(grepl("ssbci",tolower(cy2025_vbidufssbci_group_1_additional_services_condition)),"SSBCI",
                                        ifelse(grepl("uf",tolower(cy2025_vbidufssbci_group_1_additional_services_condition)),"UF",""))),
            food_and_produce_1=ifelse(grepl("13j",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("food and produce",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("healthy food",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            general_supports_for_living_1=ifelse(grepl("general supports for living",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            transportation_1=ifelse(grepl("10b",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                                      grepl("transportation",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            otc_1=ifelse(grepl("13b",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                           grepl("otc",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            meals_1=ifelse(grepl("13c",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                             grepl("meal",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            Others_1=ifelse(grepl("16b",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("16c",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("home",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pet",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pest",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("social needs",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("other",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("indoor air",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("emergency",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            
            monthly_allowance_2=ifelse(is.na(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period),1,
                                       ifelse(tolower(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period)=="every month",1,
                                              ifelse(tolower(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period)=="every three months",1/3,
                                                     ifelse(tolower(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period)=="every year",1/12,1)))) *
              ifelse(is.na(as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_2_additional_services_aggregate_limit))),
                     food_and_produce_allowance_2+healthy_food_allowance_2+general_supports_for_living_allowance_2,as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_2_additional_services_aggregate_limit))),
            food_allowance_2=food_and_produce_allowance_2+healthy_food_allowance_2,
            general_supports_for_living_allowance_2,
            
            eligibility_2=ifelse(grepl("vbid",tolower(cy2025_vbidufssbci_group_2_additional_services_condition)),"VBID",
                                 ifelse(grepl("ssbci",tolower(cy2025_vbidufssbci_group_2_additional_services_condition)),"SSBCI",
                                        ifelse(grepl("uf",tolower(cy2025_vbidufssbci_group_2_additional_services_condition)),"UF",""))),
            food_and_produce_2=ifelse(grepl("13j",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("food and produce",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("healthy food",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            general_supports_for_living_2=ifelse(grepl("general supports for living",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            transportation_2=ifelse(grepl("10b",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                                      grepl("transportation",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            otc_2=ifelse(grepl("13b",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                           grepl("otc",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            meals_2=ifelse(grepl("13c",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                             grepl("meal",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            Others_2=ifelse(grepl("16b",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("16c",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("home",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pet",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pest",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("social needs",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("other",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("indoor air",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("emergency",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            
            monthly_allowance_3=ifelse(is.na(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period),1,
                                       ifelse(tolower(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period)=="every month",1,
                                              ifelse(tolower(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period)=="every three months",1/3,
                                                     ifelse(tolower(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period)=="every year",1/12,1)))) *
              ifelse(is.na(as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_3_additional_services_aggregate_limit))),
                     food_and_produce_allowance_3+healthy_food_allowance_3+general_supports_for_living_allowance_3,as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_3_additional_services_aggregate_limit))),
            food_allowance_3=food_and_produce_allowance_3+healthy_food_allowance_3,
            general_supports_for_living_allowance_3,
            
            eligibility_3=ifelse(grepl("vbid",tolower(cy2025_vbidufssbci_group_3_additional_services_condition)),"VBID",
                                 ifelse(grepl("ssbci",tolower(cy2025_vbidufssbci_group_3_additional_services_condition)),"SSBCI",
                                        ifelse(grepl("uf",tolower(cy2025_vbidufssbci_group_3_additional_services_condition)),"UF",""))),
            food_and_produce_3=ifelse(grepl("13j",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("food and produce",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("healthy food",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            general_supports_for_living_3=ifelse(grepl("general supports for living",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            transportation_3=ifelse(grepl("10b",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                                      grepl("transportation",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            otc_3=ifelse(grepl("13b",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                           grepl("otc",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            meals_3=ifelse(grepl("13c",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                             grepl("meal",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            Others_3=ifelse(grepl("16b",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("16c",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("home",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pet",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pest",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("social needs",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("other",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("indoor air",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("emergency",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            
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
            cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)

macvat_2024_benefit_with_geo_formatted_arranged <-
  macvat_2024_benefit_with_geo_formatted %>%
  transmute(year,
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
            
            monthly_allowance_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                             monthly_allowance_1,
                                             ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, monthly_allowance_2, monthly_allowance_3)),
            
            food_and_produce_allowance_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                      food_allowance_1,
                                                      ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, food_allowance_2, food_allowance_3)),
            
            general_supports_for_living_allowance_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                                 general_supports_for_living_allowance_1,
                                                                 ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, general_supports_for_living_allowance_2, general_supports_for_living_allowance_3)),
            
            eligibility_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                       eligibility_1,
                                       ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, eligibility_2, eligibility_3)),
            
            food_and_produce_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                            food_and_produce_1,
                                            ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, food_and_produce_2, food_and_produce_3)),
            
            general_supports_for_living_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                       general_supports_for_living_1,
                                                       ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, general_supports_for_living_2, general_supports_for_living_3)),
            
            transportation_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                          transportation_1,
                                          ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, transportation_2, transportation_3)),
            
            otc_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                               otc_1,
                               ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, otc_2, otc_3)),
            
            meals_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                 meals_1,
                                 ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, meals_2, meals_3)),
            
            Others_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                  Others_1,
                                  ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, Others_2, Others_3)),
            
            Conditions_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                      cy2025_vbidufssbci_group_1_additional_services_condition,
                                      ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, cy2025_vbidufssbci_group_2_additional_services_condition, cy2025_vbidufssbci_group_3_additional_services_condition)),
            
            
            monthly_allowance_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                               ifelse(monthly_allowance_2>monthly_allowance_3,monthly_allowance_2,monthly_allowance_3),
                                               ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                      ifelse(monthly_allowance_1>monthly_allowance_3,monthly_allowance_1,monthly_allowance_3), 
                                                      ifelse(monthly_allowance_1>monthly_allowance_2,monthly_allowance_1,monthly_allowance_2))),
            
            food_and_produce_allowance_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                        ifelse(monthly_allowance_2>monthly_allowance_3,food_allowance_2,food_allowance_3),
                                                        ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                               ifelse(monthly_allowance_1>monthly_allowance_3,food_allowance_1,food_allowance_3), 
                                                               ifelse(monthly_allowance_1>monthly_allowance_2,food_allowance_1,food_allowance_2))),
            
            general_supports_for_living_allowance_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                                   ifelse(monthly_allowance_2>monthly_allowance_3,general_supports_for_living_allowance_2,general_supports_for_living_allowance_3),
                                                                   ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                                          ifelse(monthly_allowance_1>monthly_allowance_3,general_supports_for_living_allowance_1,general_supports_for_living_allowance_3), 
                                                                          ifelse(monthly_allowance_1>monthly_allowance_2,general_supports_for_living_allowance_1,general_supports_for_living_allowance_2))),
            
            eligibility_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                         ifelse(monthly_allowance_2>monthly_allowance_3,eligibility_2,eligibility_3),
                                         ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                ifelse(monthly_allowance_1>monthly_allowance_3,eligibility_1,eligibility_3), 
                                                ifelse(monthly_allowance_1>monthly_allowance_2,eligibility_1,eligibility_2))),
            
            food_and_produce_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                              ifelse(monthly_allowance_2>monthly_allowance_3,food_and_produce_2,food_and_produce_3),
                                              ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                     ifelse(monthly_allowance_1>monthly_allowance_3,food_and_produce_1,food_and_produce_3), 
                                                     ifelse(monthly_allowance_1>monthly_allowance_2,food_and_produce_1,food_and_produce_2))),
            
            general_supports_for_living_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                         ifelse(monthly_allowance_2>monthly_allowance_3,general_supports_for_living_2,general_supports_for_living_3),
                                                         ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                                ifelse(monthly_allowance_1>monthly_allowance_3,general_supports_for_living_1,general_supports_for_living_3), 
                                                                ifelse(monthly_allowance_1>monthly_allowance_2,general_supports_for_living_1,general_supports_for_living_2))),
            
            transportation_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                            ifelse(monthly_allowance_2>monthly_allowance_3,transportation_2,transportation_3),
                                            ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                   ifelse(monthly_allowance_1>monthly_allowance_3,transportation_1,transportation_3), 
                                                   ifelse(monthly_allowance_1>monthly_allowance_2,transportation_1,transportation_2))),
            
            otc_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                 ifelse(monthly_allowance_2>monthly_allowance_3,otc_2,otc_3),
                                 ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                        ifelse(monthly_allowance_1>monthly_allowance_3,otc_1,otc_3), 
                                        ifelse(monthly_allowance_1>monthly_allowance_2,otc_1,otc_2))),
            
            meals_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                   ifelse(monthly_allowance_2>monthly_allowance_3,meals_2,meals_3),
                                   ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                          ifelse(monthly_allowance_1>monthly_allowance_3,meals_1,meals_3), 
                                          ifelse(monthly_allowance_1>monthly_allowance_2,meals_1,meals_2))),
            
            Others_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                    ifelse(monthly_allowance_2>monthly_allowance_3,Others_2,Others_3),
                                    ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                           ifelse(monthly_allowance_1>monthly_allowance_3,Others_1,Others_3), 
                                           ifelse(monthly_allowance_1>monthly_allowance_2,Others_1,Others_2))),
            
            Conditions_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                        ifelse(monthly_allowance_2>monthly_allowance_3,cy2025_vbidufssbci_group_2_additional_services_condition,cy2025_vbidufssbci_group_3_additional_services_condition),
                                        ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                               ifelse(monthly_allowance_1>monthly_allowance_3,cy2025_vbidufssbci_group_1_additional_services_condition,cy2025_vbidufssbci_group_3_additional_services_condition), 
                                               ifelse(monthly_allowance_1>monthly_allowance_2,cy2025_vbidufssbci_group_1_additional_services_condition,cy2025_vbidufssbci_group_2_additional_services_condition))),
            
            
            monthly_allowance_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                           ifelse(monthly_allowance_2>monthly_allowance_3,monthly_allowance_3,monthly_allowance_2),
                                           ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                  ifelse(monthly_allowance_1>monthly_allowance_3,monthly_allowance_3,monthly_allowance_1), 
                                                  ifelse(monthly_allowance_1>monthly_allowance_2,monthly_allowance_2,monthly_allowance_1))),
            
            food_and_produce_allowance_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                    ifelse(monthly_allowance_2>monthly_allowance_3,food_allowance_3,food_allowance_2),
                                                    ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                           ifelse(monthly_allowance_1>monthly_allowance_3,food_allowance_3,food_allowance_1), 
                                                           ifelse(monthly_allowance_1>monthly_allowance_2,food_allowance_2,food_allowance_1))),
            
            general_supports_for_living_allowance_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                               ifelse(monthly_allowance_2>monthly_allowance_3,general_supports_for_living_allowance_3,general_supports_for_living_allowance_2),
                                                               ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                                      ifelse(monthly_allowance_1>monthly_allowance_3,general_supports_for_living_allowance_3,general_supports_for_living_allowance_1), 
                                                                      ifelse(monthly_allowance_1>monthly_allowance_2,general_supports_for_living_allowance_2,general_supports_for_living_allowance_1))),
            
            eligibility_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                     ifelse(monthly_allowance_2>monthly_allowance_3,eligibility_3,eligibility_2),
                                     ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                            ifelse(monthly_allowance_1>monthly_allowance_3,eligibility_3,eligibility_1), 
                                            ifelse(monthly_allowance_1>monthly_allowance_2,eligibility_2,eligibility_1))),
            
            food_and_produce_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                          ifelse(monthly_allowance_2>monthly_allowance_3,food_and_produce_3,food_and_produce_2),
                                          ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                 ifelse(monthly_allowance_1>monthly_allowance_3,food_and_produce_3,food_and_produce_1), 
                                                 ifelse(monthly_allowance_1>monthly_allowance_2,food_and_produce_2,food_and_produce_1))),
            
            general_supports_for_living_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                     ifelse(monthly_allowance_2>monthly_allowance_3,general_supports_for_living_3,general_supports_for_living_2),
                                                     ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                            ifelse(monthly_allowance_1>monthly_allowance_3,general_supports_for_living_3,general_supports_for_living_1), 
                                                            ifelse(monthly_allowance_1>monthly_allowance_2,general_supports_for_living_2,general_supports_for_living_1))),
            
            transportation_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                        ifelse(monthly_allowance_2>monthly_allowance_3,transportation_3,transportation_2),
                                        ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                               ifelse(monthly_allowance_1>monthly_allowance_3,transportation_3,transportation_1), 
                                               ifelse(monthly_allowance_1>monthly_allowance_2,transportation_2,transportation_1))),
            
            otc_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                             ifelse(monthly_allowance_2>monthly_allowance_3,otc_3,otc_2),
                             ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                    ifelse(monthly_allowance_1>monthly_allowance_3,otc_3,otc_1), 
                                    ifelse(monthly_allowance_1>monthly_allowance_2,otc_2,otc_1))),
            
            meals_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                               ifelse(monthly_allowance_2>monthly_allowance_3,meals_3,meals_2),
                               ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                      ifelse(monthly_allowance_1>monthly_allowance_3,meals_3,meals_1), 
                                      ifelse(monthly_allowance_1>monthly_allowance_2,meals_2,meals_1))),
            
            Others_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                ifelse(monthly_allowance_2>monthly_allowance_3,Others_3,Others_2),
                                ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                       ifelse(monthly_allowance_1>monthly_allowance_3,Others_3,Others_1), 
                                       ifelse(monthly_allowance_1>monthly_allowance_2,Others_2,Others_1))),
            
            Conditions_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                    ifelse(monthly_allowance_2>monthly_allowance_3,cy2025_vbidufssbci_group_3_additional_services_condition,cy2025_vbidufssbci_group_2_additional_services_condition),
                                    ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                           ifelse(monthly_allowance_1>monthly_allowance_3,cy2025_vbidufssbci_group_3_additional_services_condition,cy2025_vbidufssbci_group_1_additional_services_condition), 
                                           ifelse(monthly_allowance_1>monthly_allowance_2,cy2025_vbidufssbci_group_2_additional_services_condition,cy2025_vbidufssbci_group_1_additional_services_condition)))) %>%
  
  transmute(year,
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
            
            # cy2025_overthecounter_drug_card,
            # cy2025_overthecounter_drug_card_period,
            
            
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
            
            # monthly_allowance_third,
            # food_and_produce_allowance_third,
            # general_supports_for_living_allowance_third,
            # eligibility_third,
            # food_and_produce_third,
            # general_supports_for_living_third,
            # transportation_third,
            # otc_third,
            # meals_third,
            # Others_third,
            
            anemia=ifelse(grepl("anemia",tolower(Conditions_primary)),"Y","N"),
            asthma=ifelse(grepl("asthma",tolower(Conditions_primary)),"Y","N"),
            autoimmune_disorder=ifelse(grepl("autoimmune disorder",tolower(Conditions_primary)),"Y","N"),
            arthritis=ifelse(grepl("arthritis",tolower(Conditions_primary)),"Y","N"),
            cardiovascular_disorder=ifelse(grepl("cardiovascular disorder",tolower(Conditions_primary)),"Y","N"),
            cellulitis=ifelse(grepl("cellulitis",tolower(Conditions_primary)),"Y","N"),
            circulatory_disease=ifelse(grepl("circulatory disease",tolower(Conditions_primary)),"Y","N"),
            chronic_alcohol_and_other_drug_dependence=ifelse(grepl("chronic alcohol and other drug dependence",tolower(Conditions_primary)),"Y","N"),
            chronic_cognitive_impairment=ifelse(grepl("chronic cognitive impairment",tolower(Conditions_primary)),"Y","N"),
            chronic_heart_failure=ifelse(grepl("chronic heart failure",tolower(Conditions_primary)),"Y","N"),
            chronic_gastrointestinal_disorder=ifelse(grepl("chronic gastrointestinal disorder",tolower(Conditions_primary)) |
                                                       grepl("chronic gi disorder",tolower(Conditions_primary)),"Y","N"),
            malnutrition=ifelse(grepl("malnutrition",tolower(Conditions_primary)),"Y","N"),
            chronic_infectious_disorder=ifelse(grepl("chronic infectious disorder",tolower(Conditions_primary)),"Y","N"),
            chronic_otolaryngological_disorder=ifelse(grepl("chronic otolaryngological disorder",tolower(Conditions_primary)),"Y","N"),
            chronic_pain=ifelse(grepl("chronic pain",tolower(Conditions_primary)),"Y","N"),
            cancer=ifelse(grepl("cancer",tolower(Conditions_primary)),"Y","N"),
            chronic_lung_disorder=ifelse(grepl("chronic lung disorder",tolower(Conditions_primary)),"Y","N"),
            chronic_liver_disease=ifelse(grepl("chronic liver",tolower(Conditions_primary)) |
                                           grepl("chronic kidney/liver",tolower(Conditions_primary)),"Y","N"),
            chronic_and_disabling_mental_health_condition=ifelse(grepl("chronic and disabling mental health condition",tolower(Conditions_primary)),"Y","N"),
            chronic_kidney_disease=ifelse(grepl("chronic kidney disease",tolower(Conditions_primary)) |
                                            grepl("chronic liver/kidney",tolower(Conditions_primary)),"Y","N"),
            chronic_non_alcoholic_fatty_liver_disease=ifelse(grepl("chronic non-alcohol",tolower(Conditions_primary)),"Y","N"),
            copd=ifelse(grepl("copd",tolower(Conditions_primary)) |
                          grepl("chronic obstructive pulmonary disease",tolower(Conditions_primary)),"Y","N"),
            congestive_heart_failure=ifelse(grepl("congestive heart failure",tolower(Conditions_primary)),"Y","N"),
            coronary_artery_disease=ifelse(grepl("coronary artery disease",tolower(Conditions_primary)),"Y","N"),
            cystic_fibrosis=ifelse(grepl("cystic fibrosis",tolower(Conditions_primary)),"Y","N"),
            down_syndrome=ifelse(grepl("down syndrome",tolower(Conditions_primary)),"Y","N"),
            diabete=ifelse(grepl("diabete",tolower(Conditions_primary)),"Y","N"),
            dementia=ifelse(grepl("dementia",tolower(Conditions_primary)),"Y","N"),
            dyslipidemia=ifelse(grepl("dyslipidemia",tolower(Conditions_primary)),"Y","N"),
            eating_disorder=ifelse(grepl("eating disorder",tolower(Conditions_primary)),"Y","N"),
            end_stage_liver_disease=ifelse(grepl("end-stage liver disease",tolower(Conditions_primary)),"Y","N"),
            end_stage_renal_disease=ifelse(grepl("end-stage renal disease",tolower(Conditions_primary)),"Y","N"),
            endometriosis=ifelse(grepl("endometriosis",tolower(Conditions_primary)),"Y","N"),
            endocrine=ifelse(grepl("endocrine",tolower(Conditions_primary)),"Y","N"),
            gastrointestinal=ifelse(grepl("gastrointestinal",tolower(Conditions_primary)),"Y","N"),
            glaucoma=ifelse(grepl("glaucoma",tolower(Conditions_primary)),"Y","N"),
            hiv=ifelse(grepl("hiv",tolower(Conditions_primary)),"Y","N"),
            hepatitis=ifelse(grepl("hepatitis",tolower(Conditions_primary)),"Y","N"),
            hypertension=ifelse(grepl("hypertension",tolower(Conditions_primary)),"Y","N"),
            hyperlipidemia=ifelse(grepl("hyperlipidemia",tolower(Conditions_primary)) |
                                    grepl("chronic lipid",tolower(Conditions_primary)),"Y","N"),
            hypercholesterolemia=ifelse(grepl("hypercholesterolemia",tolower(Conditions_primary)),"Y","N"),
            inflammatory_bowel_disease=ifelse(grepl("inflammatory bowel disease",tolower(Conditions_primary)),"Y","N"),
            joints_and_spine=ifelse(grepl("joints",tolower(Conditions_primary)),"Y","N"),
            loss_of_limb=ifelse(grepl("loss of limb",tolower(Conditions_primary)),"Y","N"),
            low_back_pain=ifelse(grepl("low back pain",tolower(Conditions_primary)),"Y","N"),
            metabolic_syndrome=ifelse(grepl("metabolic syndrome",tolower(Conditions_primary)),"Y","N"),
            muscular_dystrophy=ifelse(grepl("muscular dystrophy",tolower(Conditions_primary)),"Y","N"),
            musculoskeletal_disorder=ifelse(grepl("musculoskeletal disorder",tolower(Conditions_primary)),"Y","N"),
            neurologic_disorder=ifelse(grepl("neurologic disorder",tolower(Conditions_primary)),"Y","N"),
            osteoporosis=ifelse(grepl("osteoporosis",tolower(Conditions_primary)),"Y","N"),
            obesity=ifelse(grepl("obesity",tolower(Conditions_primary)) |
                             grepl("obese",tolower(Conditions_primary)),"Y","N"),
            pneumonia=ifelse(grepl("pneumonia",tolower(Conditions_primary)),"Y","N"),
            pregnancy=ifelse(grepl("pregnancy",tolower(Conditions_primary)),"Y","N"),
            rsd=ifelse(grepl("rsd",tolower(Conditions_primary)),"Y","N"),
            sjogren=ifelse(grepl("sjogren",tolower(Conditions_primary)),"Y","N"),
            severe_hematologic_disorder=ifelse(grepl("severe hematologic disorder",tolower(Conditions_primary)),"Y","N"),
            stroke=ifelse(grepl("stroke",tolower(Conditions_primary)),"Y","N"),
            urinary_tract_infection=ifelse(grepl("urinary tract infection",tolower(Conditions_primary)),"Y","N"),
            urinary_incontinance=ifelse(grepl("urinary incontinance",tolower(Conditions_primary)),"Y","N"),
            vascular_disease=ifelse(grepl("vascular disease",tolower(Conditions_primary)),"Y","N")
            
  )

macvat_2024_benefit_with_geo_formatted_arranged_plan <-
  macvat_2024_benefit_with_geo_formatted_arranged %>% 
  select(-state,-county,-fips,-ssa) %>%
  distinct()

macvat_2024_benefit_with_geo_formatted_arranged_plan_fips <-
  macvat_2024_benefit_with_geo_formatted_arranged %>% 
  select(year,contract_plan,contract_id,PBP,segment,cy2025_plan_type,cy2025_snp_type,cy2025_snp_detail,cy2025_dual_integration_status,
         cy2025_part_c_part_d_coverage,cy2025_plan_name,parent_name,new_plan_flag,cy2025_vbidufssbci_indicator,state,county,fips,ssa) %>%
  distinct()


macvat_2023_benefit  <- dbGetQuery(con, 
                                   "
         select 2023 as year,
                a.contract_plan,
                cy2023_plan_type as cy2025_plan_type, 
                cy2023_snp_type as cy2025_snp_type,
                cy2023_snp_detail as cy2025_snp_detail,
                cy2023_dual_integration_status as cy2025_dual_integration_status,
                cy2023_part_c_part_d_coverage as cy2025_part_c_part_d_coverage,
                cy2023_plan_name as cy2025_plan_name,
                parent_name,
                case 
                when b.contract_plan is null then 'Yes'
                else 'No' end as new_plan_flag,
                cy2023_vbid_uf_ssbci_indicator as cy2025_vbidufssbci_indicator,
                
                ssa_code,
                
                cy2023_over_the_counter_drug_card_in_network as cy2025_overthecounter_drug_card,
                cy2023_over_the_counter_drug_card_period_in_network as cy2025_overthecounter_drug_card_period,
                
                cy2023_vbid_uf_ssbci_group_1_additional_services_aggregate_limit_in_network as cy2025_vbidufssbci_group_1_additional_services_aggregate_limit,
                cy2023_vbid_uf_ssbci_group_1_additional_services_aggregate_limit_period_in_network AS cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period,
                cy2023_vbid_uf_ssbci_group_1_additional_services_condition_in_network as cy2025_vbidufssbci_group_1_additional_services_condition,
                cy2023_vbid_uf_ssbci_group_1_additional_services_non_medicare_covered_benefits_in_network as cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,
                
                cy2023_vbid_uf_ssbci_group_2_additional_services_aggregate_limit_in_network as cy2025_vbidufssbci_group_2_additional_services_aggregate_limit,
                cy2023_vbid_uf_ssbci_group_2_additional_services_aggregate_limit_period_in_network AS cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period,
                cy2023_vbid_uf_ssbci_group_2_additional_services_condition_in_network as cy2025_vbidufssbci_group_2_additional_services_condition,
                cy2023_vbid_uf_ssbci_group_2_additional_services_non_medicare_covered_benefits_in_network as cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,
                
                cy2023_vbid_uf_ssbci_group_3_additional_services_aggregate_limit_in_network as cy2025_vbidufssbci_group_3_additional_services_aggregate_limit,
                cy2023_vbid_uf_ssbci_group_3_additional_services_aggregate_limit_period_in_network AS cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period,
                cy2023_vbid_uf_ssbci_group_3_additional_services_condition_in_network as cy2025_vbidufssbci_group_3_additional_services_condition,
                cy2023_vbid_uf_ssbci_group_3_additional_services_non_medicare_covered_benefits_in_network as cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,
                
                cy2023_vbid_uf_ssbci_group_4_additional_services_aggregate_limit_in_network as cy2025_vbidufssbci_group_4_additional_services_aggregate_limit,
                cy2023_vbid_uf_ssbci_group_4_additional_services_aggregate_limit_period_in_network AS cy2025_vbidufssbci_group_4_additional_services_aggregate_limit_period,
                cy2023_vbid_uf_ssbci_group_4_additional_services_condition_in_network as cy2025_vbidufssbci_group_4_additional_services_condition,
                cy2023_vbid_uf_ssbci_group_4_additional_services_non_medicare_covered_benefits_in_network as cy2025_vbidufssbci_group_4_additional_services_nonmedicare_covered_benefits,
                
                cy2023_vbid_uf_ssbci_group_5_additional_services_aggregate_limit_in_network as cy2025_vbidufssbci_group_5_additional_services_aggregate_limit,
                cy2023_vbid_uf_ssbci_group_5_additional_services_aggregate_limit_period_in_network AS cy2025_vbidufssbci_group_5_additional_services_aggregate_limit_period,
                cy2023_vbid_uf_ssbci_group_5_additional_services_condition_in_network as cy2025_vbidufssbci_group_5_additional_services_condition,
                cy2023_vbid_uf_ssbci_group_5_additional_services_non_medicare_covered_benefits_in_network as cy2025_vbidufssbci_group_5_additional_services_nonmedicare_covered_benefits
                
          from anbc-hcb-dev.growth_anlyt_hcb_dev.milliman_2023_macvat_2023_benefits_v3 as a
          left outer join (select distinct contract_plan from anbc-hcb-dev.growth_anlyt_hcb_dev.milliman_2022_macvat_2022_benefits_v3) as b
          on a.contract_plan=b.contract_plan ") %>%
  
  mutate(contract_plan = gsub(" ", "", contract_plan)) %>%
  transmute(year,
            contract_plan,
            contract_id=substr(contract_plan,1,5),
            PBP=substr(contract_plan,7,9),
            segment=substr(contract_plan,11,13),
            cy2025_plan_type,
            cy2025_snp_type,
            cy2025_snp_detail,
            cy2025_dual_integration_status,
            cy2025_part_c_part_d_coverage,
            cy2025_plan_name,
            parent_name,
            new_plan_flag,
            cy2025_vbidufssbci_indicator,
            
            ssa=sprintf("%05d", as.numeric(ssa_code)),
            
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
            cy2025_vbidufssbci_group_5_additional_services_nonmedicare_covered_benefits) %>%
  distinct()

macvat_2023_benefit_with_geo <-
  macvat_2023_benefit %>%
  left_join(fips_ssa,by=c("ssa")) %>%
  inner_join(aep_footprint_history %>% filter(year==2025) %>% select(fips,state,county))


macvat_2023_benefit_with_geo_formatted <-
  macvat_2023_benefit_with_geo %>%
  mutate(food_and_produce_string_1=str_extract(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,"(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))"),
         food_and_produce_string_1=ifelse(cy2025_snp_type=="Dual Eligible" & 
                                            parent_name=="UnitedHealth Group, Inc." &
                                            (grepl("VBID Food Allowance and Utilities Combined Benefit",cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) |
                                            grepl("VBID Food Allowance and Utilities Combined Benefit",cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) |
                                            grepl("VBID Food Allowance and Utilities Combined Benefit",cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)), paste(cy2025_overthecounter_drug_card,cy2025_overthecounter_drug_card_period),food_and_produce_string_1),
         food_and_produce_string_2=str_extract(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,"(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))"),
         food_and_produce_string_3=str_extract(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,"(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))"),
         healthy_food_string_1=str_extract(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,"\\(Healthy Food.*?\\)|\\(Living Expense Support.*?\\)"),
         healthy_food_string_2=str_extract(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,"\\(Healthy Food.*?\\)|\\(Living Expense Support.*?\\)"),
         healthy_food_string_3=str_extract(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,"\\(Healthy Food.*?\\)|\\(Living Expense Support.*?\\)"),
         
         general_supports_for_living_string_1=str_extract(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,"General Supports for Living \\(.*?\\)"),
         general_supports_for_living_string_1_alter=str_extract(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,"\\(Utilities\\), \\$[0-9]+.*"),
         general_supports_for_living_string_2=str_extract(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,"General Supports for Living \\(.*?\\)"),
         general_supports_for_living_string_2_alter=str_extract(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,"\\(Utilities\\), \\$[0-9]+.*"),
         general_supports_for_living_string_3=str_extract(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,"General Supports for Living \\(.*?\\)"),
         general_supports_for_living_string_3_alter=str_extract(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,"\\(Utilities\\), \\$[0-9]+.*"),
         food_and_produce_allowance_1=as.numeric(str_extract(food_and_produce_string_1,"[0-9]+")) *
           ifelse(grepl("every month",tolower(food_and_produce_string_1)),1,
                  ifelse(grepl("every three months",tolower(food_and_produce_string_1)),1/3,1/12)),
         food_and_produce_allowance_2=as.numeric(str_extract(food_and_produce_string_2,"[0-9]+")) *
           ifelse(grepl("every month",tolower(food_and_produce_string_2)),1,
                  ifelse(grepl("every three months",tolower(food_and_produce_string_2)),1/3,1/12)),
         food_and_produce_allowance_3=as.numeric(str_extract(food_and_produce_string_3,"[0-9]+")) *
           ifelse(grepl("every month",tolower(food_and_produce_string_3)),1,
                  ifelse(grepl("every three months",tolower(food_and_produce_string_3)),1/3,1/12)),
         healthy_food_allowance_1=as.numeric(str_extract(healthy_food_string_1,"[0-9]+")) *
           ifelse(grepl("every month",tolower(healthy_food_string_1)),1,
                  ifelse(grepl("every three months",tolower(healthy_food_string_1)),1/3,1/12)),
         healthy_food_allowance_2=as.numeric(str_extract(healthy_food_string_2,"[0-9]+")) *
           ifelse(grepl("every month",tolower(healthy_food_string_2)),1,
                  ifelse(grepl("every three months",tolower(healthy_food_string_2)),1/3,1/12)),
         healthy_food_allowance_3=as.numeric(str_extract(healthy_food_string_3,"[0-9]+")) *
           ifelse(grepl("every month",tolower(healthy_food_string_3)),1,
                  ifelse(grepl("every three months",tolower(healthy_food_string_3)),1/3,1/12)),
         general_supports_for_living_allowance_1=as.numeric(str_extract(general_supports_for_living_string_1,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_1)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_1)),1/3,1/12)),
         general_supports_for_living_allowance_1_alter=as.numeric(str_extract(general_supports_for_living_string_1_alter,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_1_alter)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_1_alter)),1/3,1/12)),
         general_supports_for_living_allowance_2=as.numeric(str_extract(general_supports_for_living_string_2,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_2)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_2)),1/3,1/12)),
         general_supports_for_living_allowance_2_alter=as.numeric(str_extract(general_supports_for_living_string_2_alter,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_2_alter)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_2_alter)),1/3,1/12)),
         general_supports_for_living_allowance_3=as.numeric(str_extract(general_supports_for_living_string_3,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_3)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_3)),1/3,1/12)),
         general_supports_for_living_allowance_3_alter=as.numeric(str_extract(general_supports_for_living_string_3_alter,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_3_alter)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_3_alter)),1/3,1/12)),
         food_and_produce_allowance_1=ifelse(is.na(food_and_produce_allowance_1),0,food_and_produce_allowance_1),
         food_and_produce_allowance_2=ifelse(is.na(food_and_produce_allowance_2),0,food_and_produce_allowance_2),
         food_and_produce_allowance_3=ifelse(is.na(food_and_produce_allowance_3),0,food_and_produce_allowance_3),
         healthy_food_allowance_1=ifelse(is.na(healthy_food_allowance_1),0,healthy_food_allowance_1),
         healthy_food_allowance_2=ifelse(is.na(healthy_food_allowance_2),0,healthy_food_allowance_2),
         healthy_food_allowance_3=ifelse(is.na(healthy_food_allowance_3),0,healthy_food_allowance_3),
         general_supports_for_living_allowance_1=ifelse(is.na(general_supports_for_living_allowance_1),general_supports_for_living_allowance_1_alter,general_supports_for_living_allowance_1),
         general_supports_for_living_allowance_1=ifelse(is.na(general_supports_for_living_allowance_1),0,general_supports_for_living_allowance_1),
         general_supports_for_living_allowance_2=ifelse(is.na(general_supports_for_living_allowance_2),general_supports_for_living_allowance_2_alter,general_supports_for_living_allowance_2),
         general_supports_for_living_allowance_2=ifelse(is.na(general_supports_for_living_allowance_2),0,general_supports_for_living_allowance_2),
         general_supports_for_living_allowance_3=ifelse(is.na(general_supports_for_living_allowance_3),general_supports_for_living_allowance_3_alter,general_supports_for_living_allowance_3),
         general_supports_for_living_allowance_3=ifelse(is.na(general_supports_for_living_allowance_3),0,general_supports_for_living_allowance_3)) %>%
  transmute(year,
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
            
            monthly_allowance_1=ifelse(is.na(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period),1,
                                       ifelse(tolower(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period)=="every month",1,
                                              ifelse(tolower(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period)=="every three months",1/3,
                                                     ifelse(tolower(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period)=="every year",1/12,1)))) *
              ifelse(is.na(as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_1_additional_services_aggregate_limit))),
                     food_and_produce_allowance_1+healthy_food_allowance_1+general_supports_for_living_allowance_1,as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_1_additional_services_aggregate_limit))),
            food_allowance_1=food_and_produce_allowance_1+healthy_food_allowance_1,
            general_supports_for_living_allowance_1,
            
            eligibility_1=ifelse(grepl("vbid",tolower(cy2025_vbidufssbci_group_1_additional_services_condition)),"VBID",
                                 ifelse(grepl("ssbci",tolower(cy2025_vbidufssbci_group_1_additional_services_condition)),"SSBCI",
                                        ifelse(grepl("uf",tolower(cy2025_vbidufssbci_group_1_additional_services_condition)),"UF",""))),
            food_and_produce_1=ifelse(grepl("13j",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("food and produce",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("healthy food",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            general_supports_for_living_1=ifelse(grepl("general supports for living",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            transportation_1=ifelse(grepl("10b",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                                      grepl("transportation",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            otc_1=ifelse(grepl("13b",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                           grepl("otc",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            meals_1=ifelse(grepl("13c",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                             grepl("meal",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            Others_1=ifelse(grepl("16b",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("16c",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("home",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pet",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pest",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("social needs",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("other",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("indoor air",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("emergency",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            
            monthly_allowance_2=ifelse(is.na(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period),1,
                                       ifelse(tolower(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period)=="every month",1,
                                              ifelse(tolower(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period)=="every three months",1/3,
                                                     ifelse(tolower(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period)=="every year",1/12,1)))) *
              ifelse(is.na(as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_2_additional_services_aggregate_limit))),
                     food_and_produce_allowance_2+healthy_food_allowance_2+general_supports_for_living_allowance_2,as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_2_additional_services_aggregate_limit))),
            food_allowance_2=food_and_produce_allowance_2+healthy_food_allowance_2,
            general_supports_for_living_allowance_2,
            
            eligibility_2=ifelse(grepl("vbid",tolower(cy2025_vbidufssbci_group_2_additional_services_condition)),"VBID",
                                 ifelse(grepl("ssbci",tolower(cy2025_vbidufssbci_group_2_additional_services_condition)),"SSBCI",
                                        ifelse(grepl("uf",tolower(cy2025_vbidufssbci_group_2_additional_services_condition)),"UF",""))),
            food_and_produce_2=ifelse(grepl("13j",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("food and produce",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("healthy food",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            general_supports_for_living_2=ifelse(grepl("general supports for living",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            transportation_2=ifelse(grepl("10b",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                                      grepl("transportation",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            otc_2=ifelse(grepl("13b",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                           grepl("otc",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            meals_2=ifelse(grepl("13c",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                             grepl("meal",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            Others_2=ifelse(grepl("16b",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("16c",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("home",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pet",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pest",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("social needs",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("other",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("indoor air",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("emergency",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            
            monthly_allowance_3=ifelse(is.na(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period),1,
                                       ifelse(tolower(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period)=="every month",1,
                                              ifelse(tolower(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period)=="every three months",1/3,
                                                     ifelse(tolower(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period)=="every year",1/12,1)))) *
              ifelse(is.na(as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_3_additional_services_aggregate_limit))),
                     food_and_produce_allowance_3+healthy_food_allowance_3+general_supports_for_living_allowance_3,as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_3_additional_services_aggregate_limit))),
            food_allowance_3=food_and_produce_allowance_3+healthy_food_allowance_3,
            general_supports_for_living_allowance_3,
            
            eligibility_3=ifelse(grepl("vbid",tolower(cy2025_vbidufssbci_group_3_additional_services_condition)),"VBID",
                                 ifelse(grepl("ssbci",tolower(cy2025_vbidufssbci_group_3_additional_services_condition)),"SSBCI",
                                        ifelse(grepl("uf",tolower(cy2025_vbidufssbci_group_3_additional_services_condition)),"UF",""))),
            food_and_produce_3=ifelse(grepl("13j",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("food and produce",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("healthy food",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            general_supports_for_living_3=ifelse(grepl("general supports for living",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            transportation_3=ifelse(grepl("10b",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                                      grepl("transportation",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            otc_3=ifelse(grepl("13b",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                           grepl("otc",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            meals_3=ifelse(grepl("13c",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                             grepl("meal",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            Others_3=ifelse(grepl("16b",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("16c",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("home",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pet",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pest",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("social needs",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("other",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("indoor air",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("emergency",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            
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
            cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)

macvat_2023_benefit_with_geo_formatted_arranged <-
  macvat_2023_benefit_with_geo_formatted %>%
  transmute(year,
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
            
            monthly_allowance_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                             monthly_allowance_1,
                                             ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, monthly_allowance_2, monthly_allowance_3)),
            
            food_and_produce_allowance_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                      food_allowance_1,
                                                      ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, food_allowance_2, food_allowance_3)),
            
            general_supports_for_living_allowance_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                                 general_supports_for_living_allowance_1,
                                                                 ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, general_supports_for_living_allowance_2, general_supports_for_living_allowance_3)),
            
            eligibility_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                       eligibility_1,
                                       ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, eligibility_2, eligibility_3)),
            
            food_and_produce_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                            food_and_produce_1,
                                            ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, food_and_produce_2, food_and_produce_3)),
            
            general_supports_for_living_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                       general_supports_for_living_1,
                                                       ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, general_supports_for_living_2, general_supports_for_living_3)),
            
            transportation_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                          transportation_1,
                                          ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, transportation_2, transportation_3)),
            
            otc_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                               otc_1,
                               ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, otc_2, otc_3)),
            
            meals_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                 meals_1,
                                 ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, meals_2, meals_3)),
            
            Others_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                  Others_1,
                                  ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, Others_2, Others_3)),
            
            Conditions_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                      cy2025_vbidufssbci_group_1_additional_services_condition,
                                      ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, cy2025_vbidufssbci_group_2_additional_services_condition, cy2025_vbidufssbci_group_3_additional_services_condition)),
            
            
            monthly_allowance_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                               ifelse(monthly_allowance_2>monthly_allowance_3,monthly_allowance_2,monthly_allowance_3),
                                               ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                      ifelse(monthly_allowance_1>monthly_allowance_3,monthly_allowance_1,monthly_allowance_3), 
                                                      ifelse(monthly_allowance_1>monthly_allowance_2,monthly_allowance_1,monthly_allowance_2))),
            
            food_and_produce_allowance_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                        ifelse(monthly_allowance_2>monthly_allowance_3,food_allowance_2,food_allowance_3),
                                                        ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                               ifelse(monthly_allowance_1>monthly_allowance_3,food_allowance_1,food_allowance_3), 
                                                               ifelse(monthly_allowance_1>monthly_allowance_2,food_allowance_1,food_allowance_2))),
            
            general_supports_for_living_allowance_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                                   ifelse(monthly_allowance_2>monthly_allowance_3,general_supports_for_living_allowance_2,general_supports_for_living_allowance_3),
                                                                   ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                                          ifelse(monthly_allowance_1>monthly_allowance_3,general_supports_for_living_allowance_1,general_supports_for_living_allowance_3), 
                                                                          ifelse(monthly_allowance_1>monthly_allowance_2,general_supports_for_living_allowance_1,general_supports_for_living_allowance_2))),
            
            eligibility_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                         ifelse(monthly_allowance_2>monthly_allowance_3,eligibility_2,eligibility_3),
                                         ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                ifelse(monthly_allowance_1>monthly_allowance_3,eligibility_1,eligibility_3), 
                                                ifelse(monthly_allowance_1>monthly_allowance_2,eligibility_1,eligibility_2))),
            
            food_and_produce_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                              ifelse(monthly_allowance_2>monthly_allowance_3,food_and_produce_2,food_and_produce_3),
                                              ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                     ifelse(monthly_allowance_1>monthly_allowance_3,food_and_produce_1,food_and_produce_3), 
                                                     ifelse(monthly_allowance_1>monthly_allowance_2,food_and_produce_1,food_and_produce_2))),
            
            general_supports_for_living_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                         ifelse(monthly_allowance_2>monthly_allowance_3,general_supports_for_living_2,general_supports_for_living_3),
                                                         ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                                ifelse(monthly_allowance_1>monthly_allowance_3,general_supports_for_living_1,general_supports_for_living_3), 
                                                                ifelse(monthly_allowance_1>monthly_allowance_2,general_supports_for_living_1,general_supports_for_living_2))),
            
            transportation_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                            ifelse(monthly_allowance_2>monthly_allowance_3,transportation_2,transportation_3),
                                            ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                   ifelse(monthly_allowance_1>monthly_allowance_3,transportation_1,transportation_3), 
                                                   ifelse(monthly_allowance_1>monthly_allowance_2,transportation_1,transportation_2))),
            
            otc_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                 ifelse(monthly_allowance_2>monthly_allowance_3,otc_2,otc_3),
                                 ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                        ifelse(monthly_allowance_1>monthly_allowance_3,otc_1,otc_3), 
                                        ifelse(monthly_allowance_1>monthly_allowance_2,otc_1,otc_2))),
            
            meals_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                   ifelse(monthly_allowance_2>monthly_allowance_3,meals_2,meals_3),
                                   ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                          ifelse(monthly_allowance_1>monthly_allowance_3,meals_1,meals_3), 
                                          ifelse(monthly_allowance_1>monthly_allowance_2,meals_1,meals_2))),
            
            Others_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                    ifelse(monthly_allowance_2>monthly_allowance_3,Others_2,Others_3),
                                    ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                           ifelse(monthly_allowance_1>monthly_allowance_3,Others_1,Others_3), 
                                           ifelse(monthly_allowance_1>monthly_allowance_2,Others_1,Others_2))),
            
            Conditions_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                        ifelse(monthly_allowance_2>monthly_allowance_3,cy2025_vbidufssbci_group_2_additional_services_condition,cy2025_vbidufssbci_group_3_additional_services_condition),
                                        ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                               ifelse(monthly_allowance_1>monthly_allowance_3,cy2025_vbidufssbci_group_1_additional_services_condition,cy2025_vbidufssbci_group_3_additional_services_condition), 
                                               ifelse(monthly_allowance_1>monthly_allowance_2,cy2025_vbidufssbci_group_1_additional_services_condition,cy2025_vbidufssbci_group_2_additional_services_condition))),
            
            
            monthly_allowance_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                           ifelse(monthly_allowance_2>monthly_allowance_3,monthly_allowance_3,monthly_allowance_2),
                                           ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                  ifelse(monthly_allowance_1>monthly_allowance_3,monthly_allowance_3,monthly_allowance_1), 
                                                  ifelse(monthly_allowance_1>monthly_allowance_2,monthly_allowance_2,monthly_allowance_1))),
            
            food_and_produce_allowance_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                    ifelse(monthly_allowance_2>monthly_allowance_3,food_allowance_3,food_allowance_2),
                                                    ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                           ifelse(monthly_allowance_1>monthly_allowance_3,food_allowance_3,food_allowance_1), 
                                                           ifelse(monthly_allowance_1>monthly_allowance_2,food_allowance_2,food_allowance_1))),
            
            general_supports_for_living_allowance_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                               ifelse(monthly_allowance_2>monthly_allowance_3,general_supports_for_living_allowance_3,general_supports_for_living_allowance_2),
                                                               ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                                      ifelse(monthly_allowance_1>monthly_allowance_3,general_supports_for_living_allowance_3,general_supports_for_living_allowance_1), 
                                                                      ifelse(monthly_allowance_1>monthly_allowance_2,general_supports_for_living_allowance_2,general_supports_for_living_allowance_1))),
            
            eligibility_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                     ifelse(monthly_allowance_2>monthly_allowance_3,eligibility_3,eligibility_2),
                                     ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                            ifelse(monthly_allowance_1>monthly_allowance_3,eligibility_3,eligibility_1), 
                                            ifelse(monthly_allowance_1>monthly_allowance_2,eligibility_2,eligibility_1))),
            
            food_and_produce_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                          ifelse(monthly_allowance_2>monthly_allowance_3,food_and_produce_3,food_and_produce_2),
                                          ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                 ifelse(monthly_allowance_1>monthly_allowance_3,food_and_produce_3,food_and_produce_1), 
                                                 ifelse(monthly_allowance_1>monthly_allowance_2,food_and_produce_2,food_and_produce_1))),
            
            general_supports_for_living_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                     ifelse(monthly_allowance_2>monthly_allowance_3,general_supports_for_living_3,general_supports_for_living_2),
                                                     ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                            ifelse(monthly_allowance_1>monthly_allowance_3,general_supports_for_living_3,general_supports_for_living_1), 
                                                            ifelse(monthly_allowance_1>monthly_allowance_2,general_supports_for_living_2,general_supports_for_living_1))),
            
            transportation_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                        ifelse(monthly_allowance_2>monthly_allowance_3,transportation_3,transportation_2),
                                        ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                               ifelse(monthly_allowance_1>monthly_allowance_3,transportation_3,transportation_1), 
                                               ifelse(monthly_allowance_1>monthly_allowance_2,transportation_2,transportation_1))),
            
            otc_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                             ifelse(monthly_allowance_2>monthly_allowance_3,otc_3,otc_2),
                             ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                    ifelse(monthly_allowance_1>monthly_allowance_3,otc_3,otc_1), 
                                    ifelse(monthly_allowance_1>monthly_allowance_2,otc_2,otc_1))),
            
            meals_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                               ifelse(monthly_allowance_2>monthly_allowance_3,meals_3,meals_2),
                               ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                      ifelse(monthly_allowance_1>monthly_allowance_3,meals_3,meals_1), 
                                      ifelse(monthly_allowance_1>monthly_allowance_2,meals_2,meals_1))),
            
            Others_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                ifelse(monthly_allowance_2>monthly_allowance_3,Others_3,Others_2),
                                ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                       ifelse(monthly_allowance_1>monthly_allowance_3,Others_3,Others_1), 
                                       ifelse(monthly_allowance_1>monthly_allowance_2,Others_2,Others_1))),
            
            Conditions_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                    ifelse(monthly_allowance_2>monthly_allowance_3,cy2025_vbidufssbci_group_3_additional_services_condition,cy2025_vbidufssbci_group_2_additional_services_condition),
                                    ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                           ifelse(monthly_allowance_1>monthly_allowance_3,cy2025_vbidufssbci_group_3_additional_services_condition,cy2025_vbidufssbci_group_1_additional_services_condition), 
                                           ifelse(monthly_allowance_1>monthly_allowance_2,cy2025_vbidufssbci_group_2_additional_services_condition,cy2025_vbidufssbci_group_1_additional_services_condition)))) %>%
  
  transmute(year,
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
            
            # cy2025_overthecounter_drug_card,
            # cy2025_overthecounter_drug_card_period,
            
            
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
            
            # monthly_allowance_third,
            # food_and_produce_allowance_third,
            # general_supports_for_living_allowance_third,
            # eligibility_third,
            # food_and_produce_third,
            # general_supports_for_living_third,
            # transportation_third,
            # otc_third,
            # meals_third,
            # Others_third,
            
            anemia=ifelse(grepl("anemia",tolower(Conditions_primary)),"Y","N"),
            asthma=ifelse(grepl("asthma",tolower(Conditions_primary)),"Y","N"),
            autoimmune_disorder=ifelse(grepl("autoimmune disorder",tolower(Conditions_primary)),"Y","N"),
            arthritis=ifelse(grepl("arthritis",tolower(Conditions_primary)),"Y","N"),
            cardiovascular_disorder=ifelse(grepl("cardiovascular disorder",tolower(Conditions_primary)),"Y","N"),
            cellulitis=ifelse(grepl("cellulitis",tolower(Conditions_primary)),"Y","N"),
            circulatory_disease=ifelse(grepl("circulatory disease",tolower(Conditions_primary)),"Y","N"),
            chronic_alcohol_and_other_drug_dependence=ifelse(grepl("chronic alcohol and other drug dependence",tolower(Conditions_primary)),"Y","N"),
            chronic_cognitive_impairment=ifelse(grepl("chronic cognitive impairment",tolower(Conditions_primary)),"Y","N"),
            chronic_heart_failure=ifelse(grepl("chronic heart failure",tolower(Conditions_primary)),"Y","N"),
            chronic_gastrointestinal_disorder=ifelse(grepl("chronic gastrointestinal disorder",tolower(Conditions_primary)) |
                                                       grepl("chronic gi disorder",tolower(Conditions_primary)),"Y","N"),
            malnutrition=ifelse(grepl("malnutrition",tolower(Conditions_primary)),"Y","N"),
            chronic_infectious_disorder=ifelse(grepl("chronic infectious disorder",tolower(Conditions_primary)),"Y","N"),
            chronic_otolaryngological_disorder=ifelse(grepl("chronic otolaryngological disorder",tolower(Conditions_primary)),"Y","N"),
            chronic_pain=ifelse(grepl("chronic pain",tolower(Conditions_primary)),"Y","N"),
            cancer=ifelse(grepl("cancer",tolower(Conditions_primary)),"Y","N"),
            chronic_lung_disorder=ifelse(grepl("chronic lung disorder",tolower(Conditions_primary)),"Y","N"),
            chronic_liver_disease=ifelse(grepl("chronic liver",tolower(Conditions_primary)) |
                                           grepl("chronic kidney/liver",tolower(Conditions_primary)),"Y","N"),
            chronic_and_disabling_mental_health_condition=ifelse(grepl("chronic and disabling mental health condition",tolower(Conditions_primary)),"Y","N"),
            chronic_kidney_disease=ifelse(grepl("chronic kidney disease",tolower(Conditions_primary)) |
                                            grepl("chronic liver/kidney",tolower(Conditions_primary)),"Y","N"),
            chronic_non_alcoholic_fatty_liver_disease=ifelse(grepl("chronic non-alcohol",tolower(Conditions_primary)),"Y","N"),
            copd=ifelse(grepl("copd",tolower(Conditions_primary)) |
                          grepl("chronic obstructive pulmonary disease",tolower(Conditions_primary)),"Y","N"),
            congestive_heart_failure=ifelse(grepl("congestive heart failure",tolower(Conditions_primary)),"Y","N"),
            coronary_artery_disease=ifelse(grepl("coronary artery disease",tolower(Conditions_primary)),"Y","N"),
            cystic_fibrosis=ifelse(grepl("cystic fibrosis",tolower(Conditions_primary)),"Y","N"),
            down_syndrome=ifelse(grepl("down syndrome",tolower(Conditions_primary)),"Y","N"),
            diabete=ifelse(grepl("diabete",tolower(Conditions_primary)),"Y","N"),
            dementia=ifelse(grepl("dementia",tolower(Conditions_primary)),"Y","N"),
            dyslipidemia=ifelse(grepl("dyslipidemia",tolower(Conditions_primary)),"Y","N"),
            eating_disorder=ifelse(grepl("eating disorder",tolower(Conditions_primary)),"Y","N"),
            end_stage_liver_disease=ifelse(grepl("end-stage liver disease",tolower(Conditions_primary)),"Y","N"),
            end_stage_renal_disease=ifelse(grepl("end-stage renal disease",tolower(Conditions_primary)),"Y","N"),
            endometriosis=ifelse(grepl("endometriosis",tolower(Conditions_primary)),"Y","N"),
            endocrine=ifelse(grepl("endocrine",tolower(Conditions_primary)),"Y","N"),
            gastrointestinal=ifelse(grepl("gastrointestinal",tolower(Conditions_primary)),"Y","N"),
            glaucoma=ifelse(grepl("glaucoma",tolower(Conditions_primary)),"Y","N"),
            hiv=ifelse(grepl("hiv",tolower(Conditions_primary)),"Y","N"),
            hepatitis=ifelse(grepl("hepatitis",tolower(Conditions_primary)),"Y","N"),
            hypertension=ifelse(grepl("hypertension",tolower(Conditions_primary)),"Y","N"),
            hyperlipidemia=ifelse(grepl("hyperlipidemia",tolower(Conditions_primary)) |
                                    grepl("chronic lipid",tolower(Conditions_primary)),"Y","N"),
            hypercholesterolemia=ifelse(grepl("hypercholesterolemia",tolower(Conditions_primary)),"Y","N"),
            inflammatory_bowel_disease=ifelse(grepl("inflammatory bowel disease",tolower(Conditions_primary)),"Y","N"),
            joints_and_spine=ifelse(grepl("joints",tolower(Conditions_primary)),"Y","N"),
            loss_of_limb=ifelse(grepl("loss of limb",tolower(Conditions_primary)),"Y","N"),
            low_back_pain=ifelse(grepl("low back pain",tolower(Conditions_primary)),"Y","N"),
            metabolic_syndrome=ifelse(grepl("metabolic syndrome",tolower(Conditions_primary)),"Y","N"),
            muscular_dystrophy=ifelse(grepl("muscular dystrophy",tolower(Conditions_primary)),"Y","N"),
            musculoskeletal_disorder=ifelse(grepl("musculoskeletal disorder",tolower(Conditions_primary)),"Y","N"),
            neurologic_disorder=ifelse(grepl("neurologic disorder",tolower(Conditions_primary)),"Y","N"),
            osteoporosis=ifelse(grepl("osteoporosis",tolower(Conditions_primary)),"Y","N"),
            obesity=ifelse(grepl("obesity",tolower(Conditions_primary)) |
                             grepl("obese",tolower(Conditions_primary)),"Y","N"),
            pneumonia=ifelse(grepl("pneumonia",tolower(Conditions_primary)),"Y","N"),
            pregnancy=ifelse(grepl("pregnancy",tolower(Conditions_primary)),"Y","N"),
            rsd=ifelse(grepl("rsd",tolower(Conditions_primary)),"Y","N"),
            sjogren=ifelse(grepl("sjogren",tolower(Conditions_primary)),"Y","N"),
            severe_hematologic_disorder=ifelse(grepl("severe hematologic disorder",tolower(Conditions_primary)),"Y","N"),
            stroke=ifelse(grepl("stroke",tolower(Conditions_primary)),"Y","N"),
            urinary_tract_infection=ifelse(grepl("urinary tract infection",tolower(Conditions_primary)),"Y","N"),
            urinary_incontinance=ifelse(grepl("urinary incontinance",tolower(Conditions_primary)),"Y","N"),
            vascular_disease=ifelse(grepl("vascular disease",tolower(Conditions_primary)),"Y","N")
            
  )

macvat_2023_benefit_with_geo_formatted_arranged_plan <-
  macvat_2023_benefit_with_geo_formatted_arranged %>% 
  select(-state,-county,-fips,-ssa) %>%
  distinct()

macvat_2023_benefit_with_geo_formatted_arranged_plan_fips <-
  macvat_2023_benefit_with_geo_formatted_arranged %>% 
  select(year,contract_plan,contract_id,PBP,segment,cy2025_plan_type,cy2025_snp_type,cy2025_snp_detail,cy2025_dual_integration_status,
         cy2025_part_c_part_d_coverage,cy2025_plan_name,parent_name,new_plan_flag,cy2025_vbidufssbci_indicator,state,county,fips,ssa) %>%
  distinct()

macvat_2022_benefit  <- dbGetQuery(con, 
                                   "
         select 2022 as year,
                a.contract_plan,
                cy2022_plan_type as cy2025_plan_type, 
                cy2022_snp_type as cy2025_snp_type,
                cy2022_snp_detail as cy2025_snp_detail,
                NULL as cy2025_dual_integration_status,
                cy2022_part_c_part_d_coverage as cy2025_part_c_part_d_coverage,
                cy2022_plan_name as cy2025_plan_name,
                parent_name,
                case 
                when b.contract_plan is null then 'Yes'
                else 'No' end as new_plan_flag,
                cy2022_vbid_uf_ssbci_indicator as cy2025_vbidufssbci_indicator,
                
                ssa_code,
                
                cy2022_over_the_counter_drug_card_in_network as cy2025_overthecounter_drug_card,
                cy2022_over_the_counter_drug_card_period_in_network as cy2025_overthecounter_drug_card_period,
                
                cy2022_vbid_uf_ssbci_group_1_additional_services_aggregate_limit_in_network as cy2025_vbidufssbci_group_1_additional_services_aggregate_limit,
                'every year' AS cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period,
                cy2022_vbid_uf_ssbci_group_1_additional_services_condition_in_network as cy2025_vbidufssbci_group_1_additional_services_condition,
                cy2022_vbid_uf_ssbci_group_1_additional_services_non_medicare_covered_benefits_in_network as cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,
                
                cy2022_vbid_uf_ssbci_group_2_additional_services_aggregate_limit_in_network as cy2025_vbidufssbci_group_2_additional_services_aggregate_limit,
                'every year' AS cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period,
                cy2022_vbid_uf_ssbci_group_2_additional_services_condition_in_network as cy2025_vbidufssbci_group_2_additional_services_condition,
                cy2022_vbid_uf_ssbci_group_2_additional_services_non_medicare_covered_benefits_in_network as cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,
                
                cy2022_vbid_uf_ssbci_group_3_additional_services_aggregate_limit_in_network as cy2025_vbidufssbci_group_3_additional_services_aggregate_limit,
                'every year' AS cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period,
                cy2022_vbid_uf_ssbci_group_3_additional_services_condition_in_network as cy2025_vbidufssbci_group_3_additional_services_condition,
                cy2022_vbid_uf_ssbci_group_3_additional_services_non_medicare_covered_benefits_in_network as cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,
                
                cy2022_vbid_uf_ssbci_group_4_additional_services_aggregate_limit_in_network as cy2025_vbidufssbci_group_4_additional_services_aggregate_limit,
                NULL AS cy2025_vbidufssbci_group_4_additional_services_aggregate_limit_period,
                cy2022_vbid_uf_ssbci_group_4_additional_services_condition_in_network as cy2025_vbidufssbci_group_4_additional_services_condition,
                cy2022_vbid_uf_ssbci_group_4_additional_services_non_medicare_covered_benefits_in_network as cy2025_vbidufssbci_group_4_additional_services_nonmedicare_covered_benefits,
                
                NULL as cy2025_vbidufssbci_group_5_additional_services_aggregate_limit,
                NULL AS cy2025_vbidufssbci_group_5_additional_services_aggregate_limit_period,
                NULL as cy2025_vbidufssbci_group_5_additional_services_condition,
                NULL as cy2025_vbidufssbci_group_5_additional_services_nonmedicare_covered_benefits
                
          from anbc-hcb-dev.growth_anlyt_hcb_dev.milliman_2022_macvat_2022_benefits_v3 as a
          left outer join (select distinct contract_plan from anbc-hcb-prod.msa_share_mcr_hcb_prod.milliman_2021_macvat_2021_benefits) as b
          on a.contract_plan=b.contract_plan ") %>%
  
  mutate(contract_plan = gsub(" ", "", contract_plan)) %>%
  transmute(year,
            contract_plan,
            contract_id=substr(contract_plan,1,5),
            PBP=substr(contract_plan,7,9),
            segment=substr(contract_plan,11,13),
            cy2025_plan_type,
            cy2025_snp_type,
            cy2025_snp_detail,
            cy2025_dual_integration_status,
            cy2025_part_c_part_d_coverage,
            cy2025_plan_name,
            parent_name,
            new_plan_flag,
            cy2025_vbidufssbci_indicator,
            
            ssa=sprintf("%05d", as.numeric(ssa_code)),
            
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
            cy2025_vbidufssbci_group_5_additional_services_nonmedicare_covered_benefits) %>%
  distinct()

macvat_2022_benefit_with_geo <-
  macvat_2022_benefit %>%
  left_join(fips_ssa,by=c("ssa")) %>%
  inner_join(aep_footprint_history %>% filter(year==2025) %>% select(fips,state,county))


macvat_2022_benefit_with_geo_formatted <-
  macvat_2022_benefit_with_geo %>%
  mutate(food_and_produce_string_1=str_extract(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,"(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))"),
         food_and_produce_string_1=ifelse(cy2025_snp_type=="Dual Eligible" & 
                                            parent_name=="UnitedHealth Group, Inc." &
                                            (grepl("OTC and VBID Food Allowance Combined Benefit",cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits) |
                                               grepl("OTC and VBID Food Allowance Combined Benefit",cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits) |
                                               grepl("OTC and VBID Food Allowance Combined Benefit",cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)), paste(cy2025_overthecounter_drug_card,cy2025_overthecounter_drug_card_period),food_and_produce_string_1),
         food_and_produce_string_2=str_extract(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,"(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))"),
         food_and_produce_string_3=str_extract(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,"(Food and Produce \\(.*?\\))|(\\(OTC VBID Food Allowance and Utilities Combined Benefit.*?\\))"),
         healthy_food_string_1=str_extract(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,"\\(Healthy Food.*?\\)|\\(Living Expense Support.*?\\)"),
         healthy_food_string_2=str_extract(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,"\\(Healthy Food.*?\\)|\\(Living Expense Support.*?\\)"),
         healthy_food_string_3=str_extract(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,"\\(Healthy Food.*?\\)|\\(Living Expense Support.*?\\)"),
         
         general_supports_for_living_string_1=str_extract(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,"General Supports for Living \\(.*?\\)"),
         general_supports_for_living_string_1_alter=str_extract(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits,"\\(Utilities\\), \\$[0-9]+.*"),
         general_supports_for_living_string_2=str_extract(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,"General Supports for Living \\(.*?\\)"),
         general_supports_for_living_string_2_alter=str_extract(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits,"\\(Utilities\\), \\$[0-9]+.*"),
         general_supports_for_living_string_3=str_extract(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,"General Supports for Living \\(.*?\\)"),
         general_supports_for_living_string_3_alter=str_extract(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits,"\\(Utilities\\), \\$[0-9]+.*"),
         food_and_produce_allowance_1=as.numeric(str_extract(food_and_produce_string_1,"[0-9]+")) *
           ifelse(grepl("every month",tolower(food_and_produce_string_1)),1,
                  ifelse(grepl("every three months",tolower(food_and_produce_string_1)),1/3,1/12)),
         food_and_produce_allowance_2=as.numeric(str_extract(food_and_produce_string_2,"[0-9]+")) *
           ifelse(grepl("every month",tolower(food_and_produce_string_2)),1,
                  ifelse(grepl("every three months",tolower(food_and_produce_string_2)),1/3,1/12)),
         food_and_produce_allowance_3=as.numeric(str_extract(food_and_produce_string_3,"[0-9]+")) *
           ifelse(grepl("every month",tolower(food_and_produce_string_3)),1,
                  ifelse(grepl("every three months",tolower(food_and_produce_string_3)),1/3,1/12)),
         healthy_food_allowance_1=as.numeric(str_extract(healthy_food_string_1,"[0-9]+")) *
           ifelse(grepl("every month",tolower(healthy_food_string_1)),1,
                  ifelse(grepl("every three months",tolower(healthy_food_string_1)),1/3,1/12)),
         healthy_food_allowance_2=as.numeric(str_extract(healthy_food_string_2,"[0-9]+")) *
           ifelse(grepl("every month",tolower(healthy_food_string_2)),1,
                  ifelse(grepl("every three months",tolower(healthy_food_string_2)),1/3,1/12)),
         healthy_food_allowance_3=as.numeric(str_extract(healthy_food_string_3,"[0-9]+")) *
           ifelse(grepl("every month",tolower(healthy_food_string_3)),1,
                  ifelse(grepl("every three months",tolower(healthy_food_string_3)),1/3,1/12)),
         general_supports_for_living_allowance_1=as.numeric(str_extract(general_supports_for_living_string_1,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_1)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_1)),1/3,1/12)),
         general_supports_for_living_allowance_1_alter=as.numeric(str_extract(general_supports_for_living_string_1_alter,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_1_alter)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_1_alter)),1/3,1/12)),
         general_supports_for_living_allowance_2=as.numeric(str_extract(general_supports_for_living_string_2,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_2)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_2)),1/3,1/12)),
         general_supports_for_living_allowance_2_alter=as.numeric(str_extract(general_supports_for_living_string_2_alter,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_2_alter)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_2_alter)),1/3,1/12)),
         general_supports_for_living_allowance_3=as.numeric(str_extract(general_supports_for_living_string_3,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_3)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_3)),1/3,1/12)),
         general_supports_for_living_allowance_3_alter=as.numeric(str_extract(general_supports_for_living_string_3_alter,"[0-9]+")) *
           ifelse(grepl("every month",tolower(general_supports_for_living_string_3_alter)),1,
                  ifelse(grepl("every three months",tolower(general_supports_for_living_string_3_alter)),1/3,1/12)),
         food_and_produce_allowance_1=ifelse(is.na(food_and_produce_allowance_1),0,food_and_produce_allowance_1),
         food_and_produce_allowance_2=ifelse(is.na(food_and_produce_allowance_2),0,food_and_produce_allowance_2),
         food_and_produce_allowance_3=ifelse(is.na(food_and_produce_allowance_3),0,food_and_produce_allowance_3),
         healthy_food_allowance_1=ifelse(is.na(healthy_food_allowance_1),0,healthy_food_allowance_1),
         healthy_food_allowance_2=ifelse(is.na(healthy_food_allowance_2),0,healthy_food_allowance_2),
         healthy_food_allowance_3=ifelse(is.na(healthy_food_allowance_3),0,healthy_food_allowance_3),
         general_supports_for_living_allowance_1=ifelse(is.na(general_supports_for_living_allowance_1),general_supports_for_living_allowance_1_alter,general_supports_for_living_allowance_1),
         general_supports_for_living_allowance_1=ifelse(is.na(general_supports_for_living_allowance_1),0,general_supports_for_living_allowance_1),
         general_supports_for_living_allowance_2=ifelse(is.na(general_supports_for_living_allowance_2),general_supports_for_living_allowance_2_alter,general_supports_for_living_allowance_2),
         general_supports_for_living_allowance_2=ifelse(is.na(general_supports_for_living_allowance_2),0,general_supports_for_living_allowance_2),
         general_supports_for_living_allowance_3=ifelse(is.na(general_supports_for_living_allowance_3),general_supports_for_living_allowance_3_alter,general_supports_for_living_allowance_3),
         general_supports_for_living_allowance_3=ifelse(is.na(general_supports_for_living_allowance_3),0,general_supports_for_living_allowance_3)) %>%
  transmute(year,
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
            
            monthly_allowance_1=ifelse(is.na(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period),1,
                                       ifelse(tolower(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period)=="every month",1,
                                              ifelse(tolower(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period)=="every three months",1/3,
                                                     ifelse(tolower(cy2025_vbidufssbci_group_1_additional_services_aggregate_limit_period)=="every year",1/12,1)))) *
              ifelse(is.na(as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_1_additional_services_aggregate_limit))),
                     food_and_produce_allowance_1+healthy_food_allowance_1+general_supports_for_living_allowance_1,as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_1_additional_services_aggregate_limit))),
            food_allowance_1=food_and_produce_allowance_1+healthy_food_allowance_1,
            general_supports_for_living_allowance_1,
            
            eligibility_1=ifelse(grepl("vbid",tolower(cy2025_vbidufssbci_group_1_additional_services_condition)),"VBID",
                                 ifelse(grepl("ssbci",tolower(cy2025_vbidufssbci_group_1_additional_services_condition)),"SSBCI",
                                        ifelse(grepl("uf",tolower(cy2025_vbidufssbci_group_1_additional_services_condition)),"UF",""))),
            food_and_produce_1=ifelse(grepl("13j",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("food and produce",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("healthy food",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            general_supports_for_living_1=ifelse(grepl("general supports for living",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            transportation_1=ifelse(grepl("10b",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                                      grepl("transportation",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            otc_1=ifelse(grepl("13b",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                           grepl("otc",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            meals_1=ifelse(grepl("13c",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                             grepl("meal",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            Others_1=ifelse(grepl("16b",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("16c",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("home",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pet",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pest",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("social needs",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("other",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("indoor air",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)) |
                              grepl("emergency",tolower(cy2025_vbidufssbci_group_1_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            
            monthly_allowance_2=ifelse(is.na(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period),1,
                                       ifelse(tolower(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period)=="every month",1,
                                              ifelse(tolower(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period)=="every three months",1/3,
                                                     ifelse(tolower(cy2025_vbidufssbci_group_2_additional_services_aggregate_limit_period)=="every year",1/12,1)))) *
              ifelse(is.na(as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_2_additional_services_aggregate_limit))),
                     food_and_produce_allowance_2+healthy_food_allowance_2+general_supports_for_living_allowance_2,as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_2_additional_services_aggregate_limit))),
            food_allowance_2=food_and_produce_allowance_2+healthy_food_allowance_2,
            general_supports_for_living_allowance_2,
            
            eligibility_2=ifelse(grepl("vbid",tolower(cy2025_vbidufssbci_group_2_additional_services_condition)),"VBID",
                                 ifelse(grepl("ssbci",tolower(cy2025_vbidufssbci_group_2_additional_services_condition)),"SSBCI",
                                        ifelse(grepl("uf",tolower(cy2025_vbidufssbci_group_2_additional_services_condition)),"UF",""))),
            food_and_produce_2=ifelse(grepl("13j",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("food and produce",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("healthy food",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            general_supports_for_living_2=ifelse(grepl("general supports for living",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            transportation_2=ifelse(grepl("10b",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                                      grepl("transportation",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            otc_2=ifelse(grepl("13b",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                           grepl("otc",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            meals_2=ifelse(grepl("13c",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                             grepl("meal",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            Others_2=ifelse(grepl("16b",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("16c",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("home",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pet",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pest",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("social needs",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("other",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("indoor air",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)) |
                              grepl("emergency",tolower(cy2025_vbidufssbci_group_2_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            
            monthly_allowance_3=ifelse(is.na(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period),1,
                                       ifelse(tolower(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period)=="every month",1,
                                              ifelse(tolower(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period)=="every three months",1/3,
                                                     ifelse(tolower(cy2025_vbidufssbci_group_3_additional_services_aggregate_limit_period)=="every year",1/12,1)))) *
              ifelse(is.na(as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_3_additional_services_aggregate_limit))),
                     food_and_produce_allowance_3+healthy_food_allowance_3+general_supports_for_living_allowance_3,as.numeric(gsub("\\$|,| ","",cy2025_vbidufssbci_group_3_additional_services_aggregate_limit))),
            food_allowance_3=food_and_produce_allowance_3+healthy_food_allowance_3,
            general_supports_for_living_allowance_3,
            
            eligibility_3=ifelse(grepl("vbid",tolower(cy2025_vbidufssbci_group_3_additional_services_condition)),"VBID",
                                 ifelse(grepl("ssbci",tolower(cy2025_vbidufssbci_group_3_additional_services_condition)),"SSBCI",
                                        ifelse(grepl("uf",tolower(cy2025_vbidufssbci_group_3_additional_services_condition)),"UF",""))),
            food_and_produce_3=ifelse(grepl("13j",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("food and produce",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                                        grepl("healthy food",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            general_supports_for_living_3=ifelse(grepl("general supports for living",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            transportation_3=ifelse(grepl("10b",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                                      grepl("transportation",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            otc_3=ifelse(grepl("13b",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                           grepl("otc",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            meals_3=ifelse(grepl("13c",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                             grepl("meal",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            Others_3=ifelse(grepl("16b",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("16c",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("home",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pet",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("pest",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("social needs",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("other",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("indoor air",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)) |
                              grepl("emergency",tolower(cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)),"Yes","No"),
            
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
            cy2025_vbidufssbci_group_3_additional_services_nonmedicare_covered_benefits)

macvat_2022_benefit_with_geo_formatted_arranged <-
  macvat_2022_benefit_with_geo_formatted %>%
  transmute(year,
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
            
            monthly_allowance_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                             monthly_allowance_1,
                                             ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, monthly_allowance_2, monthly_allowance_3)),
            
            food_and_produce_allowance_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                      food_allowance_1,
                                                      ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, food_allowance_2, food_allowance_3)),
            
            general_supports_for_living_allowance_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                                 general_supports_for_living_allowance_1,
                                                                 ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, general_supports_for_living_allowance_2, general_supports_for_living_allowance_3)),
            
            eligibility_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                       eligibility_1,
                                       ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, eligibility_2, eligibility_3)),
            
            food_and_produce_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                            food_and_produce_1,
                                            ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, food_and_produce_2, food_and_produce_3)),
            
            general_supports_for_living_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                       general_supports_for_living_1,
                                                       ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, general_supports_for_living_2, general_supports_for_living_3)),
            
            transportation_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                          transportation_1,
                                          ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, transportation_2, transportation_3)),
            
            otc_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                               otc_1,
                               ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, otc_2, otc_3)),
            
            meals_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                 meals_1,
                                 ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, meals_2, meals_3)),
            
            Others_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                  Others_1,
                                  ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, Others_2, Others_3)),
            
            Conditions_primary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                      cy2025_vbidufssbci_group_1_additional_services_condition,
                                      ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, cy2025_vbidufssbci_group_2_additional_services_condition, cy2025_vbidufssbci_group_3_additional_services_condition)),
            
            
            monthly_allowance_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                               ifelse(monthly_allowance_2>monthly_allowance_3,monthly_allowance_2,monthly_allowance_3),
                                               ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                      ifelse(monthly_allowance_1>monthly_allowance_3,monthly_allowance_1,monthly_allowance_3), 
                                                      ifelse(monthly_allowance_1>monthly_allowance_2,monthly_allowance_1,monthly_allowance_2))),
            
            food_and_produce_allowance_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                        ifelse(monthly_allowance_2>monthly_allowance_3,food_allowance_2,food_allowance_3),
                                                        ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                               ifelse(monthly_allowance_1>monthly_allowance_3,food_allowance_1,food_allowance_3), 
                                                               ifelse(monthly_allowance_1>monthly_allowance_2,food_allowance_1,food_allowance_2))),
            
            general_supports_for_living_allowance_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                                   ifelse(monthly_allowance_2>monthly_allowance_3,general_supports_for_living_allowance_2,general_supports_for_living_allowance_3),
                                                                   ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                                          ifelse(monthly_allowance_1>monthly_allowance_3,general_supports_for_living_allowance_1,general_supports_for_living_allowance_3), 
                                                                          ifelse(monthly_allowance_1>monthly_allowance_2,general_supports_for_living_allowance_1,general_supports_for_living_allowance_2))),
            
            eligibility_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                         ifelse(monthly_allowance_2>monthly_allowance_3,eligibility_2,eligibility_3),
                                         ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                ifelse(monthly_allowance_1>monthly_allowance_3,eligibility_1,eligibility_3), 
                                                ifelse(monthly_allowance_1>monthly_allowance_2,eligibility_1,eligibility_2))),
            
            food_and_produce_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                              ifelse(monthly_allowance_2>monthly_allowance_3,food_and_produce_2,food_and_produce_3),
                                              ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                     ifelse(monthly_allowance_1>monthly_allowance_3,food_and_produce_1,food_and_produce_3), 
                                                     ifelse(monthly_allowance_1>monthly_allowance_2,food_and_produce_1,food_and_produce_2))),
            
            general_supports_for_living_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                         ifelse(monthly_allowance_2>monthly_allowance_3,general_supports_for_living_2,general_supports_for_living_3),
                                                         ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                                ifelse(monthly_allowance_1>monthly_allowance_3,general_supports_for_living_1,general_supports_for_living_3), 
                                                                ifelse(monthly_allowance_1>monthly_allowance_2,general_supports_for_living_1,general_supports_for_living_2))),
            
            transportation_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                            ifelse(monthly_allowance_2>monthly_allowance_3,transportation_2,transportation_3),
                                            ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                   ifelse(monthly_allowance_1>monthly_allowance_3,transportation_1,transportation_3), 
                                                   ifelse(monthly_allowance_1>monthly_allowance_2,transportation_1,transportation_2))),
            
            otc_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                 ifelse(monthly_allowance_2>monthly_allowance_3,otc_2,otc_3),
                                 ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                        ifelse(monthly_allowance_1>monthly_allowance_3,otc_1,otc_3), 
                                        ifelse(monthly_allowance_1>monthly_allowance_2,otc_1,otc_2))),
            
            meals_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                   ifelse(monthly_allowance_2>monthly_allowance_3,meals_2,meals_3),
                                   ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                          ifelse(monthly_allowance_1>monthly_allowance_3,meals_1,meals_3), 
                                          ifelse(monthly_allowance_1>monthly_allowance_2,meals_1,meals_2))),
            
            Others_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                    ifelse(monthly_allowance_2>monthly_allowance_3,Others_2,Others_3),
                                    ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                           ifelse(monthly_allowance_1>monthly_allowance_3,Others_1,Others_3), 
                                           ifelse(monthly_allowance_1>monthly_allowance_2,Others_1,Others_2))),
            
            Conditions_secondary=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                        ifelse(monthly_allowance_2>monthly_allowance_3,cy2025_vbidufssbci_group_2_additional_services_condition,cy2025_vbidufssbci_group_3_additional_services_condition),
                                        ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                               ifelse(monthly_allowance_1>monthly_allowance_3,cy2025_vbidufssbci_group_1_additional_services_condition,cy2025_vbidufssbci_group_3_additional_services_condition), 
                                               ifelse(monthly_allowance_1>monthly_allowance_2,cy2025_vbidufssbci_group_1_additional_services_condition,cy2025_vbidufssbci_group_2_additional_services_condition))),
            
            
            monthly_allowance_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                           ifelse(monthly_allowance_2>monthly_allowance_3,monthly_allowance_3,monthly_allowance_2),
                                           ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                  ifelse(monthly_allowance_1>monthly_allowance_3,monthly_allowance_3,monthly_allowance_1), 
                                                  ifelse(monthly_allowance_1>monthly_allowance_2,monthly_allowance_2,monthly_allowance_1))),
            
            food_and_produce_allowance_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                    ifelse(monthly_allowance_2>monthly_allowance_3,food_allowance_3,food_allowance_2),
                                                    ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                           ifelse(monthly_allowance_1>monthly_allowance_3,food_allowance_3,food_allowance_1), 
                                                           ifelse(monthly_allowance_1>monthly_allowance_2,food_allowance_2,food_allowance_1))),
            
            general_supports_for_living_allowance_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                               ifelse(monthly_allowance_2>monthly_allowance_3,general_supports_for_living_allowance_3,general_supports_for_living_allowance_2),
                                                               ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                                      ifelse(monthly_allowance_1>monthly_allowance_3,general_supports_for_living_allowance_3,general_supports_for_living_allowance_1), 
                                                                      ifelse(monthly_allowance_1>monthly_allowance_2,general_supports_for_living_allowance_2,general_supports_for_living_allowance_1))),
            
            eligibility_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                     ifelse(monthly_allowance_2>monthly_allowance_3,eligibility_3,eligibility_2),
                                     ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                            ifelse(monthly_allowance_1>monthly_allowance_3,eligibility_3,eligibility_1), 
                                            ifelse(monthly_allowance_1>monthly_allowance_2,eligibility_2,eligibility_1))),
            
            food_and_produce_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                          ifelse(monthly_allowance_2>monthly_allowance_3,food_and_produce_3,food_and_produce_2),
                                          ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                 ifelse(monthly_allowance_1>monthly_allowance_3,food_and_produce_3,food_and_produce_1), 
                                                 ifelse(monthly_allowance_1>monthly_allowance_2,food_and_produce_2,food_and_produce_1))),
            
            general_supports_for_living_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                                     ifelse(monthly_allowance_2>monthly_allowance_3,general_supports_for_living_3,general_supports_for_living_2),
                                                     ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                                            ifelse(monthly_allowance_1>monthly_allowance_3,general_supports_for_living_3,general_supports_for_living_1), 
                                                            ifelse(monthly_allowance_1>monthly_allowance_2,general_supports_for_living_2,general_supports_for_living_1))),
            
            transportation_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                        ifelse(monthly_allowance_2>monthly_allowance_3,transportation_3,transportation_2),
                                        ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                               ifelse(monthly_allowance_1>monthly_allowance_3,transportation_3,transportation_1), 
                                               ifelse(monthly_allowance_1>monthly_allowance_2,transportation_2,transportation_1))),
            
            otc_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                             ifelse(monthly_allowance_2>monthly_allowance_3,otc_3,otc_2),
                             ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                    ifelse(monthly_allowance_1>monthly_allowance_3,otc_3,otc_1), 
                                    ifelse(monthly_allowance_1>monthly_allowance_2,otc_2,otc_1))),
            
            meals_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                               ifelse(monthly_allowance_2>monthly_allowance_3,meals_3,meals_2),
                               ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                      ifelse(monthly_allowance_1>monthly_allowance_3,meals_3,meals_1), 
                                      ifelse(monthly_allowance_1>monthly_allowance_2,meals_2,meals_1))),
            
            Others_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                ifelse(monthly_allowance_2>monthly_allowance_3,Others_3,Others_2),
                                ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                       ifelse(monthly_allowance_1>monthly_allowance_3,Others_3,Others_1), 
                                       ifelse(monthly_allowance_1>monthly_allowance_2,Others_2,Others_1))),
            
            Conditions_third=ifelse(monthly_allowance_1>=monthly_allowance_2 & monthly_allowance_1>=monthly_allowance_3, 
                                    ifelse(monthly_allowance_2>monthly_allowance_3,cy2025_vbidufssbci_group_3_additional_services_condition,cy2025_vbidufssbci_group_2_additional_services_condition),
                                    ifelse(monthly_allowance_2>=monthly_allowance_1 & monthly_allowance_2>=monthly_allowance_3, 
                                           ifelse(monthly_allowance_1>monthly_allowance_3,cy2025_vbidufssbci_group_3_additional_services_condition,cy2025_vbidufssbci_group_1_additional_services_condition), 
                                           ifelse(monthly_allowance_1>monthly_allowance_2,cy2025_vbidufssbci_group_2_additional_services_condition,cy2025_vbidufssbci_group_1_additional_services_condition)))) %>%
  
  transmute(year,
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
            
            # cy2025_overthecounter_drug_card,
            # cy2025_overthecounter_drug_card_period,
            
            
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
            
            # monthly_allowance_third,
            # food_and_produce_allowance_third,
            # general_supports_for_living_allowance_third,
            # eligibility_third,
            # food_and_produce_third,
            # general_supports_for_living_third,
            # transportation_third,
            # otc_third,
            # meals_third,
            # Others_third,
            
            anemia=ifelse(grepl("anemia",tolower(Conditions_primary)),"Y","N"),
            asthma=ifelse(grepl("asthma",tolower(Conditions_primary)),"Y","N"),
            autoimmune_disorder=ifelse(grepl("autoimmune disorder",tolower(Conditions_primary)),"Y","N"),
            arthritis=ifelse(grepl("arthritis",tolower(Conditions_primary)),"Y","N"),
            cardiovascular_disorder=ifelse(grepl("cardiovascular disorder",tolower(Conditions_primary)),"Y","N"),
            cellulitis=ifelse(grepl("cellulitis",tolower(Conditions_primary)),"Y","N"),
            circulatory_disease=ifelse(grepl("circulatory disease",tolower(Conditions_primary)),"Y","N"),
            chronic_alcohol_and_other_drug_dependence=ifelse(grepl("chronic alcohol and other drug dependence",tolower(Conditions_primary)),"Y","N"),
            chronic_cognitive_impairment=ifelse(grepl("chronic cognitive impairment",tolower(Conditions_primary)),"Y","N"),
            chronic_heart_failure=ifelse(grepl("chronic heart failure",tolower(Conditions_primary)),"Y","N"),
            chronic_gastrointestinal_disorder=ifelse(grepl("chronic gastrointestinal disorder",tolower(Conditions_primary)) |
                                                       grepl("chronic gi disorder",tolower(Conditions_primary)),"Y","N"),
            malnutrition=ifelse(grepl("malnutrition",tolower(Conditions_primary)),"Y","N"),
            chronic_infectious_disorder=ifelse(grepl("chronic infectious disorder",tolower(Conditions_primary)),"Y","N"),
            chronic_otolaryngological_disorder=ifelse(grepl("chronic otolaryngological disorder",tolower(Conditions_primary)),"Y","N"),
            chronic_pain=ifelse(grepl("chronic pain",tolower(Conditions_primary)),"Y","N"),
            cancer=ifelse(grepl("cancer",tolower(Conditions_primary)),"Y","N"),
            chronic_lung_disorder=ifelse(grepl("chronic lung disorder",tolower(Conditions_primary)),"Y","N"),
            chronic_liver_disease=ifelse(grepl("chronic liver",tolower(Conditions_primary)) |
                                           grepl("chronic kidney/liver",tolower(Conditions_primary)),"Y","N"),
            chronic_and_disabling_mental_health_condition=ifelse(grepl("chronic and disabling mental health condition",tolower(Conditions_primary)),"Y","N"),
            chronic_kidney_disease=ifelse(grepl("chronic kidney disease",tolower(Conditions_primary)) |
                                            grepl("chronic liver/kidney",tolower(Conditions_primary)),"Y","N"),
            chronic_non_alcoholic_fatty_liver_disease=ifelse(grepl("chronic non-alcohol",tolower(Conditions_primary)),"Y","N"),
            copd=ifelse(grepl("copd",tolower(Conditions_primary)) |
                          grepl("chronic obstructive pulmonary disease",tolower(Conditions_primary)),"Y","N"),
            congestive_heart_failure=ifelse(grepl("congestive heart failure",tolower(Conditions_primary)),"Y","N"),
            coronary_artery_disease=ifelse(grepl("coronary artery disease",tolower(Conditions_primary)),"Y","N"),
            cystic_fibrosis=ifelse(grepl("cystic fibrosis",tolower(Conditions_primary)),"Y","N"),
            down_syndrome=ifelse(grepl("down syndrome",tolower(Conditions_primary)),"Y","N"),
            diabete=ifelse(grepl("diabete",tolower(Conditions_primary)),"Y","N"),
            dementia=ifelse(grepl("dementia",tolower(Conditions_primary)),"Y","N"),
            dyslipidemia=ifelse(grepl("dyslipidemia",tolower(Conditions_primary)),"Y","N"),
            eating_disorder=ifelse(grepl("eating disorder",tolower(Conditions_primary)),"Y","N"),
            end_stage_liver_disease=ifelse(grepl("end-stage liver disease",tolower(Conditions_primary)),"Y","N"),
            end_stage_renal_disease=ifelse(grepl("end-stage renal disease",tolower(Conditions_primary)),"Y","N"),
            endometriosis=ifelse(grepl("endometriosis",tolower(Conditions_primary)),"Y","N"),
            endocrine=ifelse(grepl("endocrine",tolower(Conditions_primary)),"Y","N"),
            gastrointestinal=ifelse(grepl("gastrointestinal",tolower(Conditions_primary)),"Y","N"),
            glaucoma=ifelse(grepl("glaucoma",tolower(Conditions_primary)),"Y","N"),
            hiv=ifelse(grepl("hiv",tolower(Conditions_primary)),"Y","N"),
            hepatitis=ifelse(grepl("hepatitis",tolower(Conditions_primary)),"Y","N"),
            hypertension=ifelse(grepl("hypertension",tolower(Conditions_primary)),"Y","N"),
            hyperlipidemia=ifelse(grepl("hyperlipidemia",tolower(Conditions_primary)) |
                                    grepl("chronic lipid",tolower(Conditions_primary)),"Y","N"),
            hypercholesterolemia=ifelse(grepl("hypercholesterolemia",tolower(Conditions_primary)),"Y","N"),
            inflammatory_bowel_disease=ifelse(grepl("inflammatory bowel disease",tolower(Conditions_primary)),"Y","N"),
            joints_and_spine=ifelse(grepl("joints",tolower(Conditions_primary)),"Y","N"),
            loss_of_limb=ifelse(grepl("loss of limb",tolower(Conditions_primary)),"Y","N"),
            low_back_pain=ifelse(grepl("low back pain",tolower(Conditions_primary)),"Y","N"),
            metabolic_syndrome=ifelse(grepl("metabolic syndrome",tolower(Conditions_primary)),"Y","N"),
            muscular_dystrophy=ifelse(grepl("muscular dystrophy",tolower(Conditions_primary)),"Y","N"),
            musculoskeletal_disorder=ifelse(grepl("musculoskeletal disorder",tolower(Conditions_primary)),"Y","N"),
            neurologic_disorder=ifelse(grepl("neurologic disorder",tolower(Conditions_primary)),"Y","N"),
            osteoporosis=ifelse(grepl("osteoporosis",tolower(Conditions_primary)),"Y","N"),
            obesity=ifelse(grepl("obesity",tolower(Conditions_primary)) |
                             grepl("obese",tolower(Conditions_primary)),"Y","N"),
            pneumonia=ifelse(grepl("pneumonia",tolower(Conditions_primary)),"Y","N"),
            pregnancy=ifelse(grepl("pregnancy",tolower(Conditions_primary)),"Y","N"),
            rsd=ifelse(grepl("rsd",tolower(Conditions_primary)),"Y","N"),
            sjogren=ifelse(grepl("sjogren",tolower(Conditions_primary)),"Y","N"),
            severe_hematologic_disorder=ifelse(grepl("severe hematologic disorder",tolower(Conditions_primary)),"Y","N"),
            stroke=ifelse(grepl("stroke",tolower(Conditions_primary)),"Y","N"),
            urinary_tract_infection=ifelse(grepl("urinary tract infection",tolower(Conditions_primary)),"Y","N"),
            urinary_incontinance=ifelse(grepl("urinary incontinance",tolower(Conditions_primary)),"Y","N"),
            vascular_disease=ifelse(grepl("vascular disease",tolower(Conditions_primary)),"Y","N")
            
  )

macvat_2022_benefit_with_geo_formatted_arranged_plan <-
  macvat_2022_benefit_with_geo_formatted_arranged %>% 
  select(-state,-county,-fips,-ssa) %>%
  distinct()

macvat_2022_benefit_with_geo_formatted_arranged_plan_fips <-
  macvat_2022_benefit_with_geo_formatted_arranged %>% 
  select(year,contract_plan,contract_id,PBP,segment,cy2025_plan_type,cy2025_snp_type,cy2025_snp_detail,cy2025_dual_integration_status,
         cy2025_part_c_part_d_coverage,cy2025_plan_name,parent_name,new_plan_flag,cy2025_vbidufssbci_indicator,state,county,fips,ssa) %>%
  distinct()


macvat_2025_benefit_with_geo_formatted_arranged_plan %>%
  rbind(macvat_2024_benefit_with_geo_formatted_arranged_plan) %>%
  rbind(macvat_2023_benefit_with_geo_formatted_arranged_plan) %>%
  rbind(macvat_2022_benefit_with_geo_formatted_arranged_plan) %>%
  write_xlsx("formatted_section_19_plan_level.xlsx")

section_19_SOT <-
  macvat_2025_benefit_with_geo_formatted_arranged_plan %>%
  rbind(macvat_2024_benefit_with_geo_formatted_arranged_plan) %>%
  rbind(macvat_2023_benefit_with_geo_formatted_arranged_plan) %>%
  rbind(macvat_2022_benefit_with_geo_formatted_arranged_plan)


table = bq_table("anbc-hcb-dev","growth_anlyt_hcb_dev","section_19_sot")
bq_table_create(x = table, fields = as_bq_fields(section_19_SOT), labels = list(owner = "xiaol_aetna_com"))
bq_table_upload(x = table, values = section_19_SOT, create_disposition = "CREATE_IF_NEEDED", write_disposition = "WRITE_APPEND")


macvat_2025_benefit_with_geo_formatted_arranged_plan_fips %>%
  rbind(macvat_2024_benefit_with_geo_formatted_arranged_plan_fips) %>%
  rbind(macvat_2023_benefit_with_geo_formatted_arranged_plan_fips) %>%
  rbind(macvat_2022_benefit_with_geo_formatted_arranged_plan_fips) %>%
  write_xlsx("formatted_section_19_plan_fips_combination.xlsx")


macvat_2025_benefit_with_geo_formatted %>%
  rbind(macvat_2024_benefit_with_geo_formatted) %>%
  rbind(macvat_2023_benefit_with_geo_formatted) %>%
  rbind(macvat_2022_benefit_with_geo_formatted) %>%
  select(-ssa,-fips,-state,-county) %>% distinct() %>%
  left_join(macvat_2025_benefit_with_geo_formatted_arranged_plan %>%
              rbind(macvat_2024_benefit_with_geo_formatted_arranged_plan) %>%
              rbind(macvat_2023_benefit_with_geo_formatted_arranged_plan) %>%
              rbind(macvat_2022_benefit_with_geo_formatted_arranged_plan) %>% 
              select(year,contract_plan,monthly_allowance_primary)) %>%
  mutate(covered_or_not=ifelse((is.na(cy2025_vbidufssbci_group_1_additional_services_condition) | trimws(cy2025_vbidufssbci_group_1_additional_services_condition)=="NA") &
                                 (is.na(cy2025_vbidufssbci_group_2_additional_services_condition) | trimws(cy2025_vbidufssbci_group_2_additional_services_condition)=="NA") &
                                 (is.na(cy2025_vbidufssbci_group_3_additional_services_condition) | trimws(cy2025_vbidufssbci_group_3_additional_services_condition)=="NA"),"No","Yes"),
         allowance_identified=ifelse(monthly_allowance_1>0 | monthly_allowance_2>0 | monthly_allowance_3>0 | monthly_allowance_primary>0,"Yes","No")) %>%
  filter(cy2025_plan_type!="Cost" & cy2025_plan_type!="MSA" & cy2025_plan_type!="PFFS" & cy2025_snp_type %in% c("Not SNP","Dual Eligible")) %>%
  count(year,
        cy2025_snp_type,
        covered_or_not,
        allowance_identified) %>%
  group_by(year,
           cy2025_snp_type) %>%
  summarise(num_of_plans=sum(n),
            num_of_not_covered=sum(n[covered_or_not=="No"]),
            num_of_covered_not_identified=sum(n[covered_or_not=="Yes" & allowance_identified=="No"]),
            num_of_covered_identified=sum(n[covered_or_not=="Yes" & allowance_identified=="Yes"])) %>%
  ungroup() %>%
  mutate(identified_rate=num_of_covered_identified/(num_of_covered_identified+num_of_covered_not_identified)) %>%
  write_xlsx("section_19_benefits_coverage_summary.xlsx")


macvat_2025_benefit_with_geo_formatted %>%
  rbind(macvat_2024_benefit_with_geo_formatted) %>%
  rbind(macvat_2023_benefit_with_geo_formatted) %>%
  rbind(macvat_2022_benefit_with_geo_formatted) %>%
  select(-ssa,-fips,-state,-county) %>% distinct() %>%
  left_join(macvat_2025_benefit_with_geo_formatted_arranged_plan %>%
              rbind(macvat_2024_benefit_with_geo_formatted_arranged_plan) %>%
              rbind(macvat_2023_benefit_with_geo_formatted_arranged_plan) %>%
              rbind(macvat_2022_benefit_with_geo_formatted_arranged_plan) %>% 
              select(year,contract_plan,monthly_allowance_primary)) %>%
  mutate(covered_or_not=ifelse((is.na(cy2025_vbidufssbci_group_1_additional_services_condition) | trimws(cy2025_vbidufssbci_group_1_additional_services_condition)=="NA") &
                                 (is.na(cy2025_vbidufssbci_group_2_additional_services_condition) | trimws(cy2025_vbidufssbci_group_2_additional_services_condition)=="NA") &
                                 (is.na(cy2025_vbidufssbci_group_3_additional_services_condition) | trimws(cy2025_vbidufssbci_group_3_additional_services_condition)=="NA"),"No","Yes"),
         allowance_identified=ifelse(monthly_allowance_1>0 | monthly_allowance_2>0 | monthly_allowance_3>0 | monthly_allowance_primary>0,"Yes","No")) %>%
  filter(cy2025_plan_type!="Cost" & cy2025_plan_type!="MSA" & cy2025_plan_type!="PFFS" & cy2025_snp_type %in% c("Not SNP","Dual Eligible")) %>%
  count(year,
        parent_name,
        cy2025_snp_type,
        covered_or_not,
        allowance_identified) %>%
  group_by(year,
           parent_name,
           cy2025_snp_type) %>%
  summarise(num_of_plans=sum(n),
            num_of_not_covered=sum(n[covered_or_not=="No"]),
            num_of_covered_not_identified=sum(n[covered_or_not=="Yes" & allowance_identified=="No"]),
            num_of_covered_identified=sum(n[covered_or_not=="Yes" & allowance_identified=="Yes"])) %>%
  ungroup() %>%
  mutate(identified_rate=num_of_covered_identified/(num_of_covered_identified+num_of_covered_not_identified)) %>%
  write_xlsx("section_19_benefits_coverage_summary_carrier_ge_dsnp.xlsx")
