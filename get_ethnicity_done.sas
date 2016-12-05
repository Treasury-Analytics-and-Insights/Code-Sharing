******************************************************************************************************************************************************
PULLING OUT ETHNICITY ACROSS COLLECTIONS

Analyst: Sarah Tumen
QA Analyst:
Created date: 11 Sep 2014
Code modified : 23 June 2015 to capture youth population as at end of 2013 (aged 15 and above).

******************************************************************************************************************************************************
       Obtaining those records of unique citizen in IDI
	Notes: This includes unique citizen that have records with IRD, MOE, MSD, Justice)
	This includes records of seasonal workers, overseas residents, international students ( at later stage they will be exlcuded from population).
*******************************************************************************************************************************************************;

%macro Create_ethnicity_pop;
proc sql;
create table MOH_eth_
as select 
snz_uid 
,moh_pop_ethnic_grp1_snz_ind 
,moh_pop_ethnic_grp2_snz_ind
,moh_pop_ethnic_grp3_snz_ind 
,moh_pop_ethnic_grp4_snz_ind
,moh_pop_ethnic_grp5_snz_ind 
,moh_pop_ethnic_grp6_snz_ind
from moh.pop_cohort_demographics 
where snz_uid in (select snz_uid from &population)
order by snz_uid;


* adding MOE ethnicity;
proc sql;
create table MOE_eth_
as select 
snz_uid 
,moe_spi_ethnic_grp1_snz_ind 
,moe_spi_ethnic_grp2_snz_ind
,moe_spi_ethnic_grp3_snz_ind 
,moe_spi_ethnic_grp4_snz_ind
,moe_spi_ethnic_grp5_snz_ind 
,moe_spi_ethnic_grp6_snz_ind
from moe.student_per 
where snz_uid in (select snz_uid from &population)
order by snz_uid;


* adding MSD ethnicity;

proc sql;
create table MSD_eth_
as select 
snz_uid 
,msd_swn_ethnic_grp1_snz_ind 
,msd_swn_ethnic_grp2_snz_ind
,msd_swn_ethnic_grp3_snz_ind 
,msd_swn_ethnic_grp4_snz_ind
,msd_swn_ethnic_grp5_snz_ind 
,msd_swn_ethnic_grp6_snz_ind
from msd.msd_swn
where snz_uid in (select snz_uid from &population)
order by snz_uid;

* adding DIA ethnicity at birth;

proc sql;
create table DIA_eth_
as select 
snz_uid 
,dia_bir_ethnic_grp1_snz_ind
,dia_bir_ethnic_grp2_snz_ind
,dia_bir_ethnic_grp3_snz_ind
,dia_bir_ethnic_grp4_snz_ind
,dia_bir_ethnic_grp5_snz_ind
,dia_bir_ethnic_grp6_snz_ind
from DIA.births
where snz_uid in (select snz_uid from &population)
order by snz_uid;

* statnz ethnicity;
proc sql;
create table SNZ_eth_
as select 
snz_uid 
,snz_ethnicity_grp1_nbr
,snz_ethnicity_grp2_nbr
,snz_ethnicity_grp3_nbr
,snz_ethnicity_grp4_nbr
,snz_ethnicity_grp5_nbr
,snz_ethnicity_grp6_nbr
from data.personal_detail
where snz_uid in (select snz_uid from &population)
order by snz_uid;


data &projectlib.._IND_ethnicity_&date.; merge  
snz_eth_ DIA_eth_ MSD_eth_ MOE_eth_ MOH_eth_;by snz_uid;
run;

proc datasets lib=work;
delete snz_eth_ DIA_eth_ MSD_eth_ MOE_eth_ MOH_eth_;
run;

%mend;