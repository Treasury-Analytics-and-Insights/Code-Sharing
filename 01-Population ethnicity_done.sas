/******************************************************************************************************************************************************
PULLING OUT ETHNICITY ACROSS COLLECTIONS

Developer: Sarah Tumen(A&I, NZ Treasury)
QA Analyst:
Created date: 11 Sep 2014

******************************************************************************************************************************************************
Obtaining those records of unique citizen in IDI
Notes: This includes unique citizen that have records with IRD, MOE, MSD, Justice)
This includes records of seasonal workers, overseas residents, international students ( at later stage they will be exlcuded from population).
*******************************************************************************************************************************************************;
*/
data population;
	set project.Population1988_&last_anal_yr.;
	keep snz_uid snz_sex_code
		snz_birth_year_nbr
		snz_birth_month_nbr
		snz_ethnicity_grp1_nbr
		snz_ethnicity_grp2_nbr
		snz_ethnicity_grp3_nbr
		snz_ethnicity_grp4_nbr
		snz_ethnicity_grp5_nbr
		snz_ethnicity_grp6_nbr;
run;

data MOH_eth_;
	set moh.pop_cohort_demographics;
	keep snz_uid moh_pop_ethnic_grp1_snz_ind moh_pop_ethnic_grp2_snz_ind
		moh_pop_ethnic_grp3_snz_ind moh_pop_ethnic_grp4_snz_ind
		moh_pop_ethnic_grp5_snz_ind moh_pop_ethnic_grp6_snz_ind;
run;

proc sql;
	create table MOH_eth_nodupl as select distinct
		snz_uid
		,max(moh_pop_ethnic_grp1_snz_ind) as moh_pop_ethnic_grp1_snz_ind
		,max(moh_pop_ethnic_grp2_snz_ind) as moh_pop_ethnic_grp2_snz_ind
		,max(moh_pop_ethnic_grp3_snz_ind) as moh_pop_ethnic_grp3_snz_ind
		,max(moh_pop_ethnic_grp4_snz_ind) as moh_pop_ethnic_grp4_snz_ind
		,max(moh_pop_ethnic_grp5_snz_ind) as moh_pop_ethnic_grp5_snz_ind
		,max(moh_pop_ethnic_grp6_snz_ind) as moh_pop_ethnic_grp6_snz_ind
	from MOH_eth_
		group by snz_uid;

proc sql;
	create table MOH_ethnic as select distinct
		a.*
		,b.moh_pop_ethnic_grp1_snz_ind
		,b.moh_pop_ethnic_grp2_snz_ind
		,b.moh_pop_ethnic_grp3_snz_ind
		,b.moh_pop_ethnic_grp4_snz_ind
		,b.moh_pop_ethnic_grp5_snz_ind
		,b.moh_pop_ethnic_grp6_snz_ind
	from population a left join MOH_eth_nodupl b
		on a.snz_uid=b.snz_uid;

data MOH_ethnic;
	set MOH_ethnic;

	if moh_pop_ethnic_grp1_snz_ind=. then
		moh_pop_ethnic_grp1_snz_ind=0;

	if moh_pop_ethnic_grp2_snz_ind=. then
		moh_pop_ethnic_grp2_snz_ind=0;

	if moh_pop_ethnic_grp3_snz_ind=. then
		moh_pop_ethnic_grp3_snz_ind=0;

	if moh_pop_ethnic_grp4_snz_ind=. then
		moh_pop_ethnic_grp4_snz_ind=0;

	if moh_pop_ethnic_grp5_snz_ind=. then
		moh_pop_ethnic_grp5_snz_ind=0;

	if moh_pop_ethnic_grp6_snz_ind=. then
		moh_pop_ethnic_grp6_snz_ind=0;
run;

data MOE_eth;
	set moe.student_per;
	keep snz_uid moe_spi_ethnic_grp1_snz_ind
		moe_spi_ethnic_grp2_snz_ind
		moe_spi_ethnic_grp3_snz_ind
		moe_spi_ethnic_grp4_snz_ind
		moe_spi_ethnic_grp5_snz_ind
		moe_spi_ethnic_grp6_snz_ind;
run;

proc sql;
	create table MOE_eth_nodupl as select distinct
		snz_uid
		,max(moe_spi_ethnic_grp1_snz_ind) as moe_spi_ethnic_grp1_snz_ind
		,max(moe_spi_ethnic_grp2_snz_ind) as moe_spi_ethnic_grp2_snz_ind
		,max(moe_spi_ethnic_grp3_snz_ind) as moe_spi_ethnic_grp3_snz_ind
		,max(moe_spi_ethnic_grp4_snz_ind) as moe_spi_ethnic_grp4_snz_ind
		,max(moe_spi_ethnic_grp5_snz_ind) as moe_spi_ethnic_grp5_snz_ind
		,max(moe_spi_ethnic_grp6_snz_ind) as moe_spi_ethnic_grp6_snz_ind
	from MOE_eth
		group by snz_uid;

proc sql;
	create table MOH_MOE_ethnic as select distinct
		a.*
		,b.moe_spi_ethnic_grp1_snz_ind
		,b.moe_spi_ethnic_grp2_snz_ind
		,b.moe_spi_ethnic_grp3_snz_ind
		,b.moe_spi_ethnic_grp4_snz_ind
		,b.moe_spi_ethnic_grp5_snz_ind
		,b.moe_spi_ethnic_grp6_snz_ind
	from MOH_ethnic a left join MOE_eth_nodupl  b
		on a.snz_uid=b.snz_uid;

data MOH_MOE_ethnic;
	set MOH_MOE_ethnic;

	if moe_spi_ethnic_grp1_snz_ind=. then
		moe_spi_ethnic_grp1_snz_ind=0;

	if moe_spi_ethnic_grp2_snz_ind=. then
		moe_spi_ethnic_grp2_snz_ind=0;

	if moe_spi_ethnic_grp3_snz_ind=. then
		moe_spi_ethnic_grp3_snz_ind=0;

	if moe_spi_ethnic_grp4_snz_ind=. then
		moe_spi_ethnic_grp4_snz_ind=0;

	if moe_spi_ethnic_grp5_snz_ind=. then
		moe_spi_ethnic_grp5_snz_ind=0;

	if moe_spi_ethnic_grp6_snz_ind=. then
		moe_spi_ethnic_grp6_snz_ind=0;
run;

data MSD_eth;
	set msd.msd_swn;
	keep snz_uid 
		msd_swn_ethnic_grp1_snz_ind
		msd_swn_ethnic_grp2_snz_ind
		msd_swn_ethnic_grp3_snz_ind
		msd_swn_ethnic_grp4_snz_ind
		msd_swn_ethnic_grp5_snz_ind
		msd_swn_ethnic_grp6_snz_ind;
run;

proc sql;
	create table MSd_eth_nodupl as select distinct
		snz_uid
		,max(msd_swn_ethnic_grp1_snz_ind) as msd_swn_ethnic_grp1_snz_ind
		,max(msd_swn_ethnic_grp2_snz_ind) as msd_swn_ethnic_grp2_snz_ind
		,max(msd_swn_ethnic_grp3_snz_ind) as msd_swn_ethnic_grp3_snz_ind
		,max(msd_swn_ethnic_grp4_snz_ind) as msd_swn_ethnic_grp4_snz_ind
		,max(msd_swn_ethnic_grp5_snz_ind) as msd_swn_ethnic_grp5_snz_ind
		,max(msd_swn_ethnic_grp6_snz_ind) as msd_swn_ethnic_grp6_snz_ind
	from MSD_eth
		group by snz_uid;

proc sql;
	create table MOH_MOE_MSD_ethnic as select distinct
		a.*
		,msd_swn_ethnic_grp1_snz_ind
		,msd_swn_ethnic_grp2_snz_ind
		,msd_swn_ethnic_grp3_snz_ind
		,msd_swn_ethnic_grp4_snz_ind
		,msd_swn_ethnic_grp5_snz_ind
		,msd_swn_ethnic_grp6_snz_ind
	from MOH_MOE_ethnic a left join MSD_eth_nodupl b
		on a.snz_uid=b.snz_uid;

data MOH_MOE_MSD_ethnic;
	set MOH_MOE_MSD_ethnic;

	if msd_swn_ethnic_grp1_snz_ind=. then
		msd_swn_ethnic_grp1_snz_ind=0;

	if msd_swn_ethnic_grp2_snz_ind=. then
		msd_swn_ethnic_grp2_snz_ind=0;

	if msd_swn_ethnic_grp3_snz_ind=. then
		msd_swn_ethnic_grp3_snz_ind=0;

	if msd_swn_ethnic_grp4_snz_ind=. then
		msd_swn_ethnic_grp4_snz_ind=0;

	if msd_swn_ethnic_grp5_snz_ind=. then
		msd_swn_ethnic_grp5_snz_ind=0;

	if msd_swn_ethnic_grp6_snz_ind=. then
		msd_swn_ethnic_grp6_snz_ind=0;
run;

data DIA_eth;
	set DIA.births;
	keep snz_uid 
		dia_bir_ethnic_grp1_snz_ind
		dia_bir_ethnic_grp2_snz_ind
		dia_bir_ethnic_grp3_snz_ind
		dia_bir_ethnic_grp4_snz_ind
		dia_bir_ethnic_grp5_snz_ind
		dia_bir_ethnic_grp6_snz_ind
	;
run;

proc sql;
	create table DIA_eth_nodupl as select distinct
		snz_uid
		,max(dia_bir_ethnic_grp1_snz_ind) as dia_bir_ethnic_grp1_snz_ind
		,max(dia_bir_ethnic_grp2_snz_ind) as dia_bir_ethnic_grp2_snz_ind
		,max(dia_bir_ethnic_grp3_snz_ind) as dia_bir_ethnic_grp3_snz_ind
		,max(dia_bir_ethnic_grp4_snz_ind) as dia_bir_ethnic_grp4_snz_ind
		,max(dia_bir_ethnic_grp5_snz_ind) as dia_bir_ethnic_grp5_snz_ind
		,max(dia_bir_ethnic_grp6_snz_ind) as dia_bir_ethnic_grp6_snz_ind
	from DIA_eth
		group by snz_uid;

proc sql;
	create table population_eth1988_&last_anal_yr. as select distinct
		a.* 
		,b.dia_bir_ethnic_grp1_snz_ind
		,b.dia_bir_ethnic_grp2_snz_ind
		,b.dia_bir_ethnic_grp3_snz_ind
		,b.dia_bir_ethnic_grp4_snz_ind
		,b.dia_bir_ethnic_grp5_snz_ind
		,b.dia_bir_ethnic_grp6_snz_ind

	from MOH_MOE_MSD_ethnic a left join DIA_eth_nodupl b
		on a.snz_uid=b.snz_uid;

data project.population_eth1988_&last_anal_yr.;
	set population_eth1988_&last_anal_yr.;

	if dia_bir_ethnic_grp1_snz_ind=. then
		dia_bir_ethnic_grp1_snz_ind=0;

	if dia_bir_ethnic_grp2_snz_ind=. then
		dia_bir_ethnic_grp2_snz_ind=0;

	if dia_bir_ethnic_grp3_snz_ind=. then
		dia_bir_ethnic_grp3_snz_ind=0;

	if dia_bir_ethnic_grp4_snz_ind=. then
		dia_bir_ethnic_grp4_snz_ind=0;

	if dia_bir_ethnic_grp5_snz_ind=. then
		dia_bir_ethnic_grp5_snz_ind=0;

	if dia_bir_ethnic_grp6_snz_ind=. then
		dia_bir_ethnic_grp6_snz_ind=0;
run;

proc datasets lib=work kill nolist memtype=data;
quit;