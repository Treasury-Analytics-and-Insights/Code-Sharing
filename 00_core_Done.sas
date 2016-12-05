* Set macro variables;
%let version=archive;

* for IDI refresh version control;
%let date=20161021;

* for dataset version control;
%let sensor=31Dec2015;

* Global sensor data cut of date;
%let first_anal_yr=1988;
%let msd_left_yr=1993;
%let cyf_left_yr=1990;

* first calendar year of analysis;
%let last_anal_yr=2015;

* Last calendar year of analysis;
%let firstage=0;

* start creating variables from birth to age 1;
%let lastage=26;
%let cyf_lastage=18;

%let cohort_start=&first_anal_yr;

* start creating variables till age 24;

* Start of monthly array count;
%let start='01Jan1993'd;


%let projectlib=project;

* include A&I standard library and macros;
%let path=\\wprdsas10\TreasuryData\MAA2013-16 Citizen pathways through human services\Social Investment_2016\1_Indicator_at_age_datasets\;
%include "\\wprdsas10\TreasuryData\MAA2013-16 Citizen pathways through human services\Common Code\Std_macros_and_libs\Stand_macro_new.sas";
%include "\\wprdsas10\TreasuryData\MAA2013-16 Citizen pathways through human services\Common Code\Std_macros_and_libs\Std_libs.txt";

%include "&path.codes for rerun_21102016\FORMATS_new.sas";
%include "&path.codes for rerun_21102016\COR_macro_new.sas";
%include "&path.codes for rerun_21102016\CYF_macro_new.sas";
%include "&path.codes for rerun_21102016\MSD_macro_new.sas";
%include "&path.codes for rerun_21102016\MOE_macro_new.sas";
%include "&path.codes for rerun_21102016\INCOME_COST_macro_new.sas";
%include "&path.codes for rerun_21102016\CUST_macro_new.sas";
%include "&path.codes for rerun_21102016\get_ethnicity_new.sas";
%include "&path.codes for rerun_21102016\Relationships_macro_new.sas";
%include "&path.codes for rerun_21102016\HEALTH_macro_new.sas";
%include "&path.codes for rerun_21102016\CG_CORR_macro_new.sas";
%include "&path.codes for rerun_21102016\CG_CYF_macro_new.sas";
%include "&path.codes for rerun_21102016\SIB_CYF_macro_new.sas";
%include "&path.codes for rerun_21102016\Maternal_EDU_macro_new.sas";


libname Project "&path.Dataset_rerun_21102016";

%let population=project.Population1988_2016;
proc sort data=&population; by snz_uid;run;

options compress=yes reuse=yes ;

%Create_relationship_tables_pop; * run for population1988_2016;
%Create_ethnicity_pop;* run for population1988_2016;

%Create_MSD_ind_child_pop;* run for population1988_2016;
%Create_MSD_ind_adult_pop;* run for population1988_2016;

%Create_CYF_ind_pop;* run for population1988_2016;
%Create_CORR_ind_pop;* run for population1988_2016;

%Create_sch_enr_da_pop;* run for population1988_2016;
%Create_sch_attended_pop;* run for population1988_2016;
%Create_sch_qual_pop;* run for population1988_2016;

%Create_edu_interv_pop;* run for population1988_2016;
%Create_ter_enrol_pop;* run for population1988_2016;
%Create_IT_MA_enrol_pop;* run for population1988_2016;

%Create_ter_compl_pop;* run for population1988_2016;

%Create_Earn_pop;* running;
%Create_OS_spell_pop;* run for population1988_2016;
%Create_BEN_cost_pop;* run for population1988_2016;

%Create_MH_ind_pop;* running;

* Correction history of caregivers;
%Create_CG_corr_history(msd,1);* run for population1988_2016;
%Create_CG_corr_history(msd,2);* run for population1988_2016;

%Create_CG_corr_history(dia,1);* run for population1988_2016;
%Create_CG_corr_history(dia,2);* run for population1988_2016;

%Create_CG_corr_history(cen,1);* NOT run ;
%Create_CG_corr_history(cen,2);* NOT run ;

%Create_CG_corr_history(dol,1);* NOT run ;
%Create_CG_corr_history(dol,2);* NOT run ;

* CYF history of caregivers;
%Create_CG_CYF_history(msd,1);* run for population1988_2016;
%Create_CG_CYF_history(msd,2);* run for population1988_2016;

%Create_CG_CYF_history(dia,1);* run for population1988_2016;
%Create_CG_CYF_history(dia,2);* run for population1988_2016;

%Create_CG_CYF_history(cen,1);* NOT run ;
%Create_CG_CYF_history(cen,2);* NOT run ;

%Create_CG_CYF_history(cen,1);* NOT run ;
%Create_CG_CYF_history(cen,2);* NOT run ;

* CYF hisory of siblings;
%Create_sib_CYF_pop(msd,msd);* run for population1988_2016;
%Create_sib_CYF_pop(dia,dia);* run for population1988_2016;

* Maternal education of mothers;
%Create_Mat_edu_pop(dia);* run for population1988_2016;
%Create_Mat_edu_pop(msd);* run for population1988_2016;

%Create_Mat_edu_pop(cen);* NOT run ;
%Create_Mat_edu_pop(dol);* NOT run ;

