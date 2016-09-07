
%let version=20151012;
%let date=15102015;
%let sensor=31Dec2014;
%let first_anal_yr=1988;
%let last_anal_yr=2014;

%let firstage=0;
%let lastage=25;
%let cohort_start=&first_anal_yr;

%let start='01Jan1993'd;

%include "\\wprdfs08\TreasuryData\MAA2013-16 Citizen pathways through human services\Common Code\Std_macros_and_libs\Std_macros.txt";
%include "\\wprdfs08\TreasuryData\MAA2013-16 Citizen pathways through human services\Common Code\Std_macros_and_libs\Std_libs.txt";
%include "\\wprdfs08\TreasuryData\MAA2013-16 Citizen pathways through human services\Common Code\Std_macros_and_libs\BDD_rules_macros.txt";
%include "\\wprdfs08\TreasuryData\MAA2013-16 Citizen pathways through human services\Common Code\Std_macros_and_libs\CYF_rules_macros.txt";
%include "\\wprdfs08\TreasuryData\MAA2013-16 Citizen pathways through human services\Common Code\Std_macros_and_libs\Education_rules_macros.txt";
%include "\\wprdfs08\TreasuryData\MAA2013-16 Citizen pathways through human services\Common Code\Std_macros_and_libs\Correction_rules_macros.txt";
%include "\\wprdfs08\TreasuryData\MAA2013-16 Citizen pathways through human services\Common Code\Std_macros_and_libs\get_mother.sas";
%include "\\wprdfs08\TreasuryData\MAA2013-16 Citizen pathways through human services\Common Code\Std_macros_and_libs\get_caregivers.sas";
libname Project "\\wprdfs08\TreasuryData\MAA2013-16 Citizen pathways through human services\SarahT\2015_2_cohortanalysis\test";

%let population=project.population1988_2014;

proc sort data=&population;
	by snz_uid;
run;