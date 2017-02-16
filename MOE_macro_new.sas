***************************************************************************************************************************************
Calcualtes enrolment days by calendar year window
***************************************************************************************************************************************;

%macro enr_yr(type);* sch;

array &type._enr_da_[*] &type._enr_da_&first_anal_yr.-&type._enr_da_&last_anal_yr.;
	&type._enr_da_(i)=0;

if not((startdate > end_window) or (enddate < start_window)) then do;

					if (startdate <= start_window) and  (enddate > end_window) then
						days=(end_window-start_window)+1;
					else if (startdate <= start_window) and  (enddate <= end_window) then
						days=(enddate-start_window)+1;
					else if (startdate > start_window) and  (enddate <= end_window) then
						days=(enddate-startdate)+1;
					else if (startdate > start_window) and  (enddate > end_window) then
						days=(end_window-startdate)+1;	

					&type._enr_da_(i)=days;

end;

%mend;

***************************************************************************************************************************************
Calcualtes enrolment days by calendar year window
***************************************************************************************************************************************;

%macro enr_age(type);* sch;
 
array &type._enr_da_at_age_[*] &type._enr_da_at_age_&firstage.-&type._enr_da_at_age_&lastage.;
	&type._enr_da_at_age_(age)=0;

if not((startdate > end_window) or (enddate < start_window)) then do;

					if (startdate <= start_window) and  (enddate > end_window) then
						days=(end_window-start_window)+1;
					else if (startdate <= start_window) and  (enddate <= end_window) then
						days=(enddate-start_window)+1;
					else if (startdate > start_window) and  (enddate <= end_window) then
						days=(enddate-startdate)+1;
					else if (startdate > start_window) and  (enddate > end_window) then
						days=(end_window-startdate)+1;	

					&type._enr_da_at_age_(age)=days;

end;

%mend;


***************************************************************************************************************************************
Calculates enrolment days by calendar year window
***************************************************************************************************************************************;

%macro qual_yr(type);* sch;
array &type._[*] &type._&first_anal_yr.-&type._&last_anal_yr.;
if ind=year then &type._(i)=1; else &type._(i)=0;
%mend;

***************************************************************************************************************************************
Calculates 
***************************************************************************************************************************************;

%macro Create_sch_enr_da_pop;
proc sql;
	create table sch_enrol
		as select distinct 
			a.snz_uid
			,a.snz_moe_uid
			,input(compress(a.moe_esi_start_date,"-"),yymmdd10.) format date9. as startdate
			,input(compress(a.moe_esi_end_date,"-"),yymmdd10.) format date9. as enddate
			,input(a.moe_esi_provider_code,3.) as schoolnumber
			,input(compress(a.moe_esi_extrtn_date,"-"),yymmdd10.) format date9.  as ExtractionDate
			,b.DOB
			,year(b.DOB)+19 as year19_birth

		from moe.student_enrol a inner join &population b
		on a.snz_uid=b.snz_uid
				order by snz_uid;
quit;


data sch_enrol; set sch_enrol;
* imputing enddates for those who are over 19;
if enddate=. and ExtractionDate>intnx('YEAR',DOB,19,'S') then enddate=MDY(12,31,year19_birth); 
else if enddate=. then	enddate=ExtractionDate;

if enddate>startdate;

	* cleaning for errorness records JL;
if startdate>0;

if startdate>"&sensor"d then
		delete;

if enddate>"&sensor"d then
		enddate="&sensor"d;
drop year19_birth;
run;

* lets make sure no duplicate records of enrolments;
proc sort data=sch_enrol nodupkey;
	by snz_uid startdate enddate schoolnumber;
run;

proc sort data=sch_enrol;
	by snz_uid startdate enddate;
run;

%OVERLAP (sch_enrol);

data sch_enrol_OR; set sch_enrol_OR;
start1=MDY(1,1,&first_anal_yr.); format start1 date9.;

do ind=&first_anal_yr. to &last_anal_yr.;
			i=ind-(&first_anal_yr.-1);

			start_window=intnx('YEAR',start1,i-1,'S');
			end_window=intnx('YEAR',start1,i,'S')-1;

			%enr_yr(sch);

end;

do i=&firstage. to &lastage.;
			age=i-(&firstage.-1);
			start_window=intnx('YEAR',DOB,age-1,'S');
			end_window=intnx('YEAR',DOB,age,'S')-1;
			%enr_age(sch);
end;
proc summary data=sch_enrol_OR nway;
class snz_uid DOB;
var sch_enr_da_2006-sch_enr_da_&last_anal_yr. ;
output out=&projectlib.._IND_SCH_enr_&date.(drop=_:) sum=;
run;

proc summary data=sch_enrol_OR nway;
class snz_uid DOB;
var sch_enr_da_at_age_4-sch_enr_da_at_age_&lastage.;
output out=&projectlib.._IND_SCH_enr_at_age_&date.(drop=_:) sum=;
run;

Proc datasets lib=work;
delete sch_enrol sch_enrol_OR deletes errors;
run;
%mend;

*******************************************************************************************************************************
Create tertiary enrolment for given population
*******************************************************************************************************************************;

%macro Create_ter_enrol_pop;
%* FORMATING, CLEANING AND SENSORING;
proc sql;
	create table ter_enrol as

	SELECT distinct 
		a.snz_uid
		,a.moe_enr_year_nbr
		,input(compress(a.moe_enr_prog_start_date,"-"),yymmdd10.) format date9. as startdate
		,input(compress(a.moe_enr_prog_end_date,"-"),yymmdd10.) format date9.  as enddate
		,year(input(compress(a.moe_enr_prog_start_date,"-"),yymmdd10.)) as start_year		
		,sum(a.moe_enr_efts_consumed_nbr) as EFTS_consumed
		,a.moe_enr_efts_prog_years_nbr as EFTS_prog_yrs
		,a.moe_enr_qacc_code as qacc
		,a.moe_enr_qual_code as Qual
		,a.moe_enr_prog_nzsced_code as NZSCED
		,(case when a.moe_enr_funding_srce_code in ('05','06','07','08','11','24') and a.moe_enr_qual_type_code="D" then 1 else 0 end) as Formal
		,a.moe_enr_subsector_code as subsector format $subsector.
		,a.moe_enr_qual_level_code as level
/*		,a.moe_enr_qual_type_code as qual_type*/
		,a.moe_enr_is_domestic_ind
		,a.moe_enr_residency_status_code
		,a.moe_enr_provider_code
		,b.DOB
	FROM moe.enrolment a inner join &population b
	on a.snz_uid=b.snz_uid
		WHERE a.moe_enr_year_nbr>=&first_anal_yr and a.moe_enr_year_nbr<=&last_anal_yr
			order by snz_uid;
quit;

data ter_enrol; set ter_enrol;
if EFTS_consumed>0;
if enddate-startdate>0;
if start_year>=&first_anal_yr and start_year<=&last_anal_yr;
if enddate>"&sensor"d then enddate="&sensor"d;
if startdate>="&sensor"d then delete;

run;

%overlap(ter_enrol);

data ter_enrol_OR; set ter_enrol_OR;
start1=MDY(1,1,&first_anal_yr.); format start1 date9.;

if formal=1 then do;
		do ind=&first_anal_yr. to &last_anal_yr.;
					i=ind-(&first_anal_yr.-1);

					start_window=intnx('YEAR',start1,i-1,'S');
					end_window=intnx('YEAR',start1,i,'S')-1;

					%enr_yr(f_ter);

		end;

		do i=&firstage. to &lastage.;
					age=i-(&firstage.-1);
					start_window=intnx('YEAR',DOB,age-1,'S');
					end_window=intnx('YEAR',DOB,age,'S')-1;
					%enr_age(f_ter);
		end;
end;
if formal=0 then do;
		do ind=&first_anal_yr. to &last_anal_yr.;
					i=ind-(&first_anal_yr.-1);

					start_window=intnx('YEAR',start1,i-1,'S');
					end_window=intnx('YEAR',start1,i,'S')-1;

					%enr_yr(nf_ter);

		end;

		do i=&firstage. to &lastage.;
					age=i-(&firstage.-1);
					start_window=intnx('YEAR',DOB,age-1,'S');
					end_window=intnx('YEAR',DOB,age,'S')-1;
					%enr_age(nf_ter);
		end;
end;

run;

proc summary data=ter_enrol_OR nway;
class snz_uid DOB;
var f_ter_enr_da_1994-f_ter_enr_da_&last_anal_yr. 
nf_ter_enr_da_1994-nf_ter_enr_da_&last_anal_yr. ;
output out=&projectlib.._IND_TER_enr_&date.(drop=_:) sum=;
run;

proc summary data=ter_enrol_OR nway;
class snz_uid DOB;
var f_ter_enr_da_at_age_10-f_ter_enr_da_at_age_&lastage. 
nf_ter_enr_da_at_age_10-nf_ter_enr_da_at_age_&lastage. ;
output out=&projectlib.._IND_TER_enr_at_age_&date.(drop=_:) sum=;
run;

proc datasets lib=work;
delete ter_enrol ter_enrol_OR deletes;
run;
%mend;


*******************************************************************************************************************************
Create enrolment in Industry training and Modern apprenticeship for given population
*******************************************************************************************************************************;

%macro Create_IT_MA_enrol_pop;
* FORMATING, SENSORING AND CLEANING;
proc sql;
create table IT as select
a.snz_uid,
input(compress(moe_itl_start_date,"-"),yymmdd10.) format date9. as startdate,
input(compress(moe_itl_end_date,"-"),yymmdd10.) format date9. as enddate,
a.moe_itl_programme_type_code,
a.moe_itl_sum_units_consumed_nbr,
a.moe_itl_fund_code,
year(input(compress(moe_itl_start_date,"-"),yymmdd10.)) as year,
b.DOB
from moe.tec_it_learner a inner join &population b
on a.snz_uid=b.snz_uid
where moe_itl_programme_type_code in ("NC","TC");

data ITL MA; Set IT;
if enddate=. then enddate="&sensor"d;
if startdate>"&sensor"d then delete;
if enddate>"&sensor"d then enddate="&sensor"d;
if startdate>enddate then delete;
if year>=2003;
if moe_itl_fund_code='IT' then output ITL;
if moe_itl_fund_code='MA' then output MA;
run;

proc sort data=ITL(keep=snz_uid DOB startdate enddate moe_itl_fund_code) nodupkey ; 
by snz_uid startdate enddate ; 
proc sort data=MA (keep=snz_uid DOB startdate enddate moe_itl_fund_code) nodupkey; 
by snz_uid startdate enddate ; run;

%overlap(ITL);
%overlap(MA);

data ITL_OR; set ITL_OR;
start1=MDY(1,1,&first_anal_yr.);
do ind=&first_anal_yr. to &last_anal_yr.;
			i=ind-(&first_anal_yr.-1);

			start_window=intnx('YEAR',start1,i-1,'S');
			end_window=intnx('YEAR',start1,i,'S')-1;

			%enr_yr(IT);

end;

do i=&firstage. to &lastage.;
			age=i-(&firstage.-1);
			start_window=intnx('YEAR',DOB,age-1,'S');
			end_window=intnx('YEAR',DOB,age,'S')-1;
			%enr_age(IT);
end;
run;

data MA_OR; set MA_OR;
start1=MDY(1,1,&first_anal_yr.);
do ind=&first_anal_yr. to &last_anal_yr.;
			i=ind-(&first_anal_yr.-1);

			start_window=intnx('YEAR',start1,i-1,'S');
			end_window=intnx('YEAR',start1,i,'S')-1;

			%enr_yr(MA);

end;

do i=&firstage. to &lastage.;
			age=i-(&firstage.-1);
			start_window=intnx('YEAR',DOB,age-1,'S');
			end_window=intnx('YEAR',DOB,age,'S')-1;
			%enr_age(MA);
end;
run;

proc summary data=MA_OR nway;
class snz_uid DOB;
var MA_enr_da_2003-MA_enr_da_&last_anal_yr. ;
output out=TEMP1(drop=_:) sum=;

proc summary data=ITL_OR nway;
class snz_uid DOB;
var IT_enr_da_2003-IT_enr_da_&last_anal_yr. ;
output out=TEMP2(drop=_:) sum=;

Data &projectlib.._IND_IT_MA_ENR_&date; merge TEMP1 TEMP2; by snz_uid; run;

proc summary data=MA_OR nway;
class snz_uid DOB;
var MA_enr_da_at_age_15-MA_enr_da_at_age_&lastage.;
output out=TEMP1(drop=_:) sum=;

proc summary data=ITL_OR nway;
class snz_uid DOB;
var IT_enr_da_at_age_15-IT_enr_da_at_age_&lastage.;
output out=TEMP2(drop=_:) sum=;

Data &projectlib.._IND_IT_MA_ENR_at_age_&date; merge TEMP1 TEMP2; by snz_uid; run;


proc datasets lib=work;
delete TEMP1 TEMP2 Deletes IT MA IT_OR MA_OR ITL:;
run;

%mend;

********************************************************************************************************************************************
Create School qualifications fro popualtion of interest
********************************************************************************************************************************************;

%macro create_sch_qual_pop;
proc sql;
create table sch_qual as 
select 
a.snz_uid,
a.moe_sql_qual_code as qual,
a.moe_sql_exam_result_code as result,
a.moe_sql_award_provider_code as awardingschool,
a.moe_sql_nqf_level_code as level,
a.moe_sql_attained_year_nbr as year,
a.moe_sql_endorsed_year_nbr as end_year,
input(compress(a.moe_sql_nzqa_load_date,"-"),yymmdd10.) format date9. as nzqaloadeddate1,
year(input(compress(a.moe_sql_nzqa_load_date,"-"),yymmdd10.)) as load_year,
b.DOB,
c.QualificationCode,
c.QualificationType,
c.NQFlevel
from moe.student_qualification a inner join &population b
on a.snz_uid=b.snz_uid
left join sandmoe.moe_qualification_lookup c
on a.moe_sql_qual_code=c.qualificationtableid
order by snz_uid, year;
quit;


%* BUSINESS RULES TO DEFINE NCEA ATTAINMENT: 
* limiting to national certificates which is 99& percent of records;
* excluding qualifications gained prior to 2006, before NCEA time;

DATA sch_qual; Set sch_qual;
HA=0;
* Allows 2 years for loading qualifications;
if year=load_year or load_year-year<=2 or load_year=.; 
if NQFlevel in (0,.) then delete;

if year < 2003 then delete; 
if year>=&first_anal_yr and year<=&last_anal_yr;

if nqflevel >= 4 and QualificationType=21 then ha=41;
else if nqflevel >= 4 and QualificationType=10 then ha=40;
else if nqflevel >= 4 then ha=42;
else if qualificationcode='1039' and result='E' then HA=39;
else if qualificationcode='1039' and result='M' then HA=38;
else if qualificationcode='1039' and result='ZZ' then HA=37;
else if qualificationcode='1039' and result='N' then HA=36;
else if nqflevel=3 then HA=35;
else if (qualificationcode='0973' or qualificationcode='973') and result='E' then HA=29;
else if (qualificationcode='0973' or qualificationcode='973') and result='M' then HA=28;
else if (qualificationcode='0973' or qualificationcode='973') and result='ZZ' then HA=27;
else if (qualificationcode='0973' or qualificationcode='973') and result='N' then HA=26;
else if nqflevel=2 then HA=25;
else if (qualificationcode='0928' or qualificationcode='928') and result='E' then HA=19;
else if (qualificationcode='0928' or qualificationcode='928') and result='M' then HA=18;
else if (qualificationcode='0928' or qualificationcode='928') and result='ZZ' then HA=17;
else if (qualificationcode='0928' or qualificationcode='928') and result='N' then HA=16;
else if nqflevel=1 then HA=15;

do ind=&first_anal_yr. to &last_anal_yr.;
			i=ind-(&first_anal_yr.-1);

if HA in (19,18,17,16) then do; %qual_yr(NCEA_L1); end;
if HA=15 then do; %qual_yr(non_NCEA_L1); end;
if HA in (29,28,27,26) then  do; %qual_yr(NCEA_L2); end;
if HA=25 then  do; %qual_yr(non_NCEA_L2); end;

if HA in (39,38,37,36) then  do; %qual_yr(NCEA_L3); end;
if HA=35 then  do; %qual_yr(non_NCEA_L3); end;
if HA in (42,41,40) then  do; %qual_yr(non_NCEA_L4); end;

if HA=19 then  do; %qual_yr(NCEA_L1_E); end;
if HA=18 then  do; %qual_yr(NCEA_L1_M); end;

if HA=29 then  do; %qual_yr(NCEA_L2_E); end;
if HA=28 then  do; %qual_yr(NCEA_L2_M); end;

if HA=39 then  do; %qual_yr(NCEA_L3_E); end;
if HA=38 then  do; %qual_yr(NCEA_L3_M); end;
			
end;
cohort=year(DOB);
run;
proc summary data=sch_qual nway;
class snz_uid DOB cohort;
var NCEA_L1_2006-NCEA_L1_&last_anal_yr.
NCEA_L2_2006-NCEA_L2_&last_anal_yr.
NCEA_L3_2006-NCEA_L3_&last_anal_yr.

non_NCEA_L1_2006-non_NCEA_L1_&last_anal_yr.
non_NCEA_L2_2006-non_NCEA_L2_&last_anal_yr.
non_NCEA_L3_2006-non_NCEA_L3_&last_anal_yr.;
output out=&projectlib.._IND_SCH_QUAL_&date. (drop=_:) max=;
run;

proc datasets lib=work;
delete deletes sch_qual ;
run;

%mend;

********************************************************************************************************************************************
Create indicators of School intervention 
********************************************************************************************************************************************;
%macro Create_edu_interv_pop;
proc sql;
	create table interventions as select 
		a.snz_uid
		,input(compress(moe_inv_start_date,"-"),yymmdd10.) format date9. as startdate
		,input(compress(moe_inv_end_date,"-"),yymmdd10.) format date9. as enddate
		,input(compress(moe_inv_extrtn_date,"-"),yymmdd10.) format date9. as extractiondate
		,put(input(a.moe_inv_intrvtn_code,3.),interv_grp.) as interv_grp
		,b.DOB 
	from moe.student_interventions a inner join &population b 
		on a.snz_uid=b.snz_uid
	order by b.snz_uid;
quit;

data interventions;
	set interventions;
	if enddate='31Dec9999'd then
		enddate=Extractiondate;
	if enddate=. then
		enddate=ExtractionDate;
	if enddate>=startdate;
	* cleaning for errorness records;
	if startdate>"&sensor"d then
		delete;
	if enddate>"&sensor"d then
		enddate="&sensor"d;
run;

* Spliting dataset by each intervention type;
%macro interv(interv);
	data &interv;
		set interventions;
		if interv_grp="&interv";
		keep snz_uid DOB interv_grp startDate enddate;
	run;
%mend;

%interv(AlTED);
%interv(SUSP);
%interv(STAND);
%interv(TRUA);
%interv(SEDU);
%interv(ESOL);
%interv(EARLEX);
%interv(HOMESCH);
%interv(BOARD);
%interv(OTHINT);
%interv(HEALTH);
%interv(SECTER);
%interv(IRF);


* checking for overlap;
%overlap(AlTED);
%overlap(SUSP);
%overlap(STAND);
%overlap(TRUA);
%overlap(SEDU);
%overlap(ESOL);
%overlap(EARLEX);
%overlap(HOMESCH);
%overlap(BOARD);
%overlap(OTHINT);
%overlap(HEALTH);
%overlap(SECTER);
%overlap(IRF);


*Creating final long file;
%macro interv_year_age(interv);
data &interv._OR; set &interv._OR;
start1=MDY(1,1,&first_anal_yr.); format start1 date9.;

do ind=&first_anal_yr. to &last_anal_yr.;
			i=ind-(&first_anal_yr.-1);

			start_window=intnx('YEAR',start1,i-1,'S');
			end_window=intnx('YEAR',start1,i,'S')-1;

			%enr_yr(&interv.);

end;

do i=&firstage. to &lastage.;
			age=i-(&firstage.-1);
			start_window=intnx('YEAR',DOB,age-1,'S');
			end_window=intnx('YEAR',DOB,age,'S')-1;
			%enr_age(&interv.);


end;

proc summary data=&interv._OR nway;
class snz_uid DOB;
var &interv._enr_da_&first_anal_yr.-&interv._enr_da_&last_anal_yr.;
output out=&interv._temp1(drop=_:) sum=;
run;

proc summary data=&interv._OR nway;
class snz_uid DOB;
var 
&interv._enr_da_at_age_&firstage.-&interv._enr_da_at_age_&lastage.;
output out=&interv._temp2(drop=_:) sum=;
run;

%mend;


%interv_year_age(AlTED);
%interv_year_age(SUSP);
%interv_year_age(STAND);
%interv_year_age(TRUA);
%interv_year_age(SEDU);
%interv_year_age(ESOL);
%interv_year_age(EARLEX);
%interv_year_age(HOMESCH);
%interv_year_age(BOARD);
%interv_year_age(OTHINT);
%interv_year_age(HEALTH);
%interv_year_age(SECTER);
%interv_year_age(IRF);

data &projectlib..IND_INTERV_&date.; merge 
AlTED_TEMP1
SUSP_TEMP1
STAND_TEMP1
TRUA_TEMP1
SEDU_TEMP1
ESOL_TEMP1
EARLEX_TEMP1
HOMESCH_TEMP1
BOARD_TEMP1
OTHINT_TEMP1
HEALTH_TEMP1
SECTER_TEMP1
IRF_TEMP1;
by snz_uid;run;

data &projectlib.._IND_INTERV_at_age_&date.; merge 
AlTED_TEMP2
SUSP_TEMP2
STAND_TEMP2
TRUA_TEMP2
SEDU_TEMP2
ESOL_TEMP2
EARLEX_TEMP2
HOMESCH_TEMP2
BOARD_TEMP2
OTHINT_TEMP2
HEALTH_TEMP2
SECTER_TEMP2
IRF_TEMP2;
by snz_uid;run;

proc datasets lib=work;
delete AlTED:
SUSP:
STAND:
TRUA:
SEDU:
ESOL:
EARLEX:
HOMESCH:
BOARD:
OTHINT:
HEALTH:
SECTER:
IRF:
INTERVENTIONS
DELETES ;
run;

%mend;

******************************************************************************************************************************************
Create Tertiary completions indicators
******************************************************************************************************************************************;
%macro Create_ter_compl_pop;
proc sql;
	create table TER_compl as
		select  snz_uid,
			moe_com_year_nbr as year,
			moe_com_qacc_code as qacc,
			put(moe_com_qacc_code,$lv8id.) as att_TER_qual_type,
			input(moe_com_qual_level_code,3.) as level,
			moe_com_qual_nzsced_code
		from moe.completion
			where snz_uid in
				(select distinct snz_uid from &population)
					and MDY(12,31,moe_com_year_nbr)<="&sensor"d
			and moe_com_year_nbr>=&first_anal_yr or moe_com_year_nbr<=&last_anal_yr
	;
quit;
data TER_compl; set TER_compl;
if att_Ter_qual_type ne 'Error';

	array att_TER_L1_3cert_(*) att_TER_L1_3cert_&first_anal_yr-att_TER_L1_3cert_&last_anal_yr;
	array att_TER_L4Cert_(*) att_TER_L4Cert_&first_anal_yr-att_TER_L4Cert_&last_anal_yr;
	array att_TER_Dipl_(*) att_TER_Dipl_&first_anal_yr-att_TER_Dipl_&last_anal_yr;
	array att_TER_Bach_(*) att_TER_Bach_&first_anal_yr-att_TER_Bach_&last_anal_yr;
	array att_TER_Postgrad_(*) att_TER_Postgrad_&first_anal_yr-att_TER_Postgrad_&last_anal_yr;
	array att_TER_MastPHD_(*) att_TER_MastPHD_&first_anal_yr-att_TER_MastPHD_&last_anal_yr;

	array lev_TER_L1_3cert_(*) lev_TER_L1_3cert_&first_anal_yr-lev_TER_L1_3cert_&last_anal_yr;
	array lev_TER_L4Cert_(*) lev_TER_L4Cert_&first_anal_yr-lev_TER_L4Cert_&last_anal_yr;
	array lev_TER_Dipl_(*) lev_TER_Dipl_&first_anal_yr-lev_TER_Dipl_&last_anal_yr;
	array lev_TER_Bach_(*) lev_TER_Bach_&first_anal_yr-lev_TER_Bach_&last_anal_yr;
	array lev_TER_Postgrad_(*) lev_TER_Postgrad_&first_anal_yr-lev_TER_Postgrad_&last_anal_yr;
	array lev_TER_MastPHD_(*) lev_TER_MastPHD_&first_anal_yr-lev_TER_MastPHD_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);
		
		if i=year and att_Ter_qual_type='Level 1-3 certificates' then att_TER_L1_3cert_(ind)=1; else att_TER_L1_3cert_(ind)=0;
		if i=year and att_Ter_qual_type='Level 4 Certificates' then att_TER_L4Cert_(ind)=1; else att_TER_L4Cert_(ind)=0;
		if i=year and att_Ter_qual_type='Certificates and Diploma Level 5-7' then att_TER_Dipl_(ind)=1; else att_TER_Dipl_(ind)=0;
		if i=year and att_Ter_qual_type='Bachelor degrees' then att_TER_Bach_(ind)=1; else att_TER_Bach_(ind)=0;
		if i=year and att_Ter_qual_type='Honours, postgrad dipl' then att_TER_Postgrad_(ind)=1; else att_TER_Postgrad_(ind)=0;
		if i=year and att_Ter_qual_type in ('Masters degrees','Doctoral degrees')  then att_TER_MastPHD_(ind)=1; else att_TER_MastPHD_(ind)=0;

		if i=year and att_Ter_qual_type='Level 1-3 certificates' then lev_TER_L1_3cert_(ind)=level; else lev_TER_L1_3cert_(ind)=0;
		if i=year and att_Ter_qual_type='Level 4 Certificates' then lev_TER_L4Cert_(ind)=level; else lev_TER_L4Cert_(ind)=0;
		if i=year and att_Ter_qual_type='Certificates and Diploma Level 5-7' then lev_TER_Dipl_(ind)=level; else lev_TER_Dipl_(ind)=0;
		if i=year and att_Ter_qual_type='Bachelor degrees' then lev_TER_Bach_(ind)=level; else lev_TER_Bach_(ind)=0;
		if i=year and att_Ter_qual_type='Honours, postgrad dipl' then lev_TER_Postgrad_(ind)=level; else lev_TER_Postgrad_(ind)=0;
		if i=year and att_Ter_qual_type in ('Masters degrees','Doctoral degrees')  then lev_TER_MastPHD_(ind)=level; else lev_TER_MastPHD_(ind)=0;

	end;
run;

proc summary data= TER_compl(drop=att_ter_qual_type) nway;
	class snz_uid;
	var att_TER_:
		lev_TER_:;
	output out=&projectlib.._IND_TER_COMPL_&date(drop=_TYPE_ _FREQ_) max=;
run;

proc datasets lib=work;
delete Ter_compl temp;
run;
%mend;

****************************************************************************************************************************************
****************************************************************************************************************************************
Schools attended 
****************************************************************************************************************************************
****************************************************************************************************************************************;
%macro Create_sch_attended_pop;
proc sql;
	create table sch_enrol
		as select distinct 
			a.snz_uid
			,a.snz_moe_uid
			,input(compress(a.moe_esi_start_date,"-"),yymmdd10.) format date9. as startdate
			,input(compress(a.moe_esi_end_date,"-"),yymmdd10.) format date9. as enddate
			,input(a.moe_esi_provider_code,10.) as schoolnumber
			,input(compress(a.moe_esi_extrtn_date,"-"),yymmdd10.) format date9.  as ExtractionDate
			,b.DOB
			,year(b.DOB)+19 as year19_birth

		from moe.student_enrol a inner join &population b
		on a.snz_uid=b.snz_uid
				order by snz_uid;
quit;


data sch_enrol; set sch_enrol;
* imputing enddates for those who are over 19;
if enddate=. and ExtractionDate>intnx('YEAR',DOB,19,'S') then enddate=MDY(12,31,year19_birth); 
else if enddate=. then	enddate=ExtractionDate;
if enddate>startdate;
	* cleaning for errorness records JL;
if startdate>0;
if startdate>"&sensor"d then
		delete;
if enddate>"&sensor"d then
		enddate="&sensor"d;
drop year19_birth;
run;

* lets make sure no duplicate records of enrolments;
proc sort data=sch_enrol nodupkey;
	by snz_uid startdate enddate schoolnumber;
run;

proc sort data=sch_enrol;
	by snz_uid startdate enddate;
run;

%OVERLAP (sch_enrol);

%aggregate_by_year(sch_enrol,sch_enrol_sum,&first_anal_yr,&last_anal_yr);


data sch_enrol_sum; set sch_enrol_sum;

proc sort data=sch_enrol_sum; by snz_uid year days;

data sch_enrol_sum; set sch_enrol_sum;
	by snz_uid year days;
	non_nqf_school=0;
	if schoolnumber in 
		(29,41,52,54,62,78,81,89,130,141,278,281,436,439,440,441,456,459,460,473,484,571,620,1132,1139,1605,1626,1655,2085,4152,
		37,60,67,333,387,617,1606,1640) then
		non_nqf_school=1;

	* Agreed with MOE;
	if last.year then output;
	keep snz_uid DOB year schoolnumber days non_nqf_school;
run;

data sch_enrol_sum;
	set sch_enrol_sum;

	array school_in_(*) school_in_&first_anal_yr-school_in_&last_anal_yr;
	array nonnqf_sch_in_(*) nonnqf_sch_in_&first_anal_yr-nonnqf_sch_in_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);
	
		school_in_(ind)=0;
		nonnqf_sch_in_(ind)=0;

		if year=i then
			school_in_(ind)=schoolnumber;

		if year=i and non_nqf_school=1 then
			nonnqf_sch_in_(ind)=1;
	end;

	drop i ind;
run;

proc summary data=sch_enrol_sum nway;
	class snz_uid DOB;
	var school_in_2006-school_in_&last_anal_yr
		nonnqf_sch_in_2006-nonnqf_sch_in_&last_anal_yr;
	output out=&projectlib.._IND_sch_attended_&date (drop=_TYPE_ _FREQ_) sum=;
run;
proc datasets lib=work;
delete sch_enrol_sum sch_enrol sch_enrol_OR;
run;

%mend;



***********************************************************************************************************************************
***********************************************************************************************************************************
Macro to create monthly arrays
***********************************************************************************************************************************
***********************************************************************************************************************************;
%macro Create_mth_Sch_enrol_pop;
proc sql;
	create table sch_enrol
		as select distinct 
			a.snz_uid
			,a.snz_moe_uid
			,input(compress(a.moe_esi_start_date,"-"),yymmdd10.) format date9. as startdate
			,input(compress(a.moe_esi_end_date,"-"),yymmdd10.) format date9. as enddate
			,input(a.moe_esi_provider_code,10.) as schoolnumber
			,input(compress(a.moe_esi_extrtn_date,"-"),yymmdd10.) format date9.  as ExtractionDate
			,b.DOB
			,year(b.DOB)+19 as year19_birth,
			case when moe_esi_end_date='  ' then 1 else 0 end as sch_enddate_imputed

		from moe.student_enrol a inner join &population b
		on a.snz_uid=b.snz_uid
				order by snz_uid;
quit;


data sch_enrol; set sch_enrol;
* imputing enddates for those who are over 19;
if enddate=. and ExtractionDate>intnx('YEAR',DOB,19,'S') then enddate=MDY(12,31,year19_birth); 
else if enddate=. then	enddate=ExtractionDate;
if enddate>startdate;
	* cleaning for errorness records JL;
if startdate>0;
if startdate>"&sensor"d then
		delete;
if enddate>"&sensor"d then
		enddate="&sensor"d;
drop year19_birth;
run;

* lets make sure no duplicate records of enrolments;
proc sort data=sch_enrol nodupkey;
	by snz_uid startdate enddate schoolnumber;
run;

proc sort data=sch_enrol;
	by snz_uid startdate enddate;
run;

%OVERLAP (sch_enrol);

**CODE FOR SCHOOL ENROLMENT MONTHS;

data sch_enrol_OR  ;
set sch_enrol_OR;
format start_window end_window date9.;
array sch_enr_id_(*) sch_enr_id_&m.-sch_enr_id_&n.; * end of jun2015;
array sch_enr_da_(*) sch_enr_da_&m.-sch_enr_da_&n.; * end of jun2015;

do ind=&m. to &n.; i=ind-&m.+1;
	sch_enr_id_(i)=0;
	sch_enr_da_(i)=0;
* overwriting start and end window as interval equal to one month;

start_window=intnx('month',&start.,i-1,'S');
end_window=(intnx('month',&start.,i,'S'))-1;

if not((startdate > end_window) or (enddate < start_window)) then do;
	sch_enr_id_(i)=1; * creating inidcator of school enrolment;
	* measuring the days enrolled;
				if (startdate <= start_window) and  (enddate > end_window) then
					days=(end_window-start_window)+1;
				else if (startdate <= start_window) and  (enddate <= end_window) then
					days=(enddate-start_window)+1;
				else if (startdate > start_window) and  (enddate <= end_window) then
					days=(enddate-startdate)+1;
				else if (startdate > start_window) and  (enddate > end_window) then
					days=(end_window-startdate)+1;
				sch_enr_da_[i]=days*sch_enr_id_(i);

end;
end;
run;


proc means data=sch_enrol_OR  ;
run;

proc summary data=sch_enrol_OR nway;
class snz_uid ;
var sch_enr_id_&m.-sch_enr_id_&n.  sch_enr_da_&m.-sch_enr_da_&n. sch_enddate_imputed;
output out=&projectlib.._mth_sch_enr_&date.(drop=_:) sum=;
run;

proc datasets lib=work;
delete sch_enrol sch_enrol_OR deletes ;
run;

%mend;


********************************************************************************************************************************;
********************************************************************************************************************************;
**Tertiary enrolment monthly vectors;
**Now using formal programmes only for this vector ;
********************************************************************************************************************************;
********************************************************************************************************************************;

%macro Create_mth_Ter_enrol_pop;

%* FORMATING, CLEANING AND SENSORING;
proc sql;
	create table ter_enrol as

	SELECT distinct 
		a.snz_uid
		,a.moe_enr_year_nbr
		,input(compress(a.moe_enr_prog_start_date,"-"),yymmdd10.) format date9. as startdate
		,input(compress(a.moe_enr_prog_end_date,"-"),yymmdd10.) format date9.  as enddate
		,year(input(compress(a.moe_enr_prog_start_date,"-"),yymmdd10.)) as start_year		
		,sum(a.moe_enr_efts_consumed_nbr) as EFTS_consumed
		,a.moe_enr_efts_prog_years_nbr as EFTS_prog_yrs
		,a.moe_enr_qacc_code as qacc
		,a.moe_enr_qual_code as Qual
		,a.moe_enr_prog_nzsced_code as NZSCED
		,(case when a.moe_enr_funding_srce_code in ('05','06','07','08','11','24') and a.moe_enr_qual_type_code="D" then 1 else 0 end) as Formal
		,a.moe_enr_subsector_code as subsector format $subsector.
		,a.moe_enr_qual_level_code as level
/*		,a.moe_enr_qual_type_code as qual_type*/
		,a.moe_enr_is_domestic_ind
		,a.moe_enr_residency_status_code
		,a.moe_enr_provider_code
		,b.DOB
	FROM moe.enrolment a inner join &population b
	on a.snz_uid=b.snz_uid
		WHERE a.moe_enr_year_nbr>=&first_anal_yr and a.moe_enr_year_nbr<=&last_anal_yr
			order by snz_uid;
quit;

data ter_enrol; set ter_enrol;
if EFTS_consumed>0;
if enddate-startdate>0;
if start_year>=&first_anal_yr and start_year<=&last_anal_yr;
if enddate>"&sensor"d then enddate="&sensor"d;
if startdate>="&sensor"d then delete;
if formal=1;
run;

%overlap(ter_enrol);

data TER_ENROL_MON_temp; 
set ter_enrol_OR ;
format start_window end_window date9.;
array ter_enr_id_(*) ter_enr_id_&m.-ter_enr_id_&n.; 
array ter_enr_da_(*) ter_enr_da_&m.-ter_enr_da_&n.; 
do ind=&m. to &n.; i=ind-&m.+1;
	ter_enr_id_(i)=0;
	ter_enr_da_(i)=0;

	start_window=intnx('month',&start.,i-1,'S');
   end_window=(intnx('month',&start.,i,'S'))-1;

	if not((startdate > end_window) or (enddate < start_window)) then do;
		ter_enr_id_(i)=1; * creating inidcator of school enrolment;
		* measuring the days enrolled;
					if (startdate <= start_window) and  (enddate > end_window) then
						days=(end_window-start_window)+1;
					else if (startdate <= start_window) and  (enddate <= end_window) then
						days=(enddate-start_window)+1;
					else if (startdate > start_window) and  (enddate <= end_window) then
						days=(enddate-startdate)+1;
					else if (startdate > start_window) and  (enddate > end_window) then
						days=(end_window-startdate)+1;
					ter_enr_da_[i]=days*ter_enr_id_(i);
	end;
end;
run;

proc summary data=TER_ENROL_MON_temp nway;
class snz_uid ;
var ter_enr_id_&m.-ter_enr_id_&n.  ter_enr_da_&m.-ter_enr_da_&n.; 
output out=mth_ter_enrol(drop=_:) sum=;
run;

data &projectlib.._mth_ter_enr_&date.;
set mth_ter_enrol;
array ter_enr_id_(*) ter_enr_id_&m.-ter_enr_id_&n.; 
do ind=&m. to &n.; i=ind-&m.+1;
   if ter_enr_id_[i]>1 then ter_enr_id_[i]=1;
   end;
drop ind i;
run;

proc datasets lib=work;
delete mth_ter_enrol TER_ENROL_MON_temp ter_enrol: ;
%mend;

********************************************************************************************************************************;
********************************************************************************************************************************;
**Industry training monthly activity;
**Pick up temp dataset from the code that created IT quals, above;
********************************************************************************************************************************;
********************************************************************************************************************************;
**Industry training qualifications;

%macro Create_mth_IT_MA_enrol_pop;
* FORMATING, SENSORING AND CLEANING;
proc sql;
create table IT as select
a.snz_uid,
input(compress(moe_itl_start_date,"-"),yymmdd10.) format date9. as startdate,
input(compress(moe_itl_end_date,"-"),yymmdd10.) format date9. as enddate,
a.moe_itl_programme_type_code,
a.moe_itl_sum_units_consumed_nbr,
a.moe_itl_fund_code,
year(input(compress(moe_itl_start_date,"-"),yymmdd10.)) as year,
b.DOB
from moe.tec_it_learner a inner join &population b
on a.snz_uid=b.snz_uid
where moe_itl_programme_type_code in ("NC","TC");

data ITL MA; Set IT;
if enddate=. then enddate="&sensor"d;
if startdate>"&sensor"d then delete;
if enddate>"&sensor"d then enddate="&sensor"d;
if startdate>enddate then delete;
if year>=2003;
if moe_itl_fund_code='IT' then output ITL;
if moe_itl_fund_code='MA' then output MA;
run;

proc sort data=ITL(keep=snz_uid DOB startdate enddate moe_itl_fund_code) nodupkey ; 
by snz_uid startdate enddate ; 
proc sort data=MA (keep=snz_uid DOB startdate enddate moe_itl_fund_code) nodupkey; 
by snz_uid startdate enddate ; run;

%overlap(ITL);
%overlap(MA);

data ITL_OR; set ITL_OR;
format start_window end_window date9.;
array it_id_(*) it_id_&m.-it_id_&n.; 
array it_da_(*)  it_da_&m.-it_da_&n.; 

do ind=&m. to &n.; i=ind-&m.+1;
	it_id_(i)=0;
	it_da_(i)=0;
	
start_window=intnx('month',&start.,i-1,'S');
end_window=(intnx('month',&start.,i,'S'))-1;

if not((startdate > end_window) or (enddate < start_window)) then do;
    it_id_(i)=1;
	* measuring the days overseas;
				if (startdate <= start_window) and  (enddate > end_window) then
					days=(end_window-start_window)+1;
				else if (startdate <= start_window) and  (enddate <= end_window) then
					days=(enddate-start_window)+1;
				else if (startdate > start_window) and  (enddate <= end_window) then
					days=(enddate-startdate)+1;
				else if (startdate > start_window) and  (enddate > end_window) then
					days=(end_window-startdate)+1;
				it_da_[i]=days*it_id_(i);				
	end;
end;
run;

data MA_OR; set MA_OR;
format start_window end_window date9.;
array ma_id_(*) ma_id_&m.-ma_id_&n.; 
array ma_da_(*)  ma_da_&m.-ma_da_&n.; 

do ind=&m. to &n.; i=ind-&m.+1;
	ma_id_(i)=0;
	ma_da_(i)=0;
	
	start_window=intnx("month",&start.,i-1,"beginning"); * start is beg of the month;
	end_window=intnx("month",&start.,i-1,"end");* end is end of the month;

if not((startdate > end_window) or (enddate < start_window)) then do;
    ma_id_(i)=1;
	* measuring the days overseas;
				if (startdate <= start_window) and  (enddate > end_window) then
					days=(end_window-start_window)+1;
				else if (startdate <= start_window) and  (enddate <= end_window) then
					days=(enddate-start_window)+1;
				else if (startdate > start_window) and  (enddate <= end_window) then
					days=(enddate-startdate)+1;
				else if (startdate > start_window) and  (enddate > end_window) then
					days=(end_window-startdate)+1;
				ma_da_[i]=days*ma_id_(i);				
	end;
end;
run;

proc summary data=MA_OR nway;
class snz_uid DOB;
var MA_:;
output out=TEMP1(drop=_:) sum=;

proc summary data=ITL_OR nway;
class snz_uid DOB;
var IT_:;
output out=TEMP2(drop=_:) sum=;

Data &projectlib.._mth_IT_MA_ENR_&date; merge TEMP1 TEMP2; by snz_uid; run;

proc datasets lib=work;
delete TEMP1 TEMP2 Deletes IT MA IT_OR MA_OR ITL:;
run;
%mend;