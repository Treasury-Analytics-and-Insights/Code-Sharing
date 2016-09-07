/*******************************************************************************************************************************************************************
*******************************************************************************************************************************************************************


Developer: Sarah Tumen (A&I, NZ Treasury)
Date Created: 7 Dec 2015

In this code we will integrate the qualifications across various education subsectors to calculate the highest
qualification attained by person in a given year or at age

*/

%creating_clean_qual_table;

data qual_lookup;
	set sandmoe.moe_qualification_lookup;
	rename qualificationtableid=qual;
run;

proc sort data=qual_lookup;
	by qual;
run;

proc sort data=sec_qual;
	by qual;
run;

DATA sec_qual_event;
	merge sec_qual(in=a drop=DOB) qual_lookup(in=b);
	by qual;

	if a;
HA=0;
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

level=0;
if HA in (19,18,17,16,15) then level=1;
if HA in (29,28,27,26,25) then level=2;
if HA in (39,38,37,36,35) then level=3;
if HA in (42,41,40) then level=4;

	qual_type='SCH';
	format startdate enddate date9.;
	startdate=MDY(12,31,year);
	enddate=startdate;

	if year=load_year or load_year-year<=2 or load_year=.;
	keep snz_uid year startdate enddate qual level qual_type;
run;

proc sort data=sec_qual_event nodupkey;
	by snz_uid year startdate enddate qual level qual_type;
run;

proc format;
	value $lv8id
		"40","41","46", "60", "96", "98"      ="1"
		"36"-"37","43"                        ="2"
		"30"-"35"                       ="3"
		"20","25","21"                  ="4"
		"12"-"14"                       ="6"
		"11"                            ="7"
		"01","10"                       ="8"
		"90", "97", "99"                ="9"
		Other                           ="E";
run;

proc sql;
	create table TER_compl as
		select  snz_uid,
			moe_com_year_nbr,
			put(moe_com_qacc_code,$lv8id.) as att_TER_qual_type,
			moe_com_qual_level_code as raw_level,
			moe_com_qual_nzsced_code
		from moe.completion
			where snz_uid in
				(select distinct snz_uid from &population)
					and MDY(12,31,moe_com_year_nbr)<="&sensor"d;
quit;

proc freq data=ter_compl;
	tables att_TER_qual_type*raw_level/list missing;
run;

data Ter_qual_event;
	set Ter_compl;
	ter_qual=att_TER_qual_type*1;
	Ter_level=raw_level*1;

	IF att_ter_qual_type=1 and (raw_level=. or raw_level=1) then
		level=1; 

	IF att_ter_qual_type=1 and raw_level=2 then
		level=2;

	IF att_ter_qual_type=1 and raw_level>=3 then
		level=3; 

	IF att_ter_qual_type=2 and (raw_level=. or raw_level<=4)  then
		level=4;

	IF att_ter_qual_type=2 and raw_level>4 then
		level=4;

	IF att_ter_qual_type=3 and (raw_level=. or raw_level<=5)  then
		level=5;

	IF att_ter_qual_type=3 and raw_level>=6 then
		level=6;

	IF att_ter_qual_type=4 and (raw_level=. or raw_level<=7) then
		level=7;

	IF att_ter_qual_type=6  then
		level=8;

	IF att_ter_qual_type=7 then
		level=9;

	IF att_ter_qual_type=8  then
		level=10;


	qual_type='TER';
	format startdate enddate date9.;
	startdate=MDY(12,31,moe_com_year_nbr);
	enddate=startdate;
	if moe_com_year_nbr>=&first_anal_yr or moe_com_year_nbr<=&last_anal_yr;
year=moe_com_year_nbr;
keep snz_uid year startdate enddate level qual_type;

run;

data it deletes;
	set moe.tec_it_learner;

	if moe_itl_programme_type_code in ("NC","TC");

	format startdate enddate date9.;
	startdate=input(compress(moe_itl_start_date,"-"),yymmdd10.);

	if moe_itl_end_date ne '' then
		enddate=input(compress(moe_itl_end_date,"-"),yymmdd10.);

	if moe_itl_end_date='' then
		enddate="&sensor"d;

	if startdate>"&sensor"d then
		output deletes;

	if enddate>"&sensor"d then
		enddate="&sensor"d;

	if startdate>enddate then
		output deletes;
	else output it;
run;

proc sql;
	create table it_qual as 
		SELECT distinct
			snz_uid
			,moe_itl_year_nbr as year 
			,startdate 
			,enddate
			,moe_itl_level1_qual_awarded_nbr as L1
			,moe_itl_level2_qual_awarded_nbr as L2
			,moe_itl_level3_qual_awarded_nbr as L3
			,moe_itl_level4_qual_awarded_nbr as L4
			,moe_itl_level5_qual_awarded_nbr as L5
			,moe_itl_level6_qual_awarded_nbr as L6
			,moe_itl_level7_qual_awarded_nbr as L7
			,moe_itl_level8_qual_awarded_nbr as L8
		FROM IT
			WHERE snz_uid IN (select distinct snz_uid from &population)
				ORDER by snz_uid, year,startdate;
quit;

data IT_qual_event; set it_qual;
level=0;
	if L1=1 then level=1;
	if L2=1 then level=2;
	if L3=1 then level=3;
	if L4=1 then level=4;
	if L5=1 then level=4;
	if L6=1 then level=4;
	if L7=1 then level=4;
	if L8=1 then level=4;
if level>0;

startdate=enddate;
qual_type='ITL';
keep snz_uid startdate enddate level qual_type year;
run;

proc freq data=it_qual_event; tables level;run;

proc sort data=SEC_QUAL_EVENT; by snz_uid;
proc sort data=TER_QUAL_EVENT; by snz_uid;
proc sort data=IT_QUAL_EVENT; by snz_uid;

data Qual_event;
	set SEC_QUAL_EVENT TER_QUAL_EVENT IT_QUAL_EVENT;
	by snz_uid;
run;

proc summary data=qual_event nway;
class snz_uid year;
var level;
output out=project.IND_EDU_qual_&date(drop=_type_ _freq_) max=high_qual;
run;

data cohort_1;
	set &population (keep=snz_uid);
run;

data Qual_event_year;
	set Cohort_1 qual_event;
	by snz_uid;
	array high_qual_(*) high_qual_&first_anal_yr-high_qual_&last_anal_yr;

	do ind = &first_anal_yr to &last_anal_yr;
		i=ind-(&first_anal_yr-1);
		high_qual_(i)=0;

		if year=ind then
			high_qual_(i)=level;
	end;
	drop ind i;
run;

proc summary data=Qual_event_year nway;
class snz_uid;
var  high_qual_&first_anal_yr-high_qual_&last_anal_yr;
output out=project._IND_EDU_qual_&date(drop=_type_ _freq_) max=;
run;

proc sql;
	create table qual_event_DOB
		as select a.*
			, b.DOB  from qual_event a left join &population b
			on a.snz_uid=b.snz_uid;
quit;

data qual_event_at_age;
	set &population(keep=snz_uid DOB) qual_event_DOB;
	array high_qual_at_age_(*) high_qual_at_age_&firstage- high_qual_at_age_&lastage;

	do ind = &firstage to &lastage;
		i=ind-(&firstage-1);
		high_qual_at_age_(i)=0;
		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S');

		if ((startdate <end_window) and (startdate>=start_window)) then
			do;
				high_qual_at_age_(i)=level;
			end;
	end;
run;

proc summary data=qual_event_at_age nway;
class snz_uid DOB;
var high_qual_at_age_&firstage- high_qual_at_age_&lastage;
output out= TEMP (drop=_type_ _freq_) max=;
run; 

data project._IND_EDU_qual_at_age_&date;
	set TEMP;
	array high_qual_at_age_(*) high_qual_at_age_&firstage- high_qual_at_age_&lastage;

	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);

		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S');

		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			high_qual_at_age_(i)=.;
	end;
	
	do ind=1 to 12;
		if high_qual_at_age_(ind)>=1 then
			high_qual_at_age_(ind)=0;
	end;


	drop i ind start_window end_window;
run;