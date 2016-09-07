/******************************************************************************************************************************************

Part 1: Developer: Christopher Ball
Part 2 and 3 Developer: Sarah Tumen
Date Created: 04/08/2015

Modified: Rissa Ota 18/08/15
	1) Education level aggregated at the start of the code so that the highest education level
       is easily captured

Purpose:
To create a maternal caregiver education variable.

*/

proc datasets lib=work kill nolist memtype=data;
quit;

proc format;
	value $BDD_edu
		'A'=0
		'B', 'I' = 0.5
		'C', 'J' = 1
		'D', 'F', 'K' = 2
		'E', 'L','M','G'= 3
		'H' = 4
		other = .
	;
run;

%get_mother;

data birth_cohort;
	set &population.
		(keep=snz_uid DOB snz_birth_year_nbr snz_dia_uid
		 where=(snz_birth_year_nbr>=&cohort_start and snz_dia_uid));
run;

proc sql;
	create table birth_cohort as
	select a.*, b.*
	from birth_cohort a LEFT JOIN ChildParentMap b
	on a.snz_uid = b.snz_uid;
quit;

proc sql;
	connect to odbc(dsn="idi_clean_&version._srvprd");
	create table work.MSDEducation as
	select *
	from connection to odbc
	( select * from msd_clean.msd_education_history order by snz_uid);
	disconnect from odbc;
quit;

data MSDEducation;
	set MSDEducation;
	education_level = put(msd_edh_education_code, $BDD_edu.);
run;

data MSDEducation;
	format StartDate EndDate ddmmyy10.;
	set MSDEducation;

	StartDate = input(msd_edh_educ_lvl_start_date_text, ddmmyy10.);
	EndDate = input(msd_edh_educ_lvl_end_date_text, ddmmyy10.);
run;

proc sql;
	create table MaternalEducation as
	select a.*, 
		b.StartDate, b.EndDate, b.msd_edh_education_code, 
		b.msd_edh_education_desc_text, b.education_level
	from ChildMother a left join MSDEducation b
	on a.Mother = b.snz_uid
	order by snz_uid;
quit;

data MaternalEducationLongWide;
	set MaternalEducation;
	array Maternal_Edu_at_age_(*) Maternal_Edu_at_age_&firstage-Maternal_Edu_at_age_&lastage;

	DO ind = &firstage to &lastage; 
		i=ind-&firstage+1;
		if StartDate <=intnx('YEAR',DOB,i,'S')  and (intnx('YEAR',DOB,i,'S')<= EndDate) 
			then Maternal_Edu_at_age_(i) = education_level*1;
		else Maternal_Edu_at_age_(i) = .;
	END;

	if StartDate lt DOB then maternal_edu_prior_birth = education_level*1;

	drop i ind;
run;

proc summary data=MaternalEducationLongWide nway;
	class snz_uid;
	var Maternal_Edu_at_age_0 maternal_edu_prior_birth;
	output out=check (drop=_type_ _freq_) max=;
run;

proc summary data=MaternalEducationLongWide nway;
	class snz_uid;
	var maternal_edu_prior_birth 
		Maternal_Edu_at_age_&firstage-Maternal_Edu_at_age_&lastage;
	output out=MaternalEducationWide(drop=_type_ _freq_) max=;
run;

data Mat_Edu_BDD_&date;
	set MaternalEducationWide; 
run;

data M_Ed; 
	set MaternalEducation; 

	if startdate<=enddate and startdate>0;

	yy = year(startdate);
	level = education_level*1;
run;

proc summary data=M_Ed
					(where=(yy lt &first_anal_yr.)) 
			 nway;
	class snz_uid;
	var level;
	output out=pre_first_year1(drop=_freq_ _type_) 
		   max()=;
run;
	
%aggregate_by_year(M_ED, M_ED1, &first_anal_yr, &last_anal_yr);

data M_ED1; 
	format start_window end_window date9.;
	set M_ED1 
		pre_first_year1 (in=a);

	ED_level=education_level*1;

	if a then do;
		Ed_level = level;
		year = &first_anal_yr. - 1;
	end;
run;

proc summary data=M_ed1 nway;
	class snz_uid year;
	var ed_level;
	output out=Mat_Edu_BDD_yr_&date(drop=_TYPE_ _FREQ_) max=;
run;

data mother; set childmother; keep mother DOB; DOB=.; 
rename mother=snz_uid;
run;

%let population=mother;

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
drop qual;
run;

proc sql;
create table Mother_qual_event
as select
	a.snz_uid as mother,
	a.startdate,
	a.enddate,
	a.qual_type,
	a.level,
	b.snz_uid,
	b.DOB
from  Qual_event a inner join childmother b
on a.snz_uid=b.mother;

data qual_event_at_age;
	set Mother_qual_event;
	array high_qual_at_age_(*) high_qual_at_age_&firstage- high_qual_at_age_&lastage;

	do ind = &firstage to &lastage;
		i=ind-(&firstage-1);

		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S');

		if ((startdate <end_window) and (startdate>=start_window)) then
			do;
				high_qual_at_age_(i)=level;
			end;

		if startdate<DOB then high_qual_prior_birth=level;
	end;
run;

proc summary data=qual_event_at_age nway;
	class snz_uid DOB;
	var high_qual_prior_birth high_qual_at_age_&firstage- high_qual_at_age_&lastage;
	output out= TEMP (drop=_type_ _freq_) max=;
run; 


data Mat_Edu_Ter_NCEA_&date.;
	set TEMP;
	array high_qual_at_age_(*) high_qual_at_age_&firstage-high_qual_at_age_&lastage;

	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);

		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S');

		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			high_qual_at_age_(i)=.;
	end;

	drop i ind start_window end_window;
run;

data Mother_qual_year; 
	set Mother_qual_event; 
	yy = year(startdate);
run;

proc summary data=Mother_qual_year
					(where=(yy lt &first_anal_yr.)) 
			 nway;
	class snz_uid;
	var level;
	output out=pre_first_year1(drop=_freq_ _type_) 
		   max()=;
run;
	
%aggregate_by_year(Mother_qual_year,Mother_qual_year1,&first_anal_yr,&last_anal_yr);

data Mother_qual_year1;
	set pre_first_year1 (in=a)
		Mother_qual_year1;

	if a then year=&first_anal_yr.-1;
run;

proc summary data=Mother_qual_year1 nway;
	class snz_uid year;
	var level;
	output out=Mat_Edu_Ter_year_&date.(drop=_TYPE_ _FREQ_) max=;
run;

data MaternalEducationWide;
	set Mat_Edu_BDD_&date (in=a)
	    Mat_Edu_Ter_NCEA_&date (in=b);

	array maternal_edu_at_age_(*) maternal_edu_at_age_&firstage-maternal_edu_at_age_&lastage;
	array high_qual_at_age_(*) high_qual_at_age_&firstage- high_qual_at_age_&lastage;

	do ind=&firstage to &lastage;
	   i=ind-(&firstage-1);

		if b then do;
			maternal_edu_at_age_(i)=high_qual_at_age_(i);

			maternal_edu_prior_birth=max(maternal_edu_prior_birth, high_qual_prior_birth);
		end;
	end;
run;

proc summary data=MaternalEducationWide nway;
	class snz_uid;
	var maternal_edu_prior_birth 
		Maternal_Edu_at_age_&firstage-Maternal_Edu_at_age_&lastage;
	output out=MaternalEducationWide1(drop=_type_ _freq_) max=;
run;

data project._Mat_Educ_Comb_&date.;
	retain snz_uid ;
		set MaternalEducationWide1; 
	array maternal_edu_at_age_(*) maternal_edu_at_age_&firstage-maternal_edu_at_age_&lastage;

	keep snz_uid maternal_edu_prior_birth Maternal_Edu_at_age_&firstage-Maternal_Edu_at_age_&lastage;
run;

data TEMP;
	set MAT_EDU_BDD_YR_&date.(rename=ed_level=Level) 
		MAT_EDU_TER_YEAR_&date.;
	array maternal_edu_(*)  maternal_edu_&first_anal_yr-maternal_edu_&last_anal_yr;

	do ind=&first_anal_yr to &last_anal_yr;
		i=ind-(&first_anal_yr-1);

		if year=ind then
			maternal_edu_(i)=level;
		drop i ind;
	end;

	if year = &first_anal_yr-1 then maternal_edu_prior_&first_anal_yr = level;

run;

proc summary data=temp nway;
	class snz_uid;
	var maternal_edu_prior_&first_anal_yr 
		maternal_edu_&first_anal_yr-maternal_edu_&last_anal_yr;
	output out=project._MAT_EDUC_COMB_YR_&date.(drop=_type_ _freq_) max=;
run;

%let population=project.population1988_2014;
proc sort data=&population; by snz_uid;run;
