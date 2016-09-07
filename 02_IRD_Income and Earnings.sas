/*******************************************************************************************************************
*******************************************************************************************************************

Developer: Sarah Tumen, Sylvia Dixon (A&I, NZ Treasury) and Roger Macky (Contractor) 	
Date Created: 7-Dec-15	

This code creates indicators of earnings and income for reference person. 
Indicators include days earning wages and salaries, days on main benefit, 
parental leave, receiving student allowances, working for families tax credits,
ACC benefits and CPI adjusted ( to December 2014 dollars) amounts earned/received.


*/

%let seileftcen=01Jan1999;
%let seirigcen=31Dec2012;
%let emsleftcen=01Apr1999;
%let emsrigcen=30Jun2014;
%let pplleftcen=01Jul2002;
%let wffleftcen=01Apr2003;
%let wffrigcen=31Mar2013;

proc sql;
	Connect to sqlservr (server=WPRDSQL36\iLeed database=IDI_clean_&Version);
	create table job_summary as 

	SELECT distinct 
		snz_uid AS snz_uid,
		inc_cal_yr_year_nbr AS year,
		inc_cal_yr_income_source_code AS income_source_code,
		sum(inc_cal_yr_mth_01_amt) AS m1, 
		sum(inc_cal_yr_mth_02_amt) AS m2, 
		sum(inc_cal_yr_mth_03_amt) AS m3, 
		sum(inc_cal_yr_mth_04_amt) AS m4, 
		sum(inc_cal_yr_mth_05_amt) AS m5, 
		sum(inc_cal_yr_mth_06_amt) AS m6, 
		sum(inc_cal_yr_mth_07_amt) AS m7, 
		sum(inc_cal_yr_mth_08_amt) AS m8, 
		sum(inc_cal_yr_mth_09_amt) AS m9, 
		sum(inc_cal_yr_mth_10_amt) AS m10, 
		sum(inc_cal_yr_mth_11_amt) AS m11, 
		sum(inc_cal_yr_mth_12_amt) AS m12, 
		sum(inc_cal_yr_tot_yr_amt) AS gross_earnings_amt

	FROM data.income_cal_yr
	WHERE inc_cal_yr_year_nbr >= &first_anal_yr and inc_cal_yr_year_nbr <= &last_anal_yr 
		AND snz_uid IN (SELECT DISTINCT snz_uid FROM &population) 
		and  snz_ird_uid <> 0  
	and inc_cal_yr_income_source_code not in ('PEN','WHP')
	GROUP BY snz_uid, inc_cal_yr_year_nbr , inc_cal_yr_income_source_code
		ORDER BY snz_uid, inc_cal_yr_year_nbr , inc_cal_yr_income_source_code 
	;
quit;

proc sql;
	create table job_summary1 as select
		a.*,
		b.DOB
	from job_summary a left join &population b
		on a.snz_uid=b.snz_uid;
quit;

data job_summary2 ( drop=m1-m12 );
	set job_summary1;

	do j=1 to 12;
		cal_month =j;

		if      j=1 then
			inc = m1;
		else if j=2      then
			inc = m2;
		else if j=3 	  then
			inc = m3;
		else if j=4      then
			inc = m4;
		else if j=5      then
			inc = m5;
		else if j=6      then
			inc = m6;
		else if j=7      then
			inc = m7;
		else if j=8      then
			inc = m8;
		else if j=9      then
			inc = m9;
		else if j=10     then
			inc = m10;
		else if j=11     then
			inc = m11;
		else if j=12     then
			inc = m12;

		if cal_month<=3 then
			quarter=1;
		else if cal_month>3 and cal_month<=6 then
			quarter=2;
		else if cal_month>6 and cal_month<=9 then
			quarter=3;
		else if cal_month>=10 then
			quarter=4;

		if inc <> 0 then
			output;
	end;

	drop j;
run;

proc sql;
	create table job_summary3 as select
		a.*,
		b.cpi_index,
		a.inc*(1197/CPI_index) as inc_2014q4
	from job_summary2 a left join project.TSY_b15_04_cpi_index b
		on a.year=b.year and a.quarter=b.quarter;
quit;

proc freq data=job_summary3;
	tables income_source_code;
run;

Data job_summary4;
	set job_summary3;

	if income_source_code = 'CLM' then
		income_source_code = 'ACC';

	if income_source_code not in ('P00', 'P01', 'P02', 'C00', 'C01', 'C02', 'S00', 'S01', 'S02', 'S03');
	startdate=MDY(cal_month,1,year);
	enddate=intnx('month',startdate,0,'E');
	format startdate enddate date9.;
	days=enddate-startdate+1;

	if startdate<DOB and enddate<DOB then
		delete;

	if startdate<DOB and enddate>DOB then
		startdate=DOB;
	keep snz_uid income_source_code  year DOB cal_month inc_2014q4 startdate enddate days;
run;

proc summary data= job_summary4 nway;
	class snz_uid income_source_code year;
	var inc_2014q4 days;
	output out= job_summary5(drop= _TYPE_ _FREQ_ rename=(inc_2014q4=gross_earnings_amt days=span_da)) sum=;
run;

PROC SQL;
	Connect to sqlservr (server=WPRDSQL36\iLeed database=IDI_clean_&Version );
	CREATE TABLE wffSpells as 
		SELECT *
			FROM wff.spells 
				WHERE snz_uid IN	(SELECT DISTINCT snz_uid FROM &population)  
					ORDER BY snz_uid, wff_spe_spell_start_date
	;
QUIT;

Data wffSpells deletes;
	set wffSpells;
	startdate = input(WFF_SPE_SPELL_START_DATE,yymmdd10.);
	enddate   = input(WFF_SPE_SPELL_END_DATE,  yymmdd10.);

	if startdate>"&sensor"d then
		delete;

	if  enddate>"&sensor"d then
		enddate="&sensor"d;

	if enddate < startdate then
		output deletes;
	else output wffSpells;
run;

%OVERLAP(wffSpells);

data wffSpells_2;
	set wffSpells_OR;


	days = enddate - startdate + 1;

	if enddate = . or startdate = . then
		abort;
	income_source_code = 'FTC';
	total_children_fte=wff_spe_child_total_fte_nbr;

	if wff_spe_l_msd_ftc_paid_amt > 0 and  partner_snz_uid =. then
		do;
			gross_daily_amt = wff_spe_l_msd_ftc_paid_amt/days;
			couple_record='N';
			output;
		end;

	if partner_snz_uid ~= '.' and wff_spe_r_msd_ftc_paid_amt > 0 and wff_spe_l_msd_ftc_paid_amt =. then
		do;
			gross_daily_amt = wff_spe_r_msd_ftc_paid_amt/days;
			couple_record='Y';
			output;
		end;

	if partner_snz_uid ~= '.' and wff_spe_r_msd_ftc_paid_amt > 0 and wff_spe_l_msd_ftc_paid_amt >=0 then
		do;
			gross_daily_amt = (wff_spe_r_msd_ftc_paid_amt + wff_spe_l_msd_ftc_paid_amt) /days;
			couple_record='Y';
			output;
		end;

	keep SNZ_UID partner_snz_uid total_children_fte   startdate enddate  gross_daily_amt days income_source_code couple_record wff_spe_l_msd_ftc_paid_amt  wff_spe_r_msd_ftc_paid_amt;
run;

data wff_couples_1 (rename=(snz_uid=id gross_daily_amt2=gross_daily_amt));
	set wffSpells_2;

	gross_daily_amt2=gross_daily_amt/2.0;
	keep snz_uid  startdate enddate  gross_daily_amt2  income_source_code total_children_fte couple_record days wff_spe_l_msd_ftc_paid_amt  wff_spe_r_msd_ftc_paid_amt;

	if couple_record='Y' then
		output;
run;

data wff_couples_2 (rename=(partner_snz_uid=id gross_daily_amt2=gross_daily_amt));
	set wffSpells_2;

	gross_daily_amt2=gross_daily_amt/2.0;
	keep partner_snz_uid  startdate enddate  gross_daily_amt2 income_source_code total_children_fte couple_record days wff_spe_l_msd_ftc_paid_amt  wff_spe_r_msd_ftc_paid_amt;

	if couple_record='Y' then
		output;
run;

data wff_singles (rename=(snz_uid=id ));
	set wffSpells_2;
	keep snz_uid  startdate enddate  gross_daily_amt income_source_code couple_record total_children_fte days wff_spe_l_msd_ftc_paid_amt  wff_spe_r_msd_ftc_paid_amt;

	if couple_record='N' then
		output;
run;

data wffSpells_3 (rename=(id=snz_uid ));
	set wff_couples_1 wff_couples_2 wff_singles;
	keep id   startdate enddate  gross_daily_amt income_source_code couple_record total_children_fte;
run;

proc sql;
	create table 
		wffSpells_3a as select 
		a.*,
		b.DOB
	from wffSpells_3 a left join &population b
		on a.snz_uid=b.snz_uid where b.DOB>0;
quit;

data wffSpells_3a;
	set wffSpells_3a;
	format middate date9.;

	if startdate=enddate then
		middate=startdate;

	if startdate ne enddate then
		middate=startdate+(enddate-startdate)/2;
	mid_yy=year(middate);
	mid_qq=qtr(middate);

	if startdate<DOB and enddate<DOB then
		delete;

	if startdate<DOB and enddate>DOB then
		startdate=DOB;
run;

proc sql;
	create table wffSpells_3b as select
		a.*,
		b.cpi_index,
		a.gross_daily_amt*(1197/CPI_index) as daily_amt_2014q4,
		(a.enddate-a.startdate)+1 as span_da
	from wffSpells_3a a left join project.TSY_b15_04_cpi_index b
		on a.mid_yy=b.year and a.mid_qq=b.quarter;
quit;

%aggregate_by_year(wffSpells_3b,wffa,2003,&last_anal_yr ,gross_daily_amt = daily_amt_2014q4);

proc sort data= wffa;
	by snz_uid Year;
run;

Proc Summary data= wffa     nway;
	id income_source_code;
	var gross_earnings_amt days;
	class snz_uid year;
	output out=wff_final (drop=_type_ _freq_ rename=(days=sp_da))  sum=;
run;

Proc sql;
	Connect to sqlservr (server=WPRDSQL36\iLeed database=IDI_clean_&Version);
	create table sei_summary as 
		SELECT  distinct 
			snz_uid,
			year,
			startdate,
			enddate,
			max('SEI') AS income_source_code,	
	enddate-startdate+1 AS span_da,  				
	sum(inc_tax_yr_tot_yr_amt) AS gross_earnings_amt
	FROM 	(SELECT 
		snz_uid,
		inc_tax_yr_tot_yr_amt,						
		inc_tax_yr_year_nbr-1 as year,
		MDY(4,1,inc_tax_yr_year_nbr-1) AS startdate format date9.,
		MDY(3,31,inc_tax_yr_year_nbr) AS enddate format date9.
		FROM data.income_tax_yr
			WHERE 
				inc_tax_yr_year_nbr >= &first_anal_yr
				AND inc_tax_yr_income_source_code in ('P00', 'P01', 'P02', 'C00', 'C01', 'C02', 'S00', 'S01', 'S02', 'S03') 
				AND	snz_uid IN (SELECT DISTINCT snz_uid FROM &population)  
				) as sub_table_1

				GROUP BY snz_uid, startdate
					ORDER BY snz_uid, startdate
	;
quit;

proc sql;
	create table 
		sei_summary_a as select 
		a.*,
		b.DOB
	from sei_summary a left join &population b
		on a.snz_uid=b.snz_uid where b.DOB>0;
quit;

data sei_summary_a;
	set sei_summary_a;
	format middate date9.;

	if startdate>"&sensor"d then
		delete;

	if enddate>"&sensor"d then
		enddate="&sensor"d;

	if startdate=enddate then
		middate=startdate;

	if startdate ne enddate then
		middate=startdate+(enddate-startdate)/2;
	mid_yy=year(middate);
	mid_qq=qtr(middate);

	if startdate<DOB and enddate<DOB then
		delete;

	if startdate<DOB and enddate>DOB then
		startdate=DOB;
run;

proc sql;
	create table sei_summary_b as select
		a.*,
		b.cpi_index,
		a.gross_earnings_amt*(1197/CPI_index) as gross_earnings_amt_1
	from sei_summary_a a left join project.TSY_b15_04_cpi_index b
		on a.mid_yy=b.year and a.mid_qq=b.quarter;
quit;

data SEI_final(rename=gross_earnings_amt_1=gross_earnings_amt);
	set sei_summary_b;
	keep snz_uid year income_source_code gross_earnings_amt_1 span_da;
run;

data FACT_INC_;
	set JOB_SUMMARY5 SEI_final wff_final;
	rename span_da=empl_spanda;

	if income_source_code='W&S' then
		income_source_code='WnS';

	/*if gross_earnings_amt>1000; * Any income type that is less than 1000NZD per year is considered as not proper earnings*;*/
run;

proc sort data=FACT_INC_;
	by snz_uid year;
run;

proc transpose data=FACT_INC_ out=TEMP;
	id income_source_code;
	var gross_earnings_amt empl_spanda;
	by snz_uid year;
run;

proc sql;
	create table temp2 AS
	select SNZ_UID, year, _name_ , WnS, SEI, ACC,  PPL, STU, BEN, FTC FROM TEMP;
Quit;

data INC_;
	set temp2;

	if _NAME_='gross_earnings_amt';
	drop _NAME_;
run;

data EMPLTIME_;
	set temp2;

	if _NAME_='empl_spanda';
	rename ACC=ACC_span;
	rename BEN=BEN_span;
	rename WnS=WnS_span;
	rename STU=STU_span;
	rename SEI=SEI_span;
	rename PPL=PPL_span;
	rename FTC=FTC_span;
	drop _NAME_;
run;

data project.IND_income_&date;
	merge 	INC_ (in=a) 
		EMPLTIME_ (in=b);

	if a + b;
	by snz_uid year;

	if WnS=. then
		WnS=0;

	if SEI=. then
		SEI=0;

	if ACC=. then
		ACC=0;

	if PPL=. then
		PPL=0;

	if STU=. then
		STU=0;

	if BEN=. then
		BEN=0;

	if FTC=. then
		FTC=0;

	if WnS_span=. then
		WnS_span=0;

	if SEI_span=. then
		SEI_span=0;

	if ACC_span=. then
		ACC_span=0;

	if PPL_span=. then
		PPL_span=0;

	if STU_span=. then
		STU_span=0;

	if BEN_span=. then
		BEN_span=0;

	if FTC_span=. then
		FTC_span=0;

	/*	DROP _LABEL_ ;*/
run;

DATA _EARN;
	set project.IND_income_&date;
	array	WnS_(*) WnS_&first_anal_yr-WnS_&last_anal_yr;
	array	SEI_(*) SEI_&first_anal_yr-SEI_&last_anal_yr;
	array	ACC_(*) ACC_&first_anal_yr-ACC_&last_anal_yr;
	array	PPL_(*) PPL_&first_anal_yr-PPL_&last_anal_yr;
	array	STU_(*) STU_&first_anal_yr-STU_&last_anal_yr;
	array	BEN_(*) BEN_&first_anal_yr-BEN_&last_anal_yr;
	array	FTC_(*) FTC_&first_anal_yr-FTC_&last_anal_yr;
	array	WnS_span_(*) WnS_span_&first_anal_yr-WnS_span_&last_anal_yr;
	array	SEI_span_(*) SEI_span_&first_anal_yr-SEI_span_&last_anal_yr;
	array	ACC_span_(*) ACC_span_&first_anal_yr-ACC_span_&last_anal_yr;
	array	PPL_span_(*) PPL_span_&first_anal_yr-PPL_span_&last_anal_yr;
	array	STU_span_(*) STU_span_&first_anal_yr-STU_span_&last_anal_yr;
	array	BEN_span_(*) BEN_span_&first_anal_yr-BEN_span_&last_anal_yr;
	array	FTC_span_(*) FTC_span_&first_anal_yr-FTC_span_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);
		WnS_(ind)=0;
		SEI_(ind)=0;
		ACC_(ind)=0;
		PPL_(ind)=0;
		STU_(ind)=0;
		BEN_(ind)=0;
		FTC_(ind)=0;
		WnS_span_(ind)=0;
		SEI_span_(ind)=0;
		ACC_span_(ind)=0;
		PPL_span_(ind)=0;
		STU_span_(ind)=0;
		BEN_span_(ind)=0;
		FTC_span_(ind)=0;

		if i=year and WnS_(ind) ne . then
			WnS_(ind)=WnS;

		if i=year  and SEI_(ind) ne . then
			SEI_(ind)=SEI;

		if i=year  and ACC_(ind) ne . then
			ACC_(ind)=ACC;

		if i=year  and PPL_(ind) ne . then
			PPL_(ind)=PPL;

		if i=year  and STU_(ind) ne . then
			STU_(ind)=STU;

		if i=year  and BEN_(ind) ne . then
			BEN_(ind)=BEN;

		if i=year  and FTC_(ind) ne . then
			FTC_(ind)=FTC;

		if i=year  and WnS_span_(ind) ne . then
			WnS_span_(ind)=WnS_span;

		if i=year  and SEI_span_(ind) ne . then
			SEI_span_(ind)=SEI_span;

		if i=year  and ACC_span_(ind) ne . then
			ACC_span_(ind)=ACC_span;

		if i=year  and PPL_span_(ind) ne . then
			PPL_span_(ind)=PPL_span;

		if i=year  and STU_span_(ind) ne . then
			STU_span_(ind)=STU_span;

		if i=year  and BEN_span_(ind) ne . then
			BEN_span_(ind)=BEN_span;

		if i=year  and FTC_span_(ind) ne . then
			FTC_span_(ind)=FTC_span;
		drop i ind;
	end;
run;

proc summary data=_EARN nway;
	class snz_uid;
	var 
		WnS_&first_anal_yr-WnS_&last_anal_yr
		SEI_&first_anal_yr-SEI_&last_anal_yr
		ACC_&first_anal_yr-ACC_&last_anal_yr
		PPL_&first_anal_yr-PPL_&last_anal_yr
		STU_&first_anal_yr-STU_&last_anal_yr
		BEN_&first_anal_yr-BEN_&last_anal_yr
		FTC_&first_anal_yr-FTC_&last_anal_yr

		WnS_span_&first_anal_yr-WnS_span_&last_anal_yr
		SEI_span_&first_anal_yr-SEI_span_&last_anal_yr
		ACC_span_&first_anal_yr-ACC_span_&last_anal_yr
		PPL_span_&first_anal_yr-PPL_span_&last_anal_yr
		STU_span_&first_anal_yr-STU_span_&last_anal_yr
		BEN_span_&first_anal_yr-BEN_span_&last_anal_yr
		FTC_span_&first_anal_yr-FTC_span_&last_anal_yr;
	output out=TEMP (drop=_FREQ_ _TYPE_) sum=;
run;

data project._ind_INC_&date;
	merge &population(keep=snz_uid DOB) TEMP;
	by snz_uid;
	array	WnS_(*) WnS_&first_anal_yr-WnS_&last_anal_yr;
	array	SEI_(*) SEI_&first_anal_yr-SEI_&last_anal_yr;
	array	ACC_(*) ACC_&first_anal_yr-ACC_&last_anal_yr;
	array	PPL_(*) PPL_&first_anal_yr-PPL_&last_anal_yr;
	array	STU_(*) STU_&first_anal_yr-STU_&last_anal_yr;
	array	BEN_(*) BEN_&first_anal_yr-BEN_&last_anal_yr;
	array	FTC_(*) FTC_&first_anal_yr-FTC_&last_anal_yr;
	array	WnS_span_(*) WnS_span_&first_anal_yr-WnS_span_&last_anal_yr;
	array	SEI_span_(*) SEI_span_&first_anal_yr-SEI_span_&last_anal_yr;
	array	ACC_span_(*) ACC_span_&first_anal_yr-ACC_span_&last_anal_yr;
	array	PPL_span_(*) PPL_span_&first_anal_yr-PPL_span_&last_anal_yr;
	array	STU_span_(*) STU_span_&first_anal_yr-STU_span_&last_anal_yr;
	array	BEN_span_(*) BEN_span_&first_anal_yr-BEN_span_&last_anal_yr;
	array	FTC_span_(*) FTC_span_&first_anal_yr-FTC_span_&last_anal_yr;

	do ind=&first_anal_yr to &last_anal_yr;
		i=ind-(&first_anal_yr-1);

		if WnS_(i)=. then
			WnS_(i)=0;

		if SEI_(i)=. then
			SEI_(i)=0;

		if ACC_(i)=. then
			ACC_(i)=0;

		if PPL_(i)=. then
			PPL_(i)=0;

		if STU_(i)=. then
			STU_(i)=0;

		if BEN_(i)=. then
			BEN_(i)=0;

		if FTC_(i)=. then
			FTC_(i)=0;

		if WnS_span_(i)=. then
			WnS_span_(i)=0;

		if SEI_span_(i)=. then
			SEI_span_(i)=0;

		if ACC_span_(i)=. then
			ACC_span_(i)=0;

		if PPL_span_(i)=. then
			PPL_span_(i)=0;

		if STU_span_(i)=. then
			STU_span_(i)=0;

		if BEN_span_(i)=. then
			BEN_span_(i)=0;

		if FTC_span_(i)=. then
			FTC_span_(i)=0;
		drop i ind;
	end;
run;

data JOB_summary_temp;
	set job_summary4 (keep=snz_uid DOB income_source_code startdate enddate inc_2014q4 days);
	rate=inc_2014q4/days;
	drop inc_2014q4 days;
run;

data wff_temp;
	set WFFspells_3b (keep=snz_uid DOB income_source_code startdate enddate daily_amt_2014q4);
	rename daily_amt_2014q4=rate;
run;

data SEI_temp;
	set SEI_summary_B(keep=snz_uid DOB income_source_code startdate enddate span_da gross_earnings_amt_1);
	rate=gross_earnings_amt_1/span_da;
	drop  gross_earnings_amt_1 span_da;
run;

data INC_TEMP;
	set JOB_summary_temp SEI_temp WFF_temp;

	if income_source_code='W&S' then
		income_source_code='WnS';
	array	WnS_at_age_(*) WnS_at_age_&firstage-WnS_at_age_&lastage;
	array	SEI_at_age_(*) SEI_at_age_&firstage-SEI_at_age_&lastage;
	array	ACC_at_age_(*) ACC_at_age_&firstage-ACC_at_age_&lastage;
	array	PPL_at_age_(*) PPL_at_age_&firstage-PPL_at_age_&lastage;
	array	STU_at_age_(*) STU_at_age_&firstage-STU_at_age_&lastage;
	array	BEN_at_age_(*) BEN_at_age_&firstage-BEN_at_age_&lastage;
	array	FTC_at_age_(*) FTC_at_age_&firstage-FTC_at_age_&lastage;
	array	WnS_da_at_age_(*) WnS_da_at_age_&firstage-WnS_da_at_age_&lastage;
	array	SEI_da_at_age_(*) SEI_da_at_age_&firstage-SEI_da_at_age_&lastage;
	array	ACC_da_at_age_(*) ACC_da_at_age_&firstage-ACC_da_at_age_&lastage;
	array	PPL_da_at_age_(*) PPL_da_at_age_&firstage-PPL_da_at_age_&lastage;
	array	STU_da_at_age_(*) STU_da_at_age_&firstage-STU_da_at_age_&lastage;
	array	BEN_da_at_age_(*) BEN_da_at_age_&firstage-BEN_da_at_age_&lastage;
	array	FTC_da_at_age_(*) FTC_da_at_age_&firstage-FTC_da_at_age_&lastage;

	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);
		WnS_at_age_(i)=0;
		SEI_at_age_(i)=0;
		PPL_at_age_(i)=0;
		STU_at_age_(i)=0;
		BEN_at_age_(i)=0;
		FTC_at_age_(i)=0;
		WnS_da_at_age_(i)=0;
		SEI_da_at_age_(i)=0;
		PPL_da_at_age_(i)=0;
		STU_da_at_age_(i)=0;
		BEN_da_at_age_(i)=0;
		FTC_da_at_age_(i)=0;
		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S')-1;

		if not((startdate > end_window) or (enddate < start_window)) then
			do;
				if (startdate <= start_window) and  (enddate > end_window) then
					days=(end_window-start_window)+1;
				else if (startdate <= start_window) and  (enddate <= end_window) then
					days=(enddate-start_window)+1;
				else if (startdate > start_window) and  (enddate <= end_window) then
					days=(enddate-startdate)+1;
				else if (startdate > start_window) and  (enddate > end_window) then
					days=(end_window-startdate)+1;

				if income_source_code='WnS' then
					WnS_da_at_age_(i)=days;

				if income_source_code='WnS' then
					WnS_at_age_(i)=days*rate;

				if income_source_code='SEI' then
					SEI_da_at_age_(i)=days;

				if income_source_code='SEI' then
					SEI_at_age_(i)=days*rate;

				if income_source_code='ACC' then
					ACC_da_at_age_(i)=days;

				if income_source_code='ACC' then
					ACC_at_age_(i)=days*rate;

				if income_source_code='PPL' then
					PPL_da_at_age_(i)=days;

				if income_source_code='PPL' then
					PPL_at_age_(i)=days*rate;

				if income_source_code='STU' then
					STU_da_at_age_(i)=days;

				if income_source_code='STU' then
					STU_at_age_(i)=days*rate;

				if income_source_code='BEN' then
					BEN_da_at_age_(i)=days;

				if income_source_code='BEN' then
					BEN_at_age_(i)=days*rate;

				if income_source_code='FTC' then
					FTC_da_at_age_(i)=days;

				if income_source_code='FTC' then
					FTC_at_age_(i)=days*rate;
			end;
	end;

	drop i ind start_window end_window;
run;

proc sort data=INC_TEMP;
	by snz_uid;
run;

proc summary data=INC_TEMP nway;
	var 
		WnS_at_age_&firstage-WnS_at_age_&lastage
		SEI_at_age_&firstage-SEI_at_age_&lastage
		ACC_at_age_&firstage-ACC_at_age_&lastage
		PPL_at_age_&firstage-PPL_at_age_&lastage
		STU_at_age_&firstage-STU_at_age_&lastage
		BEN_at_age_&firstage-BEN_at_age_&lastage
		FTC_at_age_&firstage-FTC_at_age_&lastage

		WnS_da_at_age_&firstage-WnS_da_at_age_&lastage
		SEI_da_at_age_&firstage-SEI_da_at_age_&lastage
		ACC_da_at_age_&firstage-ACC_da_at_age_&lastage
		PPL_da_at_age_&firstage-PPL_da_at_age_&lastage
		STU_da_at_age_&firstage-STU_da_at_age_&lastage
		BEN_da_at_age_&firstage-BEN_da_at_age_&lastage
		FTC_da_at_age_&firstage-FTC_da_at_age_&lastage;
	by snz_uid;
	output out=TEMP(drop=_TYPE_ _FREQ_) sum=;
run;

data project._IND_INC_at_age_&date;
	merge &population (keep=snz_uid DOB) TEMP;
	by snz_uid;
	array	WnS_at_age_(*) WnS_at_age_&firstage-WnS_at_age_&lastage;
	array	SEI_at_age_(*) SEI_at_age_&firstage-SEI_at_age_&lastage;
	array	ACC_at_age_(*) ACC_at_age_&firstage-ACC_at_age_&lastage;
	array	PPL_at_age_(*) PPL_at_age_&firstage-PPL_at_age_&lastage;
	array	STU_at_age_(*) STU_at_age_&firstage-STU_at_age_&lastage;
	array	BEN_at_age_(*) BEN_at_age_&firstage-BEN_at_age_&lastage;
	array	FTC_at_age_(*) FTC_at_age_&firstage-FTC_at_age_&lastage;
	array	WnS_da_at_age_(*) WnS_da_at_age_&firstage-WnS_da_at_age_&lastage;
	array	SEI_da_at_age_(*) SEI_da_at_age_&firstage-SEI_da_at_age_&lastage;
	array	ACC_da_at_age_(*) ACC_da_at_age_&firstage-ACC_da_at_age_&lastage;
	array	PPL_da_at_age_(*) PPL_da_at_age_&firstage-PPL_da_at_age_&lastage;
	array	STU_da_at_age_(*) STU_da_at_age_&firstage-STU_da_at_age_&lastage;
	array	BEN_da_at_age_(*) BEN_da_at_age_&firstage-BEN_da_at_age_&lastage;
	array	FTC_da_at_age_(*) FTC_da_at_age_&firstage-FTC_da_at_age_&lastage;

	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);

		if WnS_at_age_(i)=. then
			WnS_at_age_(i)=0;

		if SEI_at_age_(i)=. then
			SEI_at_age_(i)=0;

		if ACC_at_age_(i)=. then
			ACC_at_age_(i)=0;

		if PPL_at_age_(i)=. then
			PPL_at_age_(i)=0;

		if STU_at_age_(i)=. then
			STU_at_age_(i)=0;

		if BEN_at_age_(i)=. then
			BEN_at_age_(i)=0;

		if FTC_at_age_(i)=. then
			FTC_at_age_(i)=0;

		if WnS_da_at_age_(i)=. then
			WnS_da_at_age_(i)=0;

		if SEI_da_at_age_(i)=. then
			SEI_da_at_age_(i)=0;

		if ACC_da_at_age_(i)=. then
			ACC_da_at_age_(i)=0;

		if PPL_da_at_age_(i)=. then
			PPL_da_at_age_(i)=0;

		if STU_da_at_age_(i)=. then
			STU_da_at_age_(i)=0;

		if BEN_da_at_age_(i)=. then
			BEN_da_at_age_(i)=0;

		if FTC_da_at_age_(i)=. then
			FTC_da_at_age_(i)=0;

		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S')-1;

		if ((end_window>"&emsrigcen"d) or (start_window<"&emsleftcen"d)) then
			WnS_at_age_(i)=.;

		if ((end_window>"&emsrigcen"d) or (start_window<"&emsleftcen"d)) then
			ACC_at_age_(i)=.;

		if ((end_window>"&emsrigcen"d) or (start_window<"&emsleftcen"d)) then
			STU_at_age_(i)=.;

		if ((end_window>"&emsrigcen"d) or (start_window<"&emsleftcen"d)) then
			BEN_at_age_(i)=.;

		if ((end_window>"&emsrigcen"d) or (start_window<"&pplleftcen"d)) then
			PPL_at_age_(i)=.;

		if ((end_window>"&wffrigcen"d) or (start_window<"&wffleftcen"d)) then
			FTC_at_age_(i)=.;

		if ((end_window>"&seirigcen"d) or (start_window<"&seileftcen"d)) then
			SEI_at_age_(i)=.;

		if ((end_window>"&emsrigcen"d) or (start_window<"&emsleftcen"d)) then
			WnS_da_at_age_(i)=.;

		if ((end_window>"&emsrigcen"d) or (start_window<"&emsleftcen"d)) then
			ACC_da_at_age_(i)=.;

		if ((end_window>"&emsrigcen"d) or (start_window<"&emsleftcen"d)) then
			STU_da_at_age_(i)=.;

		if ((end_window>"&emsrigcen"d) or (start_window<"&emsleftcen"d)) then
			BEN_da_at_age_(i)=.;

		if ((end_window>"&emsrigcen"d) or (start_window<"&pplleftcen"d)) then
			PPL_da_at_age_(i)=.;

		if ((end_window>"&wffrigcen"d) or (start_window<"&wffleftcen"d)) then
			FTC_da_at_age_(i)=.;

		if ((end_window>"&seirigcen"d) or (start_window<"&seileftcen"d)) then
			SEI_da_at_age_(i)=.;
	end;

	drop i ind start_window end_window;
run;

proc datasets lib=work kill nolist memtype=data;
quit;