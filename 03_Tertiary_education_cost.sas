/************************************************************************************************************************************************************
************************************************************************************************************************************************************

Developer: Sarah Tumen
Date Created: 7 Dec 2015


This code estimates the cost of tertiary education for those who have enrolled into SAC funded programmes
at tertiary institutions and those who enrolled into Industry training and Modern apprenticeship programmes. 
Using average per EFTS funding for SAC funded programmes and funding rates for Industry training and 
Modern Apprenticeships and EFTS consumed by individuals we calculate the direct cost of tertiary education.
This cost excludes all indirect cost associated with providing tertiary education to individuals. 
All dollars are expressed in 2014 dollars. 

*/

proc format;
	value $subsector
		"1","3"="Universities"
		"2"="Polytechnics"
		"4"="Wananga"
		"5","6"="Private Training Establishments";
run;

proc sql;
	create table enrol as

	SELECT distinct 
		snz_uid
		,moe_enr_year_nbr
		,moe_enr_prog_start_date
		,moe_enr_prog_end_date
		,sum(moe_enr_efts_consumed_nbr) as EFTS_consumed
		,moe_enr_efts_prog_years_nbr as EFTS_prog_yrs
		,moe_enr_qacc_code as qacc
		,moe_enr_qual_code as Qual
		,moe_enr_prog_nzsced_code as NZSCED
		,moe_enr_funding_srce_code as fund_source
		,moe_enr_subsector_code as subsector format $subsector.
		,moe_enr_qual_level_code as level
	FROM moe.enrolment 
		WHERE snz_uid IN 

		(SELECT DISTINCT snz_uid FROM &population) and moe_enr_year_nbr>=&first_anal_yr and moe_enr_year_nbr<=&last_anal_yr and 
			moe_enr_funding_srce_code in ('01','03','04','06','07','08')
		group by snz_uid,moe_enr_prog_start_date,moe_enr_prog_end_date, qual, NZSCED
			order by snz_uid;
quit;

data enrol;
	set enrol;
	format startdate enddate date9.;
	startdate = input(moe_enr_prog_start_date,yymmdd10.);
	enddate = input(moe_enr_prog_end_date,yymmdd10.);

	if EFTS_consumed>0;
	dur=enddate-startdate;

	if dur>0;
	start_year=year(startdate);

	if start_year>=&first_anal_yr and start_year<=&last_anal_yr;

	if startdate>="&sensor"d then
		delete;

	if enddate>"&sensor"d then
		enddate="&sensor"d;

	if fund_source='01';

	subsector1=put(subsector,$subsector.);
run;

proc summary data=enrol nway;
	class snz_uid start_year subsector1;
	var EFTS_consumed;
	output out=enrol_sum_(drop=_type_ _freq_ rename=start_year=year) sum=;
run;

proc sql;
	create table ter_enrol as select 
		a.snz_uid,
		a.subsector1,
		a.year,
		a.efts_consumed,
		b.ter_fund_rates,
		a.efts_consumed*b.ter_fund_rates as ter_cost
	from enrol_sum_ a left join project.TERTIARY_FUNDINGRATES b 
		on a.year=b.year and a.subsector1=b.subsector
	order by a.snz_uid,a.year;

proc univariate data=ter_enrol;
	var efts_consumed;
run;

data ter_enrol_1;
	merge ter_enrol(in=a) &population(keep=snz_uid DOB);
	by snz_uid;

	if a;

	if efts_consumed>2 then
		efts_consumed=2;
	ter_edu_cost=efts_consumed*ter_fund_rates;

	if year<=year(DOB) then
		delete;

run;

proc summary data=ter_enrol_1 nway;
	class snz_uid year;
	var ter_edu_cost;
	output out=LONG_COST_TEREDU_&date (drop=_type_ _freq_)  sum=;
run;


data _COST_TEREDU;
	set LONG_COST_TEREDU_&date;
	array ter_edu_cost_(*) ter_edu_cost_&first_anal_yr-ter_edu_cost_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);
		ter_edu_cost_(ind)=0;

		if year=i then
			ter_edu_cost_(ind)=ter_edu_cost;
		drop ind;
	end;
run;

proc summary data=_COST_TEREDU nway;
	class snz_uid;
	var ter_edu_cost_&first_anal_yr-ter_edu_cost_&last_anal_yr;
	output out=TEMP (drop=_TYPE_ _FREQ_) sum=;
run;

data WIDE_COST_TEREDU_&date;
	merge &population (keep=snz_uid DOB) TEMP;
	by snz_uid;
	array ter_edu_cost_(*) ter_edu_cost_&first_anal_yr-ter_edu_cost_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);

		if ter_edu_cost_(ind)=. then
			ter_edu_cost_(ind)=0;
		drop i ind;
	end;
run;

proc sql;
	create table STU_loan as
		select 
			snz_uid,
			ir_fin_return_year_nbr-1 as year,
			input('4',best1.) as quarter,
			ir_fin_loan_bal_effective_date_a as SL_BalE,
			ir_fin_loan_bal_process_date_amt as SL_BalP,
			ir_fin_period_payment_amt+ir_fin_period_refund_amt as SL_REPAY,
			ir_fin_int_wrtoff_amt as SL_int_wrtoff,
			ir_fin_period_capital_wrtoff_amt as SL_cap_wrtoff,
			ir_fin_int_wrtoff_amt+ir_fin_period_capital_wrtoff_amt as SL_total_wrtoff,
			input('1',best1.) as SL_ind
		from sla.ird_loan_financial
			where snz_uid IN (SELECT DISTINCT snz_uid FROM &population) 
				ORDER BY snz_uid;
quit;

data STU_loan_1;
	set STU_loan ( keep=snz_uid year quarter  SL_total_wrtoff);
	SL_cost_=-SL_total_wrtoff;

	if SL_cost_ ne 0;
	duration=365.25;
run;

proc sql;
	create table STU_loan_3 as select 
		a.*,
		b.CPI_index,
		SL_cost_*(1197/CPI_index) as SL_cost
	from STU_loan_1 a left join project.TSY_B15_04_CPI_INDEX b
		on a.year=b.year and a.quarter=b.quarter;
quit;

proc summary data=STU_loan_3 nway;
	class snz_uid year;
	var SL_cost;
	output out=LONG_COST_STULOAN_&date (drop=_FREQ_ _TYPE_) sum=;
run;

data _COST_STULOAN;
	set LONG_COST_STULOAN_&date;
	array SL_cost_(*) SL_cost_&first_anal_yr-SL_cost_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);
		SL_cost_(ind)=0;

		if year=i then
			SL_cost_(ind)=SL_cost;
		drop ind;
	end;
run;

proc summary data=_COST_STULOAN nway;
	class snz_uid;
	var SL_cost_&first_anal_yr-SL_cost_&last_anal_yr;
	output out=TEMP(drop=_FREQ_ _TYPE_) sum=;
run;

data WIDE_COST_STULOAN_&date;
	merge &population (keep=snz_uid DOB) TEMP;
	by snz_uid;
	array SL_cost_(*) SL_cost_&first_anal_yr-SL_cost_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);

		if SL_cost_(ind)=. then
			SL_cost_(ind)=0;
	end;

	drop i ind;
run;

data it deletes;
	set moe.tec_it_learner;

	if moe_itl_tot_credits_awarded_nbr>0 and moe_itl_sum_units_consumed_nbr>0;
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
	create table IT_1 as 
		SELECT distinct
			snz_uid
			,moe_itl_fund_code
			,moe_itl_year_nbr as year
			,sum(moe_itl_sum_units_consumed_nbr) as moe_itl_sum_units_consumed_nbr 
		FROM IT 
			WHERE snz_uid IN (select distinct snz_uid from &population)
				GROUP BY snz_uid, year, moe_itl_fund_code
					ORDER by snz_uid, year, moe_itl_fund_code;
quit;

proc summary data=IT_1 nway;
	class snz_uid moe_itl_fund_code year;
	var moe_itl_sum_units_consumed_nbr;
	output out=IT_2(drop=_freq_ _TYPE_) sum=;
run;

proc sql;
	create table IT_3 as select
		a.*,
		b.*
	from IT_2 a left join project.TSY_B15_IT_MA_FUNDING_RATES b
		on a.moe_itl_fund_code=b.moe_itl_fund_code;
quit;

proc sql;
	create table IT_3_ as select 
		a.*,
		b.DOB
	from IT_3 a left join &population b
		on a.snz_uid=b.snz_uid;
quit;

data IT_4;
	set IT_3_;
	array Y(*) Y&first_anal_yr-Y&last_anal_yr;
	IT_cost=0;
	MA_cost=0;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);
		IT_cost=0;
		MA_cost=0;

		if year=i and moe_itl_fund_code='IT' then
			IT_cost=moe_itl_sum_units_consumed_nbr*Y(ind);

		if year=i and moe_itl_fund_code='MA' then
			MA_cost=moe_itl_sum_units_consumed_nbr*Y(ind);
	end;

	if year<year(DOB) then
		delete;
	keep snz_uid DOB year moe_itl_fund_code IT_cost MA_cost;
run;

proc summary data=IT_4 nway;
	class snz_uid  year;
	var IT_cost MA_cost;
	output out=LONG_COST_IND_TRAIN_&date(drop=_FREQ_ _TYPE_ ) sum=;
run;

data  _COST_IND_TRAIN;
	set LONG_COST_IND_TRAIN_&date;
	array ter_IT_cost_(*) ter_IT_cost_&first_anal_yr-ter_IT_cost_&last_anal_yr;
	array ter_MA_cost_(*) ter_MA_cost_&first_anal_yr-ter_MA_cost_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);
		ter_IT_cost_(ind)=0;
		ter_MA_cost_(ind)=0;

		if year=i and IT_cost>0 then
			ter_IT_cost_(ind)=IT_cost;

		if year=i and MA_cost>0 then
			ter_MA_cost_(ind)=MA_cost;
	end;
run;

proc summary data=_COST_IND_TRAIN nway;
	class snz_uid;
	var ter_IT_cost_&first_anal_yr-ter_IT_cost_&last_anal_yr ter_MA_cost_&first_anal_yr-ter_MA_cost_&last_anal_yr;
	output out=TEMP (drop=_TYPE_ _FREQ_) sum=;
run;

data WIDE_COST_IND_TRAIN_&date;
	merge &population (keep=snz_uid DOB) TEMP;
	by snz_uid;
	array ter_IT_cost_(*) ter_IT_cost_&first_anal_yr-ter_IT_cost_&last_anal_yr;
	array ter_MA_cost_(*) ter_MA_cost_&first_anal_yr-ter_MA_cost_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);

		if ter_IT_cost_(ind)=. then
			ter_IT_cost_(ind)=0;

		if ter_MA_cost_(ind)=. then
			ter_MA_cost_(ind)=0;
		drop ind i;
	end;
run;

data project.COST_TER_EDU_&date;
	merge LONG_COST_TEREDU_&date LONG_COST_STULOAN_&date LONG_COST_IND_TRAIN_&date;
	by snz_uid year;

	if ter_edu_cost=. then
		ter_edu_cost=0;

	if SL_cost=. then
		SL_cost=0;

	if IT_cost=. then
		IT_cost=0;

	if MA_cost=. then
		MA_cost=0;
run;

data project._COST_TER_EDU_&date;
	merge WIDE_COST_TEREDU_&date WIDE_COST_STULOAN_&date WIDE_COST_IND_TRAIN_&date;
	by snz_uid;
run;

proc datasets lib=work kill nolist memtype=data;
quit;