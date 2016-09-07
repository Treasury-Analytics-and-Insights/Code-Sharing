/**********************************************************************************************************************************
**********************************************************************************************************************************
Developer: Sarah Tumen and Robert Templeton (A&I, NZ Treasury)
Date Created: 7 Dec 2015

This code summarise all benefit cost for reference person.
The cost includes main benefits, accommodation and supplementary benefits ( Tier 1 to 3 benefits). 
All benefit cost CPI adjusted and expressed in December 2014 dollars. 


*/

PROC SQL;
	create table FIRST as
		SELECT *
			FROM msd.msd_first_tier_expenditure 
				WHERE snz_uid IN	(SELECT DISTINCT snz_uid FROM &population)  
					ORDER BY snz_uid, msd_fte_start_date
	;
QUIT;

PROC SQL;
	create table FIRST_1 as SELECT 
		a.*,
		b.DOB
	FROM FIRST a left join &population b
		on a.snz_uid=b.snz_uid
	ORDER BY snz_uid, msd_fte_start_date
	;
QUIT;

data FIRST_2a del;
	set FIRST_1;
	format startdate enddate date9.;
	startdate=input(compress(msd_fte_start_date,"-"),yymmdd10.);
	enddate=input(compress(msd_fte_end_date,"-"),yymmdd10.);

	if startdate >"&sensor"D then
		delete;

	if enddate >"&sensor"D then
		enddate="&sensor"D;

	if enddate=. then
		enddate="&sensor"D;

	if enddate <'31DEC92'D then
		delete;

	if startdate <'31DEC92'D then
		startdate='31DEC92'D;

	daily_gross_rate=min(60,msd_fte_daily_gross_amt);
	daily_nett_rate=min(60,msd_fte_daily_nett_amt);

	if 0<msd_fte_period_nbr < 1000;

	if startdate=enddate then
		middate=startdate;

	if startdate ne enddate then
		middate=startdate+(enddate-startdate)/2;
	mid_yy=year(middate);
	mid_qq=qtr(middate);

	if startdate<DOB then
		output del;
	else output  FIRST_2a;
run;

proc sql;
	create table first_2b as select
		a.*,
		b.cpi_index,
		a.daily_gross_rate*(1197/CPI_index) as rate_2014q4,
		a.daily_nett_rate**(1197/CPI_index) as net_rate_2014q4

	from first_2a a left join project.TSY_b15_04_cpi_index b
		on a.mid_yy=b.year and a.mid_qq=b.quarter;
quit;

PROC SQL;
	create table SECOND as
		SELECT *
			FROM msd.msd_second_tier_expenditure 
				WHERE snz_uid IN	(SELECT DISTINCT snz_uid FROM &population)  
					ORDER BY snz_uid, msd_ste_start_date
	;
QUIT;

PROC SQL;
	create table SECOND_1 as SELECT 
		a.*,
		b.DOB
	FROM SECOND a left join &population b
		on a.snz_uid=b.snz_uid 
	ORDER BY snz_uid, msd_ste_start_date
	;
QUIT;

data  second_2a deletes deletes_ errors sensored;
	set second_1;
	format startdate enddate middate date9.;

	startdate = input(msd_ste_start_date,yymmdd10.);
	enddate = 	input(msd_ste_end_date,  yymmdd10.);

	income_source_code = 'STE';

	if startdate>"&sensor"d then
		output sensored;

	if enddate>"&sensor"d then
		enddate="&sensor"d;

	if enddate <'31DEC92'D then
		delete;

	if startdate <'31DEC92'D then
		startdate='31DEC92'D;

	discards = 0;

	if msd_ste_supp_serv_code not  in ('064' );

	if msd_ste_daily_gross_amt>=600 and msd_ste_period_nbr>=5 then
		discards =1;

	if enddate < startdate then
		discards =2;

	if startdate=enddate then
		middate=startdate;

	if startdate ne enddate then
		middate=startdate+(enddate-startdate)/2;
	mid_yy=year(middate);
	mid_qq=qtr(middate);

	if startdate<DOB then
		discards = 3;

	if discards = 0 then
		output second_2a;
	else if discards = 1 then
		output deletes;
	else if discards = 3 then
		output deletes_;
	else if discards = 2 then
		output errors;
run;

proc sql;
	create table second_2b as select
		a.*,
		b.cpi_index,
		a.msd_ste_daily_gross_amt*(1197/CPI_index) as rate_2014q4

	from second_2a a left join project.TSY_b15_04_cpi_index b
		on a.mid_yy=b.year and a.mid_qq=b.quarter;
quit;

PROC SQL;
	create table THIRD as
		SELECT  *
			FROM msd.msd_THIRD_tier_expenditure
				WHERE snz_uid IN	(SELECT DISTINCT snz_uid FROM &population)  
					ORDER BY snz_uid, msd_tte_decision_date
	;
QUIT;

PROC SQL;
	create table Third_1 as SELECT 
		a.*,
		b.DOB
	FROM Third a left join &population b
		on a.snz_uid=b.snz_uid 
	ORDER BY snz_uid,msd_tte_decision_date
	;
QUIT;

data  third_2a del;
	set third_1;
	format startdate enddate date9.;

	startdate = input(msd_tte_decision_date,yymmdd10.);
	enddate = 	input(msd_tte_decision_date,yymmdd10.);

	if startdate>"&sensor"d then
		delete;

	if enddate>"&sensor"d then
		enddate="&sensor"d;

	if enddate <'31DEC92'D then
		delete;

	if startdate <'31DEC92'D then
		startdate='31DEC92'D;
	year=year(startdate);
	quarter=qtr(startdate);
	income_source_code = 'TTE';

	if msd_tte_recoverable_ind='N';

	if startdate<DOB then
		output del;
	else output third_2a;
run;

proc sql;
	create table third_2b as select
		a.*,
		b.cpi_index,
		a.msd_tte_pmt_amt*(1197/CPI_index) as amt_2014q4
	from third_2a a left join project.TSY_b15_04_cpi_index b
		on a.year=b.year and a.quarter=b.quarter;
quit;

%aggregate_by_year(first_2b,first_2,&first_anal_yr,&last_anal_yr ,gross_daily_amt=rate_2014q4);
%aggregate_by_year(first_2b,n_first_2,&first_anal_yr,&last_anal_yr ,gross_daily_amt=net_rate_2014q4);
%aggregate_by_year(second_2b,second_2,&first_anal_yr,&last_anal_yr ,gross_daily_amt=rate_2014q4);
%aggregate_by_year(third_2b,third_2,&first_anal_yr,&last_anal_yr ,gross_daily_amt = amt_2014q4);

%macro sumup (datain);

	proc sort data= &datain._2;
		by snz_uid year;
	run;

	proc summary data= &datain._2 nway;
		var gross_earnings_amt;
		class 	snz_uid year;
		output out= &datain._3 (drop= _TYPE_ _FREQ_) sum=gross_earnings_amt;
	run;

%mend;

%sumup(first);
%sumup(second);
%sumup(n_first);
%sumup(third);

Data project.COST_BEN_&date;
	merge 
		first_3 (rename=gross_earnings_amt=FTE) 
		n_first_3 (rename=gross_earnings_amt=net_FTE)
		second_3 (rename=gross_earnings_amt=STE) 
		third_3 (rename=gross_earnings_amt=TTE);
	by snz_uid year;

	if FTE=. then
		FTE=0;

	if STE=. then
		STE=0;

	if net_FTE=. then
		net_FTE=0;

	if TTE=. then
		TTE=0;
run;

data BDD_exp;
	set project.COST_BEN_&date;
	array FTE_(*) FTE_&first_anal_yr-FTE_&last_anal_yr;
	array STE_(*) STE_&first_anal_yr-STE_&last_anal_yr;
	array net_FTE_(*) net_FTE_&first_anal_yr-net_FTE_&last_anal_yr;
	array TTE_(*) TTE_&first_anal_yr-TTE_&last_anal_yr;

	do ind=&first_anal_yr to &last_anal_yr;
		i=ind-(&first_anal_yr-1);
		FTE_(i)=0;
		STE_(i)=0;
		net_FTE_(i)=0;
		TTE_(i)=0;

		if year =ind and FTE ne 0 then
			FTE_(i)=FTE;

		if year =ind and STE ne 0 then
			STE_(i)=STE;

		if year =ind and net_FTE ne 0 then
			net_FTE_(i)=net_FTE;

		if year =ind and TTE ne 0 then
			TTE_(i)=TTE;
	end;

	drop i ind;
run;

proc summary data=BDD_exp nway;
	class snz_uid;
	var 
		FTE_&first_anal_yr-FTE_&last_anal_yr
		STE_&first_anal_yr-STE_&last_anal_yr

		net_FTE_&first_anal_yr-net_FTE_&last_anal_yr

		TTE_&first_anal_yr-TTE_&last_anal_yr;
	output out=TEMP(drop=_type_ _freq_) sum=;
run;

data project._COST_BEN_&date;
	merge &population (keep=snz_uid DOB) TEMP;
	by snz_uid;
	array FTE_(*) FTE_&first_anal_yr-FTE_&last_anal_yr;
	array STE_(*) STE_&first_anal_yr-STE_&last_anal_yr;
	array net_FTE_(*) net_FTE_&first_anal_yr-net_FTE_&last_anal_yr;
	array TTE_(*) TTE_&first_anal_yr-TTE_&last_anal_yr;

	do ind=&first_anal_yr to &last_anal_yr;
		i=ind-(&first_anal_yr-1);

		if FTE_(i)=. then
			FTE_(i)=0;

		if STE_(i)=. then
			STE_(i)=0;

		if net_FTE_(i)=. then
			net_FTE_(i)=0;

		if TTE_(i)=. then
			TTE_(i)=0;
	end;

	drop i ind;
run;

data first_4;
	set first_2b;
	keep snz_uid DOB startdate enddate rate_2014q4 net_rate_2014q4 income_source_code;
	income_source_code='FTE';

data second_4;
	set second_2b;
	keep snz_uid DOB startdate enddate rate_2014q4 income_source_code;
	income_source_code='STE';

data third_4;
	set third_2b(rename=amt_2014q4=rate_2014q4);
	keep snz_uid DOB startdate enddate rate_2014q4 income_source_code;
	income_source_code='TTE';
run;

data BDD_exp_at_age;
	set first_4 second_4 third_4;
	array FTE_at_age_(*) FTE_at_age_&firstage-FTE_at_age_&lastage;
	array STE_at_age_(*) STE_at_age_&firstage-STE_at_age_&lastage;
	array net_FTE_at_age_(*) net_FTE_at_age_&firstage-net_FTE_at_age_&lastage;
	array TTE_at_age_(*) TTE_at_age_&firstage-TTE_at_age_&lastage;

	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);
		FTE_at_age_(i)=0;
		STE_at_age_(i)=0;
		net_FTE_at_age_(i)=0;
		TTE_at_age_(i)=0;
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

				if income_source_code='FTE' then
					FTE_at_age_(i)=days*rate_2014q4;

				if income_source_code='STE' then
					STE_at_age_(i)=days*rate_2014q4;

				if income_source_code='FTE' then
					net_FTE_at_age_(i)=days*net_rate_2014q4;

				if income_source_code='TTE' then
					TTE_at_age_(i)=days*rate_2014q4;
			end;
	end;
run;

proc summary data=BDD_exp_at_age nway;
	class snz_uid;
	var 
		FTE_at_age_&firstage-FTE_at_age_&lastage
		STE_at_age_&firstage-STE_at_age_&lastage

		net_FTE_at_age_&firstage-net_FTE_at_age_&lastage

		TTE_at_age_&firstage-TTE_at_age_&lastage;
	output out=TEMP (drop=_TYPE_ _FREQ_) sum=;
run;

data project._COST_BEN_at_age_&date;
	merge &population (keep=snz_uid DOB) TEMP;
	by snz_uid;
	array FTE_at_age_(*) FTE_at_age_&firstage-FTE_at_age_&lastage;
	array STE_at_age_(*) STE_at_age_&firstage-STE_at_age_&lastage;
	array net_FTE_at_age_(*) net_FTE_at_age_&firstage-net_FTE_at_age_&lastage;
	array TTE_at_age_(*) TTE_at_age_&firstage-TTE_at_age_&lastage;

	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);

		if 	FTE_at_age_(i)=. then
			FTE_at_age_(i)=0;

		if 	STE_at_age_(i)=. then
			STE_at_age_(i)=0;

		if 	net_FTE_at_age_(i)=. then
			net_FTE_at_age_(i)=0;

		if 	TTE_at_age_(i)=. then
			TTE_at_age_(i)=0;

		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S')-1;

		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			FTE_at_age_(i)=.;

		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			STE_at_age_(i)=.;

		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			net_FTE_at_age_(i)=.;

		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			TTE_at_age_(i)=.;
	end;

	drop i ind start_window end_window;
run;

proc datasets lib=work kill nolist memtype=data;
quit;