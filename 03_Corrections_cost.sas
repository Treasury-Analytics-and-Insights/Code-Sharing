/****************************************************************************************************************************

Developer: Sarah Tumen and Robert Templeton (A&I, NZ Treasury)
Date Created: 7 Dec 205

Daily cost of corrections for each management period ( custody, remand in prison, home detention, community sentence and etc.) 
have been provided by Department of Corrections. 
This code applies the daily cost to the days person was in each management period and calculates the annual 
direct and indirect cost for reference person. All costs are expressed in  2014 dollars

*/

%creating_clean_corr;

proc sql;
	create table COR_2 as select 
		a.* 
		,b.direct_cost_day
		,b.total_cost_day
	from CORR_clean a left join project.TSY_B15_04_CORR_COST_LOOKUP b
		on a.cor_mmp_mmc_code=b.mmc_code;

	%aggregate_by_year(COR_2,COR_3,&first_anal_yr,&last_anal_yr,gross_daily_amt =direct_cost_day );
	%aggregate_by_year(COR_2,COR_4,&first_anal_yr,&last_anal_yr,gross_daily_amt =total_cost_day );
quit;

proc sort data=cor_3;
	by snz_uid year;

proc sort data=cor_4;
	by snz_uid year;

data COR_5;
	merge cor_3(rename=gross_earnings_amt=COR_direct_cost)
		cor_4(keep=snz_uid year gross_earnings_amt  rename=(gross_earnings_amt=COR_total_cost) );
	by snz_uid year;
run;

proc sql;
	create table project.COST_COR_&date as
		select 
			SNZ_uid,
			year,
			sum(COR_total_cost) as COR_tot_cost,
			sum(COR_direct_cost) as COR_dir_cost

		from COR_5
			group by snz_uid, year
				order by snz_uid, year;
quit;

data cohort_1;
	set &population (keep=snz_uid);
	year=0;
	Cor_tot_cost=0;
	Cor_dir_cost=0;
run;

data COR_temp;
	merge project.COST_COR_&date cohort_1;
	by snz_uid year;
	array cor_tot_cost_(*) cor_tot_cost_&first_anal_yr-cor_tot_cost_&last_anal_yr;
	array cor_dir_cost_(*) cor_dir_cost_&first_anal_yr-cor_dir_cost_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);
		cor_tot_cost_(ind)=0;
		cor_dir_cost_(ind)=0;

		if i=year then
			cor_tot_cost_(ind)=cor_tot_cost;

		if i=year then
			cor_dir_cost_(ind)=cor_dir_cost;
	end;
run;

proc summary data=COR_temp nway;
	class snz_uid;
	var cor_tot_cost_&first_anal_yr-cor_tot_cost_&last_anal_yr cor_dir_cost_&first_anal_yr-cor_dir_cost_&last_anal_yr;
	output out=project._COST_COR_&date(drop=_TYPE_ _FREQ_) sum=;
run;

data COR_TEMP;
	set COR_2 (keep=snz_uid DOB startdate enddate direct_cost_day total_cost_day);
	array cor_tot_cost_at_age_(*) cor_tot_cost_at_age_&firstage-cor_tot_cost_at_age_&lastage;
	array cor_dir_cost_at_age_(*) cor_dir_cost_at_age_&firstage-cor_dir_cost_at_age_&lastage;

	do i=&firstage to &lastage;
		ind=i-(&firstage-1);
		cor_tot_cost_at_age_(ind)=0;
		cor_dir_cost_at_age_(ind)=0;
		start_window=intnx('YEAR',DOB,ind-1,'S');
		end_window=intnx('YEAR',DOB,ind,'S')-1;

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
				cor_tot_cost_at_age_(ind)=total_cost_day*days;
				cor_dir_cost_at_age_(ind)=direct_cost_day*days;
			end;
	end;
run;

data cohort_1;
	set &population (keep=snz_uid DOB);
	array cor_tot_cost_at_age_(*) cor_tot_cost_at_age_&firstage-cor_tot_cost_at_age_&lastage;
	array cor_dir_cost_at_age_(*) cor_dir_cost_at_age_&firstage-cor_dir_cost_at_age_&lastage;

	do i=&firstage to &lastage;
		ind=i-(&firstage-1);
		cor_tot_cost_at_age_(ind)=0;
		cor_dir_cost_at_age_(ind)=0;
	end;

	drop i ind;
run;

data COR_TEMP1;
	set cohort_1 cor_temp;
	keep snz_uid DOB 
		cor_tot_cost_at_age_&firstage-cor_tot_cost_at_age_&lastage
		cor_dir_cost_at_age_&firstage-cor_dir_cost_at_age_&lastage;
run;

proc summary data=COR_TEMP1 nway;
	class snz_uid DOB;
	var 
		cor_tot_cost_at_age_&firstage-cor_tot_cost_at_age_&lastage
		cor_dir_cost_at_age_&firstage-cor_dir_cost_at_age_&lastage;
	output out=COR_TEMP2(drop=_type_ _freq_) sum=;
run;

data project._COST_COR_at_age_&date;
	set COR_TEMP2;
	array cor_tot_cost_at_age_(*) cor_tot_cost_at_age_&firstage-cor_tot_cost_at_age_&lastage;
	array cor_dir_cost_at_age_(*) cor_dir_cost_at_age_&firstage-cor_dir_cost_at_age_&lastage;

	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);

		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S')-1;

		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			cor_tot_cost_at_age_(i)=.;

		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			cor_dir_cost_at_age_(i)=.;
	end;

	drop i ind start_window end_window;
run;

proc datasets lib=work kill nolist memtype=data;
quit;