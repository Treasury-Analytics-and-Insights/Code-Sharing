
******************************************************************************************************************;
******************************************************************************************************************;

* EARNINGS EARNINGS EARNINGS EARNINGS EARNINGS EARNINGS EARNINGS EARNINGS  EARNINGS EARNINGS EARNINGS EARNINGS 

******************************************************************************************************************;
******************************************************************************************************************;
* The main source of income is through the EMS data;
* In this  program the summary tables have been used;


%macro inc_yr(type);
array &type._[*] &type._&first_anal_yr.-&type._&last_anal_yr. ;
array &type._da_[*] &type._da_&first_anal_yr.-&type._da_&last_anal_yr.;

if not((startdate > end_window) or (enddate < start_window)) and income_source_code="&type." then do;
				
					if (startdate <= start_window) and  (enddate > end_window) then
						days=(end_window-start_window)+1;
					else if (startdate <= start_window) and  (enddate <= end_window) then
						days=(enddate-start_window)+1;
					else if (startdate > start_window) and  (enddate <= end_window) then
						days=(enddate-startdate)+1;
					else if (startdate > start_window) and  (enddate > end_window) then
						days=(end_window-startdate)+1;	

					&type._[i]=days*rate;
					&type._da_[i]=days;

end;
%mend;
%macro inc_age(type);
array &type._at_age_[*] &type._at_age_&firstage.-&type._at_age_&lastage. ;
array &type._da_at_age_[*] &type._da_at_age_&firstage.-&type._da_at_age_&lastage.;
if not((startdate > end_window) or (enddate < start_window)) and income_source_code="&type." then do;
			
					if (startdate <= start_window) and  (enddate > end_window) then
						days=(end_window-start_window)+1;
					else if (startdate <= start_window) and  (enddate <= end_window) then
						days=(enddate-start_window)+1;
					else if (startdate > start_window) and  (enddate <= end_window) then
						days=(enddate-startdate)+1;
					else if (startdate > start_window) and  (enddate > end_window) then
						days=(end_window-startdate)+1;	

					&type._at_age_[i]=days*rate;
					&type._da_at_age_[i]=days;
end;
%mend;

%macro Create_Earn_pop;
proc sql;
	create table job_summary as 

	SELECT distinct 
		a.snz_uid AS snz_uid,
		a.inc_cal_yr_year_nbr AS year,
		a.inc_cal_yr_income_source_code AS income_source_code,
		sum(a.inc_cal_yr_mth_01_amt) AS m1, 
		sum(a.inc_cal_yr_mth_02_amt) AS m2, 
		sum(a.inc_cal_yr_mth_03_amt) AS m3, 
		sum(a.inc_cal_yr_mth_04_amt) AS m4, 
		sum(a.inc_cal_yr_mth_05_amt) AS m5, 
		sum(a.inc_cal_yr_mth_06_amt) AS m6, 
		sum(a.inc_cal_yr_mth_07_amt) AS m7, 
		sum(a.inc_cal_yr_mth_08_amt) AS m8, 
		sum(a.inc_cal_yr_mth_09_amt) AS m9, 
		sum(a.inc_cal_yr_mth_10_amt) AS m10, 
		sum(a.inc_cal_yr_mth_11_amt) AS m11, 
		sum(a.inc_cal_yr_mth_12_amt) AS m12, 
		b.DOB

	FROM data.income_cal_yr a inner join &population b
	ON a.snz_uid=b.snz_uid
	WHERE a.inc_cal_yr_year_nbr >= &first_anal_yr and inc_cal_yr_year_nbr <= &last_anal_yr 
		AND a.snz_ird_uid>0	AND a.inc_cal_yr_income_source_code not in ('PEN','WHP','P00', 'P01', 'P02', 'C00', 'C01', 'C02', 'S00', 'S01', 'S02', 'S03')
	GROUP BY a.snz_uid, a.inc_cal_yr_year_nbr , a.inc_cal_yr_income_source_code
		ORDER BY a.snz_uid, a.inc_cal_yr_year_nbr , a.inc_cal_yr_income_source_code 
	;
quit;

data job_summary ( drop=m1-m12 );
	set job_summary;

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

* adjusting income to 2015 Q4 dollars;
proc sql;
	create table job_summary1 as select
		a.*,
		MDY(a.cal_month,1,a.year) as startdate format date9.,
		intnx('month',MDY(a.cal_month,1,a.year),0,'E') as enddate format date9.,
		b.cpi_index,
		a.inc*(1198/CPI_index) as inc_2015q4
	from job_summary a left join sandmaa.TSY_b15_04_cpi_index b
		on a.year=b.year and a.quarter=b.quarter;
quit;


* convert to spell event data and filter out the self-employed data;
Data job_summary1;
	set job_summary1;

	*relabel the ACC payment;
	if income_source_code = 'CLM' then	income_source_code = 'ACC';
	if income_source_code='W&S' then income_source_code='WnS';
	format startdate enddate date9.;
	* exclude spells before DOB;
	if startdate<DOB and enddate<DOB then
		delete;
	if startdate<DOB and enddate>DOB then
		startdate=DOB;
	keep snz_uid income_source_code DOB inc_2015q4 startdate enddate;
run;

* Self-employment income;
Proc sql;
		create table sei_summary as 
		SELECT  distinct 
			a.snz_uid,
			b.DOB,
			a.inc_tax_yr_year_nbr-1 as year,
			MDY(4,1,inc_tax_yr_year_nbr-1) AS startdate format date9.,
			MDY(3,31,inc_tax_yr_year_nbr) AS enddate format date9.,
			max('SEI') AS income_source_code,
			sum(inc_tax_yr_tot_yr_amt) AS gross_earnings_amt
	FROM  data.income_tax_yr a inner join &population b
	ON a.snz_uid=b.snz_uid
			WHERE a.inc_tax_yr_year_nbr >= &first_anal_yr
				AND a.inc_tax_yr_income_source_code in ('P00', 'P01', 'P02', 'C00', 'C01', 'C02', 'S00', 'S01', 'S02', 'S03') 
	GROUP BY a.snz_uid, startdate
	ORDER BY a.snz_uid, startdate ;
quit;

data sei_summary;
	set sei_summary;
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

	* delete spells starting before DOB;
	if startdate<DOB and enddate<DOB then
		delete;

	if startdate<DOB and enddate>DOB then
		startdate=DOB;
run;

proc sql;
	create table sei_summary1 as select
		a.snz_uid,
		a.DOB,
		a.startdate,
		a.enddate,
		a.income_source_code,
		a.gross_earnings_amt*(1198/CPI_index) as inc_2015q4
	from sei_summary a left join sandmaa.TSY_b15_04_cpi_index b
		on a.mid_yy=b.year and a.mid_qq=b.quarter;
quit;

* COMBINE ALL THE SOURCES OF INCOME BY CALENDAR YEAR;
************************************;


data FACT_INC_;
set JOB_SUMMARY1 SEI_summary1;
rate=inc_2015q4/(enddate-startdate+1);
start1=MDY(1,1,1999); 
do ind=1999 to &last_anal_yr;
			i=ind-(1998);

			start_window=intnx('YEAR',start1,i-1,'S');
			end_window=intnx('YEAR',start1,i,'S')-1;

			%inc_yr(WnS);
			%inc_yr(SEI);
			%inc_yr(ACC);
			%inc_yr(BEN);
			%inc_yr(PPL);
			%inc_yr(STU);


end;
do ind=&firstage. to &lastage.;
			i=ind-(&firstage.-1);
			start_window=intnx('YEAR',DOB,i-1,'S');
			end_window=intnx('YEAR',DOB,i,'S')-1;
			%inc_age(WnS);
			%inc_age(SEI);
			%inc_age(ACC);
			%inc_age(BEN);
			%inc_age(PPL);
			%inc_age(STU);
end;
run;

proc summary data=FACT_INC_ nway;
class snz_uid DOB;
var WNS_: SEI_: STU_: ACC_: BEN_: PPL_: ;
output out=&projectlib.._IND_EARN_&date(drop=_: 
WNS_at_age: WNS_da_at_age:
SEI_at_age: SEI_da_at_age:
ACC_at_age: ACC_da_at_age:
BEN_at_age: BEN_da_at_age:
PPL_at_age: PPL_da_at_age:
STU_at_age: STU_da_at_age:) sum=;
run; 

proc summary data=FACT_INC_ nway;
class snz_uid DOB;
var WNS_: SEI_: STU_: ACC_: BEN_: PPL_: ;
output out=&projectlib.._IND_EARN_at_age_&date(drop=_: 
keep=snz_uid 
WNS_at_age: WNS_da_at_age:
SEI_at_age: SEI_da_at_age:
ACC_at_age: ACC_da_at_age:
BEN_at_age: BEN_da_at_age:
PPL_at_age: PPL_da_at_age:
STU_at_age: STU_da_at_age:) sum=;
run;
proc datasets lib=work;
delete JOB: SEI: fact_:;
run;

%mend; 

*******************************
Creating BDD cost
*******************************;
%macro Create_BEN_cost_pop;
PROC SQL;
	create table TEMP_FIRST as
		SELECT 
			a.*,
			input(compress(msd_fte_start_date,"-"),yymmdd10.) as startdate  format date9.,
			input(compress(msd_fte_end_date,"-"),yymmdd10.) as enddate format date9.,
			min(60,msd_fte_daily_gross_amt) as daily_gross_rate,
			min(60,msd_fte_daily_nett_amt) as daily_nett_rate,
			b.DOB
			FROM msd.msd_first_tier_expenditure a 
			inner join &population b
			on a.snz_uid=b.snz_uid
			ORDER BY a.snz_uid, msd_fte_start_date
	;
QUIT;


* FORMATING, CLEANING AND SENSORING;
data TEMP_FIRST;
	set TEMP_FIRST;

	* sensoring;
	if startdate >"&sensor"D then
		delete;

	if enddate >"&sensor"D then
		enddate="&sensor"D;

	if enddate=. then
		enddate="&sensor"D;

	* sensoring old records prior 1991;
	if enddate <'31DEC92'D then
		delete;

	if startdate <'31DEC92'D then
		startdate='31DEC92'D;

	* Fixing extreme daily rates;
	daily_gross_rate=min(60,msd_fte_daily_gross_amt);
	daily_nett_rate=min(60,msd_fte_daily_nett_amt);

	if 0<msd_fte_period_nbr < 1000;

	* calculating middate of the spell ( using MSD approach );
	if startdate=enddate then
		middate=startdate;

	if startdate ne enddate then
		middate=startdate+(enddate-startdate)/2;
	mid_yy=year(middate);
	mid_qq=qtr(middate);

	if startdate<DOB then delete;
run;

* adjusting CPI;
proc sql;
	create table TEMP_first1 as select
		a.snz_uid,
		a.DOB,
		a.startdate,
		a.enddate,
		a.daily_gross_rate*(1198/CPI_index) as rate_2015q4,
		a.daily_nett_rate**(1198/CPI_index) as net_rate_2015q4,
		'FTE' as ben_tier
	from TEMP_first a left join sandmaa.TSY_b15_04_cpi_index b
		on a.mid_yy=b.year and a.mid_qq=b.quarter;
quit;


* SECOND TIER BENEFIT INCOME;

PROC SQL;
	create table TEMP_SECOND as
		SELECT a.*,
			b.DOB
			FROM msd.msd_second_tier_expenditure a inner join &population b
			on a.snz_uid=b.snz_uid
			ORDER BY snz_uid, msd_ste_start_date;
QUIT;
* FORMATING, CLEANING AND APPLYING BUSINESS RULES;
** first clean up data and apply conditions;
data  TEMP_second;
	set TEMP_second;
	format startdate enddate middate date9.;

	* Define startdate and enddate variables;
	startdate = input(msd_ste_start_date,yymmdd10.);
	enddate = 	input(msd_ste_end_date,  yymmdd10.);

	*create a second tier label;
	income_source_code = 'STE';

	* sensored;
	if startdate>"&sensor"d then  delete;

	if enddate>"&sensor"d then
		enddate="&sensor"d;


	* sensoring old records prior 1991;
	if enddate <'31DEC92'D then
		delete;

	if startdate <'31DEC92'D then
		startdate='31DEC92'D;


	* Discards and filters;
	discards = 0;

	if msd_ste_supp_serv_code not  in ('064' );

	*does not include ftc as these are included later;
	* remove unrealistic records;
	if msd_ste_daily_gross_amt>=600 and msd_ste_period_nbr>=5 then delete;
	* deleting approx 292k from total Tier 2 expenditure;
	* if the enddate is before the startdate it is an error;
	if enddate < startdate then delete;

	* NEED TO CONSIDER WHETHER TO EXCLUDE A RECOVERABLE ADVANCE;
	if startdate=enddate then
		middate=startdate;

	if startdate ne enddate then
		middate=startdate+(enddate-startdate)/2;
	mid_yy=year(middate);
	mid_qq=qtr(middate);

	* if startdate before DOB must be linking error;
	if startdate<DOB then	delete;
run;

* adjusting daily Rate for CPI;
proc sql;
	create table TEMP_second1 as select
		a.snz_uid,
		a.DOB,
		a.startdate,
		a.enddate,
		a.msd_ste_daily_gross_amt*(1198/CPI_index) as rate_2015q4,
		'STE' as ben_tier

	from TEMP_second a left join sandmaa.TSY_b15_04_cpi_index b
		on a.mid_yy=b.year and a.mid_qq=b.quarter;
quit;

* THIRD  TIER EXPENDITURE;
PROC SQL;
	create table TEMP_THIRD as
		SELECT  a.*,
				b.DOB
			
			FROM msd.msd_THIRD_tier_expenditure a inner join &population b
			on a.snz_uid=b.snz_uid
			ORDER BY snz_uid, msd_tte_decision_date
	;
QUIT;

data  TEMP_third;
	set TEMP_third;
	format startdate enddate date9.;

	* define startdate and enddate variables (one off payments so these are the same);
	startdate = input(msd_tte_decision_date,yymmdd10.);
	enddate = 	input(msd_tte_decision_date,yymmdd10.);

	* Sensoring;
	if startdate>"&sensor"d then
		delete;

	if enddate>"&sensor"d then
		enddate="&sensor"d;

	
	* sensoring old records prior 1991;
	if enddate <'31DEC92'D then
		delete;

	if startdate <'31DEC92'D then
		startdate='31DEC92'D;


	year=year(startdate);
	quarter=qtr(startdate);

	* Need to consider whether to exclude recoverable payments;
	if msd_tte_recoverable_ind='N';

	if startdate<DOB then delete;
run;

* CPI adjusting;
proc sql;
	create table TEMP_third1 as select
		a.snz_uid,
		a.DOB,
		a.startdate,
		a.enddate,
		a.msd_tte_pmt_amt*(1198/CPI_index) as rate_2015q4,
		'TTE' as ben_tier
	from TEMP_third a left join sandmaa.TSY_b15_04_cpi_index b
		on a.year=b.year and a.quarter=b.quarter;
quit;

data TEMP_BDD_exp_;
	set TEMP_first1 TEMP_second1 TEMP_third1;

	array FTE_(*) FTE_&first_anal_yr-FTE_&last_anal_yr;
	array STE_(*) STE_&first_anal_yr-STE_&last_anal_yr;
	array net_FTE_(*) net_FTE_&first_anal_yr-net_FTE_&last_anal_yr;
	array TTE_(*) TTE_&first_anal_yr-TTE_&last_anal_yr;

	array FTE_at_age_(*) FTE_at_age_&firstage-FTE_at_age_&lastage;
	array STE_at_age_(*) STE_at_age_&firstage-STE_at_age_&lastage;
	array net_FTE_at_age_(*) net_FTE_at_age_&firstage-net_FTE_at_age_&lastage;
	array TTE_at_age_(*) TTE_at_age_&firstage-TTE_at_age_&lastage;

	do ind=&first_anal_yr. to &last_anal_yr.;
		i=ind-(&first_anal_yr.-1);

		start_window=intnx('YEAR',MDY(1,1,&first_anal_yr.),i-1,'S');
		end_window=intnx('YEAR',MDY(1,1,&first_anal_yr.),i,'S')-1;

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

				if ben_tier='FTE' then
					FTE_(i)=days*rate_2015q4;

				if ben_tier='STE' then
					STE_(i)=days*rate_2015q4;

				if ben_tier='FTE' then
					net_FTE_(i)=days*net_rate_2015q4;

				if ben_tier='TTE' then
					TTE_(i)=days*rate_2015q4;
			end;
	end;

	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);

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

				if ben_tier='FTE' then
					FTE_at_age_(i)=days*rate_2015q4;

				if ben_tier='STE' then
					STE_at_age_(i)=days*rate_2015q4;

				if ben_tier='FTE' then
					net_FTE_at_age_(i)=days*net_rate_2015q4;

				if ben_tier='TTE' then
					TTE_at_age_(i)=days*rate_2015q4;
			end;
	end;

run;

proc summary data=TEMP_BDD_exp_ nway;
	class snz_uid;
	var 
		FTE_at_age_16-FTE_at_age_&lastage
		STE_at_age_16-STE_at_age_&lastage
		net_FTE_at_age_16-net_FTE_at_age_&lastage
		TTE_at_age_16-TTE_at_age_&lastage;
	output out=&projectlib.._COST_BEN_at_age_&date (drop=_:) sum=;
run;

proc summary data=TEMP_BDD_exp_ nway;
	class snz_uid;
	var 
		FTE_1993-FTE_&last_anal_yr.
		STE_1993-STE_&last_anal_yr.
		net_FTE_1993-net_FTE_&last_anal_yr.
		TTE_1993-TTE_&last_anal_yr.;
	output out=&projectlib.._COST_BEN_&date (drop=_:) sum=;
run;

proc datasets lib=work;
delete TEMP:;
run;
%mend;