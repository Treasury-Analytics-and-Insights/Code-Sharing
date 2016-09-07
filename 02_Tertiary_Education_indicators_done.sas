/****************************************************************************************************************************************


Developer: Sarah Tumen(A&I, NZ Treasury)
Date Created: 7Dec 2015

This code creates range of indicators using Ministry of Education tertiary datasets in IDI. 
Indicators include days enrolled at school, days in education intervention
( suspension, stand downs, truancy, special education and other education interventions), 
days enrolled and EFTS consumed. This code also creates dataset on secondary school attainment 
( NCEA qualifications) and tertiary programme completion. 


*/
proc format;
	value $lv8id
		"40","41","46", "60", "96", "98"      ="1"
		"36"-"37","43"                        ="2"
		"30"-"35"                       ="3"
		"20","21","25"                       ="4"
		"12"-"14"                       ="6"
		"11"                            ="7"
		"01","10"                       ="8"
		"90", "97", "99"                ="9"
		Other                           ="E";
run;

proc format;
	value $subsector
		"1","3"="Universities"
		"2"="Polytechnics"
		"4"="Wananga"
		"5","6"="Private Training Establishments";
run;

%creating_ter_enrol_table;

%overlap(TER_ENROL_CLEAN);
%aggregate_by_year(TER_ENROL_CLEAN_OR,enrol_2,&first_anal_yr,&last_anal_yr);

proc sql;
	create table formal_enrol as
		select 
			SNZ_uid,
			year,
			sum(days) as f_ter_enr_da
		from enrol_2 where formal=1
			group by snz_uid, year
				order by snz_uid, year;
quit;

proc sql;
	create table non_formal_enrol as
		select 
			SNZ_uid,
			year,
			sum(days) as nf_ter_enr_da
		from enrol_2 where formal=0
			group by snz_uid, year
				order by snz_uid, year;
quit;

proc sql;
	create table all_enrol as
		select 
			SNZ_uid,
			year,
			sum(days) as ter_enr_da
		from enrol_2
			group by snz_uid, year
				order by snz_uid, year;
quit;

data project.ind_ter_enrol_&date; merge all_enrol formal_enrol non_formal_enrol; by snz_uid year;
if nf_ter_enr_da=. then nf_ter_enr_da=0; 
if f_ter_enr_da=. then f_ter_enr_da=0;
run;

data _TER_ENROL;
	set project.ind_ter_enrol_&date;
	array ter_enr_da_(*) ter_enr_da_&first_anal_yr-ter_enr_da_&last_anal_yr;
	array ter_enr_id_(*) ter_enr_id_&first_anal_yr-ter_enr_id_&last_anal_yr;

	array f_ter_enr_da_(*) f_ter_enr_da_&first_anal_yr-f_ter_enr_da_&last_anal_yr;
	array f_ter_enr_id_(*) f_ter_enr_id_&first_anal_yr-f_ter_enr_id_&last_anal_yr;

	array nf_ter_enr_da_(*) nf_ter_enr_da_&first_anal_yr-nf_ter_enr_da_&last_anal_yr;
	array nf_ter_enr_id_(*) nf_ter_enr_id_&first_anal_yr-nf_ter_enr_id_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);
		ter_enr_da_(ind)=0;
		ter_enr_id_(ind)=0;

		f_ter_enr_da_(ind)=0;
		f_ter_enr_id_(ind)=0;

		nf_ter_enr_da_(ind)=0;
		nf_ter_enr_id_(ind)=0;

		if i=year then
			ter_enr_da_(ind)=ter_enr_da;

		if i=year then
			ter_enr_id_(ind)=1;


		if i=year then
			f_ter_enr_da_(ind)=f_ter_enr_da;

		if i=year then
			f_ter_enr_id_(ind)=1;
			
		if i=year then
			nf_ter_enr_da_(ind)=nf_ter_enr_da;

		if i=year then
			nf_ter_enr_id_(ind)=1;

	end;
run;

proc summary data=_TER_ENROL nway;
	class snz_uid;
	var ter_enr_da_&first_anal_yr-ter_enr_da_&last_anal_yr ter_enr_id_&first_anal_yr-ter_enr_id_&last_anal_yr
f_ter_enr_da_&first_anal_yr-f_ter_enr_da_&last_anal_yr f_ter_enr_id_&first_anal_yr-f_ter_enr_id_&last_anal_yr
nf_ter_enr_da_&first_anal_yr-nf_ter_enr_da_&last_anal_yr nf_ter_enr_id_&first_anal_yr-nf_ter_enr_id_&last_anal_yr;
	output out=TEMP (drop=_TYPE_ _FREQ_) sum=;
run;

data project._IND_TER_ENROL_&date;
	merge &population (keep=snz_uid DOB) TEMP;
	by snz_uid;
	array ter_enr_da_(*) ter_enr_da_&first_anal_yr-ter_enr_da_&last_anal_yr;
	array ter_enr_id_(*) ter_enr_id_&first_anal_yr-ter_enr_id_&last_anal_yr;

	array f_ter_enr_da_(*) f_ter_enr_da_&first_anal_yr-f_ter_enr_da_&last_anal_yr;
	array f_ter_enr_id_(*) f_ter_enr_id_&first_anal_yr-f_ter_enr_id_&last_anal_yr;

	array nf_ter_enr_da_(*) nf_ter_enr_da_&first_anal_yr-nf_ter_enr_da_&last_anal_yr;
	array nf_ter_enr_id_(*) nf_ter_enr_id_&first_anal_yr-nf_ter_enr_id_&last_anal_yr;


	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);

		if ter_enr_da_(ind)=. then
			ter_enr_da_(ind)=0;

		if ter_enr_id_(ind)=. then
			ter_enr_id_(ind)=0;

		if f_ter_enr_da_(ind)=. then
			f_ter_enr_da_(ind)=0;

		if f_ter_enr_id_(ind)=. then
			f_ter_enr_id_(ind)=0;

		if nf_ter_enr_da_(ind)=. then
			nf_ter_enr_da_(ind)=0;

		if nf_ter_enr_id_(ind)=. then
			nf_ter_enr_id_(ind)=0;
		drop i ind;
	end;
run;

proc sql;
	create table All_efts as
		select 
			SNZ_uid,
			moe_enr_year_nbr as year,
			sum(EFTS_consumed) as ter_efts_cons
		from ter_enrol_clean
			group by snz_uid, year
				order by snz_uid, year;
quit;

proc sql;
	create table formal_efts as
		select 
			SNZ_uid,
			moe_enr_year_nbr as year,
			sum(EFTS_consumed) as f_ter_efts_cons
		from ter_enrol_clean where formal=1
			group by snz_uid, year
				order by snz_uid, year;
quit;

proc sql;
	create table Non_formal_efts as
		select 
			SNZ_uid,
			moe_enr_year_nbr as year,
			sum(EFTS_consumed) as nf_ter_efts_cons
		from ter_enrol_clean where formal=0
			group by snz_uid, year
				order by snz_uid, year;
quit;

data project.ind_ter_efts_&date; merge All_efts formal_efts Non_formal_efts; by snz_uid year;
	if f_ter_efts_cons=. then f_ter_efts_cons=0;
	if nf_ter_efts_cons=. then nf_ter_efts_cons=0;
run;

data _TER_ENROL;
	set project.ind_ter_efts_&date;
	array ter_efts_cons_(*) ter_efts_cons_&first_anal_yr-ter_efts_cons_&last_anal_yr;
	array f_ter_efts_cons_(*) f_ter_efts_cons_&first_anal_yr-f_ter_efts_cons_&last_anal_yr;
	array nf_ter_efts_cons_(*) nf_ter_efts_cons_&first_anal_yr-nf_ter_efts_cons_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);
		ter_efts_cons_(ind)=0;
		f_ter_efts_cons_(ind)=0;
		nf_ter_efts_cons_(ind)=0;


		if i=year then
			ter_efts_cons_(ind)=ter_efts_cons;
		if i=year then
			f_ter_efts_cons_(ind)=f_ter_efts_cons;
		if i=year then
			nf_ter_efts_cons_(ind)=nf_ter_efts_cons;

	end;
run;

proc summary data=_TER_ENROL nway;
	class snz_uid;
	var ter_efts_cons_&first_anal_yr-ter_efts_cons_&last_anal_yr
	f_ter_efts_cons_&first_anal_yr-f_ter_efts_cons_&last_anal_yr
	nf_ter_efts_cons_&first_anal_yr-nf_ter_efts_cons_&last_anal_yr;
	output out=TEMP (drop=_TYPE_ _FREQ_) sum=;
run;

data project._IND_TER_efts_&date;
	merge &population(keep=snz_uid DOB) TEMP;
	by snz_uid;
	array ter_efts_cons_(*) ter_efts_cons_&first_anal_yr-ter_efts_cons_&last_anal_yr;
	array f_ter_efts_cons_(*) f_ter_efts_cons_&first_anal_yr-f_ter_efts_cons_&last_anal_yr;
	array nf_ter_efts_cons_(*) nf_ter_efts_cons_&first_anal_yr-nf_ter_efts_cons_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);

		if ter_efts_cons_(ind)=. then
			ter_efts_cons_(ind)=0;
if f_ter_efts_cons_(ind)=. then
			f_ter_efts_cons_(ind)=0;
if nf_ter_efts_cons_(ind)=. then
			nf_ter_efts_cons_(ind)=0;

		drop ind i;
	end;
run;

proc sql;
	create table enrol_11 as 
		select snz_uid,
			put(qacc,$lv8id.) as qacc,
			qual,
			NZSCED,
			EFTS_prog_yrs,
			fund_source,
			subsector,
			level,
			sum(efts_consumed) as SUM_EFTS,
			sum(dur) as dur,
			min(startdate) as first_startdate,
			max(enddate) as last_enddate
		from ter_enrol_clean
			group by snz_uid,qacc,qual,nzsced, EFTS_prog_yrs, fund_source, subsector
				order by snz_uid,qacc,qual, nzsced, subsector;
quit;

proc sort data=enrol_11;
	by snz_uid qual descending SUM_EFTS;
run;

data enrol_11;
	set enrol_11;
	by snz_uid qual descending SUM_EFTS;

	if first.qual=0 then
		delete;
	enrol_id=1;
run;

proc sort data=enrol_11;
	by snz_uid first_startdate;
run;

data enrol_first;
	set enrol_11;
	by snz_uid first_startdate;

	if first.snz_uid then
		do;
			first_enr_qacc=qacc;
			first_enr_qual=qual;
			first_enr_NZSCED=NZSCED;
			first_EFTS_prog_yrs=EFTS_prog_yrs;
			first_enr_fund_source=fund_source;
			first_enr_subsector=subsector;
			first_enr_SUM_EFTS=SUM_EFTS;
			first_enr_dur=dur;
			first_enr_startdate=first_startdate;
			first_enr_last_enddate=last_enddate;
			first_enr_level=level;
			output;
		end;

	keep snz_uid first_enr_qacc first_enr_qual first_enr_NZSCED first_EFTS_prog_yrs first_enr_fund_source first_enr_subsector first_enr_level
		first_enr_SUM_EFTS first_enr_dur first_enr_startdate first_enr_last_enddate;
run;

proc sort data=enrol_11;
	by snz_uid descending qacc;
run;

data enrol_high;
	set enrol_11;
	by snz_uid descending qacc;

	if first.snz_uid then
		do;
			high_enr_qacc=qacc;
			high_enr_qual=qual;
			high_enr_NZSCED=NZSCED;
			high_EFTS_prog_yrs=EFTS_prog_yrs;
			high_enr_fund_source=fund_source;
			high_enr_subsector=subsector;
			high_enr_SUM_EFTS=SUM_EFTS;
			high_enr_dur=dur;
			high_enr_startdate=first_startdate;
			high_enr_last_enddate=last_enddate;
			high_enr_level=level;
			output;
		end;

	keep snz_uid high_enr_qacc high_enr_qual high_enr_NZSCED high_enr_fund_source high_EFTS_prog_yrs high_enr_subsector high_enr_level
		high_enr_SUM_EFTS high_enr_dur high_enr_startdate high_enr_last_enddate;
run;

proc sql;
	create table Enrol_summary as
		select distinct 
			snz_uid,
			sum(sum_efts) as total_efts_consumed,
			sum(dur) as total_enrol_da,
			min(first_startdate) as first_enrol_date ,
			max(last_enddate) as last_enrol_date
		from enrol_11
			group by snz_uid
				order by snz_uid;
Quit;

data Enrol_summary;
	set enrol_summary;
	first_enrol_month=month(first_enrol_date);
	first_enrol_year=year(first_enrol_date);
	enrol_id=1;
run;

proc sort data=&population;
	by snz_uid;
run;

proc sort data=enrol_first;
	by snz_uid;
run;

proc sort data=enrol_high;
	by snz_uid;
run;

proc sort data=enrol_summary;
	by snz_uid;
run;

data project._IND_TER_ENROL_SUM_&date;
	retain snz_uid ter_enr_id first_enrol_month first_enrol_year;
	merge &population(in=a keep=snz_uid DOB) enrol_summary(in=b) enrol_first enrol_high;
	by snz_uid;

	if a;
	format first_enrol_date last_enrol_date 
		first_enr_startdate first_enr_last_enddate
		high_enr_startdate high_enr_last_enddate date9.;
	rename enrol_id=ter_enr_id;
run;

%contents(project._IND_TER_ENROL_SUM_&date);

proc sql;
	create table TER_compl as
		select  snz_uid,
			moe_com_year_nbr,
			put(moe_com_qacc_code,$lv8id.) as att_TER_qual_type,
			moe_com_qual_level_code as level,
			moe_com_qual_nzsced_code
		from moe.completion
			where snz_uid in
				(select distinct snz_uid from &population)
					and MDY(12,31,moe_com_year_nbr)<="&sensor"d
	;
quit;

data Ter_compl;
	set Ter_compl;
	ter_qual=att_TER_qual_type*1;
	Ter_level=level*1;

	if moe_com_year_nbr>=&first_anal_yr or moe_com_year_nbr<=&last_anal_yr;
run;

proc sql;
	create table TER_compl_1 as 
		select distinct
			snz_uid,
			moe_com_year_nbr as year,
			max(ter_qual) as TER_qual_type,
			max(ter_level) as Ter_qual_level
		from TER_compl
			group by snz_uid, year
				order by snz_uid, year;
quit;

data project.IND_Ter_compl_&date;
	set TER_compl_1;
	att_TER_L1_3Cert=0;
	att_TER_L4Cert=0;
	att_TER_Dipl=0;
	att_TER_Bach=0;
	att_TER_Postgrad=0;
	att_TER_MastPHD=0;
	att_TER_oth=0;

	if TER_qual_type in (7,8) then
		att_TER_MastPHD=1;

	if TER_qual_type=6 then
		att_TER_Postgrad=1;

	if TER_qual_type=4 then
		att_TER_Bach=1;

	if TER_qual_type=3 then
		att_TER_Dipl=1;

	if TER_qual_type=2 then
		att_TER_L4Cert=1;

	if TER_qual_type=1 then
		att_TER_L1_3Cert=1;

	if TER_qual_type=9 then
		att_TER_oth=1;

	if TER_qual_type in (7,8) then
		lev_TER_MastPHD=ter_qual_level;

	if TER_qual_type=6 then
		lev_TER_Postgrad=ter_qual_level;

	if TER_qual_type=4 then
		lev_TER_Bach=ter_qual_level;

	if TER_qual_type=3 then
		lev_TER_Dipl=ter_qual_level;

	if TER_qual_type=2 then
		lev_TER_L4Cert=ter_qual_level;

	if TER_qual_type=1 then
		lev_TER_L1_3Cert=ter_qual_level;

	if TER_qual_type=9 then
		lev_TER_oth=ter_qual_level;
	drop TER_qual_type ter_qual_level;
run;

data _TER_compl;
	set project.IND_ter_compl_&date;
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
		att_TER_L1_3cert_(ind)=0;
		att_TER_L4Cert_(ind)=0;
		att_TER_Dipl_(ind)=0;
		att_TER_Bach_(ind)=0;
		att_TER_Postgrad_(ind)=0;
		att_TER_MastPHD_(ind)=0;
		lev_TER_L1_3cert_(ind)=0;
		lev_TER_L4Cert_(ind)=0;
		lev_TER_Dipl_(ind)=0;
		lev_TER_Bach_(ind)=0;
		lev_TER_Postgrad_(ind)=0;
		lev_TER_MastPHD_(ind)=0;

		if i=year and att_TER_L1_3cert=1 then
			att_TER_L1_3cert_(ind)=1;

		if i=year and att_TER_L4Cert=1 then
			att_TER_L4Cert_(ind)=1;

		if i=year and att_TER_Dipl=1 then
			att_TER_Dipl_(ind)=1;

		if i=year and att_TER_Bach=1 then
			att_TER_Bach_(ind)=1;

		if i=year and att_TER_Postgrad=1 then
			att_TER_Postgrad_(ind)=1;

		if i=year and att_TER_MastPHD=1 then
			att_TER_MastPHD_(ind)=1;

		if i=year and att_TER_L1_3cert=1 then
			lev_TER_L1_3cert_(ind)=lev_TER_L1_3cert;

		if i=year and att_TER_L4Cert=1 then
			lev_TER_L4Cert_(ind)=lev_TER_L4Cert;

		if i=year and att_TER_Dipl=1 then
			lev_TER_Dipl_(ind)=lev_TER_Dipl;

		if i=year and att_TER_Bach=1 then
			lev_TER_Bach_(ind)=lev_TER_Bach;

		if i=year and att_TER_Postgrad=1 then
			lev_TER_Postgrad_(ind)=lev_TER_Postgrad;

		if i=year and att_TER_MastPHD=1 then
			lev_TER_MastPHD_(ind)=lev_TER_MastPHD;
	end;
run;

proc summary data=_TER_COMPL nway;
	class snz_uid;
	var 
		att_TER_L1_3cert_&first_anal_yr-att_TER_L1_3cert_&last_anal_yr
		att_TER_L4Cert_&first_anal_yr-att_TER_L4Cert_&last_anal_yr
		att_TER_Dipl_&first_anal_yr-att_TER_Dipl_&last_anal_yr
		att_TER_Bach_&first_anal_yr-att_TER_Bach_&last_anal_yr
		att_TER_Postgrad_&first_anal_yr-att_TER_Postgrad_&last_anal_yr
		att_TER_MastPHD_&first_anal_yr-att_TER_MastPHD_&last_anal_yr
		lev_TER_L1_3cert_&first_anal_yr-lev_TER_L1_3cert_&last_anal_yr
		lev_TER_L4Cert_&first_anal_yr-lev_TER_L4Cert_&last_anal_yr
		lev_TER_Dipl_&first_anal_yr-lev_TER_Dipl_&last_anal_yr
		lev_TER_Bach_&first_anal_yr-lev_TER_Bach_&last_anal_yr
		lev_TER_Postgrad_&first_anal_yr-lev_TER_Postgrad_&last_anal_yr
		lev_TER_MastPHD_&first_anal_yr-lev_TER_MastPHD_&last_anal_yr
	;
	output out=TEMP (drop=_TYPE_ _FREQ_) sum=;
run;

data project._IND_TER_COMPL_&date;
	merge &population (keep=snz_uid DOB) TEMP;
	by snz_uid;
run;

proc freq data=project._IND_TER_COMPL_&date;
	tables  att_TER_L1_3cert_&first_anal_yr-att_TER_L1_3cert_&last_anal_yr;
run;

proc sort data=Ter_compl;
	by snz_uid moe_com_year_nbr ter_qual;
run;

data first_compl(drop=att_ter_qual_type level);
	set Ter_compl;
	by snz_uid moe_com_year_nbr ter_qual;

	if first.snz_uid then
		output;
	rename moe_com_year_nbr=first_comp_year
		ter_qual=first_comp_qual
		ter_level=first_comp_qual_level
		moe_com_qual_nzsced_code=first_comp_nzsced_code;
run;

proc sort data=Ter_compl;
	by snz_uid ter_qual ter_level moe_com_year_nbr;
run;

data high_compl(drop=att_ter_qual_type level);
	set Ter_compl;
	by snz_uid ter_qual ter_level moe_com_year_nbr;

	if last.snz_uid then
		output;
	rename moe_com_year_nbr=high_comp_year
		ter_qual=high_comp_qual
		ter_level=high_comp_qual_level
		moe_com_qual_nzsced_code=high_comp_nzsced_code;
run;

proc sort data=high_compl;
	by snz_uid;

proc sort data=first_compl;
	by snz_uid;

data project._ind_ter_compl_sum_&date;
	merge &population(keep=snz_uid DOB) first_compl high_compl;
	by snz_uid;
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
	create table itl_CR as 
		SELECT distinct
			snz_uid
			,moe_itl_year_nbr as year
			,sum(moe_itl_tot_credits_awarded_nbr) as moe_itl_credits_nbr
		FROM IT 
			WHERE snz_uid IN (select distinct snz_uid from &population)
				GROUP BY snz_uid, year
					ORDER by snz_uid, year;
quit;

proc sort data=IT nodupkey Out=IT_dur;
	by snz_uid startdate enddate;
run;

%overlap(it_dur);

%aggregate_by_year(IT_DUR_OR, IT2, &first_anal_yr ,&last_anal_yr);

proc sql;
	create table int_duration as
		select 
			SNZ_uid,
			year,
			sum(days) as IT_da
		from IT2
			group by snz_uid, year
				order by snz_uid, year;
quit;

proc sort data=int_duration;
	by snz_uid year;
run;

proc sort data=itl_CR;
	by snz_uid year;
run;

data project.ind_ITL_&date;
	merge int_duration (in=a) itl_cr (in=b rename=moe_itl_credits_nbr=IT_CR);

	if a and b;
	IND_TRAIN_ID=1;
	by snz_uid year;
run;

data _ITL;
	set project.ind_ITL_&date;
	array IT_CR_(*) IT_CR_&first_anal_yr -IT_CR_&last_anal_yr;
	array IT_da_(*) IT_da_&first_anal_yr -IT_da_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);
		IT_CR_(ind)=0;
		IT_da_(ind)=0;

		if year=i then
			IT_CR_(ind)=IT_CR;

		if year=i then
			IT_da_(ind)=IT_da;
	end;
run;

proc summary data=_ITL nway;
	class snz_uid;
	var IT_CR_&first_anal_yr -IT_CR_&last_anal_yr IT_da_&first_anal_yr -IT_da_&last_anal_yr;
	output out=TEMP (drop=_TYPE_ _FREQ_) sum=;
run;

data project._ind_ITL_&date;
	merge &population(keep=snz_uid DOB) TEMP;
	by snz_uid;
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

data itl1 deletes;
	set it_qual;
	array L(*) L1-L8;

	if sum(of L1-L8)=0 then
		output deletes;
	else output itl1;
run;

proc sql;
	create table itl2
		as select distinct * from itl1;
quit;

proc summary data=itl2 nway;
	class snz_uid year;
	var L1-L8;
	output out=itl3(drop=_freq_ _type_) sum=;
run;

data project.ind_ITL_qual_&date;
	set ITL3;
	array L(*) L1-L8;
	array IT_qual_L(*) IT_qual_L1-IT_qual_L8;
	high_IT_qual=0;

	do i=1 to 8;
		IT_qual_L(i)=L(i);

		if L(i)>1 then
			L(i)=1;

		if L(i)=1 then
			high_IT_qual=i;
	end;

	drop i L1-L8;
run;

data _ITL;
	set project.ind_ITL_qual_&date;
	array IT_qual_(*) IT_qual_&first_anal_yr -IT_qual_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);
		IT_qual_(ind)=0;

		if year=i then
			IT_qual_(ind)=high_it_qual;
	end;
run;

proc summary data=_ITL nway;
	class snz_uid;
	var IT_qual_&first_anal_yr -IT_qual_&last_anal_yr;
	output out=TEMP (drop=_TYPE_ _FREQ_) sum=;
run;

Data project._ind_ITL_qual_&date;
	merge &population (keep=snz_uid DOB) TEMP;
	by snz_uid;
run;

proc datasets lib=work kill nolist memtype=data;
quit;