/********************************************************************************************************************************************
********************************************************************************************************************************************
Developer: Sarah Tumen 
Date Created: 7 December 2015


This code uses CYF client legal status dataset to calculate the days of CYF and Youth Justice care episodes. 
Legal status table is not integrated as main IDI table, therefore the using msd unique identifier (snz_msd_uid) 
in the concordance table snz_uid is attached. 
In future if this table is to be integrated as IDI main table, the subsequent changes in the code are necessary.

CYF care episode was brought late as an event table in the Sandpit
*/

proc SQL;
	Connect to sqlservr (server=WPRDSQL36\iLeed database=IDI_clean_&version);
	create table CONC as select * from connection to  sqlservr
		(select 
			snz_uid, 
			snz_ird_uid, 
			snz_dol_uid, 
			snz_moe_uid, 
			snz_msd_uid,
			snz_dia_uid,
			snz_moh_uid, 
			snz_jus_uid from security.concordance);
quit;

proc sort data=sandcyf.cyf_ev_cli_legal_status_cys_f out=care_e(drop=data_extracted_datetime);
	by snz_composite_event_uid;

proc sort data=sandcyf.cyf_dt_cli_legal_status_cys_d out=care_d(drop=data_extracted_datetime);
	by snz_composite_event_uid;
run;

data care;
	merge care_e(in=a) care_d;
	by snz_composite_event_uid;

	if a;
	format startdate enddate date9.;
	startdate=input(compress(event_from_date_wid,"-"),yymmdd10.);
	enddate=input(compress(event_to_date_wid,"-"),yymmdd10.);

	if startdate<"&sensor"d;

	if enddate>"&sensor"d then
		enddate="&sensor"d;

	if startdate<=enddate;

	if in_custody_yn='Y';
	year=year(startdate);

	if (legal_status_group="CNP Custody" and in_favour_of_type_code='CEO') or 
		(legal_status_group="CNP Custody" and in_favour_of_type_code='CEMSD') or 
		(legal_status_group="CNP Custody" and in_favour_of_type_code='UAC') or 
		(legal_status_group="CNP Guardianship" and in_favour_of_type_code='CEMSD' and legal_status_code='S1102A') or 
		(legal_status_group="CNP Guardianship" and in_favour_of_type_code='CEO' and legal_status_code='S1102A') or 
		(legal_status_group="CNP Guardianship" and in_favour_of_type_code='UAC' and legal_status_code='S1102A') then
		output;

	if legal_status_group in ("YJU Custody","YJU Supervision") then
		output;
run;

proc sort data=care nodupkey;
	by snz_msd_uid  startdate enddate;
run;

proc freq data=care;
	tables business_area_type_code*legal_status_group/list missing;
run;

proc sql;
	create table care_1 as select
		a.*,
		b.snz_uid
	from care a inner join CONC b
		on a.snz_msd_uid=b.snz_msd_uid
	order by snz_uid, startdate;

	%overlap(care_1);
quit;

data care_OR_diff;
	set Care_1_OR;
	by snz_uid startdate;
	lag_enddate=lag(enddate);
	lag_snz_uid=lag(snz_uid);

	if snz_uid=lag_snz_uid then
		diff=enddate-lag_enddate;

	if diff=. then
		diff=.;
	else if diff lt 0 then
		diff=0;
run;

data care_OR_diff;
	set care_OR_diff;
	by snz_uid;
	retain episode 0;

	if first.snz_uid then
		episode=1;
	else if diff le 28 then
		episode=episode;
	else episode=episode+1;
run;

proc sort data=care_OR_diff;
	by snz_uid episode;
run;

data c_episode;
	set care_OR_diff;
	by snz_uid episode;
	retain episode_start 0 episode_end 0;

	if first.snz_uid or first.episode then
		episode_end=enddate;
	else episode_end=max(episode_end, enddate);

	if first.snz_uid or first.episode then
		episode_start=startdate;
	else episode_start=episode_start;
	format episode_start episode_end lag_enddate date9.;
run;

proc sort data= c_episode;
	by snz_uid episode;
run;

data c_c_episode;
	set c_episode;
	by snz_uid episode;

	if last.episode;
	dur_new=episode_end-episode_start+1;
	year=year(episode_start);
	keep snz_uid  episode_start episode_end business_area_type_code year;
run;

proc freq data=c_c_episode;
	tables year*business_area_type_code/nocol norow nopercent;
run;

proc sql;
	create table care_episodes
		as select 
			a.snz_uid,
			a.episode_start as startdate,
			a.episode_end as enddate,
			a.business_area_type_code,
			b.DOB
		from c_c_episode a inner join &population b
			on a.snz_uid=b.snz_uid;

	%aggregate_by_year(care_episodes,care_episodes_1,&first_anal_yr,&last_anal_yr);

proc summary data=care_episodes_1 nway;
	class snz_uid year business_area_type_code;
	var days;
	output out=temp sum=;
run;

data TEMP1;
	set TEMP;
	Child_CYF_ce_da=0;
	Child_YJ_ce_da=0;

	if business_area_type_code='CNP' then
		Child_CYF_ce_da=days;
	else if business_area_type_code='YJU' then
		Child_YJ_ce_da=days;
run;

proc summary data=TEMP1 nway;
	class snz_uid year;
	var Child_CYF_ce_da Child_YJ_ce_da;
	output out=project.IND_CYF_care_&date(drop=_TYPE_ _FREQ_) sum=;
run;

data cohort_1;
	set &population(keep=snz_uid DOB);
run;

data TEMP2;
	set project.IND_CYF_care_&date Cohort_1;
	array Child_CYF_ce_da_(*) Child_CYF_ce_da_&first_anal_yr-Child_CYF_ce_da_&last_anal_yr;
	array Child_YJ_ce_da_(*) Child_YJ_ce_da_&first_anal_yr-Child_YJ_ce_da_&last_anal_yr;

	do ind=&first_anal_yr to &last_anal_yr;
		i=ind-(&first_anal_yr-1);
		Child_CYF_ce_da_(i)=0;
		Child_YJ_ce_da_(i)=0;

		if year=ind then
			Child_CYF_ce_da_(i)=Child_CYF_ce_da;

		if year=ind then
			Child_YJ_ce_da_(i)=Child_YJ_ce_da;
	end;
run;

proc summary data=TEMP2 nway;
	class snz_uid;
	var Child_CYF_ce_da_&first_anal_yr-Child_CYF_ce_da_&last_anal_yr Child_YJ_ce_da_&first_anal_yr-Child_YJ_ce_da_&last_anal_yr;
	output out=project._IND_CYF_care_&date(drop=_TYPE_ _FREQ_) sum=;
run;

data Care_episodes_at_age;
	set care_episodes cohort_1;
	array Child_CYF_ce_da_at_age_(*) Child_CYF_ce_da_at_age_&firstage-Child_CYF_ce_da_at_age_&lastage;
	array Child_YJ_ce_da_at_age_(*) Child_YJ_ce_da_at_age_&firstage-Child_YJ_ce_da_at_age_&lastage;

	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);
		Child_CYF_ce_da_at_age_(i)=0;
		Child_YJ_ce_da_at_age_(i)=0;
		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S');

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

				if business_area_type_code='CNP' then
					Child_CYF_ce_da_at_age_(i)=days;

				if business_area_type_code='YJU' then
					Child_YJ_ce_da_at_age_(i)=days;
			end;
	end;
run;

proc summary data=Care_episodes_at_age nway;
	class snz_uid DOB;
	var Child_CYF_ce_da_at_age_&firstage-Child_CYF_ce_da_at_age_&lastage 
		Child_YJ_ce_da_at_age_&firstage-Child_YJ_ce_da_at_age_&lastage;
	output out=TEMP (drop=_TYPE_ _FREQ_) sum=;
run;

data project._IND_CYF_care_at_age_&date;
	set TEMP;
	array Child_CYF_ce_da_at_age_(*) Child_CYF_ce_da_at_age_&firstage-Child_CYF_ce_da_at_age_&lastage;
	array Child_YJ_ce_da_at_age_(*) Child_YJ_ce_da_at_age_&firstage-Child_YJ_ce_da_at_age_&lastage;
	array Child_ce_at_age_(*) Child_ce_at_age_&firstage-Child_ce_at_age_&lastage;
	array Child_YJ_ce_at_age_(*) Child_YJ_ce_at_age_&firstage-Child_YJ_ce_at_age_&lastage;

	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);
		Child_CYF_ce_at_age_(i)=Child_CYF_ce_da_at_age_(i);

		if Child_CYF_ce_da_at_age_(i)>0 then
			Child_CYF_ce_at_age_(i)=1;
		Child_YJ_ce_at_age_(i)=Child_YJ_ce_da_at_age_(i);

		if Child_YJ_ce_da_at_age_(i)>0 then
			Child_YJ_ce_at_age_(i)=1;

		start_window=intnx('YEAR',DOB,ind-1,'S');
		end_window=intnx('YEAR',DOB,ind,'S');

		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			Child_CYF_ce_da_at_age_(i)=.;

		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			Child_YJ_ce_da_at_age_(i)=.;

		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			Child_CYF_ce_at_age_(i)=.;

		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			Child_YJ_ce_at_age_(i)=.;
	end;

	drop start_window end_window ind i;
run;