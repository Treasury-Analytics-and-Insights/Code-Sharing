/****************************************************************************************************************************************
****************************************************************************************************************************************

Developer : Sarah Tumen (A&I, NZ Treasury)
Date created: 14Jan2015

This code creates range of indicators using Ministry of Education schooling datasets in IDI. 
Indicators include days enrolled at school, days in education intervention 
( suspension, stand downs, truancy, special education and other education interventions).
This code also creates dataset on secondary school attainment ( NCEA qualifications) and school profiles. 

*/

%creating_clean_sch_enrol;

proc sort data=sch_enrol_clean nodupkey out=enrol;
	by snz_uid startdate enddate schoolnumber;
run;

proc sort data=enrol;
	by snz_uid startdate enddate;
run;

%OVERLAP (enrol);
%aggregate_by_year(enrol_OR,enrol_OR_1,&first_anal_yr, &last_anal_yr);

data DECILE_year;
	set sandmoe.moe_school_decile_history;
	format StartDate EndDate date9.;
	startdate=input(compress(decilestartdate,"-"),yymmdd10.);
	enddate=input(compress(decileenddate,"-"),yymmdd10.);

	if startdate<"&sensor"D;

	if enddate="31DEC9999"d then
		enddate="&sensor"D;
	snz_uid=institutionnumber;
	keep snz_uid InstitutionNumber DecileCode startdate enddate;
	rename InstitutionNumber=schoolnumber;
run;

%overlap(Decile_year);
%aggregate_by_year(DECILE_year_OR,DECILE_year1,&first_anal_yr,&last_anal_yr,gross_daily_amt=);

data DECILE_year1;
	set DECILE_year1;
	keep schoolnumber year DecileCode;
run;

proc sort data=DECILE_year1;
	by schoolnumber year;
run;

proc sort data=enrol_OR_1;
	by schoolnumber year;
run;

data enrol_2;
	merge enrol_OR_1(in=a) DECILE_year1;
	by schoolnumber year;

	if a;

	if decilecode=. then
		Decilecode=999;
	rename decilecode=hist_decile;
	format start_window end_window date9.;
run;

data enrol_22;
	set enrol_2;
	drop enddate startdate days;
	rename end_window=enddate;
	rename start_window=startdate;
run;

proc sql;
	create table enrol_222 as select 
		a.*,
		b.schoolgender,
		b.decile as cur_decile,
		b.schooltype,
		b.school_authority

	from enrol_22 a left join project.schoolprofile_open_&date b
		on a.schoolnumber=b.schoolnumber;

data enrol_3;
	set enrol_222;
	array sch_enr_id_at_age_(*)	sch_enr_id_at_age_&firstage-sch_enr_id_at_age_&lastage;
	array sch_enr_da_at_age_(*)	sch_enr_da_at_age_&firstage-sch_enr_da_at_age_&lastage;
	array school_at_age_(*)	 school_at_age_&firstage- school_at_age_&lastage;
	array hist_decile_at_age_(*)	hist_decile_at_age_&firstage- hist_decile_at_age_&lastage;
	array cur_decile_at_age_(*)	$50 cur_decile_at_age_&firstage- cur_decile_at_age_&lastage;
	array sch_auth_at_age_(*) $50 sch_auth_at_age_&firstage- sch_auth_at_age_&lastage;
	array sch_type_at_age_(*) $50	sch_type_at_age_&firstage- sch_type_at_age_&lastage;

	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);
		sch_enr_da_at_age_(i)=0;
		sch_enr_id_at_age_(i)=0;
		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S');

		if not((startdate > end_window) or (enddate < start_window)) then
			do;
				sch_enr_id_at_age_(i)=1;

				if (startdate <= start_window) and  (enddate > end_window) then
					days=(end_window-start_window)+1;
				else if (startdate <= start_window) and  (enddate <= end_window) then
					days=(enddate-start_window)+1;
				else if (startdate > start_window) and  (enddate <= end_window) then
					days=(enddate-startdate)+1;
				else if (startdate > start_window) and  (enddate > end_window) then
					days=(end_window-startdate)+1;
				sch_enr_da_at_age_(i)=days*sch_enr_id_at_age_(i);
				school_at_age_(i)=schoolnumber*sch_enr_id_at_age_(i);
				hist_decile_at_age_(i)=hist_decile*sch_enr_id_at_age_(i);
				cur_decile_at_age_(i)=cur_decile;
				sch_auth_at_age_(i)=school_authority;
				sch_type_at_age_(i)=schooltype;
			end;
	end;
run;

%macro longest_attended;
	%do i=&firstage %to &lastage;

		data longest_&i;
			set enrol_3;
			keep snz_uid sch_enr_da_at_age_&i hist_decile_at_age_&i school_at_age_&i cur_decile_at_age_&i sch_auth_at_age_&i sch_type_at_age_&i;

			if sch_enr_da_at_age_&i>0;

		proc sort data=longest_&i;
			by snz_uid descending sch_enr_da_at_age_&i;

		data longest_&i;
			set longest_&i;
			by snz_uid descending sch_enr_da_at_age_&i;

			if first.snz_uid then
				output;
			drop sch_enr_da_at_age_&i;
		run;

	%end;
%mend;

%longest_attended;

data consolidated;
	retain snz_uid school_at_age_0-school_at_age_24
		hist_decile_at_age_0-hist_decile_at_age_24
		cur_decile_at_age_0-cur_decile_at_age_24
		sch_auth_at_age_0-sch_auth_at_age_24
		sch_type_at_age_0-sch_type_at_age_24;
	merge longest_:;
	by snz_uid;
run;

proc summary data=enrol_3 nway;
	class snz_uid;
	var sch_enr_da_at_age_&firstage-sch_enr_da_at_age_&lastage;
	output out=TEMP_enr_da (drop=_type_ _freq_) sum=;
run;

data project._IND_SCH_ENR_EXT_at_age_&date;
	merge &population(keep=snz_uid DOB) TEMP_enr_da Consolidated;
	by snz_uid;
	array sch_enr_da_at_age_(*)	sch_enr_da_at_age_&firstage-sch_enr_da_at_age_&lastage;

	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);

		if sch_enr_da_at_age_(i)=. then
			sch_enr_da_at_age_(i)=0;

		* Now sensoring for not fully observed data;
		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S');

		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			sch_enr_da_at_age_(i)=.;
	end;

	drop i ind;
	drop start_window end_window;
run;

%creating_clean_interv_table;

proc sort data=interventions;
	by  snz_uid interv_grp startDate;
run;

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

%macro at_age_calc(intervention);

	data &intervention;
		set &intervention._OR;
		array &intervention._da_at_age_(*) &intervention._da_at_age_&firstage-&intervention._da_at_age_&lastage;

		do ind=&firstage to &lastage;
			i=ind-(&firstage-1);
			&intervention._da_at_age_(i)=0;
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
					&intervention._da_at_age_[i]=days;
				end;
		end;

		keep snz_uid &intervention._da_at_age_&firstage-&intervention._da_at_age_&lastage;
	run;

	proc summary data=&intervention nway;
		class snz_uid;
		var &intervention._da_at_age_&firstage-&intervention._da_at_age_&lastage;
		output out=temp (drop=_type_ _freq_) sum=;
	run;

	data &intervention._at_age;
		merge &population(keep=snz_uid DOB) temp;
		by snz_uid;
		array &intervention._da_at_age_(*) &intervention._da_at_age_&firstage-&intervention._da_at_age_&lastage;

		do ind=&firstage to &lastage;
			i=ind-(&firstage-1);

			if &intervention._da_at_age_(i)=. then
				&intervention._da_at_age_(i)=0;

			* Now sensoring for not fully observed data;
			start_window=intnx('YEAR',DOB,i-1,'S');
			end_window=intnx('YEAR',DOB,i,'S');

			if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
				&intervention._da_at_age_(i)=.;
		end;

		drop ind i start_window end_window;
	run;

%mend;

%at_age_calc(AlTED);
%at_age_calc(SUSP);
%at_age_calc(STAND);
%at_age_calc(TRUA);
%at_age_calc(SEDU);
%at_age_calc(ESOL);
%at_age_calc(EARLEX);
%at_age_calc(HOMESCH);
%at_age_calc(BOARD);
%at_age_calc(OTHINT);

data project._IND_INTERVEN_at_age_&date;
	merge AlTED_at_age SUSP_at_age STAND_at_age TRUA_at_age SEDU_at_age ESOL_at_age
		EARLEX_at_age  HOMESCH_at_age BOARD_at_age OTHINT_at_age;
	by snz_uid;
run;

proc datasets lib=work kill nolist memtype=data;
quit;