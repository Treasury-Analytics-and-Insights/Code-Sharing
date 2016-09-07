/****************************************************************************************************************************************
****************************************************************************************************************************************

Developer: Sarah Tumen (A&I, NZ Treasury)
Date created: 14Jan2015

This code selected tertiary education indicators at the age of reference person. 
Indicators include tertiary participation, education qualifications at age of the reference person.

*/

proc format 
    value $lv8id
          "40"-"41","46", "60", "96", "98"      ="1"
          "36"-"37","43"                        ="2"
          "30"-"35"                       ="3"
          "20","25"                       ="4"
          "21","12"-"14"                  ="6"
          "11"                            ="7"
          "01","10"                       ="8"
          "90", "97", "99"                ="9"
          Other                           ="E"

    value $lv8d
          "1"     =      "Level 1-3 certificates"
          "2"     =      "Level 4 certificates"
          "3"     =      "Diplomas"
          "4"     =      "Bachelors"
          "6"     =      "Level 7-8 graduate honours certs/dips"
          "7"     =      "Masters"
          "8"     =      "Doctorates"
          "9"     =      "Non formal"
         
run;

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

%overlap(ter_enrol_clean);
%aggregate_by_year(ter_enrol_clean_OR,enrol_2,&first_anal_yr,&last_anal_yr);

data _enrol_2;
	set enrol_2;

	array ter_enr_da_at_age_(*)	ter_enr_da_at_age_&firstage-ter_enr_da_at_age_&lastage;
	array ter_enr_id_at_age_(*)	ter_enr_id_at_age_&firstage-ter_enr_id_at_age_&lastage;

	array f_ter_enr_da_at_age_(*)	f_ter_enr_da_at_age_&firstage-f_ter_enr_da_at_age_&lastage;
	array f_ter_enr_id_at_age_(*)	f_ter_enr_id_at_age_&firstage-f_ter_enr_id_at_age_&lastage;

	array nf_ter_enr_da_at_age_(*)	nf_ter_enr_da_at_age_&firstage-nf_ter_enr_da_at_age_&lastage;
	array nf_ter_enr_id_at_age_(*)	nf_ter_enr_id_at_age_&firstage-nf_ter_enr_id_at_age_&lastage;

	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);
		ter_enr_da_at_age_(i)=0;
		ter_enr_id_at_age_(i)=0;

		f_ter_enr_da_at_age_(i)=0;
		f_ter_enr_id_at_age_(i)=0;

		nf_ter_enr_da_at_age_(i)=0;
		nf_ter_enr_id_at_age_(i)=0;

		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S');

		if not((startdate > end_window) or (enddate < start_window)) then
			do;
			ter_enr_id_at_age_(i)=1;
			f_ter_enr_id_at_age_(i)=1;
			nf_ter_enr_id_at_age_(i)=1;

				if (startdate <= start_window) and  (enddate > end_window) then
					days=(end_window-start_window)+1;
				else if (startdate <= start_window) and  (enddate <= end_window) then
					days=(enddate-start_window)+1;
				else if (startdate > start_window) and  (enddate <= end_window) then
					days=(enddate-startdate)+1;
				else if (startdate > start_window) and  (enddate > end_window) then
					days=(end_window-startdate)+1;
				ter_enr_da_at_age_[i]=days*ter_enr_id_at_age_(i);
				f_ter_enr_da_at_age_[i]=days*f_ter_enr_id_at_age_(i);
				nf_ter_enr_da_at_age_[i]=days*nf_ter_enr_id_at_age_(i);

			end;

		drop i ind;
	end;
run;

proc summary data=_enrol_2 nway;
	class snz_uid;
	var 
ter_enr_da_at_age_&firstage-ter_enr_da_at_age_&lastage ter_enr_id_at_age_&firstage-ter_enr_id_at_age_&lastage
f_ter_enr_da_at_age_&firstage-f_ter_enr_da_at_age_&lastage f_ter_enr_id_at_age_&firstage-f_ter_enr_id_at_age_&lastage
nf_ter_enr_da_at_age_&firstage-nf_ter_enr_da_at_age_&lastage nf_ter_enr_id_at_age_&firstage-nf_ter_enr_id_at_age_&lastage;
	output out= TEMP (drop=_TYPE_ _FREQ_) sum=;
run;

data project._IND_TER_ENROL_at_age_&date;
	merge &population(keep=snz_uid DOB) TEMP;
	by snz_uid;
	array ter_enr_id_at_age_(*)	ter_enr_id_at_age_&firstage-ter_enr_id_at_age_&lastage;
	array ter_enr_da_at_age_(*)	ter_enr_da_at_age_&firstage-ter_enr_da_at_age_&lastage;

	array f_ter_enr_id_at_age_(*)	f_ter_enr_id_at_age_&firstage-f_ter_enr_id_at_age_&lastage;
	array f_ter_enr_da_at_age_(*)	f_ter_enr_da_at_age_&firstage-f_ter_enr_da_at_age_&lastage;

	array nf_ter_enr_id_at_age_(*)	nf_ter_enr_id_at_age_&firstage-nf_ter_enr_id_at_age_&lastage;
	array nf_ter_enr_da_at_age_(*)	nf_ter_enr_da_at_age_&firstage-nf_ter_enr_da_at_age_&lastage;

	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);

		if ter_enr_id_at_age_(i)=. then
			ter_enr_id_at_age_(i)=0;

		if ter_enr_id_at_age_(i)>1 then
			ter_enr_id_at_age_(i)=1;

		if ter_enr_da_at_age_(i)=. then
			ter_enr_da_at_age_(i)=0;


		if f_ter_enr_id_at_age_(i)=. then
			f_ter_enr_id_at_age_(i)=0;

		if f_ter_enr_id_at_age_(i)>1 then
			f_ter_enr_id_at_age_(i)=1;

		if f_ter_enr_da_at_age_(i)=. then
			f_ter_enr_da_at_age_(i)=0;


		if nf_ter_enr_id_at_age_(i)=. then
			nf_ter_enr_id_at_age_(i)=0;

		if nf_ter_enr_id_at_age_(i)>1 then
			nf_ter_enr_id_at_age_(i)=1;

		if nf_ter_enr_da_at_age_(i)=. then
			nf_ter_enr_da_at_age_(i)=0;

		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S');

		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			ter_enr_id_at_age_(i)=.;

		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			ter_enr_da_at_age_(i)=.;


		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			f_ter_enr_id_at_age_(i)=.;

		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			f_ter_enr_da_at_age_(i)=.;

			
		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			nf_ter_enr_id_at_age_(i)=.;

		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			nf_ter_enr_da_at_age_(i)=.;
	end;

	drop i ind start_window end_window;
run;

data _enrol;
	set ter_enrol_clean;

	array ter_efts_cons_at_age_(*)	ter_efts_cons_at_age_&firstage-ter_efts_cons_at_age_&lastage;
	array f_ter_efts_cons_at_age_(*)	f_ter_efts_cons_at_age_&firstage-f_ter_efts_cons_at_age_&lastage;
	array nf_ter_efts_cons_at_age_(*)	nf_ter_efts_cons_at_age_&firstage-nf_ter_efts_cons_at_age_&lastage;

	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);
		ter_efts_cons_at_age_(i)=0;
		f_ter_efts_cons_at_age_(i)=0;
		nf_ter_efts_cons_at_age_(i)=0;

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
				ter_efts_cons_at_age_[i]=(EFTS_consumed/dur)*days;
				if formal=1 then f_ter_efts_cons_at_age_[i]=(EFTS_consumed/dur)*days;
				if formal=0 then nf_ter_efts_cons_at_age_[i]=(EFTS_consumed/dur)*days;

			end;

		drop i ind;
	end;
run;

proc summary data=_enrol nway;
	class snz_uid;
	var ter_efts_cons_at_age_&firstage-ter_efts_cons_at_age_&lastage
	f_ter_efts_cons_at_age_&firstage-f_ter_efts_cons_at_age_&lastage
	nf_ter_efts_cons_at_age_&firstage-nf_ter_efts_cons_at_age_&lastage;
	output out=TEMP (drop=_TYPE_ _FREQ_) sum=;
run;

data project._IND_TER_EFTS_at_age_&date;
	merge &population (keep=snz_uid DOB) TEMP;
	by snz_uid;
	array ter_efts_cons_at_age_(*)	ter_efts_cons_at_age_&firstage-ter_efts_cons_at_age_&lastage;
	array f_ter_efts_cons_at_age_(*)	f_ter_efts_cons_at_age_&firstage-f_ter_efts_cons_at_age_&lastage;
	array nf_ter_efts_cons_at_age_(*)	nf_ter_efts_cons_at_age_&firstage-nf_ter_efts_cons_at_age_&lastage;

	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);

		if ter_efts_cons_at_age_(i)=. then
			ter_efts_cons_at_age_(i)=0;
		if f_ter_efts_cons_at_age_(i)=. then
			f_ter_efts_cons_at_age_(i)=0;
		if nf_ter_efts_cons_at_age_(i)=. then
			nf_ter_efts_cons_at_age_(i)=0;

		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S');

		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			ter_efts_cons_at_age_(i)=.;
		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			f_ter_efts_cons_at_age_(i)=.;
		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			nf_ter_efts_cons_at_age_(i)=.;
	end;

	drop i ind start_window end_window;
run;

proc format;
	value Ter_qual
		0='No Ter Qual'
		1='Ter Other qual'
		2='Ter L1-3 Cert'
		3='Ter L4 Cert'
		4='Ter Diploma'
		5='Bachelor degrees'
		6='Postgrad Dip Cert'
		7='Masters PHD';
run;

data Compl;
	set project.IND_TER_COMPL_&date;
	high_ter_comp_qual=0;

	if att_TER_oth=1 then
		high_ter_comp_qual=1;

	if att_TER_L1_3Cert=1 then
		high_ter_comp_qual=2;

	if att_TER_L4Cert=1 then
		high_ter_comp_qual=3;

	if att_TER_Dipl=1 then
		high_ter_comp_qual=4;

	if att_TER_Bach=1 then
		high_ter_comp_qual=5;

	if att_TER_Postgrad=1 then
		high_ter_comp_qual=6;

	if att_TER_MastPHD=1 then
		high_ter_comp_qual=7;

	format startdate enddate date9.;
	startdate=MDY(12,30,year);
	enddate=startdate+1;

run;

proc sql;
	create table Compl_at_age
		as select 
			a.*,
			b.DOB
		from COMPL a left join &population b
			on a.snz_uid=b.snz_uid;
quit;

data cohort_1;
	set &population(keep=snz_uid DOB);
run;

data Compl_at_age1;
	set Compl_at_age cohort_1;
	array high_ter_qual_at_age_(*)	high_ter_qual_at_age_&firstage-high_ter_qual_at_age_&lastage;

	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);
		high_ter_qual_at_age_(i)=0;
		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S');

		if not((startdate > end_window) or (enddate < start_window)) then
			do;
				high_ter_qual_at_age_(i)=high_ter_comp_qual;
			end;

		drop i ind start_window end_window;
	end;
run;

proc summary data=Compl_at_age1 nway;
	class snz_uid DOB;
	var high_ter_qual_at_age_&firstage-high_ter_qual_at_age_&lastage;
	output out=TEMP(drop=_TYPE_ _FREQ_) sum=;
run;

data project._IND_TER_COMPL_at_age_&date;
	set TEMP;
	array high_ter_qual_at_age_(*)	high_ter_qual_at_age_&firstage-high_ter_qual_at_age_&lastage;

	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);

		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S');

		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			high_ter_qual_at_age_(i)=.;
		drop i ind start_window end_window;
	end;
run;

data IT_Compl;
	set project.IND_ITL_QUAL_&date;
	format startdate enddate date9.;
	startdate=MDY(12,30,year);
	enddate=startdate+1;
	keep snz_uid year high_IT_qual startdate enddate;
run;

proc sql;
	create table IT_Compl_at_age
		as select 
			a.*,
			b.DOB
		from IT_Compl a left join &population b
			on a.snz_uid=b.snz_uid;

data cohort_1;
	set &population(keep=snz_uid DOB);
run;

data IT_Compl_at_age1;
	set IT_Compl_at_age cohort_1;
	array high_IT_qual_at_age_(*)	high_IT_qual_at_age_&firstage-high_IT_qual_at_age_&lastage;

	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);
		high_IT_qual_at_age_(i)=0;
		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S');

		if not((startdate > end_window) or (enddate < start_window)) then
			do;
				high_IT_qual_at_age_(i)=high_IT_qual;
			end;

		drop i ind start_window end_window;
	end;
run;

proc summary data=IT_Compl_at_age1 nway;
	class snz_uid DOB;
	var high_IT_qual_at_age_&firstage-high_IT_qual_at_age_&lastage;
	output out=TEMP(drop=_TYPE_ _FREQ_) sum=;
run;

data project._IND_IT_COMPL_at_age_&date;
	set TEMP;
	array high_IT_qual_at_age_(*)	high_IT_qual_at_age_&firstage-high_IT_qual_at_age_&lastage;

	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);

		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S');

		if ((end_window>"&sensor"d) or (start_window>"&sensor"d)) then
			high_IT_qual_at_age_(i)=.;
		drop i ind start_window end_window;
	end;
run;

data enrol_;
	set ter_enrol_clean;
	array ter_enr_id_at_age_(*)	ter_enr_id_at_age_&firstage-ter_enr_id_at_age_&lastage;
	array f_ter_enr_id_at_age_(*)	f_ter_enr_id_at_age_&firstage-f_ter_enr_id_at_age_&lastage;
	array nf_ter_enr_id_at_age_(*)	nf_ter_enr_id_at_age_&firstage-nf_ter_enr_id_at_age_&lastage;
	array ter_enr_ld_at_age_(*)	ter_enr_ld_at_age_&firstage-ter_enr_ld_at_age_&lastage;
	array f_ter_enr_ld_at_age_(*)	f_ter_enr_ld_at_age_&firstage-f_ter_enr_ld_at_age_&lastage;
	array nf_ter_enr_ld_at_age_(*)	nf_ter_enr_ld_at_age_&firstage-nf_ter_enr_ld_at_age_&lastage;
	format ter_enr_ld_at_age_: f_ter_enr_ld_at_age_:  nf_ter_enr_ld_at_age_: date9.;
	array ter_enr_lev_at_age_(*)	ter_enr_lev_at_age_&firstage-ter_enr_lev_at_age_&lastage;
	array f_ter_enr_lev_at_age_(*)	f_ter_enr_lev_at_age_&firstage-f_ter_enr_lev_at_age_&lastage;
	array nf_ter_enr_lev_at_age_(*)	nf_ter_enr_lev_at_age_&firstage-nf_ter_enr_lev_at_age_&lastage;

	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);
		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S');

		if not((startdate > end_window) or (enddate < start_window)) then
			do;
				ter_enr_id_at_age_(i)=1;

				if formal=1 then
					f_ter_enr_id_at_age_(i)=1;

				if formal=0 then
					nf_ter_enr_id_at_age_(i)=1;

				if (startdate <= start_window) and  (enddate > end_window) then
					last_day=end_window;
				else if (startdate <= start_window) and  (enddate <= end_window) then
					last_day=enddate;
				else if (startdate > start_window) and  (enddate <= end_window) then
					last_day=enddate;
				else if (startdate > start_window) and  (enddate > end_window) then
					last_day=end_window;
				ter_enr_lev_at_age_[i]=level*ter_enr_id_at_age_(i);

				if formal=1 then
					f_ter_enr_lev_at_age_[i]=level*f_ter_enr_id_at_age_(i);

				if formal=0 then
					nf_ter_enr_lev_at_age_[i]=level*nf_ter_enr_id_at_age_(i);
				ter_enr_ld_at_age_[i]=last_day*ter_enr_id_at_age_(i);

				if formal=1 then
					f_ter_enr_ld_at_age_[i]=last_day*f_ter_enr_id_at_age_(i);

				if formal=0 then
					nf_ter_enr_ld_at_age_[i]=last_day*nf_ter_enr_id_at_age_(i);
			end;

		drop i ind;
	end;
run;

%macro lastlevel;
	%do i=&firstage %to &lastage;

		data lastlevel_&i;
			set enrol_;
			keep snz_uid ter_enr_id_at_age_&i ter_enr_ld_at_age_&i ter_enr_lev_at_age_&i;

			if ter_enr_id_at_age_&i=1;

		data f_lastlevel_&i;
			set enrol_;
			keep snz_uid f_ter_enr_id_at_age_&i f_ter_enr_ld_at_age_&i f_ter_enr_lev_at_age_&i;

			if f_ter_enr_id_at_age_&i=1;

		data nf_lastlevel_&i;
			set enrol_;
			keep snz_uid nf_ter_enr_id_at_age_&i nf_ter_enr_ld_at_age_&i nf_ter_enr_lev_at_age_&i;

			if nf_ter_enr_id_at_age_&i=1;

		proc sort data=lastlevel_&i;
			by snz_uid descending ter_enr_ld_at_age_&i;

		proc sort data=f_lastlevel_&i;
			by snz_uid descending f_ter_enr_ld_at_age_&i;

		proc sort data=nf_lastlevel_&i;
			by snz_uid descending nf_ter_enr_ld_at_age_&i;

		data lastlevel_&i;
			set lastlevel_&i;
			by snz_uid descending ter_enr_ld_at_age_&i;

			if first.snz_uid then
				output;
			drop ter_enr_ld_at_age_&i;

		data f_lastlevel_&i;
			set f_lastlevel_&i;
			by snz_uid descending f_ter_enr_ld_at_age_&i;

			if first.snz_uid then
				output;
			drop f_ter_enr_ld_at_age_&i;

		data nf_lastlevel_&i;
			set nf_lastlevel_&i;
			by snz_uid descending nf_ter_enr_ld_at_age_&i;

			if first.snz_uid then
				output;
			drop nf_ter_enr_ld_at_age_&i;
		run;

		data consolidated_&i;
			merge lastlevel_&i f_lastlevel_&i nf_lastlevel_&i;
			by snz_uid;
		run;

	%end;
%mend;

%lastlevel;

data project._ind_edu_last_ter_lev_&date.; retain snz_uid ter_enr: f_ter_enr: nf_ter_enr: ;
merge &population(keep=snz_uid DOB) LASTLEVEL_0-LASTLEVEL_24 NF_LASTLEVEL_0-NF_LASTLEVEL_24 F_LASTLEVEL_0-F_LASTLEVEL_24 ; by snz_uid;
run;

proc datasets lib=work kill nolist memtype=data;
quit;