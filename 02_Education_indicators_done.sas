/****************************************************************************************************************************************
****************************************************************************************************************************************

Developer : Sarah Tumen (A&I, NZ Treasury)
Date created: 14Jan2015


This code selected education indicators at the age of reference person. 
It includes days enrolled at school, education interventions, tertiary participation, 
education qualifications at age of the reference person.

*/

%creating_clean_sch_enrol;

proc sort data= sch_enrol_clean nodupkey out=enrol;
	by snz_uid startdate enddate schoolnumber;
run;

proc sort data=enrol;
	by snz_uid startdate enddate;
run;

%OVERLAP (enrol);

data enrol_2;
	set enrol_OR;
	keep snz_uid schoolnumber startdate enddate;
run;

%aggregate_by_year(enrol_2,enrol_2_sum,&first_anal_yr,&last_anal_yr);

proc sql;
	create table project.IND_SCH_ENROL_&date as
		select 
			SNZ_uid,
			year,
			sum(days) as sch_enr_da
		from enrol_2_sum
			group by snz_uid, year
				order by snz_uid, year;
quit;

data _ENROL;
	set;
	set project.IND_SCH_ENROL_&date;
	array sch_enr_da_(*) sch_enr_da_&first_anal_yr-sch_enr_da_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);
		sch_enr_da_(ind)=0;

		if i=year then
			sch_enr_da_(ind)=sch_enr_da;
	end;
run;

proc summary data=_ENROL nway;
	class snz_uid;
	var sch_enr_da_&first_anal_yr-sch_enr_da_&last_anal_yr;
	output out=TEMP (drop=_type_ _freq_) sum=;
run;

data project._IND_SCH_ENROL_&date;
	merge &population (keep=snz_uid DOB) TEMP;
	by snz_uid;
	array sch_enr_da_(*) sch_enr_da_&first_anal_yr-sch_enr_da_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);

		if sch_enr_da_(ind)=. then
			sch_enr_da_(ind)=0;
		drop i ind;
	end;
run;

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

DATA QualsHA;
	merge sec_qual(in=a) qual_lookup(in=b);
	by qual;

	if a;
	HA=0;

	if year=load_year or load_year-year<=2 or load_year=.;

	if NQFlevel in (0,.) then
		delete;

	if year < 2003 then
		delete;

	if year>=&first_anal_yr and year<=&last_anal_yr;

	if nqflevel >= 4 and QualificationType=21 then
		ha=41;
	else if nqflevel >= 4 and QualificationType=10 then
		ha=40;
	else if nqflevel >= 4 then
		ha=42;
	else if qualificationcode='1039' and result='E' then
		HA=39;
	else if qualificationcode='1039' and result='M' then
		HA=38;
	else if qualificationcode='1039' and result='ZZ' then
		HA=37;
	else if qualificationcode='1039' and result='N' then
		HA=36;
	else if nqflevel=3 then
		HA=35;
	else if (qualificationcode='0973' or qualificationcode='973') and result='E' then
		HA=29;
	else if (qualificationcode='0973' or qualificationcode='973') and result='M' then
		HA=28;
	else if (qualificationcode='0973' or qualificationcode='973') and result='ZZ' then
		HA=27;
	else if (qualificationcode='0973' or qualificationcode='973') and result='N' then
		HA=26;
	else if nqflevel=2 then
		HA=25;
	else if (qualificationcode='0928' or qualificationcode='928') and result='E' then
		HA=19;
	else if (qualificationcode='0928' or qualificationcode='928') and result='M' then
		HA=18;
	else if (qualificationcode='0928' or qualificationcode='928') and result='ZZ' then
		HA=17;
	else if (qualificationcode='0928' or qualificationcode='928') and result='N' then
		HA=16;
	else if nqflevel=1 then
		HA=15;
	NCEA_L1=0;
	NCEA_L2=0;
	NCEA_L3=0;
	non_NCEA_L1=0;
	non_NCEA_L2=0;
	non_NCEA_L3=0;
	non_NCEA_L4=0;

	if HA in (19,18,17,16) then
		NCEA_L1=1;
	else if HA=15 then
		non_NCEA_L1=1;
	else if HA in (29,28,27,26) then
		NCEA_L2=1;
	else if HA=25 then
		non_NCEA_L2=1;
	else if HA in (39,38,37,36) then
		NCEA_L3=1;
	else if HA=35 then
		non_NCEA_L3=1;
	else if HA in (42,41,40) then
		non_NCEA_L4=1;
	keep snz_uid year HA NCEA_L1 NCEA_L2 NCEA_L3 non_NCEA_L1 non_NCEA_L2 non_NCEA_L3 non_NCEA_L4;
run;

proc sql;
	create table project.ind_NCEA_qual_&date as select
		snz_uid,
		year,
		max(NCEA_L1) as NCEA_L1,
		max(NCEA_L2) as NCEA_L2,
		max(NCEA_L3) as NCEA_L3,
		max(non_NCEA_L1) as non_NCEA_L1,
		max(non_NCEA_L2) as non_NCEA_L2,
		max(non_NCEA_L3) as non_NCEA_L3,
		max(non_NCEA_L4) as non_NCEA_L4

	from QualsHA
		group by snz_uid, year
			order by snz_uid, year;

quit;

data sec_qual2;
	set project.ind_NCEA_qual_&date;
	array ncea_L1_(*) ncea_L1_&first_anal_yr-ncea_L1_&last_anal_yr;
	array ncea_L2_(*) ncea_L2_&first_anal_yr-ncea_L2_&last_anal_yr;
	array ncea_L3_(*) ncea_L3_&first_anal_yr-ncea_L3_&last_anal_yr;
	array non_ncea_L1_(*) non_ncea_L1_&first_anal_yr-non_ncea_L1_&last_anal_yr;
	array non_ncea_L2_(*) non_ncea_L2_&first_anal_yr-non_ncea_L2_&last_anal_yr;
	array non_ncea_L3_(*) non_ncea_L3_&first_anal_yr-non_ncea_L3_&last_anal_yr;
	array non_ncea_L4_(*) non_ncea_L4_&first_anal_yr-non_ncea_L4_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);
		ncea_L1_(ind)=0;
		ncea_L2_(ind)=0;
		ncea_L3_(ind)=0;
		non_ncea_L1_(ind)=0;
		non_ncea_L2_(ind)=0;
		non_ncea_L3_(ind)=0;
		non_ncea_L4_(ind)=0;

		if ncea_l1=1 and year=i then
			NCEA_L1_(ind)=1;

		if ncea_l2=1 and year=i then
			NCEA_L2_(ind)=1;

		if ncea_l3=1 and year=i then
			NCEA_L3_(ind)=1;

		if non_ncea_l1=1 and year=i then
			non_NCEA_L1_(ind)=1;

		if non_ncea_l2=1 and year=i then
			non_NCEA_L2_(ind)=1;

		if non_ncea_l3=1 and year=i then
			non_NCEA_L3_(ind)=1;

		if non_ncea_l4=1 and year=i then
			non_NCEA_L4_(ind)=1;
	end;
run;

Proc summary data=sec_qual2 nway;
	class snz_uid;
	var ncea_L1_&first_anal_yr-ncea_L1_&last_anal_yr ncea_L2_&first_anal_yr-ncea_L2_&last_anal_yr ncea_L3_&first_anal_yr-ncea_L3_&last_anal_yr
		non_ncea_L1_&first_anal_yr-non_ncea_L1_&last_anal_yr
		non_ncea_L2_&first_anal_yr-non_ncea_L2_&last_anal_yr
		non_ncea_L3_&first_anal_yr-non_ncea_L3_&last_anal_yr
		non_ncea_L4_&first_anal_yr-non_ncea_L4_&last_anal_yr;
	;
	output out=TEMP (drop=_type_ _freq_) max=;
run;

data project._IND_NCEA_qual_&date;
	merge &population (keep=snz_uid DOB) TEMP;
	by snz_uid;
	array ncea_L1_(*) ncea_L1_&first_anal_yr-ncea_L1_&last_anal_yr;
	array ncea_L2_(*) ncea_L2_&first_anal_yr-ncea_L2_&last_anal_yr;
	array ncea_L3_(*) ncea_L3_&first_anal_yr-ncea_L3_&last_anal_yr;
	array non_ncea_L1_(*) non_ncea_L1_&first_anal_yr-non_ncea_L1_&last_anal_yr;
	array non_ncea_L2_(*) non_ncea_L2_&first_anal_yr-non_ncea_L2_&last_anal_yr;
	array non_ncea_L3_(*) non_ncea_L3_&first_anal_yr-non_ncea_L3_&last_anal_yr;
	array non_ncea_L4_(*) non_ncea_L4_&first_anal_yr-non_ncea_L4_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);

		if ncea_L1_(ind)=. then
			ncea_L1_(ind)=0;

		if ncea_L2_(ind)=. then
			ncea_L2_(ind)=0;

		if ncea_L3_(ind)=. then
			ncea_L3_(ind)=0;

		if non_ncea_L1_(ind)=. then
			non_ncea_L1_(ind)=0;

		if non_ncea_L2_(ind)=. then
			non_ncea_L2_(ind)=0;

		if non_ncea_L3_(ind)=. then
			non_ncea_L3_(ind)=0;

		if non_ncea_L4_(ind)=. then
			non_ncea_L4_(ind)=0;
		drop i ind;
	end;
run;

%creating_clean_interv_table;

proc sort data=interventions;
	by  snz_uid interv_grp startDate;
run;

%macro interv(interv);

	data &interv;
		set interventions;

		if interv_grp="&interv";
		keep snz_uid interv_grp startDate enddate;
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

%aggregate_by_year(AlTED_OR,AlTED2, &first_anal_yr ,&last_anal_yr);
%aggregate_by_year(SUSP_OR,SUSP2, &first_anal_yr ,&last_anal_yr);
%aggregate_by_year(STAND_OR,STAND2, &first_anal_yr ,&last_anal_yr);
%aggregate_by_year(TRUA_OR,TRUA2, &first_anal_yr ,&last_anal_yr);
%aggregate_by_year(SEDU_OR,SEDU2, &first_anal_yr ,&last_anal_yr);
%aggregate_by_year(ESOL_OR,ESOL2, &first_anal_yr ,&last_anal_yr);
%aggregate_by_year(EARLEX_OR,EARLEX2, &first_anal_yr ,&last_anal_yr);
%aggregate_by_year(HOMESCH_OR,HOMESCH2, &first_anal_yr ,&last_anal_yr);
%aggregate_by_year(BOARD_OR,BOARD2, &first_anal_yr ,&last_anal_yr);
%aggregate_by_year(OTHINT_OR,OTHINT2, &first_anal_yr ,&last_anal_yr);

%macro file4agg(intervention);

	proc sql;
		create table &intervention._ as
			select 
				SNZ_uid,
				year,
				sum(days) as &intervention._da

			from &intervention.2
				group by snz_uid, year
					order by snz_uid, year;
	quit;

%mend;

%file4agg(AlTED);
%file4agg(SUSP);
%file4agg(STAND);
%file4agg(TRUA);
%file4agg(SEDU);
%file4agg(ESOL);
%file4agg(EARLEX);
%file4agg(HOMESCH);
%file4agg(BOARD);
%file4agg(OTHINT);

data project.IND_INTERVENTIONS_&date;
	merge AlTED_ SUSP_ STAND_ TRUA_ SEDU_ ESOL_ EARLEX_ HOMESCH_ BOARD_ OTHINT_;
	by snz_uid year;

	if AlTED_da=. then
		AlTED_da=0;

	if SUSP_da=. then
		SUSP_da=0;

	if STAND_da=. then
		STAND_da=0;

	if TRUA_da=. then
		TRUA_da=0;

	if SEDU_da=. then
		SEDU_da=0;

	if ESOL_da=. then
		ESOL_da=0;

	if EARLEX_da=. then
		EARLEX_da=0;

	if HOMESCH_da=. then
		HOMESCH_da=0;

	if BOARD_da=. then
		BOARD_da=0;

	if OTHINT_da=. then
		OTHINT_da=0;
run;

data _INTERVENTIONS;
	set;
	set project.IND_INTERVENTIONS_&date;
	array ALTED_da_(*) ALTED_da_&first_anal_yr-ALTED_da_&last_anal_yr;
	array SUSP_da_(*) SUSP_da_&first_anal_yr-SUSP_da_&last_anal_yr;
	array STAND_da_(*) STAND_da_&first_anal_yr-STAND_da_&last_anal_yr;
	array TRUA_da_(*) TRUA_da_&first_anal_yr-TRUA_da_&last_anal_yr;
	array SEDU_da_(*) SEDU_da_&first_anal_yr-SEDU_da_&last_anal_yr;
	array ESOL_da_(*) ESOL_da_&first_anal_yr-ESOL_da_&last_anal_yr;
	array EARLEX_da_(*) EARLEX_da_&first_anal_yr-EARLEX_da_&last_anal_yr;
	array BOARD_da_(*) BOARD_da_&first_anal_yr-BOARD_da_&last_anal_yr;
	array OTHINT_da_(*) OTHINT_da_&first_anal_yr-OTHINT_da_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);
		ALTED_da_(ind)=0;
		SUSP_da_(ind)=0;
		STAND_da_(ind)=0;
		TRUA_da_(ind)=0;
		SEDU_da_(ind)=0;
		ESOL_da_(ind)=0;
		EARLEX_da_(ind)=0;
		BOARD_da_(ind)=0;
		OTHINT_da_(ind)=0;

		if i=year then
			ALTED_da_(ind)=ALTED_da;

		if i=year then
			SUSP_da_(ind)=SUSP_da;

		if i=year then
			STAND_da_(ind)=STAND_da;

		if i=year then
			TRUA_da_(ind)=TRUA_da;

		if i=year then
			SEDU_da_(ind)=SEDU_da;

		if i=year then
			ESOL_da_(ind)=ESOL_da;

		if i=year then
			EARLEX_da_(ind)=EARLEX_da;

		if i=year then
			BOARD_da_(ind)=BOARD_da;

		if i=year then
			OTHINT_da_(ind)=OTHINT_da;
	end;
run;

proc summary data=_INTERVENTIONS nway;
	class snz_uid;
	var 
		ALTED_da_&first_anal_yr-ALTED_da_&last_anal_yr
		SUSP_da_&first_anal_yr-SUSP_da_&last_anal_yr
		STAND_da_&first_anal_yr-STAND_da_&last_anal_yr
		TRUA_da_&first_anal_yr-TRUA_da_&last_anal_yr
		SEDU_da_&first_anal_yr-SEDU_da_&last_anal_yr
		ESOL_da_&first_anal_yr-ESOL_da_&last_anal_yr
		EARLEX_da_&first_anal_yr-EARLEX_da_&last_anal_yr
		BOARD_da_&first_anal_yr-BOARD_da_&last_anal_yr
		OTHINT_da_&first_anal_yr-OTHINT_da_&last_anal_yr;
	output out=TEMP (drop=_type_ _freq_) sum=;
run;

data project._IND_INTERVENTIONS_&date;
	merge &population(keep=snz_uid DOB) TEMP;
	by snz_uid;
	array ALTED_da_(*) ALTED_da_&first_anal_yr-ALTED_da_&last_anal_yr;
	array SUSP_da_(*) SUSP_da_&first_anal_yr-SUSP_da_&last_anal_yr;
	array STAND_da_(*) STAND_da_&first_anal_yr-STAND_da_&last_anal_yr;
	array TRUA_da_(*) TRUA_da_&first_anal_yr-TRUA_da_&last_anal_yr;
	array SEDU_da_(*) SEDU_da_&first_anal_yr-SEDU_da_&last_anal_yr;
	array ESOL_da_(*) ESOL_da_&first_anal_yr-ESOL_da_&last_anal_yr;
	array EARLEX_da_(*) EARLEX_da_&first_anal_yr-EARLEX_da_&last_anal_yr;
	array BOARD_da_(*) BOARD_da_&first_anal_yr-BOARD_da_&last_anal_yr;
	array OTHINT_da_(*) OTHINT_da_&first_anal_yr-OTHINT_da_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);

		if ALTED_da_(ind)=. then
			ALTED_da_(ind)=0;

		if SUSP_da_(ind)=. then
			SUSP_da_(ind)=0;

		if STAND_da_(ind)=. then
			STAND_da_(ind)=0;

		if TRUA_da_(ind)=. then
			TRUA_da_(ind)=0;

		if SEDU_da_(ind)=. then
			SEDU_da_(ind)=0;

		if ESOL_da_(ind)=. then
			ESOL_da_(ind)=0;

		if EARLEX_da_(ind)=. then
			EARLEX_da_(ind)=0;

		if BOARD_da_(ind)=. then
			BOARD_da_(ind)=0;

		if OTHINT_da_(ind)=. then
			OTHINT_da_(ind)=0;
	end;

	drop i ind;
run;

proc sort data=enrol_2_sum;
	by snz_uid year days;
run;

data project.ind_sch_attended_&date;
	set enrol_2_sum;
	by snz_uid year days;
	sch_enrol_da=days;
	non_nqf_school=0;

	if schoolnumber in 
		(29,41,52,54,62,78,81,89,130,141,278,281,436,439,440,441,456,459,460,484,571,620,1132,1139,1605,1626,1655,2085,4152,
		37,60,67,333,387,617,1606,1640) then
		non_nqf_school=1;

	if last.year then
		output;
	keep snz_uid year schoolnumber sch_enrol_da non_nqf_school;
run;

data _school;
	set project.ind_sch_attended_&date;
	array enrol_id_(*) enrol_id_&first_anal_yr-enrol_id_&last_anal_yr;
	array school_in_(*) school_in_&first_anal_yr-school_in_&last_anal_yr;
	array enr_at_sch_in_(*) enr_at_sch_&first_anal_yr-enr_at_sch_&last_anal_yr;
	array nonnqf_sch_in_(*) nonnqf_sch_in_&first_anal_yr-nonnqf_sch_in_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);
		enrol_id_(ind)=0;
		school_in_(ind)=0;
		enr_at_sch_in_(ind)=0;
		nonnqf_sch_in_(ind)=0;

		if year=i then
			enrol_id_(ind)=1;

		if year=i then
			school_in_(ind)=schoolnumber;

		if year=i then
			enr_at_sch_in_(ind)=sch_enrol_da;

		if year=i and non_nqf_school=1 then
			nonnqf_sch_in_(ind)=1;
	end;

	drop i ind;
run;

proc sort data=_school nodupkey;
	by snz_uid year;
run;

proc summary data=_school nway;
	class snz_uid;
	var enrol_id_&first_anal_yr-enrol_id_&last_anal_yr 
		school_in_&first_anal_yr-school_in_&last_anal_yr
		enr_at_sch_&first_anal_yr-enr_at_sch_&last_anal_yr
		nonnqf_sch_in_&first_anal_yr-nonnqf_sch_in_&last_anal_yr;
	output out=TEMP (drop=_TYPE_ _FREQ_) sum=;
run;

data project._IND_sch_attended_&date;
	merge &population(keep=snz_uid DOB) TEMP;
	by snz_uid;
run;

PROC FORMAT;
	value decile
		43000='Decile1'
		43001='Decile2'
		43002='Decile3'
		43003='Decile4'
		43004='Decile5'
		43005='Decile6'
		43006='Decile7'
		43007='Decile8'
		43008='Decile9'
		43009='Decile10'
		43010='DecileNA'
		9999='DecileNA';
run;

proc format;
	value insttype
		10000	=	'Casual-Education and Care'
		10001	=	'Free Kindergarten'
		10002	=	'Playcentre'
		10003	=	'Education & Care Service'
		10004	=	'Homebased Network'
		10005	=	'Te Kohanga Reo'
		10007	=	'Licence Exempt Kohanga Reo'
		10008	=	'Hospitalbased'
		10009	=	'Playgroup'
		10010	=	'Private Training Establishment'
		10011	=	'Government Training Establishment'
		10012	=	'Polytechnic'
		10013	=	'College of Education'
		10014	=	'University'
		10015	=	'Wananga'
		10016	=	'Other Tertiary Education Provider'
		10017	=	'Industry Training Organisation'
		10018	=	'Other Certifying Authorities'
		10019	=	'OTEP Resource Centre'
		10020	=	'OTEP RS24(Completes RS24)'
		10021	=	'Government Agency'
		10022	=	'Peak Body'
		10023	=	'Full Primary (Year 1-8)'
		10024	=	'Contributing (Year 1-6)'
		10025	=	'Intermediate (Year 7 & 8)'
		10026	=	'Special School'
		10027	=	'Centre for Extra Support'
		10028	=	'Correspondence Unit'
		10029	=	'Secondary (Year 7-15)'
		10030	=	'Composite (Year 1-15)'
		10031	=	'Correspondence School'
		10032	=	'Restricted Composite (Year 7-10)'
		10033	=	'Secondary (Year 9-15)'
		10034	=	'Teen Parent Unit'
		10035	=	'Alternative Education Provider'
		10036	=	'Activity Centre'
		10037	=	'Kura Teina - Primary'
		10038	=	'Side-school'
		10039	=	'Special Unit'
		10040	=	'Kura Teina - Composite'
		10041	=	'Land Site'
		10042	=	'Manual Training Centre (stand alone)'
		10043	=	'Community Education/Resource/Youth Learning Centre'
		10044	=	'Rural Education Activities Programme (REAP)'
		10045	=	'Special Education Service Centre'
		10047	=	'Examination Centre'
		10048	=	'School cluster (for NZQA)'
		10049	=	'School Camp'
		10050	=	'Subsidiary Provider'
		10051	=	'Miscellaneous'
		10052	=	'Kindergarten Association'
		10053	=	'Playcentre Association'
		10054	=	'Commercial ECE Service Provider'
		10055	=	'Other ECE Service Provider'
		10056	=	'Board of Trustees'
		10057	=	'Private School Provider'
		10058	=	'Campus'
		10059	=	'Local Office'
		10060	=	'Special Unit Funded';
run;

proc format;
	value authority
		42000='State'
		42001='StateIntegrated'
		42002,42003='Private'
		42004,42010,42011,42012='Other'
		42005='Public Tertiary Institution'
		42006='Privately Owned Tertiary Institution'
		42007='Tertiary prov est under own Act of Parliament'
		42008='Tertiary inst owned by a Trust'
		42009='Tertiary inst owned by an Incorporated Society';
run;

proc format;
	value TLA
		9999='Cannot be determined'
		70000='Far North District'
		70001='Whangarei District'
		70002='Kaipara District'
		70003='Rodney District'
		70004='North Shore City'
		70005='Waitakere City'
		70006='Auckland City'
		70007='Manukau City'
		70008='Papakura District'
		70009='Franklin District'
		70010='Thames-Coromandel District'
		70011='Hauraki District'
		70012='Waikato District'
		70013='Matamata-Piako District'
		70014='Hamilton City'
		70015='Waipa District'
		70016='Otorohanga District'
		70017='South Waikato District'
		70018='Waitomo District'
		70019='Taupo District'
		70020='Western Bay of Plenty District'
		70021='Tauranga City'
		70022='Rotorua District'
		70023='Whakatane District'
		70024='Kawerau District'
		70025='Opotiki District'
		70026='Gisborne District'
		70027='Wairoa District'
		70028='Hastings District'
		70029='Napier City'
		70030='Central Hawkes Bay District'
		70031='New Plymouth District'
		70032='Stratford District'
		70033='South Taranaki District'
		70034='Ruapehu District'
		70035='Wanganui District'
		70036='Rangitikei District'
		70037='Manawatu District'
		70038='Palmerston North City'
		70039='Tararua District'
		70040='Horowhenua District'
		70041='Kapiti Coast District'
		70042='Porirua City'
		70043='Upper Hutt City'
		70044='Lower Hutt City'
		70045='Wellington City'
		70046='Masterton District'
		70047='Carterton District'
		70048='South Wairarapa District'
		70049='Tasman District'
		70050='Nelson City'
		70051='Marlborough District'
		70052='Kaikoura District'
		70053='Buller District'
		70054='Grey District'
		70055='Westland District'
		70056='Hurunui District'
		70057='Waimakariri District'
		70058='Christchurch City'
		70059='Selwyn District'
		70060='Ashburton District'
		70061='Timaru District'
		70062='Mackenzie District'
		70063='Waimate District'
		70064='Chatham Islands Territory'
		70065='Waitaki District'
		70066='Central Otago District'
		70067='Queenstown-Lakes District'
		70068='Dunedin City'
		70069='Clutha District'
		70070='Southland District'
		70071='Gore District'
		70072='Invercargill City'
		70073='Area Outside Territorial Authority'
		70074='Auckland';
run;

data project.schoolprofile_open_&date;
	set sandmoe.moe_school_profile;
	keep 
		SchoolNumber
		SchoolAuthorityID
		SchoolTypeID
		DecileID
		SchoolRegion
		SchoolLocalOffice
		schooltype TerritorialAuthority decile school_authority
		SchoolGender2;
	rename SchoolGender2=SchoolGender;

	if SchoolAuthorityID in (42002,42003) and DecileID in (43010,9999) then
		DecileID=43008;

	if SchoolAuthorityID in (42012) and DecileID in (43010,9999) then
		DecileID=43002;
	schooltype=put(schooltypeid,insttype.);
	decile=put(DecileID,decile.);
	school_authority=put(SchoolAuthorityID,authority.);
run;

proc freq data=project.schoolprofile_open_&date;
	tables 
		SchoolRegion
		SchoolLocalOffice
		schooltype TerritorialAuthority decile school_authority schoolgender;
	;
run;

data project.schoolprofile_closed_&date;
	set sandmoe.moe_school_profile_closed;
	keep
		schoolnumber
		SchoolAuthority
		SchoolType
		SchoolRegion
		SchoolLocalOffice
		TerritorialAuthority;
run;

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

proc sort data= project.IND_SCH_ATTENDED_&date. Out=sch_attended;
	by schoolnumber year;
run;

data project.IND_SCH_ATTENDED_DECILE_&date.;
	merge sch_attended (in=a) decile_year1(keep=schoolnumber year decilecode);
	by schoolnumber year;

	if a;

	if decilecode=. then
		decilecode=999;
	rename decilecode=hist_decile;
run;

data _school;
	set project.ind_sch_attended_decile_&date;
	array enrol_id_(*) enrol_id_&first_anal_yr-enrol_id_&last_anal_yr;
	array school_in_(*) school_in_&first_anal_yr-school_in_&last_anal_yr;
	array decile_in_(*) decile_in_&first_anal_yr-decile_in_&last_anal_yr;
	array enr_at_sch_in_(*) enr_at_sch_&first_anal_yr-enr_at_sch_&last_anal_yr;
	array nonnqf_sch_in_(*) nonnqf_sch_in_&first_anal_yr-nonnqf_sch_in_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);
		enrol_id_(ind)=0;
		school_in_(ind)=0;
		decile_in_(ind)=0;
		enr_at_sch_in_(ind)=0;
		nonnqf_sch_in_(ind)=0;

		if year=i then
			enrol_id_(ind)=1;

		if year=i then
			school_in_(ind)=schoolnumber;

		if year=i then
			decile_in_(ind)=hist_decile;

		if year=i then
			enr_at_sch_in_(ind)=sch_enrol_da;

		if year=i and non_nqf_school=1 then
			nonnqf_sch_in_(ind)=1;
	end;

	drop i ind;
run;

proc sort data=_school nodupkey;
	by snz_uid year;
run;

proc summary data=_school nway;
	class snz_uid;
	var enrol_id_&first_anal_yr-enrol_id_&last_anal_yr 
		school_in_&first_anal_yr-school_in_&last_anal_yr
		decile_in_&first_anal_yr-decile_in_&last_anal_yr
		enr_at_sch_&first_anal_yr-enr_at_sch_&last_anal_yr
		nonnqf_sch_in_&first_anal_yr-nonnqf_sch_in_&last_anal_yr;
	output out=TEMP (drop=_TYPE_ _FREQ_) sum=;
run;

data project._IND_sch_att_decile_&date;
	merge &population(keep=snz_uid DOB) TEMP;
	by snz_uid;
run;

proc datasets lib=work kill nolist memtype=data;
quit;