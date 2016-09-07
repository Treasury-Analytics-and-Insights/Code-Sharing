/************************************************************************************************************************************
*************************************************************************************************************************************
Developer: Sarah Tumen and Robert Templeton ( A&I, NZ Treasury)
Date created: 7 Dec 2015


This code creates MSD days spent in a year on main benefit as an adult.


*/
%let population=project.Population1988_2013;

%creating_MSD_SPEL;

data msd_spel;
	set msd_spel;
	spell=msd_spel_spell_nbr;
	keep snz_uid spell servf spellfrom spellto ben ben_new;
run;

proc sort data=msd_spel out=mainbenefits(rename=(spellfrom=startdate spellto=enddate));
	by snz_uid spell spellfrom spellto;
run;

data icd_bdd_ptnr;
	set msd.msd_partner;
	format ptnrfrom ptnrto date9.;
	spell=msd_ptnr_spell_nbr;
	ptnrfrom=input(compress(msd_ptnr_ptnr_from_date,"-"), yymmdd10.);
	ptnrto=input(compress(msd_ptnr_ptnr_to_date,"-"), yymmdd10.);

	if ptnrfrom>"&sensor"d then
		delete;

	if ptnrto=. then
		ptnrto="&sensor"d;

	if ptnrto>"&sensor"d then
		ptnrto="&sensor"d;
	keep snz_uid partner_snz_uid spell ptnrfrom ptnrto;
run;

proc sql;
	create table prim_mainben_prim_data as
		select
			s.snz_uid, s.spellfrom as startdate, s.spellto as enddate, s.ben, s.ben_new, s.spell,
			t.DOB
		from
			msd_spel  s inner join &population t
			on t.snz_uid= s.snz_uid;
run;

proc sql;
	create table prim_mainben_part_data as
		select
			s.partner_snz_uid, s.ptnrfrom as startdate, s.ptnrto as enddate,s.spell,
			s.snz_uid as main_snz_uid,
			t.DOB
		from  icd_bdd_ptnr  s inner join &population t
			on t.snz_uid = s.partner_snz_uid
		order by s.snz_uid, s.spell;
quit;

proc sort data=mainbenefits out=main nodupkey;
	by snz_uid spell startdate enddate;
run;

proc sort data=prim_mainben_part_data out=partner(rename=(main_snz_uid=snz_uid)) nodupkey;
	by main_snz_uid spell startdate enddate;
run;

data fullymatched  unmatched(drop=ben ben_new servf);
	merge partner (in = a)
		main (in = b);
	by snz_uid spell startdate enddate;

	if a and b then
		output fullymatched;
	else if a and not b then
		output unmatched;
run;

proc sql;
	create table partlymatched as
		select a.partner_snz_uid, a.snz_uid, a.spell, a.dob, a.startdate, a.enddate,
			b.ben, b.ben_new, b.servf
		from unmatched a left join main b
			on a.snz_uid=b.snz_uid and a.spell=b.spell and a.startdate>=b.startdate and a.enddate<=b.enddate;
quit;

run;

data prim_mainben_part_data_2;
	set fullymatched partlymatched;
run;

proc freq data=prim_mainben_part_data_2;
	tables ben_new ben;
run;

data prim_bennzs_data_1 del;
	set prim_mainben_prim_data (in=a)
		prim_mainben_part_data_2 (in=b);

	if b then
		snz_uid=partner_snz_uid;

	if startdate<DOB then
		output del;
	else output prim_bennzs_data_1;
run;

proc sort data = prim_bennzs_data_1;
	by snz_uid startdate enddate;
run;

%overlap(prim_bennzs_data_1,examine=F);

%aggregate_by_year(prim_bennzs_data_1_OR,prim_bennzs_data_2,&first_anal_yr,&last_anal_yr,gross_daily_amt=);

%create_BDD_wide(infile=prim_bennzs_data_2, outfile=project.IND_BDD_adult_&date.);

%create_BDD_long(merge1=project.IND_BDD_adult_&date., merge2=cohort_1, outfile=project._IND_BDD_adult_&date.);

%create_BDD_at_age(infile=prim_bennzs_data_1_OR, outfile=project._IND_BDD_adult_at_age_&date);

proc datasets lib=work kill nolist memtype=data;
quit;