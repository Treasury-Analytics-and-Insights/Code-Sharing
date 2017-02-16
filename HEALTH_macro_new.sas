%Macro Create_MH_ind_pop;
*PRIMHD;
proc sql;
	create table TEMP_prmhd as
	select 
		 a.snz_uid
		,a.DOB
		,input(compress(b.moh_mhd_activity_start_date,"-"),yymmdd8.) format date9. as date
		,b.moh_mhd_activity_type_code as activity_type_code
		,b.moh_mhd_team_code as team_code
		,c.team_type
		,(case when activity_type_code in ('T09') then 'Psychotic' 
when (activity_type_code in ('T16','T17','T18','T19','T20') or b.moh_mhd_team_code in ('03','10','11','21','23')) then 'Substance_use'
when b.moh_mhd_team_code = '16' then 'Eating disorder'
when activity_type_code not in ('T09','T16','T17','T18','T19','T20') then 'Any_MH_disorder' end ) as indicator format $20.
	from &population a 
		inner join MOH.PRIMHD b
			on a.snz_uid = b.snz_uid
		left join sandmoh2.moh_PRIMHD_team_lookup c
			on b.moh_mhd_team_code=c.team_code
	where a.snz_uid>0
	order by snz_uid
	;
quit;

* NMDS Data ;

proc sql;
   create table TEMP_events
   as select 
      a.snz_uid
      ,a.moh_evt_event_id_nbr as event_id
      ,input(compress(a.moh_evt_evst_date,"-"),yymmdd8.) format date9. as date
	  ,b.DOB
   FROM moh.pub_fund_hosp_discharges_event a inner join &population. b
   on a.snz_uid=b.snz_uid;
quit;

proc sql;
create table TEMP_diag
as select distinct
a.moh_dia_event_id_nbr
,a.moh_dia_clinical_sys_code
,a.moh_dia_clinical_code as code
,b.DOB
from moh.pub_fund_hosp_discharges_diag a inner join TEMP_events b
on a.moh_dia_event_id_nbr=b.event_id and 
a.moh_dia_submitted_system_code = a.moh_dia_clinical_sys_code ;

data TEMP_diag; set TEMP_diag;
format indicator $20.;
*** Hosps 1999 to 2014 coding ***;

   * ADHD ;
   if substr(code,1,4) = 'F900' then  indicator = 'ADHD'; 

   * Anxiety disorders ;
   if 'F40' <= substr(code,1,3) <= 'F48' then  indicator = 'Anxiety'; 

   * Autism spectrum ;
   if substr(code,1,3) = 'F84' then  indicator = 'Autism'; 

   * Dementia ;
   if 'F00' <= substr(code,1,3) <= 'F03' then  indicator = 'Dementia'; 

   * Eating disorders ;
   if substr(code,1,3) = 'F50' then  indicator = 'Eating';

   * Gender Identity ;
   if substr(code,1,4) in ('F640','F642','F648','F649') then  indicator = 'Gender_identity'; 

   * Mood disorders ;
   if 'F30' <= substr(code,1,3) <= 'F39' then  indicator = 'Mood'; 

   * Other MH disorders ;
   if 'F04' <= substr(code,1,3) <= 'F09' then  indicator = 'Other'; 
   if 'F51' <= substr(code,1,3) <= 'F53' then  indicator = 'Other'; 
   if substr(code,1,3) in ('F59', 'F63','F68','F69','F99') then  indicator = 'Other'; 
   if substr(code,1,4) in ('F930','F931','F932') then  indicator = 'Other'; 

   * Personality disorders ;
   if 'F60' <= substr(code,1,3) <= 'F62' then  indicator = 'Personality'; 

   * Psychotic disorders ;
   if 'F20' <= substr(code,1,3) <= 'F29' then  indicator = 'Psychotic'; 

   * Substance use ;
   if 'F10' <= substr(code,1,3) <= 'F16' then  indicator = 'Substance_use'; 
   if 'F18' <= substr(code,1,3) <= 'F19' then  indicator = 'Substance_use'; 
   if substr(code,1,3) in ('F55') then  indicator = 'Substance_use'; 

   *** Hosps 1988 to 1999 ***;

   * ADHD ;
   if substr(code,1,5) in ('31400','31401') then  indicator = 'ADHD';  

   * Anxiety disorders ;
   if '30000' <= substr(code,1,5) <= '30015' then  indicator = 'Anxiety'; 
   if substr(code,1,4) in ('3002','3003','3099') then  indicator = 'Anxiety'; 
   if '3005' <= substr(code,1,4) <= '3009' then  indicator = 'Anxiety'; 
   if '3060' <= substr(code,1,4) <= '3064' then  indicator = 'Anxiety'; 
   if substr(code,1,5) in ('30650','30652','30653','30659','30780','30789','30989') then  indicator = 'Anxiety'; 
   if '3066' <= substr(code,1,4) <= '3069' then  indicator = 'Anxiety'; 
   if '3080' <= substr(code,1,4) <= '3091' then  indicator = 'Anxiety'; 
   if '30922' <= substr(code,1,4) <= '30982' then  indicator = 'Anxiety'; 

   * Autism spectrum ;
   if substr(code,1,5) in ('29900','29901','29910') then  indicator = 'Autism'; 

   * Dementia ;
   if substr(code,1,3) = '290' then  indicator = 'Dementia'; 
   if substr(code,1,4) = '2941' then  indicator = 'Dementia'; 

   * Eating disorders ;
   if substr(code,1,4) = '3071' then  indicator = 'Eating'; 
   if substr(code,1,5) in ('30750','30751','30754','30759') then  indicator = 'Eating'; 

   * Gender Identity ;
   if substr(code,1,4) = '3026' then  indicator = 'Gender_identity'; 
   if substr(code,1,5) in ('30250','30251','30252','30253','30285') then  indicator = 'Gender_identity'; 

   * Mood disorders ;
   if substr(code,1,3) in ('296','311') then  indicator = 'Mood'; 
   if substr(code,1,4) = '3004' then  indicator = 'Mood'; 
   if substr(code,1,5) = '30113' then  indicator = 'Mood'; 

   * Other MH disorders ;
   if '2930' <= substr(code,1,4) <= '2940' then  indicator = 'Other'; 
   if substr(code,1,4) in ('2948','2949','3027','3074','3123','3130','3131') then  indicator = 'Other'; 
   if '29911' <= substr(code,1,5) <= '29991' then  indicator = 'Other'; 
   if substr(code,1,5) in ('30016','30019','30151','30651','30921') then  indicator = 'Other'; 
   if substr(code,1,3) in ('310') then  indicator = 'Other'; 

   * Personality disorders ;
   if substr(code,1,4) = '3010' then  indicator = 'Personality'; 
   if substr(code,1,5) in ('30110','30111','30112','30159') then  indicator = 'Personality'; 
   if '30120' <= substr(code,1,5) <= '30150' then  indicator = 'Personality'; 
   if '3016' <= substr(code,1,4) <= '3019' then  indicator = 'Personality'; 

   * Psychotic disorders ;
   if '2950' <= substr(code,1,4) <= '2959' then  indicator = 'Psychotic'; 
   if '2970' <= substr(code,1,4) <= '2989' then  indicator = 'Psychotic'; 

   * Substance use ;
   if substr(code,1,3) in ('291','292') then  indicator = 'Substance_use'; 
   if '3030' <= substr(code,1,4) <= '3050' then  indicator = 'Substance_use'; 
   if '3052' <= substr(code,1,4) <= '3059' then  indicator = 'Substance_use'; 
   if indicator ne '';
 run;

proc sort data=TEMP_diag (keep=moh_dia_event_id_nbr DOB indicator) nodupkey ; by moh_dia_event_id_nbr; run;

proc sql;
   create table TEMP_HospEvents
   as select 
      a.*
	  ,b.indicator
	  ,'NMDS_diag' as source

   FROM TEMP_events a 
   inner join TEMP_diag b on
   a.event_id=b.moh_dia_event_id_nbr
WHERE a.snz_uid in (select distinct snz_uid from &population);
quit;

* PHARMS Data ;

proc sql;
	create table TEMP_pharms as
	select 
		 a.snz_uid
		 ,a.DOB
	    ,input(compress(b.moh_pha_dispensed_date,"-"),yymmdd8.) format date9. as date
		,b.moh_pha_dim_form_pack_code
		,put(c.chemical_id,4.) as code 
	from &population a
		inner join moh.pharmaceutical b
			on a.snz_uid = b.snz_uid
		inner join sandmoh5.moh_pharmaceutical_lookup_table c
			on input(b.moh_pha_dim_form_pack_code,8.) = c.dim_form_pack_subsidy_key
	where c.chemical_id in (3887,1809,3880,
							1166,6006,1780,
							3750,3923,
							1069,1193,1437,1438,1642,2466,3753,1824,1125,2285,1955,2301,3901,
							1080,1729,1731,2295,2484,
							3884,3878,1078,1532,2820,1732,1990,1994,2255,2260,
							2367,1432,3793,
							2632,1315,3926,2636,1533,1535,1760,2638,1140,1911,6009,1950,1183,1011,3927,1030,1180,3785,3873
							) and a.snz_uid>0
	order by snz_uid;
quit;

data TEMP_pharms ;
set TEMP_pharms (keep=snz_uid DOB  date code);
source = 'Pharms'; 
format indicator $20.;

	/* ADHD */
if code in ('3887','1809','3880') then indicator = 'ADHD';

	/* Anxiety disorders */
if code in ('1166','6006','1780') then indicator = 'Anxiety';

	/* Dementia */
if code in ('3750','3923')then indicator = 'Dementia';

	/* Mood disorders */
if code in ('1069','1437','1438','2466','3753','1824','1125','2285','1955','2301','3901') then indicator = 'Mood';

	/* Citalopram */
	/* flag this separately and then apply at end as Moodanx indicator if no dementia indicator */
if code in ('1193') then indicator = 'Citalopram';

	/* Other MH disorders */
if code in ('1080','1729','1731','2295','2484') then indicator = 'Other';

	/* Psychotic disorder */
if code in ('3884','3878','1078','1532','2820','1732','1990','1994','2255','2260') then indicator = 'Psychotic';

	/* Substance use */
if code in ('2367','1432','3793') then indicator = 'Substance_use';

	/* Combined Mood and Anxiety */
if code in ('2632','3926','1760','2638','3927','1030','1180','3785')then indicator = 'Mood_anxiety';

	/* General MH */
if code in ('1315','2636','1533','1535','1140','1911','6009','1950','1183','1011','3873','1642')then indicator = 'Any_MH_disorder';
if indicator ne '';

keep snz_uid DOB date indicator source code;
run;

* LABS Data;
proc sql;
   create table TEMP_lab
   as select 
      a.snz_uid
	  ,b.DOB
      ,a.moh_lab_test_code as test_code
      ,input(compress(a.moh_lab_visit_date,"-"),yymmdd8.) format date9. as date

   FROM moh.lab_claims a inner join &population b 
   on a.snz_uid=b.snz_uid
   WHERE  test_code = 'BM2'
   order by snz_uid, date;
quit;

data TEMP_lab (keep=date snz_uid DOB episode weight);
   set TEMP_lab(keep=snz_uid DOB date);
   by snz_uid;
   retain episode;
   weight = 1;
   lagdate = lag(date);
   format lagdate ddmmyy10.;
   if first.snz_uid then episode = 1;
   else do;
      if date < (lagdate + 120) then episode = episode;
      else episode = episode + 1;
   end;
run;

proc sort data = TEMP_lab; by snz_uid episode; run;

proc summary data = TEMP_lab(keep=snz_uid DOB episode weight);
   by snz_uid episode;
   var weight;
   output out = TEMP_lab_summary (drop = _freq_ _type_) 
   sum = tests;
run;

data TEMP_lab_summary(keep=snz_uid episode tests);
   set TEMP_lab_summary(keep=snz_uid episode tests);
   where tests > 2;
run;

proc sql;
   create table TEMP_Labs_MentalHealth as
   select 	a.snz_uid ,
		a.episode as episode_1,
		b.*
   from TEMP_lab_summary a left join TEMP_lab b 
		on
      a.snz_uid = b.snz_uid and
      a.episode = b.episode
      where a.snz_uid is not null;
quit;



data  TEMP_Labs_MentalHealth;
   set  TEMP_Labs_MentalHealth(keep=snz_uid DOB date);
   source = 'Labs'; 
   indicator = "Mood";
   code = "BM2";
   keep snz_uid DOB date source indicator code ;
run;

* Combine;

data TEMP_all;
   length indicator $20;
   set 
       TEMP_HospEvents (keep=snz_uid DOB date indicator)
       TEMP_pharms            (keep=snz_uid DOB  date indicator)
       TEMP_prmhd         (keep=snz_uid DOB date indicator)
       TEMP_Labs_MentalHealth      (keep=snz_uid DOB date indicator);
run;


data TEMP_ALL;
set TEMP_ALL;
array subs_use_ (*) subs_use_&first_anal_yr-subs_use_&last_anal_yr;
array oth_mh_ (*) oth_mh_&first_anal_yr-oth_mh_&last_anal_yr;
array any_mh_ (*) any_mh_&first_anal_yr-any_mh_&last_anal_yr;

array subs_use_at_age_(*) subs_use_at_age_&firstage-subs_use_at_age_&lastage;
array oth_mh_at_age_(*) oth_mh_at_age_&firstage-oth_mh_at_age_&lastage;
array any_mh_at_age_(*) any_mh_at_age_&firstage-any_mh_at_age_&lastage;

	do ind=&firstage. to &lastage.;
		i=ind-(&firstage.-1);

		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S');
			subs_use_at_age_(i)=0;
			oth_mh_at_age_(i)=0;
			any_mh_at_age_(i)=0;
		if ((date < end_window) and (date >= start_window)) then do;
			if indicator = 'Substance_use' then subs_use_at_age_(i) = 1;
			if indicator not in ('Substance_use') then oth_mh_at_age_(i) = 1;
			any_mh_at_age_(i) = 1;
		end;
	end;

		do ind=&first_anal_yr. to &last_anal_yr.;
		i=ind-(&first_anal_yr.-1);
			subs_use_(i)=0;
			oth_mh_(i)=0;
			any_mh_(i)=0;

		start_window=intnx('YEAR',MDY(1,1,&first_anal_yr.),i-1,'S');
		end_window=intnx('YEAR',MDY(1,1,&first_anal_yr.),i,'S');

		if ((date < end_window) and (date >= start_window)) then do;
			if indicator = 'Substance_use' then subs_use_(i) = 1;
			if indicator not in ('Substance_use') then oth_mh_(i) = 1;
			any_mh_(i) = 1;
		end;
	end;
run;

proc summary data=TEMP_ALL nway;
	class snz_uid DOB;
	var subs_use_at_age_&firstage.-subs_use_at_age_&lastage.
		oth_mh_at_age_&firstage.-oth_mh_at_age_&lastage.
		any_mh_at_age_&firstage.-any_mh_at_age_&lastage.;
	output out=&projectlib.._ind_mhealth_at_age_&date (drop=_type_ _freq_) max=;
run;

proc summary data=TEMP_ALL nway;
	class snz_uid DOB;
	var subs_use_&first_anal_yr.-subs_use_&last_anal_yr.
		oth_mh_&first_anal_yr.-oth_mh_&last_anal_yr.
		any_mh_&first_anal_yr.-any_mh_&last_anal_yr.;
	output out=&projectlib.._ind_mhealth_&date (drop=_type_ _freq_) max=;
run;

proc datasets lib=work;
delete temp:;
run;
%mend;


******;
%Macro Create_MH_PRIM_ind_pop;
*PRIMHD;
proc sql;
	create table TEMP_prmhd as
	select 
		 a.snz_uid
		,a.DOB
		,input(compress(b.moh_mhd_activity_start_date,"-"),yymmdd8.) format date9. as date
		,b.moh_mhd_activity_type_code as activity_type_code
		,b.moh_mhd_team_code as team_code
		,c.team_type
		,(case when activity_type_code in ('T09') then 'Psychotic' 
when (activity_type_code in ('T16','T17','T18','T19','T20') or b.moh_mhd_team_code in ('03','10','11','21','23')) then 'Substance_use'
when b.moh_mhd_team_code = '16' then 'Eating disorder'
when activity_type_code not in ('T09','T16','T17','T18','T19','T20') then 'Any_MH_disorder' end ) as indicator format $20.
	from &population a 
		inner join MOH.PRIMHD b
			on a.snz_uid = b.snz_uid
		left join sandmoh2.moh_PRIMHD_team_lookup c
			on b.moh_mhd_team_code=c.team_code
	where a.snz_uid>0
	order by snz_uid
	;
quit;


* Combine;

data TEMP_all;
   length indicator $20;
   set 

       TEMP_prmhd         (keep=snz_uid DOB date indicator);
run;


data TEMP_ALL;
set TEMP_ALL;
array subs_use_ (*) subs_use_&first_anal_yr-subs_use_&last_anal_yr;
array oth_mh_ (*) oth_mh_&first_anal_yr-oth_mh_&last_anal_yr;
array any_mh_ (*) any_mh_&first_anal_yr-any_mh_&last_anal_yr;

array subs_use_at_age_(*) subs_use_at_age_&firstage-subs_use_at_age_&lastage;
array oth_mh_at_age_(*) oth_mh_at_age_&firstage-oth_mh_at_age_&lastage;
array any_mh_at_age_(*) any_mh_at_age_&firstage-any_mh_at_age_&lastage;

	do ind=&firstage. to &lastage.;
		i=ind-(&firstage.-1);

		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S');
			subs_use_at_age_(i)=0;
			oth_mh_at_age_(i)=0;
			any_mh_at_age_(i)=0;
		if ((date < end_window) and (date >= start_window)) then do;
			if indicator = 'Substance_use' then subs_use_at_age_(i) = 1;
			if indicator not in ('Substance_use') then oth_mh_at_age_(i) = 1;
			any_mh_at_age_(i) = 1;
		end;
	end;

		do ind=&first_anal_yr. to &last_anal_yr.;
		i=ind-(&first_anal_yr.-1);
			subs_use_(i)=0;
			oth_mh_(i)=0;
			any_mh_(i)=0;

		start_window=intnx('YEAR',MDY(1,1,&first_anal_yr.),i-1,'S');
		end_window=intnx('YEAR',MDY(1,1,&first_anal_yr.),i,'S');

		if ((date < end_window) and (date >= start_window)) then do;
			if indicator = 'Substance_use' then subs_use_(i) = 1;
			if indicator not in ('Substance_use') then oth_mh_(i) = 1;
			any_mh_(i) = 1;
		end;
	end;
run;

proc summary data=TEMP_ALL nway;
	class snz_uid DOB;
	var subs_use_at_age_&firstage-subs_use_at_age_&lastage
		oth_mh_at_age_&firstage-oth_mh_at_age_&lastage
		any_mh_at_age_&firstage-any_mh_at_age_&lastage;
	output out=&projectlib.._ind_mh_prim_at_age_&date (drop=_type_ _freq_) max=;
run;

proc summary data=TEMP_ALL nway;
	class snz_uid DOB;
	var subs_use_&first_anal_yr.-subs_use_&last_anal_yr.
		oth_mh_&first_anal_yr.-oth_mh_&last_anal_yr.
		any_mh_&first_anal_yr.-any_mh_&last_anal_yr.;
	output out=&projectlib.._ind_mh_prim_&date (drop=_type_ _freq_) max=;
run;

proc datasets lib=work;
delete temp:;
run;
%mend;