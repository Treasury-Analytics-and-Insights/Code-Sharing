/* Developers: Sylvia Dixon & Robert Templeton (A&I, NZ Treasury)
Date Created: July 2015 */

%let latest_ver=20150513;

libname data ODBC dsn=idi_clean_&latest_ver._srvprd schema=data;

libname moe ODBC dsn=idi_clean_&latest_ver._srvprd schema=moe_clean;
libname msd ODBC dsn=idi_clean_&latest_ver._srvprd schema=msd_clean;
libname dol ODBC dsn=idi_clean_&latest_ver._srvprd schema=dol_clean;
libname ir ODBC dsn=idi_clean_&latest_ver._srvprd schema=ir_clean;

libname sandir ODBC dsn=idi_sandpit_srvprd schema="clean_read_MOH_Health_Tracker";


%macro overlap_days(start1,end1,start2,end2,days);
spell2 is within spell1 
spell2 finishes after spell1 
spell1 is within spell2 
spell1 starts after spell2 ;
if not ((&end1.<&start2.) or (&start1.>&end2.)) then do;
   if (&start1. <= &start2.) and  (&end1. >= (&end2.)) then &days.=(&end2.-&start2.)+1;
   else if (&start1. <= &start2.) and  (&end1. <= (&end2.)) then &days.=(&end1.-&start2.)+1;
   else if (&start1. >= &start2.) and  (&end1. <= (&end2.)) then &days.=(&end1.-&start1.)+1;
   else if (&start1. >= &start2.) and  (&end1. >= (&end2.)) then &days.=(&end2.-&start1.)+1;
end;
else  &days.=0;
%mend;


%let cohort=90_91;

%let birth_criteria=%str(((snz_birth_year_nbr=1990 and snz_birth_month_nbr>6) or 
		(snz_birth_year_nbr=1991 and snz_birth_month_nbr<=6)));

%let msd_birth_criteria=%str(((birth_year_msd=1990 and birth_month_msd>6) or 
		(birth_year_msd=1991 and birth_month_msd<=6)));

%let os_year_start=2003;
%let os_year_end=2007;

%let daysos_cutoff=2*365;

%let sch_year_start=2003;
%let sch_year_end=2007;

%let tax_year_start=2005;
%let tax_year_end=2007;

%let msd_year_start=2005;
%let msd_year_end=2008;

%let health_years=2006_2007;

%let death_year=2005;

%let decision_date_cut='31Dec2007'd;


proc sql;
create table people_&cohort._1
as select snz_uid, snz_spine_ind
        , case  when snz_sex_code = '1' then 1 
	    when snz_sex_code is null then .
        else 2 end as sex
        ,snz_birth_month_nbr as birth_month
	    ,snz_birth_year_nbr as birth_year
		,snz_deceased_year_nbr as death_year
		,snz_deceased_month_nbr as death_month
  FROM data.personal_detail 
  where snz_spine_ind=1 
        and &birth_criteria.		
  order by snz_uid;
quit;

proc sql;
create table os_spells
as select snz_uid, 
   datepart(pos_applied_date) format date9.  as startdate, 
   datepart(pos_ceased_date) format date9. as enddate
from data.person_overseas_spell
where snz_uid IN 
             (SELECT DISTINCT snz_uid FROM people_90_91_1 ) 
order by snz_uid, startdate;
quit;


proc freq data=os_spells;
tables startdate enddate;
format startdate enddate year4.;
run;



data os_spells2;
set os_spells;
format windowstart windowend date9.;
windowstart=mdy(1,1,&os_year_start);
windowend=mdy(12,31,&os_year_end);
format startdate enddate date9.; 

	if year(enddate)=9999 then enddate='31Dec2015'd;
	if year(startdate)=1900 then startdate='1Jul1997'd;
run;

proc sort data=os_spells2;
by snz_uid startdate enddate;
run;

data two(keep=snz_uid daysos);
set os_spells2;
by snz_uid startdate;
retain daysos ; 
if first.snz_uid then do;
daysos=0; end;
   if startdate<windowstart and enddate>=windowstart then startdate=windowstart;
   if enddate>windowend and startdate<=windowend then enddate=windowend;
   if startdate<=windowend and enddate>=windowstart then do;
       days=enddate - startdate +1;
       daysos=daysos+days;  end;
if last.snz_uid then output;
run;

data away_2yrs;
set two;
if daysos>=&daysos_cutoff.;
run;

data people_&cohort._2;
merge people_&cohort._1(in=a) away_2yrs(in=b keep=snz_uid);
by snz_uid;
if a and not b;
run;

data school_active international;
merge moe.student_enrol(in=enrolled) data.personal_detail;
by snz_uid;
if enrolled;

if first.snz_uid then enroldays_sum=0;

retain enroldays_sum;

start_date=input(compress(moe_esi_start_date,"-"),yymmdd10.);
end_date=input(compress(moe_esi_end_date,"-"),yymmdd10.);
ExtractionDate=input(compress(moe_esi_extrtn_date,"-"),yymmdd10.);

if end_date=. then end_date=ExtractionDate;

%overlap_days(start_date,end_date,mdy(1,1,&sch_year_start.),mdy(12,31,&sch_year_end.),enroldays);

enroldays_sum+enroldays;

if last.snz_uid and enroldays_sum>0 and snz_spine_ind=1 and &birth_criteria.
and (moe_esi_domestic_status_code not in ('60004','60010','60011'))

then output school_active;

if last.snz_uid and enroldays_sum>0 and snz_spine_ind=1 and &birth_criteria.
and (moe_esi_domestic_status_code in ('60004','60010','60011'))

then output international;
run;

proc sql;
create table taxyrs as 
   select snz_uid
   ,inc_cal_yr_year_nbr as year
   ,sum(inc_cal_yr_tot_yr_amt) as amt
from data.income_cal_yr
where snz_uid in (select unique snz_uid from people_&cohort._2)
group by snz_uid, inc_cal_yr_year_nbr
order by snz_uid, inc_cal_yr_year_nbr;
quit;

proc freq data=taxyrs;
tables year /list missing;
run;

data taxyrs2;
set taxyrs;
array years(*) t1-t3;
  do i=(&tax_year_start.) to (&tax_year_end.);
  if year=i then years(i-(&tax_year_start.-1))=1;
     else years(i-(&tax_year_start.-1))=0;
  end;
run;

proc summary data=taxyrs2 nway;
class snz_uid ;
var t1-t3;
output out=stats sum=;
run;

data tax_payers;
set stats;
array years(*) t1-t3;
  do i=1 to dim(years);
  if years(i)>1 then years(i)=1;
  if years(i)=. then years(i)=0; 
  end; 
run;

proc sql;
create table taxyrsb as 
   select snz_uid
   ,inc_tax_yr_year_nbr as year
   ,sum(inc_tax_yr_tot_yr_amt) as amt
from data.income_tax_yr
where snz_uid in (select unique snz_uid from people_&cohort._2)
  and inc_tax_yr_income_source_code in ( 'C01', 'C02', 'P01', 'P02', 'S01', 'S02', 'WHP' )
group by snz_uid, inc_tax_yr_year_nbr
order by snz_uid, inc_tax_yr_year_nbr;
quit;

data taxyrs2b;
set taxyrsb;
array years(*) tb1-tb3;
  do i=(&tax_year_start.+1) to (&tax_year_end.+1);
  if year=i then years(i-(&tax_year_start.))=1;
     else years(i-(&tax_year_start.))=0;
  end;
run;

proc summary data=taxyrs2b nway;
class snz_uid ;
var tb1-tb3;
output out=taxstatsb sum=;
run;

proc means data=taxstatsb;
var tb1-tb3;
run;

data jointax(keep=snz_uid tax&tax_year_start.-tax&tax_year_end. rename=(tax&tax_year_start.-tax&tax_year_end.=t1-t3));
merge tax_payers(keep=snz_uid t1-t3) taxstatsb(keep=snz_uid tb1-tb3);
by snz_uid;
array ems(*) t1-t3;
array ann(*) tb1-tb3;
array final(*) tax&tax_year_start.-tax&tax_year_end. ;
do i=1 to dim(final);
  if ems(i)=1 or ann(i)=1 then final(i)=1;
  else final(i)=0;
  end;
run;

proc means data=jointax;
run;

proc sql;
create table msd_swn
as select snz_uid 
     ,msd_swn_sex_snz_code as sex_msd 
     ,msd_swn_birth_month_nbr as birth_month_msd
     ,msd_swn_birth_year_nbr as birth_year_msd
     ,msd_swn_ethnic_code as eth_msd
  from msd.msd_swn
  where &msd_birth_criteria.
order by snz_uid, eth_msd;
quit;

data msd_swn_onerecord;
set msd_swn;
by snz_uid ;
if last.snz_uid;
run;

proc sql;
create table tier1alsoB
as select distinct snz_uid
from msd.msd_first_tier_expenditure a
where a.snz_uid in (select distinct snz_uid from people_90_91_2)
and mdy(1,1,&msd_year_start.)<=input(compress(msd_fte_start_date,"-"),yymmdd10.)<mdy(1,1,&msd_year_end.)
order by snz_uid;
quit;

proc sql;
create table tier1also
as select distinct snz_uid
from msd.msd_first_tier_expenditure a
where a.snz_uid in (select distinct snz_uid from msd_swn_onerecord)
and mdy(1,1,&msd_year_start.)<=input(compress(msd_fte_start_date,"-"),yymmdd10.)<mdy(1,1,&msd_year_end.)
order by snz_uid;
quit;

proc sort data=msd.msd_child(drop=snz_uid) out=bdd_child_spells(rename=(child_snz_uid=snz_uid));
by child_snz_uid;
run;

data bdd_child_spells_pers;
merge bdd_child_spells(in=child_bdd) data.personal_detail;
by snz_uid;

if first.snz_uid then bdd_days_sum=0;
retain bdd_days_sum;

if not child_bdd then delete;

start_date=input(compress(msd_chld_child_from_date,"-"),yymmdd10.);
end_date=input(compress(msd_chld_child_to_date,"-"),yymmdd10.);

if end_date=. then end_date=mdy(1,1,2014);

%overlap_days(start_date,end_date,mdy(1,1,&tax_year_start.),mdy(12,31,&tax_year_end.),bdd_days);

bdd_days_sum+bdd_days;

if last.snz_uid and bdd_days_sum>0  and &birth_criteria. then output;
run;

proc univariate data=bdd_child_spells_pers;
var bdd_days_sum;
run;

proc sql;
    connect to sqlservr (server=WPRDSQL36\ileed database=idi_clean_&latest_ver.);
    create table health_id_link as
    select * from connection to sqlservr
    (select snz_uid     ,     snz_moh_uid
     from security.concordance
    );
quit;

proc sort data=health_id_link;
by snz_uid;
run;

data healthpop(keep=snz_moh_uid);
set sandir.moh_Health_Tracker_pop_201503;
if pop&health_years.='1' then output;
run;

data people_health_id;
merge people_&cohort._2(in=inpop) health_id_link;
by snz_uid;
if inpop and snz_moh_uid^=. ;
run;

proc sort data=people_health_id;
by snz_moh_uid;
proc sort data=healthpop;
by snz_moh_uid;

data active_health;
merge healthpop(in=inhealth) people_health_id(in=in_snzid);
by snz_moh_uid;
if inhealth and in_snzid;
run;

proc sort data=active_health;
by snz_uid;
run;

data active_ids;
merge school_active (in=in_enrol keep=snz_uid)
bdd_child_spells_pers(in=bdd_child keep=snz_uid)
tier1alsob(in=bdd_adult keep=snz_uid)  
tax_payers(in=taxpaid keep=snz_uid)
active_health(IN=INHEALTH) ;
by snz_uid;
if in_enrol then ed=1; else ed=0;
if bdd_child then welfare_child=1; else welfare_child=0;
if bdd_adult then welfare_adult=1; else welfare_adult=0;
if taxpaid then tax=1; else tax=0;
if inhealth then health=1; else health=0;
run;

data active_ids;
set active_ids;
by snz_uid;
if first.snz_uid;
run;

data people_&cohort._3;
merge people_&cohort._2(in=ever_in_nz) active_ids(in=active keep=snz_uid) ;
by snz_uid;
if ever_in_nz and active;
run;

proc SQL; 
Connect to sqlservr (server=WPRDSQL36\iLeed database=IDI_clean_&latest_ver.);
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

proc sql;
create table links
as select a.snz_uid
   ,b.snz_ird_uid
   ,b.snz_moe_uid 
   ,b.snz_dol_uid 
   ,b.snz_msd_uid
   ,b.snz_moh_uid
   ,b.snz_dia_uid 
from people_&cohort._3 a
left join conc b
on a.snz_uid=b.snz_uid
order by a.snz_uid;
quit;

data links2;
set links;
if snz_msd_uid>0 then msdlink=1;
if snz_moe_uid>0 then moelink=1;
if snz_moh_uid>0 then mohlink=1;
if snz_ird_uid>0 then irdlink=1;
if snz_dia_uid>0 then dialink=1;
run;

data people_&cohort._3b;
merge people_&cohort._3(in=a) links2(keep=snz_uid irdlink moelink);
by snz_uid;
if a and irdlink=1 and moelink=1;
run;

proc freq data=people_&cohort._3b;
tables irdlink mohlink moelink msdlink dialink /list missing;
run;

proc sql;
create table births
as select a.snz_uid
      ,case when dia_bir_sex_snz_code='1' then 1 when dia_bir_sex_snz_code='2' then 2 else . end as dia_sex
      ,dia_bir_birth_month_nbr as dia_birth_month
      ,dia_bir_birth_year_nbr as dia_birth_year
from people_&cohort._3b a
join dia.births b
on a.snz_uid=b.snz_uid
where dia_bir_still_birth_code is null 
order by snz_uid;
quit;

data birth;
set births;
by snz_uid;
if first.snz_uid;
run;

proc sql;
create table residentsB
as select snz_uid
  ,snz_dol_uid
  ,input(dol_dec_decision_date,yymmdd10.) format date9.  as decision_date
  ,dol_dec_application_type_code as app_code
  ,dol_dec_application_stream_text as stream 
  ,dol_dec_nationality_code as nationality
      ,dol_dec_birth_month_nbr as dol_birth_month
      ,dol_dec_birth_year_nbr as dol_birth_year
      ,case when dol_dec_sex_snz_code='1' then 1 when dol_dec_sex_snz_code='2' then 2 else . end as dol_sex
from dol.decisions
     where dol_dec_decision_type_code='A' /*Approval*/
     and dol_dec_application_type_code in ('16', '17', '18')
	 and input(dol_dec_decision_date,yymmdd10.)<=&decision_date_cut.
   and snz_uid in (select distinct snz_uid from people_&cohort._3b)
 order by snz_uid,  input(dol_dec_decision_date,yymmdd10.);
 quit;

data resB;
set residentsB;
by snz_uid decision_date;
if first.snz_uid;
run;

proc sql;
create table nonresidents
as select snz_uid
  ,snz_dol_uid
  ,input(dol_dec_decision_date,yymmdd10.) format date9.  as decision_date
  ,dol_dec_application_type_code as app_code
  ,dol_dec_application_stream_text as stream 
  ,dol_dec_nationality_code as nationality
      ,dol_dec_birth_month_nbr as dol_birth_month
      ,dol_dec_birth_year_nbr as dol_birth_year
      ,case when dol_dec_sex_snz_code='1' then 1 when dol_dec_sex_snz_code='2' then 2 else . end as dol_sex
from dol.decisions
     where dol_dec_decision_type_code='A' /*Approval*/
	 and input(dol_dec_decision_date,yymmdd10.)<='31Dec2014'd
     and dol_dec_application_type_code in ('11', '12', '13', '14', '19', '20', '21', '22')
	 and snz_uid in (select distinct snz_uid from people_&cohort._3b)
 order by snz_uid,  input(dol_dec_decision_date,yymmdd10.);
 quit;

data nonres;
set nonresidents;
by snz_uid decision_date;
if first.snz_uid;
run;

data alltemp;
merge international(in=z keep=snz_uid) nonres(in=y keep=snz_uid);
by snz_uid;
run;

data alltempb;
merge alltemp(in=z keep=snz_uid) birth(in=b keep=snz_uid) resB(in=c keep=snz_uid)  ;
by snz_uid;
if z and not b and not c;
run;

proc contents data=people_&cohort._3b;
run;

proc means data=people_&cohort._3b;
var birth_month birth_year;
run;

data people_&cohort._4;
merge people_&cohort._3b(in=a) alltempb(in=b keep=snz_uid);
by snz_uid;
if a and not b;
dob=input('15'||put(birth_month,z2.)||put(birth_year,z4.),ddmmyy8.);
if death_year~=. then do;
  dod=input('15'||put(death_month,z2.)||put(death_year,z4.),ddmmyy8.);
  end;
bday_23=intnx('YEAR',dob,23,'S'); 
if death_year~=. and dod<bday_23 then flag_died_by_23rd_birthday=1;
  else flag_died_by_23rd_birthday=0;
if death_year=. or death_year>=&death_year.;
run;

proc freq data=people_&cohort._4;
tables death_year flag_died_by_23rd_birthday/missing;
run;

