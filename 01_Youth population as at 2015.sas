

************************************************************************************************************************************;
********SELECT RESIDENT YOUTH POPULATION IN 2015 = AGES 0-24 AT 31 DECEMBER 2015 AND RESIDENT FOR AT LEAST 6 MONTHS IN 2015********;

*****population used for youth funding review;
****Program by SD 21 July 2015;
***********************************************************************************************************;

* Run 00_code.sas code to get macros and libraries; 



********************************************************************;
**1. Select PEOPLE ON SPINE WITH RELEVANT BIRTHDATES and find which IDI datasets they have been linked to;
********************************************************************;

proc sql;
create table spine
as select snz_uid
   ,snz_birth_month_nbr as birth_month
   ,snz_birth_year_nbr as birth_year
   ,case when snz_sex_code='1' then 1 when snz_sex_code='2' then 2 else . end as snz_sex
   ,snz_spine_ind as spine
   ,snz_deceased_year_nbr as death_year
   ,snz_deceased_month_nbr as death_month
from data.personal_detail 
  where snz_person_ind=1 and snz_spine_ind=1
  and (birth_year>=1989 and birth_year<=2015)
  and (death_year is null or death_year>=2015)
order by snz_uid;
quit;


proc freq data=spine;
tables birth_year death_year  /list missing;
run;

**find out if person also has IRD, MoE and MoH links;
**we can't require IRD links for children under 18 or so;
**But we might want to discard some of these people later;

proc SQL; 
Connect to sqlservr (server=WPRDSQL36\iLeed database=IDI_clean);
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
create table spine2
as select a.*
   ,b.snz_ird_uid
   ,b.snz_moe_uid 
   ,b.snz_dol_uid 
   ,b.snz_msd_uid
   ,b.snz_moh_uid
   ,b.snz_dia_uid 
from spine a
left join conc b
on a.snz_uid=b.snz_uid
order by a.snz_uid;
quit;

data spine3;
set spine2;
if snz_msd_uid>0 then msdlink=1;
if snz_moe_uid>0 then moelink=1;
if snz_moh_uid>0 then mohlink=1;
if snz_ird_uid>0 then irdlink=1;
if snz_dia_uid>0 then dialink=1;
dob=input('15'||put(birth_month,z2.)||put(birth_year,z4.),ddmmyy8.);
if death_year~=. then do;
  dod=input('15'||put(death_month,z2.)||put(death_year,z4.),ddmmyy8.);
  end;
format dob dod date9.;
ageat31Dec2015=floor((intck('month',dob,'31Dec2015'd)- (day('31Dec2015'd) < day(dob))) / 12);
run;


*************************************************;
**2. DIA birthplace records, perm and temp residence records; 
**Here visa status is defined as at the end of 2013;
**************************************************;

proc sql;
create table births
as select a.*
      ,case when dia_bir_sex_snz_code='1' then 1 when dia_bir_sex_snz_code='2' then 2 else . end as dia_sex
      ,dia_bir_birth_month_nbr as dia_birth_month
      ,dia_bir_birth_year_nbr as dia_birth_year
from spine3 a
join dia.births b
on a.snz_uid=b.snz_uid
where dia_bir_still_birth_code is null and dia_birth_year is not null
order by snz_uid;
quit;

proc print data=births(obs=100);
run;
proc means data=births;
var dia_birth_year;
run;


**Became Perm residents by end Dec 2012 = 1 year before reference year;

proc sql;
create table residents
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
	 and input(dol_dec_decision_date,yymmdd10.)<='31Dec2015'd
	 and snz_uid in (select distinct snz_uid from spine3)
 order by snz_uid,  input(dol_dec_decision_date,yymmdd10.);
 quit;

data res;
set residents;
by snz_uid decision_date;
if first.snz_uid;
run;


**Temp residents;
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
	 and input(dol_dec_decision_date,yymmdd10.)<='31Dec2015'd
     and dol_dec_application_type_code in ('11', '12', '13', '14', '19', '20', '21', '22')
	 and snz_uid in (select distinct snz_uid from spine3)
 order by snz_uid,  input(dol_dec_decision_date,yymmdd10.);
 quit;

 **two thirds of these visas are student, 22% work, 13% visitors;
 
proc freq data=nonresidents;
tables stream /list missing;
run;

data nonres;
set nonresidents;
by snz_uid decision_date;
if first.snz_uid;
run;

**Create visa status indicators;
**NOTE the three populations overlap even though people who were born in NZ shouldn't have to apply for perm or temp visas;
**In reality some do;

data spine4;
merge spine3(in=a) births(in=b keep=snz_uid) res(in=c keep=snz_uid) nonres(in=d keep=snz_uid );
by snz_uid;
if a;
if b then birth=1; else birth=0;
if c and not b then permres=1; else permres=0;
if d and not b and not c then tempres=1; else tempres=0;
if birth=1 then status=1;
else if permres=1 then status=2;
else if tempres=1 then status=3;
else status=4;
run;

proc format;
value status
1='NZborn'
2='PermRes'
3='Tempres'
4='Other';
run;

proc freq data=spine4;
tables ageat31Dec2015*status /nofreq nocol nopercent missing;
format status status.;
run;
**There are more people with 'other' status in the cohorts that were born in 1989-1996
   before the visa records began, as we would expect;


****Now drop the temporary residents;

data spine4;
merge spine3(in=a) births(in=b keep=snz_uid) res(in=c keep=snz_uid) nonres(in=d keep=snz_uid );
by snz_uid;
if a;
if b then birth=1; else birth=0;
if c and not b then permres=1; else permres=0;
if d and not b and not c then tempres=1; else tempres=0;
if birth=1 then status=1;
else if permres=1 then status=2;
else if tempres=1 then status=3;
else status=4;
if status~=3 then output;
run;

proc freq data=spine4;
tables birth permres tempres status /missing;
run;



***********************************;
**PART 3 INDICATORS OF ACTIVITY IN NZ AFTER 1 JULY 1997;
***********************************;

**Get indicators of school and tertiary domestic enrolments for each year; 

proc sql;
create table enrol
as select snz_uid
   ,input(compress(moe_esi_start_date,"-"),yymmdd10.) format date9. as startdate
   ,input(compress(moe_esi_end_date,"-"),yymmdd10.) format date9. as enddate
   ,input(compress(moe_esi_extrtn_date,"-"),yymmdd10.) format date9. as ExtractionDate
   ,moe_esi_provider_code  as schoolnumber
   ,moe_esi_domestic_status_code as moe_dom_code
   from moe.student_enrol
   where moe_esi_domestic_status_code in ('60000', '60001', '60002', '60003', '60005', '60006', '60007', '60012' )
   and snz_uid in (select distinct snz_uid from spine4)
order by snz_uid;
quit;

**Drop if startdate missing or enddate>startdate;
data enrol_1; set enrol; 
if enddate=. then enddate=ExtractionDate;
if startdate~=. and enddate>startdate; 
keep snz_uid schoolnumber startdate enddate ;
run;

*  make sure no duplicate records of enrolments;
proc sort data=enrol_1 nodupkey; 
by snz_uid startdate enddate schoolnumber;run;
 
proc sort data=enrol_1; by snz_uid startdate enddate; run;

%OVERLAP (enrol_1,examine=T); 
%OVERLAP (enrol_1); 

data enrol_2; 
set enrol_1_OR; 
keep snz_uid startdate enddate;
run;

%aggregate_by_year(enrol_2,enrol_2_sum,2006,2015);


data enrol_arrays; set enrol_2_sum; 
	array sch_enr_da_(*) sch_enr_da_2006-sch_enr_da_2015;	
do i=2006 to 2015;
ind=i-2005;
sch_enr_da_(ind)=0;
	if i=year and days>0 then do;
		sch_enr_da_(ind)=days;
	end;
end;
run;

proc summary data=enrol_arrays nway;
class snz_uid;
var sch_enr_da_2006-sch_enr_da_2015;
output out=sch_enrol_arrays(drop=_type_ _freq_) sum=;
run; 

data school_arrays;
set sch_enrol_arrays(rename=(sch_enr_da_2006-sch_enr_da_2015 = s2006-s2015));
array years(*) s2006-s2015;
  do i=1 to dim(years);
  if years(i)>0 then years(i)=1; else years(i)=0;
  end;
run;


**Tertiary enrolments;

proc sort data=spine4;
by snz_uid;
run;

data tertiary_enrol;
merge spine4(in=a keep=snz_uid) moe.enrolment(keep=snz_uid moe_enr_prog_start_date
    moe_enr_funding_srce_code moe_enr_is_domestic_ind moe_enr_prog_start_date);
by snz_uid;
if a and moe_enr_funding_srce_code~='02' and moe_enr_is_domestic_ind='1';
startdate = input(moe_enr_prog_start_date,yymmdd10.);
format startdate date9.;
year=year(startdate);
array years(*) y2006-y2015;
  do i=2006 to 2015;
  if year=i then years(i-2005)=1;
     else years(i-2005)=0;
  end;
run;

proc summary data=tertiary_enrol nway;
class snz_uid ;
var y2006-y2015;
output out=stats sum=;
run;

data tertiary_arrays;
set stats;
array years(*) y2006-y2015;
  do i=1 to dim(years);
  if years(i)>1 then years(i)=1;
  end; 
run;

data allstudy;
merge tertiary_arrays(keep=snz_uid y2006-y2015) school_arrays(keep=snz_uid s2006-s2015);
by snz_uid ;
array totals(3,10) s2006-s2015 y2006-y2015 e2006-e2015;
do i=1 to 10;
   if totals(1,i)=1 or totals(2,i)=1 then totals(3,i)=1; 
   else totals(3,i)=0; 
   end;
run;


***4. Get indictors of taxed income by calendar year;

proc sql;
create table taxyrs as 
   select snz_uid
   ,inc_cal_yr_year_nbr as year
   ,sum(inc_cal_yr_tot_yr_amt) as amt
from data.income_cal_yr
where snz_uid in (select unique snz_uid from spine4)
group by snz_uid, inc_cal_yr_year_nbr
order by snz_uid, inc_cal_yr_year_nbr;
quit;

data taxyrs2;
set taxyrs;
array years(*) ta2000-ta2015;
  do i=2000 to 2015;
  if year=i then years(i-1999)=1;
     else years(i-1999)=0;
  end;
run;

proc summary data=taxyrs2 nway;
class snz_uid ;
var ta2000-ta2015;
output out=taxstats sum=;
run;

**Add people with any self employment income in the (approximate) tax year;
**Using data for the tax year that ended 3 months after the end of the calendar year;

proc sql;
create table taxyrsb as 
   select snz_uid
   ,inc_tax_yr_year_nbr as year
   ,sum(inc_tax_yr_tot_yr_amt) as amt
from data.income_tax_yr
where snz_uid in (select unique snz_uid from spine4)
  and inc_tax_yr_income_source_code in ( 'C01', 'C02', 'P01', 'P02', 'S01', 'S02', 'WHP' )
group by snz_uid, inc_tax_yr_year_nbr
order by snz_uid, inc_tax_yr_year_nbr;
quit;


proc freq data=taxyrsb;
tables year /list missing;
run;

**Rename the 2014 tax year t2015 and so on;

data taxyrs2b;
set taxyrsb;
array years(*) tb1999-tb2015;
  do i=2000 to 2015;
  if year=i then years(i-1999)=1;
     else years(i-1999)=0;
  end;
run;

proc means data=taxyrs2b;
var tb1999-tb2015;
run;

proc summary data=taxyrs2b nway;
class snz_uid ;
var tb1999-tb2015;
output out=taxstatsb sum=;
run;

proc means data=taxstatsb;
var tb1999-tb2015;
run;

data jointax;
merge taxstats taxstatsb(keep=snz_uid tb2000-tb2015);
by snz_uid;
array ems(*) ta2000-ta2015;
array ann(*) tb2000-tb2015;
array final(*) t2000-t2015;
do i=1 to dim(final);
  if ems(i)=1 or ann(i)=1 then final(i)=1;
  else final(i)=0;
  end;
run;

proc means data=jointax;
var t2000-t2015;
run;




**5.Get other indicators of social service use from 1998 onwards - 
  using evidence of a PHO enrolment or consultation, hospital admission, outpatient or emergency
  department event, whether the person was a child covered by a parent's benefit;
**If a child hasn't been linked to an MOH id, it may be useful to look for other social service use indicators;
**At this point I'm creating yes/no indicators for the entire period rather than annual indicators;

**PHO enrolments;
**I want to select people who had either an enrolment date or a consultation date
  from 1998 onwards;

proc sql;
create table pho_basedata
as select snz_uid
      ,moh_pho_year_and_quarter_text as refdate
      ,moh_pho_dhb_of_pho_code as dhb
      ,input(moh_pho_last_consul_date,yymmdd10.) format date9.  as last_consult
      ,input(moh_pho_enrolment_date,yymmdd10.) format date9. as enrolment_date
	  ,year(input(moh_pho_last_consul_date,yymmdd10.)) as year_consult
	  ,year(input(moh_pho_enrolment_date,yymmdd10.)) as year_enrol
from moh.pho_enrolment
where snz_uid in (select distinct snz_uid from spine4)
      and (year(input(moh_pho_last_consul_date,yymmdd10.))>=1998 or 
           year(input(moh_pho_enrolment_date,yymmdd10.))>=1998) 
	  ORDER BY snz_uid, refdate;
quit;

proc print data=pho_basedata(obs=500);
where year_enrol<1998;
run;

proc freq data=pho_basedata;
tables year_enrol year_consult /list missing;
run;

proc sql;
create table allpho
as select distinct snz_uid
from pho_basedata
order by snz_uid;
quit;

**Any hospital discharge or ED/OP visit from 1998 onwards;

proc sql;
create table hospdata_basedata
as select snz_uid 
  ,input(moh_evt_evst_date,yymmdd10.) format date9.  as startdate
  ,moh_evt_los_nbr as los
  ,moh_evt_shrtsty_ed_flg_ind as edflag
FROM moh.pub_fund_hosp_discharges_event
		where snz_uid in (select distinct snz_uid from spine4)
		and year(input(moh_evt_evst_date,yymmdd10.))>=1998
		ORDER BY snz_uid, startdate;
quit;

proc sql;
create table allhosp
as select distinct snz_uid
from hospdata_basedata
order by snz_uid;
quit;

proc sql;
create table oped
as select snz_uid
  ,input(moh_nnp_service_date,yymmdd10.) format date9. as date
  ,moh_nnp_event_type_code as event_type
FROM moh.nnpac
where snz_uid in (select distinct snz_uid from spine4)
   and year(input(moh_nnp_service_date,yymmdd10.))>=1998
   and (moh_nnp_event_type_code='ED' or moh_nnp_event_type_code='OP')
ORDER BY snz_uid, date;
quit;

proc sql;
create table alloped
as select distinct snz_uid
from oped
order by snz_uid;
quit;

data healthcare;
merge allpho allhosp alloped;
by snz_uid;
run;

**Whether covered by an adult's benefit in the period from 1998 onwards;

proc sql;
create table parents
as select snz_uid as par_snz_uid,
    child_snz_uid,
	input(compress(msd_chld_child_from_date,"-"),yymmdd10.) format date9.  as startdate,
    input(compress(msd_chld_child_to_date,"-"),yymmdd10.) format date9.  as enddate
	from msd.msd_child
   where child_snz_uid in (select distinct snz_uid from spine4)
   and year(input(compress(msd_chld_child_to_date,"-"),yymmdd10.))>=1998   
ORDER BY snz_uid, startdate;
quit;

**As a rough approximation, take one record per child, assuming a benefit was actually
  paid to the parent at some point from 1998 onwards or the child wouldnt be in the child table;
proc sort data=parents;
by child_snz_uid startdate;
run;

data child(keep=child_snz_uid rename=(child_snz_uid=snz_uid));
set parents;
by child_snz_uid startdate;
if first.child_snz_uid;
*parentonben_since_1998=1;
run;

**Whether subject of a CYF placement with a period ending in 1998 or later;
**Only about 5 children are added using this dataset - I assume this is because the kids in the CYF table are already
  in the benefits data through their parents, or in health, or in school enrolments;

proc sql;
create table cyf
as select snz_uid,
    input(compress(cyf_ple_event_from_date_wid_date,"-"),yymmdd10.) format date9.  as startdate,
    input(compress(cyf_ple_event_to_date_wid_date,"-"),yymmdd10.) format date9.  as enddate
from cyf.cyf_placements_event
where snz_uid in (select distinct snz_uid from spine4)
   and year(input(compress(cyf_ple_event_to_date_wid_date,"-"),yymmdd10.))>=1998   
ORDER BY snz_uid, startdate;
quit;

data placement(keep=snz_uid);
set cyf;
by snz_uid;
if first.snz_uid;
run;


***ALL ACTIVITY INDICATORS;
**Combine the enrolment and tax paid and health service use and benefit indicators;

data activity;
merge spine4(in=a keep=snz_uid birth_year ageat31Dec2015 status) 
     allstudy(in=e keep=snz_uid s2006-s2015 y2006-y2015 e2006-e2015) 
      jointax(in=f keep=snz_uid t2000-t2015)
    healthcare(in=b) child(in=c) placement(in=d);
by snz_uid;
if a and (b or c or d or e or f);
if b then MoH_activity_since_1998=1; else MoH_activity_since_1998=0;
if c then par_benefit_since_1998=1; else par_benefit_since_1998=0;
if d then cyf_placement_since_1998=1; else cyf_placement_since_1998=0;
array taxyrs(*) t2000-t2014;
  do i=1 to dim(taxyrs);
  if taxyrs(i)=. then taxyrs(i)=0;
  end;
array totals(3,10) e2006-e2015 t2006-t2015 taxORen2006-taxORen2015 ;
do i=1 to 10;
   if totals(1,i)=. then totals(1,i)=0;
   if totals(2,i)=. then totals(2,i)=0;
   if totals(1,i) =1 or totals(2,i)=1 then totals(3,i) =1;
     else totals(3,i)=0;
  end;
array totals2(*) taxORen2000-taxORen2006;
  do i=1 to 7;
  if taxyrs(i)=1 then totals2(i)=1; else totals2(i)=0; 
  end;
nbryrsenrol=sum(of e2006-e2015);
nbryrstax=sum(of t2000-t2015);
nbryrstaxorenrol=sum(of taxORen2000-taxORen2015);
*if sum(of taxORen2011-taxORen2013)>=2 then activity1113=1;
if moelink=. then moelink=0;
if mohlink=. then mohlink=0;
if irdlink=. then irdlink=0;
if msdlink=. then msdlink=0;
*birthdate=input('15'||put(snz_birth_month,z2.)||put(snz_birth_year,z4.),ddmmyy8.);
*ageDec2013 = floor((intck('month',birthdate,'31Dec2013'd)- (day('31Dec2013'd) < day(birthdate))) / 12);
run;


proc freq data=activity;
tables ageat31Dec2015 ageat31Dec2015*status /nofreq nocol nopercent missing;
format status status.;
run;

/*

proc freq data=spine5;
tables snz_birth_year/list missing;
where spine=1 and activity1113=1;
title 'People on spine with an enrolment or taxed income for at least 2/3 years 2011-13';
run;

proc freq data=spine5;
tables ageDec2013*snz_sex ageDec2013 /nocol norow nopercent missing;
where spine=1 and activity1113=1 and tempres~=1;
title 'People on spine with an enrolment or taxed income for at least 2/3 years 2011-13';
title2 'Excluding temp residents';
run;


**What is the composition of this population?;
proc freq data=spine5;
tables ageDec2013*status /nocol norow nopercent missing;
where spine=1 and activity1113=1 and tempres~=1;
format status status.;
title 'Numbers with each status by year of birth';
run;

proc freq data=spine5;
tables snz_birth_year*status /nocol nofreq nopercent missing;
where spine=1 and activity1113=1 and tempres~=1;
format status status.;
title 'Proportions with each status by year of birth';
run;

**What proportions have MoElinks, IRD links, MoHlinks?;
**95% of the 15 year olds have IRD links, this rises to 99.5% at 18 years;

proc means data=spine5 mean;
class ageDec2013;
var irdlink moelink mohlink msdlink ;
where spine=1 and activity1113=1 and tempres~=1;
run;
*/



***************************************************************************;
**PART 4 DETERMINE IF PERSON WAS LIVING IN NZ FOR AT LEAST 6 MONTHS IN 2013;
**Get indicators of time spent in NZ each calendar year since birth, from start of 1998 onwards;
**If not born yet or less than 1 year of age set indicator to missing;
***************************************************************************;

proc sql;
create table os_spells
as select snz_uid, 
   datepart(pos_applied_date) format date9.  as startdate, 
   datepart(pos_ceased_date) format date9. as enddate
from data.person_overseas_spell
where snz_uid IN 
             (SELECT DISTINCT snz_uid FROM spine4 ) 
order by snz_uid, startdate;
quit;

data os_spells2;
set os_spells;
	if year(enddate)=9999 then enddate='31Dec2015'd;
	if year(startdate)=1900 then startdate='1Jan1997'd;
run;

proc sort data=os_spells2;
by snz_uid startdate enddate;
run;

**Count all days spent overseas in each calendar year;

data os_spells3;
set os_spells2;
start='01Jan1998'd; 
array os(*) o1998-o2015;
array osdays [*] os_da_1998-os_da_2015 ; * days os;
do i=1 to dim(osdays);
   start_window=intnx('YEAR',start,i-1,'S');
   end_window=(intnx('YEAR',start,i,'S'))-1;
   *format start_window_i end_window_i date9.;  
   if not((startdate > end_window) or (enddate < start_window)) then do;	              
		            os[i]=1;
		           * days;
		            if (startdate <= start_window) and  (enddate > end_window) then days=(end_window-start_window)+1;
		            else if (startdate <= start_window) and  (enddate <= end_window) then days=(enddate-start_window)+1;
		            else if (startdate > start_window) and  (enddate <= end_window) then days=(enddate-startdate)+1;
		            else if (startdate > start_window) and  (enddate > end_window) then days=(end_window-startdate)+1;
       	     
		           osdays[i]=days*os(i);				   
		         end;
	end;	          
run;

proc summary data=os_spells3 nway;
class snz_uid;
var os_da_1998-os_da_2015 ;
output out=OSstats  sum=osday1998-osday2015;
run;

proc means data=OSstats;
var osday1998-osday2015;
run;


******************************************************************************;
**PART 5 COMBINE INDICATORS;
**Must have some measure of activity in IDI unless still aged 0-5 as at 31 Dec 2013;
**Must have spent at least 6 months in NZ in 2013;
**If aged 6-21 at 31 Dec 2013 then must have an MoE link;
**If aged 19+ at 31 Dec 2013 then must have an IRD link;
*******************************************************************************;

data activity2;
merge spine4(in=a) activity(keep=snz_uid in=b);
by snz_uid;
if a and (ageat31Dec2015<=5 or b);
run;

data spine5 exclusions;
merge activity2(in=a) OSstats(in=c keep=snz_uid osday1998-osday2015);
by snz_uid;
if a and osday2015<182;  *retains people with osday2013=.;
array os(*) osday1998-osday2015;
array osb(*) os1998-os2015;
do i=1 to dim(os);
  if os(i)=. then os(i)=0;
  end;
if ageat31Dec2015>=19 and irdlink~=1 then exclude=1;
if birth_year>=1992 and ageat31Dec2015>=6 and moelink~=1 then exclude=1;
if exclude~=1 then output spine5;  else output exclusions;
run;


proc freq data=spine5;
tables ageat31Dec2015   ageat31Dec2015*status  /nofreq nocol nopercent missing;
format status status.;
run;

data spine6 exclusions;
set spine5;
run;

proc freq data=spine5;
tables ageat31Dec2015  ageat31Dec2015*status   /nofreq nocol nopercent missing;
format status status.;
run;

proc freq data=spine5;
tables ageat31Dec2015*(irdlink moelink mohlink msdlink) / nopercent norow nocol missing ;
run;


data Project.currentpopn2015(drop=status);
set spine5(keep=snz_uid birth_month birth_year dob dod death_month death_year
   snz_sex status ageat31Dec2015); 
if status=2 then res_status='Permres';
else if status=1 then res_status='NZborn';
else if status=4 then res_status='Othres';
run;
/* */
/*proc sort data=indic.currentpopn2013;*/
/*by snz_uid;*/
/*run;*/
/**/
/**/
/*proc summary data=indic.currentpopn2013 nway;*/
/*class ageat31Dec2013;*/
/*var snz_uid;*/
/*output out=stats n=n;*/
/*run;*/


proc summary data=spine5 nway;
class ageat31Dec2015;
var snz_uid;
output out=stats n=n;
run;


proc datasets lib=work kill nolist memtype=data;
quit;









































