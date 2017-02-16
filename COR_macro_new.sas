
****************************************************************************************************************************
****************************************************************************************************************************

CORRECTIONS DATA CORRECTIONS DATA CORRECTIONS DATA CORRECTIONS DATA CORRECTIONS DATA CORRECTIONS DATA
		PRISON	Prison sentenced	PRISON
		REMAND	Remanded in custody		PRISON
		ESO	Extended supervision order	COMMUNITY
		HD_REL	Released to HD	HOME
		PAROLE	Paroled	COMMUNITY
		ROC	Released with conditions	COMMUNITY
		HD_SENT	Home detention sentenced	HOME
		PDC	Post detention conditions	COMMUNITY
		INT_SUPER	Intensive supervision	COMMUNITY
		COM_DET	Community detention	COMMUNITY
		SUPER	Supervision	COMMUNITY
		CW	Community work		COMMUNITY
		PERIODIC	Periodic detention		COMMUNITY
		COM_PROG	Community programme		COMMUNITY
		COM_SERV	Community service		COMMUNITY
		OTH_COM	Other community		COMMUNITY


***********************************************************************************************************************
***********************************************************************************************************************
* COURTS DATASET INDICATORS 
* Nbr APPEARANCES IN YOUTH COURT, Nbr PROVEN OFFENCES (incl youth and adult courts), Nbr CONVICTIONS(youth and adult courts);
* This section of code written by Sylvia Dixon on 25 September;
* The measure of proven offences is generated because Youth Courts generally don't convict offenders even when the charge is proven.
   Adult courts also discharge some offenders without conviction;
**Note that breaches (of a sentences for a prior offence) are excluded from the 'conviction' and 'proven offences' 
  indicators created here, following advice from Charles Sullivan;
**The conviction and proven offence measures count all charges that the person was convicted of - note that there
  can be multiple charges associated with one criminal act, and multiple charges and convictions on a single day;
**Use the outcome dates if you wish to create a measure of the number of court appearances;

****************************************************************************************************************************
TWO macros that assist in creation of correction indicators for reference person
****************************************************************************************************************************;

%macro corr_type_yr(type);
 
array corr_&type._id_[*] corr_&type._id_&first_anal_yr.-corr_&type._id_&last_anal_yr. ;
array corr_da_&type._[*] corr_&type._&first_anal_yr.-corr_&type._&last_anal_yr.;

	corr_&type._id_(i)=0;
	corr_da_&type._(i)=0;

if not((startdate > end_window) or (enddate < start_window)) then do;
					if sentence="&type." then corr_&type._id_(i)=1;

					if (startdate <= start_window) and  (enddate > end_window) then
						days=(end_window-start_window)+1;
					else if (startdate <= start_window) and  (enddate <= end_window) then
						days=(enddate-start_window)+1;
					else if (startdate > start_window) and  (enddate <= end_window) then
						days=(enddate-startdate)+1;
					else if (startdate > start_window) and  (enddate > end_window) then
						days=(end_window-startdate)+1;	

					corr_da_&type._[i]=days*corr_&type._id_[i];

end;
%mend;

%macro corr_type_age(type);

array corr_&type._id_at_age_[*] corr_&type._id_at_age_&firstage.-corr_&type._id_at_age_&lastage. ;
array corr_da_&type._at_age_[*] corr_da_&type._at_age_&firstage.-corr_da_&type._at_age_&lastage.;
	corr_&type._id_at_age_(age)=0;
	corr_da_&type._at_age_(age)=0;

if not((startdate > end_window) or (enddate < start_window)) then do;
					if sentence="&type." then corr_&type._id_at_age_(age)=1;

					if (startdate <= start_window) and  (enddate > end_window) then
						days=(end_window-start_window)+1;
					else if (startdate <= start_window) and  (enddate <= end_window) then
						days=(enddate-start_window)+1;
					else if (startdate > start_window) and  (enddate <= end_window) then
						days=(enddate-startdate)+1;
					else if (startdate > start_window) and  (enddate > end_window) then
						days=(end_window-startdate)+1;	

					corr_da_&type._at_age_[age]=days*corr_&type._id_at_age_[age];

end;

%mend;

*********************************************************************************************************************************************************
Create Correction indicators for population of interest
*********************************************************************************************************************************************************;

%macro Create_CORR_ind_pop;
proc sql;
	create table COR as
		SELECT distinct 
		 snz_uid,
			input(cor_mmp_period_start_date,yymmdd10.) as startdate,
			input(cor_mmp_period_end_date, yymmdd10.) as enddate,
			cor_mmp_mmc_code,  
           /* Creating wider correction sentence groupings */
	    	(case when cor_mmp_mmc_code in ('PRISON','REMAND' ) then 'Cust'
			     when cor_mmp_mmc_code in ('HD_SENT','HD_SENT', 'HD_rel' ) then 'HD'
                 when cor_mmp_mmc_code in ('ESO','PAROLE','ROC','PDC' ) then 'Post_Re'
				 when cor_mmp_mmc_code in ('COM_DET','CW','COM_PROG','COM_SERV' ,'OTH_COMM','INT_SUPER','SUPER','PERIODIC') then 'Comm'
                 else 'OTH' end) as sentence 
		FROM COR.ov_major_mgmt_periods 
		where snz_uid in (SELECT DISTINCT snz_uid FROM &population) 
		/* exclude birthdate and aged out records */
		AND cor_mmp_mmc_code IN ('PRISON','REMAND','HD_SENT','HD_REL','ESO','PAROLE','ROC','PDC','PERIODIC',
			'COM_DET','CW','COM_PROG','COM_SERV','OTH_COMM','INT_SUPER','SUPER')

		ORDER BY snz_uid,startdate;
quit;

proc sql;
create table COR_1 as select
a.* ,
b.DOB
from COR a left join &population b
on a.snz_uid=b.snz_uid
order by a.snz_uid, startdate;

data COR_clean; set COR_1; by snz_uid startdate;
format startdate enddate date9.;

if startdate>"&sensor"d then delete;
if enddate>"&sensor"d then enddate="&sensor"d;

if startdate >intnx('YEAR',DOB,7,'S'); 
run;

* Remove overlaps;

%OVERLAP (COR_clean); 

data TEMP; 
set COR_clean_OR;
		start1=MDY(1,1,&first_anal_yr.); format start1 date9.;

array corr_id_(*) corr_id_&first_anal_yr.-corr_id_&last_anal_yr.;
array corr_id_at_age_(*) corr_id_at_age_&firstage.-corr_id_at_age_&lastage.;

do ind=&first_anal_yr. to &last_anal_yr;
			i=ind-(&first_anal_yr.-1);

			start_window=intnx('YEAR',start1,i-1,'S');
			end_window=intnx('YEAR',start1,i,'S')-1;

			%corr_type_yr(Cust);
			%corr_type_yr(HD);
			%corr_type_yr(Post_Re);
			%corr_type_yr(Comm);
			%corr_type_yr(OTH);
			
corr_id_(i)=max(of corr_Cust_id_(i),corr_HD_id_(i),corr_Post_re_id_(i),corr_Comm_id_(i),corr_OTH_id_(i));

end;

do i=&firstage. to &lastage.;
			age=i-(&firstage.-1);
			start_window=intnx('YEAR',DOB,age-1,'S');
			end_window=intnx('YEAR',DOB,age,'S')-1;
			%corr_type_age(Cust);
			%corr_type_age(HD);
			%corr_type_age(Post_Re);
			%corr_type_age(Comm);
			%corr_type_age(OTH);

corr_id_at_age_(age)=max(of corr_Cust_id_at_age_(age),corr_HD_id_at_age_(age),corr_Post_Re_id_at_age_(age),corr_Comm_id_at_age_(age),corr_OTH_id_at_age_(age));
			

end;
run;
proc summary data=TEMP nway;
class snz_uid DOB;
var corr_id_:;
output out=TEMP1(drop=_:) max=;
run;

proc summary data=TEMP nway;
class snz_uid DOB;
var corr_:;
output out=TEMP2(drop=_: corr_id_:) sum=;
run;


proc sql;
create table base as
select  z.snz_uid
        ,z.dob	
	    ,input((case when moj_chg_charge_outcome_date is not null then moj_chg_charge_outcome_date
                   else moj_chg_last_court_hearing_date end), yymmdd10.) format date9.  as outcome_date
       ,moj_chg_offence_code as code
	   ,moj_chg_offence_classfn_code as asoc_code
       ,asoc_division
       ,asoc_subdiv
       ,asoc_name 
       ,b.charge_outcome_6cat as outcome
	   ,moj_chg_last_court_id_code as court_id
	   ,1 as proven_charge
	   ,(case when b.charge_outcome_6cat='1Convicted' then 1 else 0 end) as conviction
	   ,year(input((case when moj_chg_charge_outcome_date is not null then moj_chg_charge_outcome_date
                   else moj_chg_last_court_hearing_date end), yymmdd10.)) as year
from &population z
left join moj.charges a
on z.snz_uid=a.snz_uid
left join sandmoj.CHARGE_OUTCOME_TYPE b
on a.moj_chg_charge_outcome_type_code=b.charge_outcome_code  
left join sandmoj.ASOC_TYPE c
on a.moj_chg_offence_classfn_code=c.asoc_code
where b.charge_outcome_6cat in ('1Convicted', '2YC proved', '3Discharge w/o conviction', '4adult diversion', 'YC discharge' ) and 
substr(asoc_code, 1,3) not in ('151', '152')
order by snz_uid, outcome_date;
quit;


data base (keep=snz_uid dob conviction proven_charge outcome_date year);
set base;
age_at_decision=floor((intck('month',dob,outcome_date)- (day(outcome_date) < day(dob))) / 12);
if age_at_decision>12;
run;

************;
proc sql;
create table youth as
select  z.snz_uid
       ,z.dob
	   ,input((case when moj_chg_charge_outcome_date is not null then moj_chg_charge_outcome_date
                   else moj_chg_last_court_hearing_date end), yymmdd10.) format date9.  as outcome_date
       ,moj_chg_offence_code as code
	   ,moj_chg_offence_classfn_code as asoc_code
       ,asoc_division
       ,asoc_subdiv
       ,asoc_name 
       ,b.charge_outcome_6cat as outcome
	   ,(case when b.charge_outcome_6cat='1Convicted' then 1 else 0 end) as conviction
	   ,moj_chg_last_court_id_code as court_id
	   
from &population z
left join moj.charges a
on z.snz_uid=a.snz_uid
left join sandmoj.CHARGE_OUTCOME_TYPE b
on a.moj_chg_charge_outcome_type_code=b.charge_outcome_code  
left join sandmoj.ASOC_TYPE c
on a.moj_chg_offence_classfn_code=c.asoc_code
where substr(moj_chg_offence_classfn_code, 1,3) not in ('151', '152')
order by snz_uid, outcome_date; * breach excluded;
quit;


data youth;
set youth;
court=court_id*1;
if 201<=court<=296 then youth_court=1;
if outcome_date>0;
year=year(outcome_date);
age_at_appearance=floor((intck('month',dob,outcome_date)- (day(outcome_date) < day(dob))) / 12);
if 12<=age_at_appearance<=17 and youth_court=1 then output;
run;

**Keep one record per Youth Court app date;
proc sort data=youth;
by snz_uid outcome_date conviction;
data youth; set youth; 
by snz_uid outcome_date conviction;
if last.outcome_date;
run;

************************************;

data alldates;
set base (in=a keep=snz_uid dob outcome_date conviction proven_charge year) youth(in=b keep=snz_uid dob outcome_date year);
if b then YC_appearance=1; 
array convictions_(*) convictions_at_age_&firstage-convictions_at_age_&lastage;
array proven_charges_(*) proven_charges_at_age_&firstage-proven_charges_at_age_&lastage;
array YC_appearances_(*) YC_appearances_at_age_&firstage-YC_appearances_at_age_&lastage;
	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);	
		convictions_(i)=0;
		proven_charges_(i)=0;
		YC_appearances_(i)=0;

		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S')-1;

		if start_window<=outcome_date<=end_window then
			do;
				if conviction=1 then convictions_(i)+1;
				if proven_charge=1 then proven_charges_(i)+1;
                if YC_appearance=1 then YC_appearances_(i)+1;				
			end;
	end;

array convictions_yr_(*) convictions_&first_anal_yr-convictions_&last_anal_yr;
array proven_charges_yr_(*) proven_charges_&first_anal_yr-proven_charges_&last_anal_yr;
array YC_appearances_yr_(*) YC_appearances_&first_anal_yr-YC_appearances_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);		
        	
		if i=year then	convictions_yr_(ind)=conviction; else convictions_yr_(ind)=0;
		if i=year then  proven_charges_yr_(ind)=proven_charge; else proven_charges_yr_(ind)=0;
		if i=year then	YC_appearances_yr_(ind)=YC_appearance; else YC_appearances_yr_(ind)=0;	
		drop i ind;
	end;
run;

proc summary data=alldates nway;
class snz_uid DOB;
var 
convictions_at_age_12-convictions_at_age_&lastage
proven_charges_at_age_12-proven_charges_at_age_&lastage
YC_appearances_at_age_12-YC_appearances_at_age_17
convictions_&first_anal_yr-convictions_&last_anal_yr
proven_charges_&first_anal_yr-proven_charges_&last_anal_yr
YC_appearances_&first_anal_yr-YC_appearances_&last_anal_yr;
Output out=TEMP3(drop=_:) sum=;
run;

data &projectlib.._IND_CORR_&date;
merge TEMP1 TEMP2 TEMP3; by snz_uid;
drop 
corr_id_at_age_:
corr_Cust_id_at_age_:
corr_HD_id_at_age_:
corr_Post_Re_id_at_age_:
corr_Comm_id_at_age_:
corr_OTH_id_at_age_:

corr_da_Cust_at_age_:
corr_da_HD_at_age_:
corr_da_Post_Re_at_age_:
corr_da_Comm_at_age_:
corr_da_OTH_at_age_:
convictions_at_age_:
proven_charges_at_age_:
YC_appearances_at_age_: ;

run;


data &projectlib.._IND_CORR_at_age_&date;
merge TEMP1 TEMP2 TEMP3; by snz_uid;
keep snz_uid 
corr_id_at_age_:
corr_Cust_id_at_age_:
corr_HD_id_at_age_:
corr_Post_Re_id_at_age_:
corr_Comm_id_at_age_:
corr_OTH_id_at_age_:
convictions_at_age_:
proven_charges_at_age_:
YC_appearances_at_age_: ;

run;


proc datasets lib=work;
delete Temp Temp1 Temp2 COR COR_1 COR_clean COR_clean_OR Deletes
base youth alldates TEMP3;
run;

%mend;



********************************************************************************************************************************;
********************************************************************************************************************************;
***ANY CUSTODIAL OR COMMUNITY SENTENCES SERVED;
**by month - from Jan 2006 to June 2015;
********************************************************************************************************************************;
********************************************************************************************************************************;
%macro Create_mth_CORR_ind_pop;
proc sql;
	create table COR as
		SELECT distinct 
		 snz_uid,
			input(cor_mmp_period_start_date,yymmdd10.) as startdate,
			input(cor_mmp_period_end_date, yymmdd10.) as enddate,
			cor_mmp_mmc_code,  
           /* Creating wider correction sentence groupings */
	    	(case when cor_mmp_mmc_code in ('PRISON','REMAND' ) then 'Cust'
			     when cor_mmp_mmc_code in ('HD_SENT','HD_SENT', 'HD_rel' ) then 'HD'
                 when cor_mmp_mmc_code in ('ESO','PAROLE','ROC','PDC' ) then 'Post_Re'
				 when cor_mmp_mmc_code in ('COM_DET','CW','COM_PROG','COM_SERV' ,'OTH_COMM','INT_SUPER','SUPER','PERIODIC') then 'Comm'
                 else 'OTH' end) as sentence 
		FROM COR.ov_major_mgmt_periods 
		where snz_uid in (SELECT DISTINCT snz_uid FROM &population) 
		/* exclude birthdate and aged out records */
		AND cor_mmp_mmc_code IN ('PRISON','REMAND','HD_SENT','HD_REL','ESO','PAROLE','ROC','PDC','PERIODIC',
			'COM_DET','CW','COM_PROG','COM_SERV','OTH_COMM','INT_SUPER','SUPER')

		ORDER BY snz_uid,startdate;
quit;

proc sql;
create table COR_1 as select
a.* ,
b.DOB
from COR a left join &population b
on a.snz_uid=b.snz_uid
order by a.snz_uid, startdate;

data COR_clean; set COR_1; by snz_uid startdate;
format startdate enddate date9.;

if startdate>"&sensor"d then delete;
if enddate>"&sensor"d then enddate="&sensor"d;

if startdate >intnx('YEAR',DOB,7,'S'); 
run;

* Remove overlaps;

%OVERLAP (COR_clean); 

data cor_spells(drop=i start_window end_window days);
set COR_clean_OR;
array custdays [*] cust_da_&m.-cust_da_&n. ; 
array commdays [*] comm_da_&m.-comm_da_&n. ; 
do i=1 to dim(custdays);

	start_window=intnx('month',&start.,i-1,'S');
 	end_window=(intnx('month',&start.,i,'S'))-1;

   format start_window end_window date9.;  
   if not((startdate > end_window) or (enddate < start_window)) and sentence='Custody' then do;	              
		            if (startdate <= start_window) and  (enddate > end_window) then days=(end_window-start_window)+1;
		            else if (startdate <= start_window) and  (enddate <= end_window) then days=(enddate-start_window)+1;
		            else if (startdate > start_window) and  (enddate <= end_window) then days=(enddate-startdate)+1;
		            else if (startdate > start_window) and  (enddate > end_window) then days=(end_window-startdate)+1;     	     
		            custdays[i]=days;	                 
		         end;
   if not((startdate > end_window) or (enddate < start_window)) and sentence='Comm' then do;	              
		            if (startdate <= start_window) and  (enddate > end_window) then days=(end_window-start_window)+1;
		            else if (startdate <= start_window) and  (enddate <= end_window) then days=(enddate-start_window)+1;
		            else if (startdate > start_window) and  (enddate <= end_window) then days=(enddate-startdate)+1;
		            else if (startdate > start_window) and  (enddate > end_window) then days=(end_window-startdate)+1;     	     
		            commdays[i]=days;	                 
		         end;
	end;	          
run;

proc summary data=cor_spells nway;
class snz_uid;
var cust_da_&m.-cust_da_&n. comm_da_&m.-comm_da_&n. ; 
output out=corr(drop=_:)  sum=;
run;

data project._mth_corr_&date.(drop=i);
set corr;
array custdays [*] cust_da_&m-cust_da_&n ; 
array commdays [*] comm_da_&m-comm_da_&n ; 
array cust [*] cust_id_&m-cust_id_&n ; 
array comm [*] comm_id_&m-comm_id_&n ; 
do i=1 to dim(custdays);
   if custdays(i)>0 then cust(i)=1; else cust(i)=0;
   if commdays(i)>0 then comm(i)=1; else comm(i)=0;
   end;
run;

%mend;
************************************************************************************************************************************;