


*********************************************************************************************************************************
*********************************************************************************************************************************
This macro creates days overseas using Customs data
*********************************************************************************************************************************
*********************************************************************************************************************************;
%Macro Create_OS_spell_pop;
proc sql;
	create table Overseas as 
		SELECT 
			a.snz_uid,
			datepart(a.pos_applied_date) as startdate format date9.,
			datepart(a.pos_ceased_date) as enddate  format date9.,
			b.DOB
		FROM data.person_overseas_spell a inner join &population b
				on a.snz_uid = b.snz_uid
			ORDER BY a.snz_uid, a.pos_applied_date;
quit;


data Overseas;
	set Overseas;

	if startdate<"&sensor"d;

	if enddate>"&sensor"d then
		enddate="&sensor"d;

	if startdate<DOB and enddate>DOB then
		startdate=DOB;

	if startdate <DOB and enddate<DOB then
		delete;
run;

%overlap(Overseas);

data Overseas_OR;
	set Overseas_OR;
	array OS_da_(*)	OS_da_&first_anal_yr.-OS_da_&last_anal_yr.;

	do ind=&first_anal_yr. to &last_anal_yr.;
		i=ind-(&first_anal_yr.-1);

		start1=MDY(1,1,&first_anal_yr.);
		start_window=intnx('YEAR',start1,i-1,'S');
		end_window=intnx('YEAR',start1,i,'S')-1;
		format start_window end_window start1 date9.;

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
				OS_da_(i)=days;
			end;
	end;

	array OS_da_at_age_(*)	OS_da_at_age_&firstage-OS_da_at_age_&lastage;

	do ind=&firstage to &lastage;
		i=ind-(&firstage-1);
		OS_da_at_age_(i)=0;
		start_window=intnx('YEAR',DOB,i-1,'S');
		end_window=intnx('YEAR',DOB,i,'S')-1;
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
				OS_da_at_age_(i)=days;
			end;
	end;
run;

proc summary data=Overseas_OR nway;
	class snz_uid DOB;
	var OS_da_:;
	output out=&projectlib.._ind_OS_spells_&date(drop=_: OS_da_at_age_:) sum=;
run;

proc summary data=Overseas_OR nway;
	class snz_uid DOB;
	var OS_da_at_age_:;
	output out=&projectlib.._ind_OS_spells_at_age_&date(drop=_: ) sum=;
run;

proc datasets lib=work;
delete Overseas_OR Overseas deletes;
run;

%mend;


************;
%macro Create_mth_OS_spell_pop;
proc sql;
	create table Overseas as 
		SELECT 
			a.snz_uid,
			datepart(a.pos_applied_date) as startdate format date9.,
			datepart(a.pos_ceased_date) as enddate  format date9.,
			b.DOB
		FROM data.person_overseas_spell a inner join &population b
				on a.snz_uid = b.snz_uid
			ORDER BY a.snz_uid, a.pos_applied_date;
quit;


data Overseas;
	set Overseas;

	if startdate<"&sensor"d;

	if enddate>"&sensor"d then
		enddate="&sensor"d;

	if startdate<DOB and enddate>DOB then
		startdate=DOB;

	if startdate <DOB and enddate<DOB then
		delete;
run;

%overlap(Overseas);
**Count all days spent overseas in each calendar month from Jan 2004 to Jun 2015;
**Use LEED dates to index each month;

data Overseas_OR(drop=i start_window end_window days);
set Overseas_OR;
array osdays [*] os_da_&m-os_da_&n ; * days os;
do i=1 to dim(osdays);
   start_window=intnx('month',&start.,i-1,'S');
   end_window=(intnx('month',&start.,i,'S'))-1;
   format start_window end_window date9.;  
   if not((startdate > end_window) or (enddate < start_window)) then do;	              
		            if (startdate <= start_window) and  (enddate > end_window) then days=(end_window-start_window)+1;
		            else if (startdate <= start_window) and  (enddate <= end_window) then days=(enddate-start_window)+1;
		            else if (startdate > start_window) and  (enddate <= end_window) then days=(enddate-startdate)+1;
		            else if (startdate > start_window) and  (enddate > end_window) then days=(end_window-startdate)+1;     	     
		            osdays[i]=days;				   
		         end;
	end;	          
run;


proc summary data=Overseas_OR nway;
class snz_uid;
var os_da_&m-os_da_&n;
output out=&projectlib.._mth_os_&date. (drop=_:)  sum=;
run;

proc datasets lib=work;
delete Overseas_OR Overseas deletes;
run;
%mend;
