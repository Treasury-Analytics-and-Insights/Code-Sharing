/*************************************************************************************************************************************
*************************************************************************************************************************************
Developer: Sarah Tumen
Date Created: 7 Dec 2015

This code estimates the cost of schooling for individuals using average per student funding rates. 
This cost is calculated for those individuals who attended state and state integrated schools. 
School enrolment information available only from 2006, for those who started schooling prior to 2006, 
we have imputed days enrolled as difference between calendar years and days spent overseas. 
All costs are expressed in 2014 dollars.

*/

data open_;
	set project.SCHOOLPROFILE_OPEN_&date.;

	if school_authority not in ('State','StateIntegrated');
	keep SchoolNumber;

data closed_;
	set project.SCHOOLPROFILE_CLOSED_&date.;

	if schoolauthority not in ('State','State : Integrated');
	keep SchoolNumber;

data Private;
	set open_ closed_;
	priv_sch=1;
run;

proc sort data=private nodupkey;
	by SchoolNumber;
run;

data Enrol_OS;
	retain snz_uid dob ed_cohort days_&first_anal_yr-days_&last_anal_yr
		OS_da_&first_anal_yr-OS_da_&last_anal_yr;
	merge project._IND_SCH_ENROL_&date project._IND_OS_spells_&date(drop=DOB) 
		project._IND_SCH_ATTENDED_&date(keep=snz_uid enr_at_sch_&first_anal_yr-enr_at_sch_&last_anal_yr);
	by snz_uid;

	if month(DOB)>=7 then
		ed_cohort=year(DOB);

	if month(DOB)<7 then
		ed_cohort=year(DOB)-1;
	start_school=ed_cohort+6;

	Y11=start_school+10;

	array days_(*) days_&first_anal_yr-days_&last_anal_yr;

	array OS_da_(*) OS_da_&first_anal_yr-OS_da_&last_anal_yr;
	array sch_enr_da_(*) sch_enr_da_&first_anal_yr-sch_enr_da_&last_anal_yr;
	array imp_sch_enr_da_(*) imp_sch_enr_da_&first_anal_yr-imp_sch_enr_da_&last_anal_yr;
	array imp_sch_enr_id_(*) imp_sch_enr_id_&first_anal_yr-imp_sch_enr_id_&last_anal_yr;
	array sch_enr_id_(*) sch_enr_id_&first_anal_yr-sch_enr_id_&last_anal_yr;

	do ind=&first_anal_yr to &last_anal_yr;
		i=ind-(&first_anal_yr-1);
		days_(i)=0;
		days_(i)=intck('DAY',MDY(01,1,ind),MDY(12,31,ind))+1;

		sch_enr_id_(i)=sch_enr_da_(i);

		if sch_enr_da_(i)>0 then
			sch_enr_id_(i)=1;

		if (ind>=start_school and ind<=Y11) then
			imp_sch_enr_da_(i)=days_(i)-OS_da_(i);

		if ed_cohort<=1988 and ind>=2006 then
			imp_sch_enr_da_(i)=sch_enr_da_(i);

		if ed_cohort<=1988 and sch_enr_da_2006>0 then
			imp_sch_enr_da_2005=days_2005;

		if ed_cohort=1987 and sch_enr_da_2006>0 then
			imp_sch_enr_da_2004=days_2004;

		if ed_cohort>=1989 and ind>=2006 then
			imp_sch_enr_da_(i)=sch_enr_da_(i);

		if ind<start_school then
			imp_sch_enr_da_(i)=.;

		imp_sch_enr_id_(i)=imp_sch_enr_da_(i);

		if imp_sch_enr_da_(i)>0 then
			imp_sch_enr_id_(i)=1;
		drop days_&first_anal_yr-days_&last_anal_yr Y11;
	end;

	if (ed_cohort>=1990 and ed_cohort<=2000)  and (sch_enr_da_2007>0 and sch_enr_da_2006=0) and OS_da_2006 ne 365 then
		imp_sch_enr_da_2006=imp_sch_enr_da_2007;

	if (ed_cohort>=1990  and ed_cohort<=2000) and (sch_enr_da_2007>0 and sch_enr_da_2006=0) and OS_da_2006 ne 365 then
		imp_sch_enr_id_2006=imp_sch_enr_id_2007;
run;

proc summary  data=Enrol_OS nway;
	class ed_cohort;
	var sch_enr_id_1993-sch_enr_id_2014
		imp_sch_enr_id_1993-imp_sch_enr_id_2014;
	output out=temp sum=;
run;

proc summary data=Enrol_OS nway;
	class ed_cohort;
	var sch_enr_da_1993-sch_enr_da_2014
		imp_sch_enr_da_1993-imp_sch_enr_da_2014;
	output out=temp1 mean=;
run;

data enrol_OS_1;
	set ENROL_OS;
	keep snz_uid ed_cohort start_school imp_sch_enr_id_&first_anal_yr-imp_sch_enr_id_&last_anal_yr
		enr_at_sch_1988-enr_at_sch_2014;
run;

proc datasets library=work;
	delete ENROL_OS;
run;

%macro merge_private(year);
	%do i=&first_anal_yr %to &last_anal_yr;

		proc sql;
			create table
				ENROL_OS_1
			as select 
				a.*,
				b.priv_sch as priv_sch_&i
			from ENROL_OS_1 a left join private b
				on a.enr_at_sch_&i=b.schoolnumber;
		quit;

	%end;
%mend;

%merge_private;

data ENROL_OS_1;
	set ENROL_OS_1;
	array imp_sch_enr_id_(*) imp_sch_enr_id_&first_anal_yr-imp_sch_enr_id_&last_anal_yr;
	array priv_sch_(*) priv_sch_&first_anal_yr-priv_sch_&last_anal_yr;

	do ind=&first_anal_yr to &last_anal_yr;
		i=ind-(&first_anal_yr-1);

		if priv_sch_(i)=1 then
			imp_sch_enr_id_(i)=0;
		drop i ind;
	end;
run;

proc sql;
	create table school_cost as select 
		a.*,
		b.*
	from ENROl_OS_1 a left join project.TSY_B15_04_PERSTUDENTFUNDING b
		on a.ed_cohort=b.cohort;

data project._COST_Schooling_&date;
	set school_cost;
	year_16=start_school+15;
	array imp_sch_enr_id_(*) imp_sch_enr_id_&first_anal_yr-imp_sch_enr_id_&last_anal_yr;
	post15=0;

	do ind=&first_anal_yr to &last_anal_yr;
		i=ind-(&first_anal_yr-1);

		if imp_sch_enr_id_(i)=. then
			imp_sch_enr_id_(i)=0;

		if ind>=year_16 and imp_sch_enr_id_(i)=1 then
			post15+1;

		if start_school=ind then
			sch_cost_Y1=Y1*imp_sch_enr_id_(i);

		if start_school+1=ind then
			sch_cost_Y2=Y2*imp_sch_enr_id_(i);

		if start_school+2=ind then
			sch_cost_Y3=Y3*imp_sch_enr_id_(i);

		if start_school+3=ind then
			sch_cost_Y4=Y4*imp_sch_enr_id_(i);

		if start_school+4=ind then
			sch_cost_Y5=Y5*imp_sch_enr_id_(i);

		if start_school+5=ind then
			sch_cost_Y6=Y6*imp_sch_enr_id_(i);

		if start_school+6=ind then
			sch_cost_Y7=Y7*imp_sch_enr_id_(i);

		if start_school+7=ind then
			sch_cost_Y8=Y8*imp_sch_enr_id_(i);

		if start_school+8=ind then
			sch_cost_Y9=Y9*imp_sch_enr_id_(i);

		if start_school+9=ind then
			sch_cost_Y10=Y10*imp_sch_enr_id_(i);

		if start_school+10=ind then
			sch_cost_Y11=Y11*imp_sch_enr_id_(i);

		if start_school+11=ind then
			sch_cost_Y12=Y12*imp_sch_enr_id_(i);

		if start_school+12=ind then
			sch_cost_Y13=Y13*imp_sch_enr_id_(i);

		if start_school+13=ind then
			sch_cost_Y14=Y14*imp_sch_enr_id_(i);

		if start_school+14=ind then
			sch_cost_Y15=Y15*imp_sch_enr_id_(i);
	end;

	Post15_cost=post15*Y15;
	Sch_total_cost=sum(of sch_cost_Y1-sch_cost_Y15) +Post15_cost;
	drop i ind;
	keep snz_uid ed_cohort 
		imp_sch_enr_id_&first_anal_yr-imp_sch_enr_id_&last_anal_yr
		sch_cost_Y1-sch_cost_Y15 Post15 Post15_cost Sch_total_cost;
run;

proc datasets lib=work kill nolist memtype=data;
quit;