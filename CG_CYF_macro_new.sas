*************************************************************************************************************************************
*************************************************************************************************************************************

DICLAIMER:
This code has been created for research purposed by Analytics and Insights Team, The Treasury. 
The business rules and decisions made in this code are those of author(s) not Statistics New Zealand and The Treasury. 
This code can be modified and customised by users to meet the needs of specific research projects and in all cases, 
Analytics and Insights Team, NZ Treasury must be acknowledged as a source. 
While all care and diligence has been used in developing this code, Statistics New Zealand and The Treasury gives no warranty 
it is error free and will not be liable for any loss or damage suffered by the use directly or indirectly.

*************************************************************************************************************************************
*************************************************************************************************************************************;
%macro Create_CG_CYF_history(rel,sex);

proc sql;
	select max(floor(yrdif(dob,"&sensor."d))) into: maxage separated by "" from &population.;
quit;

proc sort data=&projectlib..parenttochildmap_&date out=parenttochildmap_&date;
	by snz_uid parent event_date;
run;

proc sort data=parenttochildmap_&date out=temp_parenttochildmap(where=((source="&rel." or source1="&rel.") and parent_sex="&sex.")) nodupkey;
	by snz_uid parent parent_sex;
run;

data temp_pop_ch_parent_map temp_ch_parentcount;
	merge &population (in=inpop) Temp_parenttochildmap;
	by snz_uid;

	if inpop;

	if first.snz_uid then
		parent_count = 0;
	retain parent_count;

	if parent ne . then
		parent_count + 1;
	noparent = parent_count = 0;

	if last.snz_uid then
		output temp_ch_parentcount;
	output temp_pop_ch_parent_map;
run;

proc sort data=temp_pop_ch_parent_map;
	by parent;
run;

data temp_parents_single temp_parents_ch;
	set temp_pop_ch_parent_map;
	by parent;
	child_snz_uid = snz_uid;
	snz_uid = parent; 

	if first.parent then
		countchild = 1;
	retain countchild;

	if first.parent = 0 and parent ne . then
		countchild + 1;

	if parent ne . then
		output temp_parents_ch;

	if last.parent and  parent ne . then
		output temp_parents_single;
run;

proc sql;
	select max(countchild) into: maxchild separated by "" from temp_parents_single;
quit;

%Create_clean_CYF_tables;

proc sql;
	create table TEMP_cyf_n1 as
		select
			s.parent,s.child_snz_uid, s.event_date,s.dob,
			t.*
		from
			temp_parents_ch s inner join
			cyf_intake_clean t
			on
			s.parent = t.snz_uid
		order by s.child_snz_uid;
run;

proc sql;
	create table TEMP_cyf_a1 as
		select
			s.parent,s.child_snz_uid, s.event_date,s.dob,
			t.*
		from
			temp_parents_ch s inner join
			cyf_abuse_clean t
			on
			s.parent = t.snz_uid
		order by s.child_snz_uid;
run;

proc sql;
	create table TEMP_cyf_p1 as
		select
			s.parent,s.child_snz_uid, s.event_date,s.dob,
			t.*
		from
			temp_parents_ch s inner join
			cyf_place_clean t
			on
			s.parent = t.snz_uid
		order by s.child_snz_uid;
run;

data  TEMP_CYF_N2;
	set  TEMP_CYF_N1;
	by child_snz_uid;
	retain
		cg_&sex._not_ever
		cg_&sex._Pol_FV_not_ever
		cg_&sex._YJ_referral_ever;

	%cyf_notifications(child_snz_uid,dob,%str(-99),%str(99),cg_&sex.,ever);


	if last.child_snz_uid then
		output;
		keep child_snz_uid cg_&sex._not_ever
		cg_&sex._Pol_FV_not_ever
		cg_&sex._YJ_referral_ever;
run;

data TEMP_CYF_a2;
	set TEMP_CYF_a1;
	by child_snz_uid;
	retain       	
		cg_&sex._fdgs_neglect_ever
		cg_&sex._fdgs_phys_abuse_ever
		cg_&sex._fdgs_emot_abuse_ever
		cg_&sex._fdgs_sex_abuse_ever
		cg_&sex._fdgs_behav_rel_ever
		cg_&sex._fdgs_sh_suic_ever
		cg_&sex._any_fdgs_abuse_ever;


	%cyf_findings(child_snz_uid,dob,-99,99,cg_&sex.,ever);

	
	if last.child_snz_uid then
		output;

	keep child_snz_uid 
		cg_&sex._fdgs_neglect_ever
		cg_&sex._fdgs_phys_abuse_ever
		cg_&sex._fdgs_emot_abuse_ever
		cg_&sex._fdgs_sex_abuse_ever
		cg_&sex._fdgs_behav_rel_ever
		cg_&sex._fdgs_sh_suic_ever
		cg_&sex._any_fdgs_abuse_ever;
run;

data  TEMP_CYF_p2;
	set  TEMP_CYF_p1;
	by child_snz_uid;
	retain
		cg_&sex._CYF_place_ever
		cg_&sex._YJ_place_ever;

	%cyf_placements(child_snz_uid,dob,-99,99,cg_&sex.,ever);

	if last.child_snz_uid then
		output;
	keep child_snz_uid 
		cg_&sex._CYF_place_ever
		cg_&sex._YJ_place_ever;
run;

data &projectlib.._&rel._cg_cyf_&sex._&date.; retain snz_uid DOB;
	merge &population (in=inpop rename=(snz_uid=child_snz_uid)) TEMP_CYF_n2 (in=in_notifications) 
		TEMP_CYF_p2 (in=in_placements) TEMP_CYF_a2
		(in=in_findings);
	by child_snz_uid;
	snz_uid=child_snz_uid;
	drop child_snz_uid;

	if not in_notifications Then
		cg_&sex._not_ever=0;

	IF not in_notifications Then
		cg_&sex._Pol_FV_not_ever=0;

	IF not in_notifications Then
		cg_&sex._YJ_referral_ever=0;

	IF not in_findings Then
		cg_&sex._fdgs_neglect_ever=0;

	IF not in_findings Then
		cg_&sex._fdgs_phys_abuse_ever=0;

	IF not in_findings Then
		cg_&sex._fdgs_emot_abuse_ever=0;

	IF not in_findings Then
		cg_&sex._fdgs_sex_abuse_ever=0;

	IF not in_findings Then
		cg_&sex._fdgs_behav_rel_ever=0;

	IF not in_findings Then
		cg_&sex._fdgs_sh_suic_ever=0;

	IF not in_placements Then
		cg_&sex._CYF_place_ever=0;

	IF not in_placements Then
		cg_&sex._YJ_place_ever=0;

	if not in_findings then
		cg_&sex._any_fdgs_abuse_ever=0;
	keep snz_uid DOB cg_:;
run;
proc datasets lib=work;
delete temp: CYF:;
run;
%mend;