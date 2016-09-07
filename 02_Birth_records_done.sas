/*******************************************************************************************************************************************
*******************************************************************************************************************************************
Developer: Sarah Tumen (A&I, Treasury)
Date Created: 7 Dec 2015

This code creates number of children mothered/fathered by reference person in a given calendar year

*/
proc sql;
	create table parent1 as select 
		snz_uid as child_snz_uid,
		dia_bir_sex_snz_code as child_sex_snz_code,
		dia_bir_still_birth_code as child_still_birth,
		dia_bir_multiple_birth_code as miltiple_birth,
		dia_bir_birth_month_nbr as birth_month,
		dia_bir_birth_year_nbr as birth_year,
		dia_bir_parent1_child_rel_text as child_parent1_rel,
		parent1_snz_uid
	from dia.births
		where parent1_snz_uid  in (select snz_uid from &population) 
			and MDY(dia_bir_birth_month_nbr,15,dia_bir_birth_year_nbr)<="&sensor"d
			and (parent1_snz_uid ne parent2_snz_uid or parent2_snz_uid=.)
		order by parent1_snz_uid;
quit;

proc sql;
	create table parent2 as select 
		snz_uid as child_snz_uid,
		dia_bir_sex_snz_code as child_sex_snz_code,
		dia_bir_still_birth_code as child_still_birth,
		dia_bir_multiple_birth_code as miltiple_birth,
		dia_bir_birth_month_nbr as birth_month,
		dia_bir_birth_year_nbr as birth_year,
		dia_bir_parent2_child_rel_text as child_parent2_rel,
		parent2_snz_uid
	from dia.births
		where parent2_snz_uid  in (select snz_uid from &population)
			and MDY(dia_bir_birth_month_nbr,15,dia_bir_birth_year_nbr)<="&sensor"d
			and (parent2_snz_uid ne parent1_snz_uid or parent1_snz_uid=.)
		order by parent2_snz_uid;
quit;

data parent1;
	set parent1;

	if child_still_birth not in ("S","D");
run;

data parent2;
	set parent2;

	if child_still_birth not in ("S","D");
run;

proc summary data=parent1 nway;
	class parent1_snz_uid birth_year;
	var child_snz_uid;
	output out=mother(drop=_type_ _freq_ rename=parent1_snz_uid=snz_uid) N=mother_child_nbr;
run;

proc summary data=parent2 nway;
	class parent2_snz_uid birth_year;
	var child_snz_uid;
	output out=father(drop=_type_ _freq_ rename=parent2_snz_uid=snz_uid)  N=father_child_nbr;
run;

data project.IND_parent_&date;
	merge mother father;
	by snz_uid birth_year;

	if mother_child_nbr=. then
		mother_child_nbr=0;

	if father_child_nbr=. then
		father_child_nbr=0;
	rename birth_year=year;

run;

data _ind_parent;
	merge project.ind_parent_&date &population(keep=snz_uid DOB);
	by snz_uid;
	array parent_id_(*) parent_id_&first_anal_yr-parent_id_&last_anal_yr;
	array mother_(*) mother_&first_anal_yr-mother_&last_anal_yr;
	array father_(*) father_&first_anal_yr-father_&last_anal_yr;

	do i=&first_anal_yr to &last_anal_yr;
		ind=i-(&first_anal_yr-1);
		mother_(ind)=0;
		father_(ind)=0;
		parent_id_(ind)=0;

		if i=birth_year and mother_child_nbr>0 then
			mother_(ind)=mother_child_nbr;

		if i=birth_year and father_child_nbr>0 then
			father_(ind)=father_child_nbr;

		if i=birth_year and (father_child_nbr>0 or mother_child_nbr>0) then
			parent_id_(ind)=1;
	end;

	drop ind i;
run;

proc summary data=_ind_parent nway;
	class snz_uid;
	var parent_id_&first_anal_yr-parent_id_&last_anal_yr 
		mother_&first_anal_yr-mother_&last_anal_yr
		father_&first_anal_yr-father_&last_anal_yr;
	output out=project._ind_parent_&date (drop=_type_ _freq_) sum=;
run;

proc datasets lib=work kill nolist memtype=data;
quit;