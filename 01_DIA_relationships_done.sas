proc sql;
	connect to odbc(dsn="idi_clean_&version._srvprd");
	create table work.ParentChildMap as
		select distinct snz_uid, dia_bir_birth_year_nbr, dia_bir_birth_month_nbr, 
						parent1_snz_uid, parent2_snz_uid
			from connection to odbc
				( select * from dia_clean.births );
	disconnect from odbc;
quit;

proc sql;
	connect to odbc(dsn = IDI_clean_&version._srvprd);
	create table 
		conc_dia 
	as 
	select 
		* 
	from 
		connection to odbc
	(select 
		snz_uid, 
		snz_dia_uid
	from 
		security.concordance
	where
		snz_dia_uid is not null
	order by snz_dia_uid);
quit;

proc sql;
	create table work.bir_parent_dia_uid_mapping as
		select 
			 a.from_dia_uid
			,a.to_dia_uid
			,p1.snz_uid as from_snz_uid
			,p2.snz_uid as to_snz_uid
			 
		from sanddia.bir_parent_dia_uid_mapping a 
		left join Work.conc_dia p1
			on a.from_dia_uid = p1.snz_dia_uid
		left join Work.conc_dia p2
			on a.to_dia_uid = p2.snz_dia_uid;
quit;

proc sql;
	create table work.ParentChildMap as
		select 
			a.snz_uid
			,a.dia_bir_birth_year_nbr
			,a.dia_bir_birth_month_nbr
			,a.parent1_snz_uid as parent1_snz_uid_old
			,a.parent2_snz_uid as parent2_snz_uid_old
			,p1.to_snz_uid as parent1_snz_uid
			,p2.to_snz_uid as parent2_snz_uid
		from Work.ParentChildMap as a
		left join Work.bir_parent_dia_uid_mapping p1
			on a.parent1_snz_uid = p1.from_snz_uid
			and a.parent1_snz_uid
		left join Work.bir_parent_dia_uid_mapping p2
			on a.parent2_snz_uid = p2.from_snz_uid
			and a.parent2_snz_uid
		;
quit;

data ParentChildMap;
	set ParentChildMap;

	if parent1_snz_uid_old and parent1_snz_uid = . then 
	   parent1_snz_uid = parent1_snz_uid_old;

	if parent2_snz_uid_old and parent2_snz_uid = . then 
	   parent2_snz_uid = parent2_snz_uid_old;
run;

data work.ParentChildMap;
	format eventdate ddmmyy10.;
	set work.ParentChildMap;

	eventdate = mdy(dia_bir_birth_month_nbr,01,dia_bir_birth_year_nbr);
	source = 'dia';
	
	if parent1_snz_uid = . and Parent2_snz_uid = . then
		parent1 = .;
	else parent1 = min(parent1_snz_uid,Parent2_snz_uid);

	if parent1_snz_uid = . or Parent2_snz_uid = . then
		parent2 = .;
	else parent2 = max(parent1_snz_uid,Parent2_snz_uid);
run;

proc sort data=ParentChildMap nodupkey dupout=dup2;
	by snz_uid;
run;

data work.PC1;
	set work.parentchildmap;
	parent = parent1_snz_uid;

	if parent = . then delete;

	drop dia_bir_birth_month_nbr dia_bir_birth_year_nbr parent1_snz_uid parent2_snz_uid;
run;

data work.PC2;
	set work.parentchildmap;
	parent = parent2_snz_uid;

	if parent = . then delete;

	drop dia_bir_birth_month_nbr dia_bir_birth_year_nbr parent1_snz_uid parent2_snz_uid;
run;

proc append base = Work.PC1 data = Work.PC2 force;
run;

proc sort data=work.PC1 noduprecs;
	by parent snz_uid eventdate;
run;

data project.ChildToParentMap_&date (drop=dia_bir_birth_year_nbr dia_bir_birth_month_nbr 
		parent1_snz_uid parent2_snz_uid);
	set Work.parentchildmap;
run;

data project.ParentToChildMap_&date;
	set Work.PC1;
run;

proc sql;
	create table work.SiblingBase as
		select a.snz_uid as snz_uid,
			b.snz_uid as sibling, 
			a.parent as parent,
			a.eventdate as start1, 
			b.eventdate as start2, 
			a.source as Source, 
			b.source as SibSource 
		from Work.PC1 a 
		left join Work.PC1 b
			on a.parent = b.parent
		where a.snz_uid ne b.snz_uid;

	create table work.SiblingExtended as
		select distinct snz_uid, sibling, 1 as CNT
		from work.SiblingBase
		order by snz_uid;

	create table work.SiblingEvent as
		select snz_uid, sibling, parent, 
				max(start1,start2) as startdate format = ddmmyy10., 
				Source, SibSource
		from work.SiblingBase
		order by snz_uid;

	create table work.SiblingEvCnt as
		select distinct snz_uid, sibling, 1 as CNT
		from work.SiblingEvent
		order by snz_uid;
quit;

data project.ChildSiblingMapExt_&date;
	set work.SiblingExtended;
run;

data project.ChildSiblMapEvent_&date;
	set work.SiblingEvent;
run;
