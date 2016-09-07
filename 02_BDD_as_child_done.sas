/*************************************************************************************************************************************
*************************************************************************************************************************************
Developer: Sarah Tumen and Robert Templeton ( A&I, NZ Treasury)
Date created: 7 Dec 2015

This code uses the child caregiver relationship from benefit data and calculates days in year supported by benefit using caregiver's 
benefit history.


*/
%creating_MSD_spel;

proc freq data=msd_spel;
	tables BEN*ben_new/list missing;
run;

proc freq data=msd_spel;
	where prereform='675';
	tables BEN*ben_new/list missing;
run;

proc freq data=msd_spel;
	where prereform='370';
	tables BEN*ben_new/list missing;
run;

%create_BDD_child_spell;

data msd_spel;
	set msd_spel;
	spell=msd_spel_spell_nbr;
	keep snz_uid spellfrom spellto spell servf ben ben_new;
run;

proc sort data = ICD_BDD_chd;
	by snz_uid spell;
run;

proc sort data = msd_spel;
	by snz_uid spell;
run;

data chd_mainben;
	merge    ICD_BDD_chd (in = y) msd_spel (keep = snz_uid ben ben_new spell);
	by snz_uid spell;

	if y;
run;

proc sort data=chd_mainben;
	by child_snz_uid chfrom chto;
run;

data feed;
	set chd_mainben;
	format startdate enddate date9.;
	rename snz_uid=adult_snz_uid;
	rename child_snz_uid=snz_uid;
	startdate=chfrom;
	enddate=chto;
run;

%overlap(feed);

proc sql;
	create table feed_new1
		as select 
			a.*,
			b.DOB
		from feed_OR a inner join &population b
			on a.snz_uid=b.snz_uid where startdate>(DOB-45)
		order by snz_uid, startdate 
	;
quit;

%aggregate_by_year(feed_new1,feed_new2,&first_anal_yr,&last_anal_yr,gross_daily_amt=);

data feed_new2;
	set feed_new2;
	da_DPB=0;
	da_IB=0;
	da_ub=0;
	da_IYB=0;
	da_SB=0;
	da_UCB=0;
	da_OTHBEN=0;
	da_YP=0;
	da_YPP=0;
	da_SPSR=0;
	da_SLP_C=0;
	da_SLP_HCD=0;
	da_JSHCD=0;
	da_JSWR=0;
	da_JSWR_TR=0;
	da_OTH=0;

	if ben='DPB' then
		da_DPB=days;

	if ben='IB' then
		da_IB=days;

	if ben='UB' then
		da_UB=days;

	if ben='IYB' then
		da_IYB=days;

	if ben='SB' then
		da_SB=days;

	if ben='UCB' then
		da_UCB=days;

	if ben='OTH' then
		da_OTHBEN=days;

	if ben_new='JSHCD' then
		da_JSHCD=days;

	if ben_new='JSWR' then
		da_JSWR=days;

	if ben_new='JSWR_TR' then
		da_JSWR_TR=days;

	if ben_new='OTH' then
		da_OTH=days;

	if ben_new='SLP_C' then
		da_SLP_C=days;

	if ben_new='SLP_HCD' then
		da_SLP_HCD=days;

	if ben_new='SPSR' then
		da_SPSR=days;

	if ben_new='YP' then
		da_YP=days;

	if ben_new='YPP' then
		da_YPP=days;
run;

proc sort data=feed_new2;
	by snz_uid year;
run;

proc summary data=feed_new2 nway;
	var days da_dpb da_ib da_ub da_iyb da_sb da_ucb da_othben
		da_YP da_YPP da_SPSR da_SLP_C da_SLP_HCD da_JSHCD da_JSWR  da_JSWR_TR da_OTH;
	by snz_uid year;
	output out=project.IND_BDD_child_&date(drop=_type_ _freq_ rename=days=total_da_onben) sum=;
run;

%create_BDD_long(merge1=project.IND_BDD_child_&date,merge2=cohort_1,outfile=project._IND_BDD_child_&date);

%create_BDD_at_age_child(infile=feed_new1,outfile=project._IND_BDD_child_at_age_&date);

data project.IND_BDD_child_&date;
	set project.IND_BDD_child_&date;
	rename 	total_da_onben	=ch_total_da_onben;
	rename 	da_DPB	=ch_da_DPB;
	rename 	da_IB	=ch_da_IB;
	rename 	da_ub	=ch_da_ub;
	rename 	da_IYB	=ch_da_IYB;
	rename 	da_SB	=ch_da_SB;
	rename 	da_UCB	=ch_da_UCB;
	rename 	da_OTHBEN	=ch_da_OTHBEN;
	rename 	da_YP	=ch_da_YP;
	rename 	da_YPP	=ch_da_YPP;
	rename 	da_SPSR	=ch_da_SPSR;
	rename 	da_SLP_C	=ch_da_SLP_C;
	rename 	da_SLP_HCD	=ch_da_SLP_HCD;
	rename 	da_JSHCD	=ch_da_JSHCD;
	rename 	da_JSWR	=ch_da_JSWR;
	rename 	da_JSWR_TR	=ch_da_JSWR_TR;
	rename 	da_OTH	=ch_da_OTH;
run;

data project._IND_BDD_child_at_age_&date;
	set project._IND_BDD_child_at_age_&date;
	rename total_da_onben_at_age_&firstage-total_da_onben_at_age_&lastage=ch_total_da_onben_at_age_&firstage-ch_total_da_onben_at_age_&lastage;
	rename da_DPB_at_age_&firstage-da_DPB_at_age_&lastage=ch_da_DPB_at_age_&firstage-ch_da_DPB_at_age_&lastage;
	rename da_UB_at_age_&firstage-da_UB_at_age_&lastage=ch_da_UB_at_age_&firstage-ch_da_UB_at_age_&lastage;
	rename da_SB_at_age_&firstage-da_SB_at_age_&lastage=ch_da_SB_at_age_&firstage-ch_da_SB_at_age_&lastage;
	rename da_IB_at_age_&firstage-da_IB_at_age_&lastage=ch_da_IB_at_age_&firstage-ch_da_IB_at_age_&lastage;
	rename da_IYB_at_age_&firstage-da_IYB_at_age_&lastage=ch_da_IYB_at_age_&firstage-ch_da_IYB_at_age_&lastage;
	rename da_OTHBEN_at_age_&firstage-da_OTHBEN_at_age_&lastage=ch_da_OTHBEN_at_age_&firstage-ch_da_OTHBEN_at_age_&lastage;
	rename da_UCB_at_age_&firstage-da_UCB_at_age_&lastage=ch_da_UCB_at_age_&firstage-ch_da_UCB_at_age_&lastage;
	rename da_YP_at_age_&firstage-da_YP_at_age_&lastage=ch_da_YP_at_age_&firstage-ch_da_YP_at_age_&lastage;
	rename da_YPP_at_age_&firstage-da_YPP_at_age_&lastage=ch_da_YPP_at_age_&firstage-ch_da_YPP_at_age_&lastage;
	rename da_SPSR_at_age_&firstage-da_SPSR_at_age_&lastage=ch_da_SPSR_at_age_&firstage-ch_da_SPSR_at_age_&lastage;
	rename da_JSWR_at_age_&firstage-da_JSWR_at_age_&lastage=ch_da_JSWR_at_age_&firstage-ch_da_JSWR_at_age_&lastage;
	rename da_JSWR_TR_at_age_&firstage-da_JSWR_TR_at_age_&lastage=ch_da_JSWR_TR_at_age_&firstage-ch_da_JSWR_TR_at_age_&lastage;
	rename da_JSHCD_at_age_&firstage-da_JSHCD_at_age_&lastage=ch_da_JSHCD_at_age_&firstage-ch_da_JSHCD_at_age_&lastage;
	rename da_SLP_C_at_age_&firstage-da_SLP_C_at_age_&lastage=ch_da_SLP_C_at_age_&firstage-ch_da_SLP_C_at_age_&lastage;
	rename da_SLP_HCD_at_age_&firstage-da_SLP_HCD_at_age_&lastage=ch_da_SLP_HCD_at_age_&firstage-ch_da_SLP_HCD_at_age_&lastage;
	rename da_OTH_at_age_&firstage-da_OTH_at_age_&lastage=ch_da_OTH_at_age_&firstage-ch_da_OTH_at_age_&lastage;
run;

data project._IND_BDD_child_&date;
	set project._IND_BDD_child_&date;
	rename total_da_onben_&first_anal_yr-total_da_onben_&last_anal_yr=ch_total_da_onben_&first_anal_yr-ch_total_da_onben_&last_anal_yr;
	rename da_DPB_&first_anal_yr-da_DPB_&last_anal_yr=ch_da_DPB_&first_anal_yr-ch_da_DPB_&last_anal_yr;
	rename da_UB_&first_anal_yr-da_UB_&last_anal_yr=ch_da_UB_&first_anal_yr-ch_da_UB_&last_anal_yr;
	rename da_SB_&first_anal_yr-da_SB_&last_anal_yr=ch_da_SB_&first_anal_yr-ch_da_SB_&last_anal_yr;
	rename da_IB_&first_anal_yr-da_IB_&last_anal_yr=ch_da_IB_&first_anal_yr-ch_da_IB_&last_anal_yr;
	rename da_IYB_&first_anal_yr-da_IYB_&last_anal_yr=ch_da_IYB_&first_anal_yr-ch_da_IYB_&last_anal_yr;
	rename da_OTHBEN_&first_anal_yr-da_OTHBEN_&last_anal_yr=ch_da_OTHBEN_&first_anal_yr-ch_da_OTHBEN_&last_anal_yr;
	rename da_UCB_&first_anal_yr-da_UCB_&last_anal_yr=ch_da_UCB_&first_anal_yr-ch_da_UCB_&last_anal_yr;
	rename da_YP_&first_anal_yr-da_YP_&last_anal_yr=ch_da_YP_&first_anal_yr-ch_da_YP_&last_anal_yr;
	rename da_YPP_&first_anal_yr-da_YPP_&last_anal_yr=ch_da_YPP_&first_anal_yr-ch_da_YPP_&last_anal_yr;
	rename da_SPSR_&first_anal_yr-da_SPSR_&last_anal_yr=ch_da_SPSR_&first_anal_yr-ch_da_SPSR_&last_anal_yr;
	rename da_JSWR_&first_anal_yr-da_JSWR_&last_anal_yr=ch_da_JSWR_&first_anal_yr-ch_da_JSWR_&last_anal_yr;
	rename da_JSWR_TR_&first_anal_yr-da_JSWR_TR_&last_anal_yr=ch_da_JSWR_TR_&first_anal_yr-ch_da_JSWR_TR_&last_anal_yr;
	rename da_JSHCD_&first_anal_yr-da_JSHCD_&last_anal_yr=ch_da_JSHCD_&first_anal_yr-ch_da_JSHCD_&last_anal_yr;
	rename da_SLP_C_&first_anal_yr-da_SLP_C_&last_anal_yr=ch_da_SLP_C_&first_anal_yr-ch_da_SLP_C_&last_anal_yr;
	rename da_SLP_HCD_&first_anal_yr-da_SLP_HCD_&last_anal_yr=ch_da_SLP_HCD_&first_anal_yr-ch_da_SLP_HCD_&last_anal_yr;
	rename da_OTH_&first_anal_yr-da_OTH_&last_anal_yr=ch_da_OTH_&first_anal_yr-ch_da_OTH_&last_anal_yr;
run;

proc datasets lib=work kill nolist memtype=data;
quit;