/****************************************************************************************************************************************
****************************************************************************************************************************************

Code to aggregate all the history for the children by year
and calculate 

Developer: Rissa Ota ( on secondment to A&I from MSD)
Date: 28/09/2015


In order to calculate 4 risk factors for children the following 
*/

options mprint;

%let first_anal_yr2 = %eval(&first_anal_yr.+1);
%let last_anal_yr2 = %eval(&last_anal_yr.+1);

%macro transform_year_2_by_yr(var_name, summary);
	array &var_name._year_[*] &var_name._year_&first_anal_yr.-&var_name._year_&last_anal_yr.;
	array &var_name._by_yr_[*] &var_name._by_yr_&first_anal_yr2.-&var_name._by_yr_&last_anal_yr2.;

	&var_name._by_yr_&first_anal_yr2. = .;

	do ind  =  &first_anal_yr2.+1 to &last_anal_yr2.;
		i = ind-(&first_anal_yr2.-1);

		&var_name._by_yr_[i] = &summary.(&var_name._by_yr_[i-1]
									    ,&var_name._year_[i]);
	end;
%mend transform_year_2_by_yr;

%macro transform_noyr_2_by_yr(var_name, summary);
	array &var_name._[*] &var_name._&first_anal_yr.-&var_name._&last_anal_yr.;
	array &var_name._by_yr_[*] &var_name._by_yr_&first_anal_yr2.-&var_name._by_yr_&last_anal_yr2.;

	&var_name._by_yr_&first_anal_yr2. = .;

	do ind  =  &first_anal_yr2.+1 to &last_anal_yr2.;
		i = ind-(&first_anal_yr2.-1);

		&var_name._by_yr_[i] = &summary.(&var_name._by_yr_[i-1]
									    ,&var_name._[i]);
	end;
%mend transform_noyr_2_by_yr;

%macro transform_edu_noyr_2_by_yr(var_name, summary);
	array &var_name._[*] &var_name._&first_anal_yr.-&var_name._&last_anal_yr.;
	array &var_name._by_yr_[*] &var_name._by_yr_&first_anal_yr2.-&var_name._by_yr_&last_anal_yr2.;

	&var_name._by_yr_&first_anal_yr2. = &var_name._prior_&first_anal_yr.;

	do ind  =  &first_anal_yr2.+1 to &last_anal_yr2.;
		i = ind-(&first_anal_yr2.-1);

		&var_name._by_yr_[i] = &summary.(&var_name._by_yr_[i-1]
									    ,&var_name._[i]);
	end;
%mend transform_edu_noyr_2_by_yr;

proc sort data=&population.;
	by snz_uid;

data project.risk_ind_yr_&date.
		(drop =  i ind);
	merge &population (in=a)
	 	  project._ind_bdd_child_at_age_&date. (keep = snz_uid supp:)
		  project._ind_bdd_child_&date. (keep=snz_uid ch_total_da_onben:)
		  project._IND_CG_CORR_&date.
		  project._IND_CYF_&date.
		  project._IND_CYF_CARE_&date.
		  project._IND_SIBLING_CYF_&date.
		  project._MAT_EDUC_COMB_YR_&date.
	;
	by snz_uid; 
	if a;
	array prop_of_life_onben_by_yr_[*] prop_of_life_onben_by_yr_&first_anal_yr2.-prop_of_life_onben_by_yr_&last_anal_yr2.;
	array WI_onben_ge75_by_yr_[*] WI_onben_ge75_by_yr_&first_anal_yr2.-WI_onben_ge75_by_yr_&last_anal_yr2.;
	array child_all_not_by_yr_[*] child_all_not_by_yr_&first_anal_yr2.-child_all_not_by_yr_&last_anal_yr2.;


	array maternal_no_edu_by_yr_[*] maternal_no_edu_by_yr_&first_anal_yr2.-maternal_no_edu_by_yr_&last_anal_yr2.;
	array CYF_risk_by_yr_[*] CYF_risk_by_yr_&first_anal_yr2.-CYF_risk_by_yr_&last_anal_yr2.;
	array CYF_risk_v2_by_yr_[*] CYF_risk_v2_by_yr_&first_anal_yr2.-CYF_risk_v2_by_yr_&last_anal_yr2.;
	array CYF_risk_v3_by_yr_[*] CYF_risk_v3_by_yr_&first_anal_yr2.-CYF_risk_v3_by_yr_&last_anal_yr2.;
	array CYF_risk_v4_by_yr_[*] CYF_risk_v4_by_yr_&first_anal_yr2.-CYF_risk_v4_by_yr_&last_anal_yr2.;
	array CORR_risk_by_yr_[*] CORR_risk_by_yr_&first_anal_yr2.-CORR_risk_by_yr_&last_anal_yr2.;
	array WI_risk_by_yr_[*] WI_risk_by_yr_&first_anal_yr2.-WI_risk_by_yr_&last_anal_yr2.;
	array risk_factors_by_year_[*] risk_factors_by_year_&first_anal_yr2.-risk_factors_by_year_&last_anal_yr2.;
	array risk_factors_2plus_by_year_[*] risk_factors_2plus_by_year_&first_anal_yr2.-risk_factors_2plus_by_year_&last_anal_yr2.;
	array risk_factors_v2_by_year_[*] risk_factors_v2_by_year_&first_anal_yr2.-risk_factors_v2_by_year_&last_anal_yr2.;
	array risk_factors_v2_2plus_by_yr_[*] risk_factors_v2_2plus_by_yr_&first_anal_yr2.-risk_factors_v2_2plus_by_yr_&last_anal_yr2.;

	%transform_noyr_2_by_yr(ch_total_da_onben, sum); 

	%transform_year_2_by_yr(child_not, sum);
	%transform_year_2_by_yr(child_Pol_FV_not, sum);

	%transform_year_2_by_yr(child_any_fdgs_abuse, sum);
	%transform_year_2_by_yr(child_fdgs_emot_abuse, sum);
	%transform_year_2_by_yr(child_fdgs_neg, sum);
	%transform_year_2_by_yr(child_fdgs_phys_abuse, sum);
	%transform_year_2_by_yr(child_fdgs_sex_abuse, sum);
	%transform_year_2_by_yr(child_fdgs_behav_rel, sum);

	%transform_year_2_by_yr(child_CYF_place, sum);
	%transform_year_2_by_yr(child_CYF_CE, sum);
	%transform_year_2_by_yr(child_YJ_CE, sum);

	%transform_year_2_by_yr(othchd_any_fdgs_abuse, sum);
	%transform_year_2_by_yr(othchd_not, sum);
	%transform_year_2_by_yr(othchd_Pol_FV_not, sum);
	%transform_year_2_by_yr(othchd_CYF_place, sum);
	%transform_year_2_by_yr(othchd_CYF_CE, sum);
	%transform_year_2_by_yr(othchd_YJ_CE, sum);

	%transform_edu_noyr_2_by_yr(maternal_edu, max);
	%transform_year_2_by_yr(cg_cust, sum);
	%transform_year_2_by_yr(cg_comm, sum);

	cohort = year(dob);

	do ind  =  &first_anal_yr2. to &last_anal_yr2.;
		i = ind-(&first_anal_yr2.-1);
		
		child_all_not_by_yr_[i] = sum(child_not_by_yr_[i], child_Pol_FV_not_by_yr_[i]);

	end;

	supp_ben_at_birth = max(supp_JSHCD_atbirth,
							supp_JSWR_TR_atbirth,
							supp_JSWR_atbirth,
							supp_OTH_atbirth,
							supp_SLP_C_atbirth,
							supp_SLP_HCD_atbirth,
							supp_SPSR_atbirth,
							supp_YPP_atbirth,
							supp_YP_atbirth,
							supp_dpb_atbirth,
							supp_ib_atbirth,
							supp_othben_atbirth,
							supp_sb_atbirth,
							supp_ub_atbirth,
							supp_ucb_atbirth
							);

	supp_ben_sole_parent_at_birth = supp_SPSR_atbirth;

	if (supp_ben_sole_parent_at_birth and mother) or 
	   (dia_no_father_in_birth_reg and mother) or
	    dia_parents_not_in_rel then mother_single_at_birth = 1;
	else if mother then mother_single_at_birth = 0;
	else mother_single_at_birth = .;



	do ind  =  &first_anal_yr2. to &last_anal_yr2.;
		i = ind-(&first_anal_yr2.-1);
		days_of_life = mdy(1,1,ind)-1 - dob;

		if days_of_life>0 
			then prop_of_life_onben_by_yr_[i]= ch_total_da_onben_by_yr_[i]/days_of_life;

		WI_onben_ge75_by_yr_[i]= (prop_of_life_onben_by_yr_[i] ge 0.75);

		if (ind-1) lt cohort then do;
			WI_risk_by_yr_[i] = .; 
		end;
		else if (ind-1) = cohort then do;
			if supp_ben_at_birth or WI_onben_ge75_by_yr_[i]
			then WI_risk_by_yr_[i] = 1; 
			else WI_risk_by_yr_[i] = 0;
		end;
		else if (ind-1) le cohort+1 then do;
			if supp_ben_at_birth or WI_onben_ge75_by_yr_[i]
			then WI_risk_by_yr_[i] = 1; 
			else WI_risk_by_yr_[i] = 0;
		end;
		else do;
			if WI_onben_ge75_by_yr_[i] 
			then WI_risk_by_yr_[i] = 1; 
			else WI_risk_by_yr_[i] = 0; 
		end;
	end;

	do ind  =  &first_anal_yr2. to &last_anal_yr2.;
		i = ind-(&first_anal_yr2.-1);

		CYF_risk_v3_by_yr_[i] = ((sum(child_any_fdgs_abuse_by_yr_[i]
									  ,child_CYF_place_by_yr_[i]
									  ,child_CYF_CE_by_yr_[i]
									  ,child_all_not_by_yr_[i]
								 ))>0);

		CYF_risk_v4_by_yr_[i] = ((sum(child_any_fdgs_abuse_by_yr_[i]
									  ,child_CYF_place_by_yr_[i]
									  ,child_CYF_CE_by_yr_[i]
									  ,child_all_not_by_yr_[i]
									  ,othchd_any_fdgs_abuse_by_yr_[i]
									  ,othchd_not_by_yr_[i]
									  ,othchd_CYF_place_by_yr_[i]
									  ,othchd_CYF_CE_by_yr_[i]
								  	  ,othchd_Pol_FV_not_by_yr_[i]
									 ))>0);

		if ind lt cohort then do;
			CYF_risk_by_yr_[i] = .; 
			CYF_risk_v2_by_yr_[i] = .; 
			CYF_risk_v3_by_yr_[i] = .; 
			CYF_risk_v4_by_yr_[i] = .; 
		end;
		else if (ind-1) = cohort then do;
			CYF_risk_by_yr_[i] = ((sum(child_any_fdgs_abuse_by_yr_[i]
									  ,child_CYF_place_by_yr_[i]
									  ,child_CYF_CE_by_yr_[i]
									  ,child_not_by_yr_[i]
									  ,othchd_any_fdgs_abuse_by_yr_[i]
									  ,othchd_not_by_yr_[i]
									  ,othchd_CYF_place_by_yr_[i]
									  ,othchd_CYF_CE_by_yr_[i]
									 ))>0);

			CYF_risk_v2_by_yr_[i] = ((sum(CYF_risk_by_yr_[i]							  
									  	 ,child_all_not_by_yr_[i]
									  	 ,othchd_Pol_FV_not_by_yr_[i]
									 ))>0);

		end;
		else if (ind-1) le cohort+2 then do;
			CYF_risk_by_yr_[i] = (sum(child_any_fdgs_abuse_by_yr_[i]
									 ,child_CYF_place_by_yr_[i]
									 ,child_CYF_CE_by_yr_[i]
									 ,othchd_any_fdgs_abuse_by_yr_[i]
									 ,othchd_CYF_place_by_yr_[i]
									 ,othchd_CYF_CE_by_yr_[i]
									 )>0);

			CYF_risk_v2_by_yr_[i] = ((sum(CYF_risk_by_yr_[i]							  
									  	 ,child_all_not_by_yr_[i]
									  	 ,othchd_Pol_FV_not_by_yr_[i]
									 ))>0);

		end;
		else do;
			CYF_risk_by_yr_[i] = (sum(child_any_fdgs_abuse_by_yr_[i]
									 ,child_CYF_place_by_yr_[i]
									 ,child_CYF_CE_by_yr_[i]
									 )>0);

			CYF_risk_v2_by_yr_[i] = ((sum(CYF_risk_by_yr_[i]							  
									  	,child_all_not_by_yr_[i]
									 ))>0);
		end;
	end;

	do ind  =  &first_anal_yr2. to &last_anal_yr2.;
		i = ind-(&first_anal_yr2.-1);

		if ind lt cohort then maternal_no_edu_by_yr_[i] = .; 
		else maternal_no_edu_by_yr_[i] = (maternal_edu_by_yr_[i] in (0, 0.5));
	end;

	do ind  =  &first_anal_yr2. to &last_anal_yr2.;
		i = ind-(&first_anal_yr2.-1);

		CORR_risk_by_yr_[i] = (sum(cg_cust_by_yr_[i], cg_comm_by_yr_[i])>0);
	end;

	do ind  =  &first_anal_yr2. to &last_anal_yr2.;
		i = ind-(&first_anal_yr2.-1);

		if (ind-1) lt cohort then do;
			risk_factors_by_year_[i] = .; 
			risk_factors_2plus_by_year_[i] = .;
			risk_factors_v2_by_year_[i] = .; 
			risk_factors_v2_2plus_by_yr_[i] = .;
		end;
		else do;
			risk_factors_by_year_[i]= sum(WI_risk_by_yr_[i]
									  ,CYF_risk_by_yr_[i]
									  ,CORR_risk_by_yr_[i]
									  ,maternal_no_edu_by_yr_[i]
									  ,0);

			risk_factors_2plus_by_year_[i]=(risk_factors_by_year_[i] ge 2);

			risk_factors_v2_by_year_[i]= sum(WI_risk_by_yr_[i]
									  ,CYF_risk_v2_by_yr_[i]
									  ,CORR_risk_by_yr_[i]
									  ,maternal_no_edu_by_yr_[i]
									  ,0);

			risk_factors_v2_2plus_by_yr_[i]=(risk_factors_v2_by_year_[i] ge 2);

		end;
	end;

run;
