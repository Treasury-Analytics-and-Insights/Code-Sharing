
proc format;
	value $lv8idd
		"40","41","46", "60", "96", "98"      ="1"
		"36"-"37","43"                        ="2"
		"30"-"35"                       ="3"
		"20","25","21"                  ="4"
		"12"-"14"                       ="6"
		"11"                            ="7"
		"01","10"                       ="8"
		"90", "97", "99"                ="9"
		Other                           ="0";
run;

proc format;
	value $lv8id
		"40","41","46", "60", "96", "98"      ="Level 1-3 certificates"
		"36"-"37","43"                        ="Level 4 Certificates"
		"30"-"35"                       ="Certificates and Diploma Level 5-7"
		"20","21","25"                       ="Bachelor degrees"
		"12"-"14"                       ="Honours, postgrad dipl"
		"11"                            ="Masters degrees"
		"01","10"                       ="Doctoral degrees"
		"90", "97", "99"                ="Non formal programmes"
		Other                           ="Error";
run;


proc format;
	value $subsector
		"1","3"="Universities"
		"2"="Polytechnics"
		"4"="Wananga"
		"5","6"="Private Training Establishments";
run;

proc format;
value $field
'01'='Natural and Physical Sciences'
'02'='Information Technology'
'03'='Engineering and Related Technologies'
'04'='Agriculture and Building'
'05'='Agriculture, Environmental and Related Studies'
'06'='Health'
'07'='Education'
'08'='Management and Commerce'
'09'='Society and Culture'
'10'='Creative Arts'
'11'='Food, Hospitality and PErsonal Services'
'12'='Mixed Field Programme';
run;


proc format ;
VALUE $bengp_pre2013wr                  /* Jane suggest to add the old format */
    '020','320' = "Invalid's Benefit"
    '030','330' = "Widow's Benefit"
    '040','044','340','344'
                = "Orphan's and Unsupported Child's benefits"
    '050','350','180','181'
    = "New Zealand Superannuation and Veteran's and Transitional Retirement Benefit"
    '115','604','605','610'
                = "Unemployment Benefit and Unemployment Benefit Hardship"
    '125','608' = "Unemployment Benefit (in Training) and Unemployment Benefit Hardship (in Training)"
    '313','613','365','665','366','666','367','667'
                = "Domestic Purposes related benefits"
    '600','601' = "Sickness Benefit and Sickness Benefit Hardship"
    '602','603' = "Job Search Allowance and Independant Youth Benefit"
    '607'       = "Unemployment Benefit Student Hardship"
    '609','611' = "Emergency Benefit"
    '839','275' = "Non Beneficiary"
    'YP ','YPP' = "Youth Payment and Young Parent Payment"
        ' '     = "No Benefit"
 ;

value $bennewgp 

'020'=	"Invalid's Benefit"
'320'=	"Invalid's Benefit"

'330'=	"Widow's Benefit"
'030'=	"Widow's Benefit"

'040'=	"Orphan's and Unsupported Child's benefits"
'044'=	"Orphan's and Unsupported Child's benefits"
'340'=	"Orphan's and Unsupported Child's benefits"
'344'=	"Orphan's and Unsupported Child's benefits"

'050'=	"New Zealand Superannuation and Veteran's and Transitional Retirement Benefit"
'180'=	"New Zealand Superannuation and Veteran's and Transitional Retirement Benefit"
'181'=	"New Zealand Superannuation and Veteran's and Transitional Retirement Benefit"
'350'=	"New Zealand Superannuation and Veteran's and Transitional Retirement Benefit"

'115'=	"Unemployment Benefit and Unemployment Benefit Hardship"
'604'=	"Unemployment Benefit and Unemployment Benefit Hardship"
'605'=	"Unemployment Benefit and Unemployment Benefit Hardship"
'610'=	"Unemployment Benefit and Unemployment Benefit Hardship"
'607'=	"Unemployment Benefit Student Hardship"
'608'=	"Unemployment Benefit (in Training) and Unemployment Benefit Hardship (in Training)"
'125'=	"Unemployment Benefit (in Training) and Unemployment Benefit Hardship (in Training)"


'313'=  "Domestic Purposes related benefits"
'365'=	"Sole Parent Support "					/* renamed */
'366'=	"Domestic Purposes related benefits"
'367'=	"Domestic Purposes related benefits"
'613'=	"Domestic Purposes related benefits"
'665'=	"Domestic Purposes related benefits"
'666'=	"Domestic Purposes related benefits"
'667'=	"Domestic Purposes related benefits"

'600'=	"Sickness Benefit and Sickness Benefit Hardship"
'601'=	"Sickness Benefit and Sickness Benefit Hardship"

'602'=	"Job Search Allowance and Independant Youth Benefit"
'603'=	"Job Search Allowance and Independant Youth Benefit"

'611'=	"Emergency Benefit"

'315'=	"Family Capitalisation"
'461'=	"Unknown"
'000'=	"No Benefit"
'839'=	"Non Beneficiary"

/* new codes */
'370'=  "Supported Living Payment related"
'675'=  "Job Seeker related"
'500'=  "Work Bonus"
;
run  ;

proc format;
value $ADDSERV
'YP'	='Youth Payment'
'YPP'	='Young Parent Payment'
'CARE'	='Carers'
'FTJS1'	='Job seeker Work Ready '
'FTJS2'	='Job seeker Work Ready Hardship'
'FTJS3'	='Job seeker Work Ready Training'
'FTJS4'	='Job seeker Work Ready Training Hardship'
'MED1'	='Job seeker Health Condition and Disability'
'MED2'	='Job seeker Health Condition and Disability Hardship'
'PSMED'	='Health Condition and Disability'
''		='.';
run;

Proc format;
value HA 
42='National Diploma at level 4 or above'
41='National Certificate at level 4 or above'
40='New Zealand Scholarship award'
39='NCEA level 3 (with Excellence)'
38='NCEA level 3 (with Merit)'
37='NCEA level 3 (with Achieve)'
36='NCEA level 3 (No Endorsement)'
35='Other NQF Qualification at level 3'
29='NCEA level 2 (with Excellence)'
28='NCEA level 2 (with Merit)'
27='NCEA level 2 (with Achieve)'
26='NCEA level 2 (No Endorsement)'
25='Other NQF Qualification at level 2'
19='NCEA level 1 (with Excellence)'
18='NCEA level 1 (with Merit)'
17='NCEA level 1 (with Achievement)'
16='NCEA level 1 (No Endorsement)'
15='Other NQF Qualification at level 1';

value HA_grouped
42,41,40='Level 4 Qualification or above'
39,38,37,36='NCEA level 3 Qualification'
35='Other NQF Qualification at level 3'
29,28,27,26='NCEA level 2 Qualification'
25='Other NQF Qualification at level 2'
19,18,17,16='NCEA level 1 Qualification'
15='Other NQF Qualification at level 1';

Value ha_grp
42,41,40,39,38,37,36,35='NCEA level 3 or above'
29,28,27,26,25='Level 2 Qualification'
19,18,17,16,15='NCEA level 1 Qualification'
0,.='No Formal NCEA Attainment';

run;


* Grouping interventions into groader categores
5	ESOL (English for Speakers of Other Languages)- should not appear for our domestic students
6	Alternative Education- for kids that dont fit into mainstream schools
7	Suspensions-suspections
8	Stand downs-suspentions
9	Non Enrolment Truancy Services-TRUANCY
10	Early Leaving exemptions-other
11	Homeschooling-Homeschooling
12	Section 9-enrolment over legal age
13	Mapihi Pounamu-similar to Boarding burs
14	Boarding bursaries
16	Reading Recovery
17	Off Sites Centres (teen parenting, altern education centres )
24	Special Education Service-SPECIAL EDU ( Physical and mental disabilities)
25	ORRS					-SPECIAL EDU ( Physical and mental disabilities)
26	Over 19 at Secondary	
27	High Health				-SPECIAL EDU ( Physical and mental disabilities)
28	Special School			-SPECIAL EDU ( Physical and mental disabilities)
29	Over 14 at Primary-		-LEARNING DIFF
30	SE Other
31	Resource Teachers: Literacy
32	Truancy (Unjustified Absence)-TRUANCY
36	ENROL- no records
33 	Hearing and eye test conduced at school HELATH
34  Gateway
35  Trade academies and other
37  Interim response fund
******************************************************;

proc format;
	value interv_grp
		5='ESOL'
		6,17='AlTED'
		7='SUSP'
		8='STAND'
		9,32='TRUA'
		12,26,29,24,25,27,28,30='SEDU'
		10='EARLEX'
		11='HOMESCH'
		13,14='BOARD'
		16,31='OTHINT'
		33='HEALTH'
		34,35='SECTER'
		37='IRF';
run;


*******************************************************;
* Education level code:
O 	 Unknown (converted from FM)
A 	 No formal school quals or < 3yrs
B 	 less than 3 SC passes or equivalent
C 	 3 or more SC passes or equivalent
D 	 Sixth form cert, UE or equivalent
E 	 Scholarship, Bursary, HSC 
F 	 Other School Quals
G 	 Post Secodary Quals
H 	 Degree or Professional Quals
I  	 (NCEA1) :1-79 credits
J 	 (NCEA) Level 1:>=80 credits
K 	 (NCEA) Level 2:>=80 credits
L 	 (NCEA) Level 3:>=80 credits
M 	 (NCEA) Level 4: >=72 credits
N 	 Sixth Form Certificate Transitional
P 	 Unknown - auto-enrolled;

proc format;
	value $BDD_edu
		'A', 'B', 'I' = 0
		'C', 'J' = 1
		'D', 'F', 'K' = 2
		'E', 'L','M','G'= 3
		'H' = 4
		other = .
	;
run;
