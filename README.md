# GrayMatterAnalyticsProgrammingChallenge

Develop a program to compute score for each measure. The measures include; AMI, COPD, HF, HWR, PN and THA-TKA.

The program is invoked with the following command line:
 <program_name> <measure_name>
Sample
<program_name> ami

Your solution will be evaluated for approach, selection of technologies and quality of code unit.

Programming Steps
Steps to compute measure score:
1. Select a measure
2. Filter records with diagnosis_codes that corresponds to the selected measure Refer to the Diagnosis Codes table for diagnosis_codes for a given measure
3. Calculate comorbidity value for each row depending on set of comorbidity columns for the selected measure
Comorbidity value for a row can be calculated as count of ‘yes’ in corresponding comorbidity column set.Refer to the Comorbidity Columns table to find comorbidity columns for a given measure
4. Calculate lace score for each row in the dataset as sum of points for each LACE variable Refer to the Lace Index table to find lace point for a given LACE variable
5. Select count of records as denominator
6. Select count of records with lace score > 9 as numerator
7. Calculate score for selected measure as numerator/denominator
