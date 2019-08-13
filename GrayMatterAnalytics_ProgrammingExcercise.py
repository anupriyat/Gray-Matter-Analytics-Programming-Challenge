#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SQLContext
from pyspark.sql.functions import col


# In[2]:


class ComputeScore:
    
    # Step 1: Define measure 
    def __init__(self, program_name, measure_name):
        self.program_name = program_name
        self.measure_name = measure_name     
    
    
    # Step 2: filter Diagnosis Codes for the measure Type
    def filter_diagnosisDF(self, program_name, measure_name, df):
        if measure_name == "AMI":
            return df.filter("diagnosis_code == 410.00 or diagnosis_code == 410.01 or diagnosis_code == 410.10               or diagnosis_code == 410.11 or diagnosis_code == 410.20 or diagnosis_code == 410.21               or diagnosis_code == 410.30  or diagnosis_code == 410.31  or diagnosis_code == 410.40               or diagnosis_code == 410.41  or diagnosis_code == 410.50  or diagnosis_code == 410.51                or diagnosis_code == 410.60 or diagnosis_code == 410.61 or diagnosis_code == 410.70               or diagnosis_code == 410.71 or diagnosis_code == 410.80  or diagnosis_code == 410.81               or diagnosis_code == 410.90  or diagnosis_code == 410.91")
        elif measure_name == "COPD":
            return df.filter("diagnosis_code == 491.21 or diagnosis_code == 491.22 or diagnosis_code == 491.8               or diagnosis_code == 491.9 or diagnosis_code == 492.8 or diagnosis_code == 493.20               or diagnosis_code == 493.21  or diagnosis_code == 493.22  or diagnosis_code == 496               or diagnosis_code == 518.81  or diagnosis_code == 518.82  or diagnosis_code == 518.84                or diagnosis_code == 799.1")
        elif measure_name == "HF":
            return df.filter("diagnosis_code == 402.01 or diagnosis_code == 402.11 or diagnosis_code == 402.91               or diagnosis_code == 404.01 or diagnosis_code == 404.03 or diagnosis_code == 404.11               or diagnosis_code == 404.13  or diagnosis_code == 404.91  or diagnosis_code == 404.93               or diagnosis_code == '428.xx'")
        elif measure_name == "HWR":
            return df.filter("diagnosis_code == 'CCS'")
        elif measure_name == "PN":
            return df.filter("diagnosis_code == 480.0 or diagnosis_code == 480.1 or diagnosis_code == 480.2               or diagnosis_code == 480.3 or diagnosis_code == 480.8 or diagnosis_code == 480.9               or diagnosis_code == 481  or diagnosis_code == 482.0  or diagnosis_code == 482.1               or diagnosis_code == 482.2  or diagnosis_code == 482.30  or diagnosis_code == 482.31                or diagnosis_code == 482.32 or diagnosis_code == 482.39 or diagnosis_code == 482.40               or diagnosis_code == 482.41 or diagnosis_code == 482.42  or diagnosis_code == 482.49               or diagnosis_code == 482.81  or diagnosis_code == 482.82 or diagnosis_code == 482.83                or diagnosis_code == 482.84 or diagnosis_code == 482.89 or diagnosis_code == 482.9               or diagnosis_code == 483.0 or diagnosis_code == 483.1  or diagnosis_code == 483.8               or diagnosis_code == 485  or diagnosis_code == 486 or diagnosis_code == 487.0               or diagnosis_code == 488.11")
        elif measure_name == "THA-TKA":
            return df.filter("diagnosis_code == 81.51 or diagnosis_code == 81.54")
        else:
            return None
     
    
    # Step 3: Calculte Comorbidity Value for each score
    def calculate_comorbidityDF(self, program_name, measure_name, df): 
        column_names = sqlContext.read.format('com.databricks.spark.csv').options(header='true', 
                inferschema='true').load('Comorbidity.csv').filter("Measure == '" + measure_name + "'").select("ComorbidityColumns").collect()[0].ComorbidityColumns
        df_with_comorbidityValue = df.withColumn("ComorbidityValue", (sum(col(x) for x in df.columns if x in column_names.split(", "))).alias("ComorbidityValue"))
        print("Comorbidity Value: ")
        df_with_comorbidityValue.select(['ComorbidityValue']).describe().show()
        return df_with_comorbidityValue
    
    # Step 4: Calculate LACE Score
    def calculate_LaceScore(self, program_name, measure_name, df, sqlContext): 
        df.registerTempTable('patientData')
        df_LaceScore = sqlContext.sql("""SELECT * 
                                        , (CASE WHEN LengthOfStay < 1 THEN 0 ELSE  
                                                        CASE WHEN LengthOfStay >= 4 AND LengthOfStay <= 6 THEN 4 ELSE 
                                                        CASE WHEN LengthOfStay >= 7 AND LengthOfStay <= 13 THEN 5 ELSE 
                                                        CASE WHEN LengthOfStay >= 14 THEN 7 ELSE 
                                                        CAST(LengthOfStay AS INT) END END END END) AS LengthOfStayPoints
                                        , (CASE WHEN Inpatient_visits > 0 THEN 3 ELSE 
                                                        0 END ) AS EmergencyAdmissionPoints
                                        , (CASE WHEN ComorbidityValue >= 4 THEN 5 ELSE 
                                                        CAST(ComorbidityValue AS INT) END) AS ComorbidityValuePoints
                                        , (CASE WHEN ED_visits >=4 THEN 4 ELSE  CAST(ED_visits AS INT) END) AS EDVisitPoints
                             FROM patientData""")
        df_with_LaceScore = (df_LaceScore.withColumn("LaceScore", (sum(col(x) for x in df_LaceScore.columns if x in ['LengthOfStayPoints',
                                                                            'EmergencyAdmissionPoints',
                                                                            'ComorbidityValuePoints',
                                                                            'EDVisitPoints',
                                                                            'LaceScore'])).alias("LaceScore")))
        print("Lace Score: ")
        df_with_LaceScore.select(['LaceScore']).describe().show()
        return df_with_LaceScore
    
    # Step 7: Calculate Measure Score
    def calculate_MeasureScore(self, program_name, measure_name, df): 
        # Step 5: Select count of records as denominator
        denominator = df.count()
        # Step 6: Select count of records with lace score > 9 as numerator
        numerator = df.filter(col("LaceScore") > 9).count()
        return numerator/denominator


# In[3]:


def main():
    print("Data Solutions Engineer Position - Gray Matter Analytics") 
    spark = SparkSession.builder.appName("Programming Exercise").getOrCreate()

    # Read Patient Data
    sqlContext = SQLContext(sc)
    dfPatient = sqlContext.read.format('com.databricks.spark.csv').options(header='true', 
                inferschema='true').load('SampleData2016.csv')
    dfPatient = dfPatient.replace('Yes','1').replace('No','0')
    print("Patient data has:", dfPatient.count(), "rows \n")
    print("***********************************************************")
    
    # Compute AMI Score
    ami = ComputeScore("MeasureScoreProgram","AMI")
    df_ami = ami.filter_diagnosisDF("MeasureScoreProgram","AMI", dfPatient)
    print("AMI measure has:", df_ami.count(), "rows")
    df_ami = ami.calculate_comorbidityDF("MeasureScoreProgram","AMI", df_ami)
    df_ami = ami.calculate_LaceScore("MeasureScoreProgram","AMI", df_ami, sqlContext)
    print("AMI Measure Score is: ", ami.calculate_MeasureScore("MeasureScoreProgram","AMI", df_ami))
    print("***********************************************************")
    
    # Compute COPD Score
    copd = ComputeScore("MeasureScoreProgram","COPD")
    df_copd = copd.filter_diagnosisDF("MeasureScoreProgram","COPD", dfPatient)
    print("COPD measure has:", df_copd.count(), "rows")
    df_copd = copd.calculate_comorbidityDF("MeasureScoreProgram","COPD", df_copd)
    df_copd = copd.calculate_LaceScore("MeasureScoreProgram","COPD", df_copd, sqlContext)
    print("COPD Measure Score is: ", copd.calculate_MeasureScore("MeasureScoreProgram","COPD", df_copd))
    print("***********************************************************")
    
    # Compute HF Score
    hf = ComputeScore("MeasureScoreProgram","HF")
    df_hf = hf.filter_diagnosisDF("MeasureScoreProgram","HF", dfPatient)
    print("HF measure has:", df_hf.count(), "rows")
    df_hf = hf.calculate_comorbidityDF("MeasureScoreProgram","HF", df_hf)
    df_hf = hf.calculate_LaceScore("MeasureScoreProgram","HF", df_hf, sqlContext)
    print("HF Measure Score is: ", hf.calculate_MeasureScore("MeasureScoreProgram","HF", df_hf))
    print("***********************************************************")
    
    # Compute HWR Score
    hwr = ComputeScore("MeasureScoreProgram","HWR")
    df_hwr = hwr.filter_diagnosisDF("MeasureScoreProgram","HWR", dfPatient)
    print("HWR measure has:", df_hwr.count(), "rows")
    df_hwr = hwr.calculate_comorbidityDF("MeasureScoreProgram","HWR", df_hwr)
    df_hwr = hwr.calculate_LaceScore("MeasureScoreProgram","HWR", df_hwr, sqlContext)
    print("HWR Measure Score is: ", hwr.calculate_MeasureScore("MeasureScoreProgram","HWR", df_hwr))
    print("***********************************************************")
    
    # Compute PN Score
    pn = ComputeScore("MeasureScoreProgram","PN")
    df_pn = pn.filter_diagnosisDF("MeasureScoreProgram","PN", dfPatient)
    print("PN measure has:", df_pn.count(), "rows")
    df_pn = pn.calculate_comorbidityDF("MeasureScoreProgram","PN", df_pn)
    df_pn = pn.calculate_LaceScore("MeasureScoreProgram","PN", df_pn, sqlContext)
    print("PN Measure Score is: ", pn.calculate_MeasureScore("MeasureScoreProgram","PN", df_pn))
    print("***********************************************************")
    
    # Compute THA-TKA Score
    thatka = ComputeScore("MeasureScoreProgram","THA-TKA")
    df_thatka = thatka.filter_diagnosisDF("MeasureScoreProgram","THA-TKA", dfPatient)
    print("THA-TKA measure has:", df_thatka.count(), "rows")
    df_thatka = thatka.calculate_comorbidityDF("MeasureScoreProgram","THA-TKA", df_thatka)
    df_thatka = thatka.calculate_LaceScore("MeasureScoreProgram","THA-TKA", df_thatka, sqlContext)
    print("THA-TKA Measure Score is: ", thatka.calculate_MeasureScore("MeasureScoreProgram","THA-TKA", df_thatka))
    print("***********************************************************")
    
    # Validating invalid Measure names
    unknown = ComputeScore("MeasureScoreProgram","unknown")
    df_unknown = unknown.filter_diagnosisDF("MeasureScoreProgram","unknown", dfPatient)
    if(df_unknown == None):
        print("Invalid Entry!")
        print("No diagnosis code found for the measure type named: unknown")
    print("***********************************************************")

# Invoke main function
if __name__== "__main__":
  main()

