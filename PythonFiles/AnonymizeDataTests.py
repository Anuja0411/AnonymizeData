import unittest
import argparse
import os, sys
from AnonymizeDataset import AnonymizeData
from chispa.column_comparer import assert_column_equality
from spark_dependency import stop_spark
import pandas as pd
filepath = ''
data = ''
required_cols = []
anonymized_cols = []
spark_ctx = ''
spark_sql_ctx = ''

class AnonyzizeDataTests(unittest.TestCase):
    
    
    
    def test_CheckFileExists(self):
        '''
            Test Description: Test if the local data file exists 
            Test Input : Filepath
            Test Output: "True" if exists , "False" otherwise

        '''
        AnonymizeData.__init__(self)
        global filepath
        filepath = self.filepath
        global required_cols
        required_cols = self.req_cols
        global anonymized_cols
        anonymized_cols = self.anon_cols
        global spark_ctx
        spark_ctx = self.sc
        global spark_sql_ctx
        spark_sql_ctx = self.sqlc
        #stop_spark(self.sc)
        print ("________________________________________")
        print ("Testing if file "+  filepath +" exits ")
        self.assertTrue(os.path.exists(filepath))
        

    def test_DataExistsInFile(self):
        '''
            Test Description: Test if the data  exists in the file
            Test Input : Filepath
            Test Output: "None" if does not exist, "Datframe" otherwise

        '''
        self.filepath = filepath
        self.sqlc = spark_sql_ctx
        print ("_____________________________________________")
        print ("Testing if data exists in  "+self.filepath)
        print (filepath)
        global data
        data = AnonymizeData.read_data(self)
        self.assertIsNotNone(data)
        
    
    def test_GenerateAddress(self):
        '''
            Test Description: Test if the class function "generateAddressColumn" generates address
            Test Input : Sample Test dataframe
            Test Output: Equality Comparison between the expected and actual ouput

        '''
        print ("_____________________________________________")
        print ("testing adrress generation by combining Place, country, city, state and zip")
        test_df = pd.DataFrame([['Armadale','Australia','Melbourne','Victoria','3143']], columns=['Place Name', 'County', 'City', 'State', 'Zip'])
        spark_test_df = spark_sql_ctx.createDataFrame(test_df)
        self.org_df = spark_test_df
        data = AnonymizeData.generateAddressColumn(self)
        print (data.select('Address').collect()[0].__getitem__('Address'))
        required_output = "Armadale, Australia, Melbourne, Victoria, 3143"
        self.assertEqual(data.select('Address').collect()[0].__getitem__('Address'),required_output)
        
    
    def test_ifAllRequiredColsExist(self):
        '''
            Test Description: Test the class function "checkRequiredCols" 
            Test Input : Sample Test dataframe
            Test Output: "True" if exists

        '''
        print ("_____________________________________________")
        print ("Testing if all required columns - First Name, Last name, Date Of Birth and Address exist")
        test_df = pd.DataFrame([['444','Anuja','Jain','31/12/2000','Victoria,3144','F']], columns=['Emp ID','First Name', 'Last Name', 'Date of Birth', 'Address','Gender'])
        spark_test_df = spark_sql_ctx.createDataFrame(test_df)
        self.org_df = spark_test_df
        self.req_cols = required_cols
        self.assertTrue(AnonymizeData.checkRequiredCols(self))
        
    
    def test_ifDuplicatesExist(self):
        '''
            Test Description: Test the class function "checkduplicates" 
            Test Input : Sample Test dataframe
            Test Output: Equality Comparison between the expected and actual ouput

        '''
        print ("_____________________________________________")
        print ("Testing if duplicates exist")
        test_df = pd.DataFrame([['444','Anuja','Jain','31/12/2000','Victoria,3144','F'],['444','Anuja','Jain','31/12/2000','Victoria,3144','F']], columns=['Emp ID','First Name', 'Last Name', 'Date of Birth', 'Address','Gender'])
        spark_test_df = spark_sql_ctx.createDataFrame(test_df)
        self.org_df = spark_test_df
        self.req_cols = required_cols
        dub_cnt = AnonymizeData.checkduplicates(self)
        print (dub_cnt)
        self.assertEqual(dub_cnt,1)
      
    
    def test_ifNullValuesExistInRequiredCols(self):
        '''
            Test Description: Test the class function "filternullrows" 
            Test Input : Sample Test dataframe
            Test Output: Equality Comparison between the expected and actual ouput

        '''
        print ("_____________________________________________")
        print ("Testing for null values")
        test_df = pd.DataFrame([['333','Anuja',None,'31/12/2000','Victoria,3144','F'],['444','Anna',None,'31/12/2001','Queensland,3144','F'],['444','Elise','Jordan','31/12/1990','Sydney,3144','F']], columns=['Emp ID','First Name', 'Last Name', 'Date of Birth', 'Address','Gender'])
        spark_test_df = spark_sql_ctx.createDataFrame(test_df)
        self.org_df = spark_test_df
        self.req_cols = required_cols
        final_cnt_after_null_drop = AnonymizeData.filternullrows(self)
        self.assertEqual(final_cnt_after_null_drop,1)
    
    def test_ifTheDataTypesAreCorrect(self):
        '''
            Test Description: Test the class function "checkdatatypes" 
            Test Input : Sample Test dataframe
            Test Output: "True" if correct

        '''
        print ("_____________________________________________")
        print ("Testing if datatypes are  correct (all are string)")
        test_df = pd.DataFrame([['444','Anuja','Jain','31/12/2000','Victoria,3144','F'],['445','Anna','Jordan','30/12/2000','Victoria,3154','F']], columns=['Emp ID','First Name', 'Last Name', 'Date of Birth', 'Address','Gender'])
        spark_test_df = spark_sql_ctx.createDataFrame(test_df)
        self.selc_df = spark_test_df
        self.assertTrue(AnonymizeData.checkdatatypes(self))

    def test_IfAnonymizedDataIsNotSameAsOrginal(self):
        '''
            Test Description: Test the class function "anonymize" 
            Test Input : Sample Test dataframe
            Test Output: Check if The Anonymized data is different than the original data

        '''
        print ("___________________________________________________")
        print ("Testing if the anonymization is working correctly (Fake and original values are different)")
        test_df = pd.DataFrame([['444','Hermione','Granger','31/12/2000','Melbourne,3144','F']], columns=['Emp ID','First Name', 'Last Name', 'Date of Birth', 'Address','Gender'])
        spark_test_df = spark_sql_ctx.createDataFrame(test_df)
        self.selc_df = spark_test_df
        anonymized_data = AnonymizeData.anonymize(self)
        self.assertNotEquals(spark_test_df,anonymized_data)
        






    

if __name__ == '__main__':
    
    unittest.main()

        


