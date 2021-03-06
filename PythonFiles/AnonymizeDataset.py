import os
import sys
import findspark
import time

# Initialize Findspark to locate Pyspark installed in the system
findspark.init()

# Import Pyspark libraries and functions
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext , SparkSession
from pyspark.sql.functions import concat, col, lit, udf, when
from pyspark.sql.types import ArrayType, IntegerType, StringType , DateType

import spark_dependency as spark
# Import Faker (3rd party library) used to generate anonymized data
from faker import Factory


class AnonymizeData():

    def __init__(self):
        # start Spark application and get Sparkcontext anbd sqlcontext
        self.sc, self.sqlc = spark.start_spark()

        # Set class object attributes 
        self.filepath = './Dataset/Records5m.csv' # Path to the 2GB data
        self.req_cols = ['Emp ID','First Name', 'Last Name','Date of Birth','Address','Gender'] # Required Columns
        self.anon_cols = ['First Name', 'Last Name','Address'] # Columns that need to be anonymized
        
        

    def main(self):
        """ Purpose : Main  script definition.
          - 1. Prepare the data that has to be anonymized
          - 2. Anonymize the data
          - 3. Stop spark 
        Argument: Class Object
        Output: None
        """
        
        
        self.org_df = self.read_data()
        if self.org_df != None:
            print ("Preparing data for Anonymizing")
            self.prepare_data()

            print ("Anonymizing required columns")
            self.anonymize()
        spark.stop_spark(self.sc)


    def read_data(self):
        ''' Purpose : Read the data from local
            Argument : Class Object
            Output : Returns the dataframe or None
        
        '''
        df = self.sqlc.read.csv(self.filepath, header = True)
        nrows = df.count()
        if nrows > 0:
            print ("No. of records:", nrows)
            return df
        else:
            print ("ERROR: Empty File")
            return None
    
    def generateAddressColumn(self):
        ''' Purpose : Create a new "Address" column by combining Place, Country, City, State and Zip
            Argument : Class Object
            Output : Returns the dataframe with new Column
        
        '''
        self.org_df = self.org_df.withColumn('Address', concat(col('Place Name'), lit(', '), col('County'),
                                 lit(', '),  col('City'), lit(', '),col('State'),lit(', '),col('Zip')))
        return self.org_df
    
    def checkRequiredCols(self):
        ''' Purpose : Check if all the required columns are present in the data
            Argument : Class Object
            Output : Returns "True" is present else "False"
        
        '''
        print ("Checking if all required columns are present")
        
        if all(col in self.org_df.columns for col in self.req_cols):
            print (self.req_cols)
            return True
        else:
            return False
        

    def filternullrows(self):
        ''' Purpose : Filter out rows that contain "Null" in any of the required column
            Argument : Class Object
            Output : Returns the filtered dataframe
        
        '''
        for col in self.req_cols:
            self.org_df = self.org_df.filter(self.org_df[col].isNotNull())
        if self.org_df.count() > 0:
            self.selc_df = self.org_df.select(*self.req_cols)
            self.selc_df.show(10)
            return self.selc_df.count()
        
    def checkdatatypes(self):
        ''' Purpose : Check the datatype of all required columns 
            Argument : Class Object
            Output : Returns "True" is all columns are of "String" datatype
        
        '''
        for dt in self.selc_df.dtypes:
            if dt[1] == 'string':
                return True
        print (self.selc_df.dtypes)

    def checkduplicates(self):
        ''' Purpose : Drop duplicate rows
            Argument : Class Object
            Output : Returns the cleaned dataframe without duplicates
        
        '''
        
        nrows = self.org_df.count()
        self.org_df = self.org_df.dropDuplicates()
        dnrows = self.org_df.count()
        dub_cnt = nrows-dnrows
        print ("No. of duplicates found : ", dub_cnt)
        return dub_cnt


    
    def prepare_data(self):
        ''' Purpose : Prepare the data by performing data cleansing and ETL 
            Argument : Class Object
            Output : None
        
        '''
        if self.generateAddressColumn():
            print ("Combining columns like Place, country, city, state and zip to generate Address column")

        if self.checkRequiredCols():
            print ("Required Columns - First Name, Last name, Date Of Birth and Address exists")
        
        if self.filternullrows():
            print ("Discared null rows")
        
        self.checkduplicates()
        
        if self.checkdatatypes():
            print ("Data Type check complete")

    def anonymize(self):
        ''' Purpose : Anonymize the cleansed data by replacing the original data with fake data. For "First Name" and
                    "Last Name" the fake name that is generated is gender specific
            Argument : Class Object
            Output : Returns the dataframe with anonymized data
        
        '''
        faker  = Factory.create()
        faker  = Factory.create()
        anonymize_male = udf(lambda n : faker.first_name_male(), StringType())
        anonymize_female = udf(lambda n : faker.first_name_female(), StringType())
        anonymize_female_lst = udf(lambda n : faker.last_name_female(), StringType())
        anonymize_male_lst = udf(lambda n : faker.last_name_male(), StringType())
        anonymize_addr = udf(lambda n: faker.address(), StringType())

        self.fake_df = self.selc_df.withColumn("Fake_First_name", when(col("Gender") == "M",anonymize_male(col("First Name")))
                            .when(col("Gender") == "F",anonymize_female(col("First Name")))) \
                        .withColumn("Fake_Last_name", when(col("Gender") == "M",anonymize_male_lst(col("Last Name")))
                        .when(col("Gender") == "F",anonymize_female_lst(col("Last Name")))) \
                            .withColumn("Fake Addrs", anonymize_addr(col("Address")))
        print ("Showing original data..")
        self.fake_df.select('Emp ID','First Name','Last Name','Date of Birth','Address').show(10)
        self.final_df = self.fake_df.select('Emp ID','Fake_First_name','Fake_Last_name','Date of Birth','Fake Addrs')
        self.final_df = self.final_df.withColumnRenamed('Fake_First_name','First Name').withColumnRenamed('Fake_Last_name','Last Name') \
            .withColumnRenamed('Fake Addrs','Address')
        print ("Showing Anonymized data...")
        self.final_df.show(10)
        return self.final_df
        

        


if __name__ == '__main__':
    AnonymizeData().main()


