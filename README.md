# Anonymize large data using Apache Spark

Anonymity of data is acheived by generating fake data and replacing it with the original data. 

For example - If the original First Name is "Anuja", it will be replaced by "Anna" . 

"Faker" open source library is used to generate the required fake data.   

Apache Spark framework is used to speed up the distributed processing of large dataset.

## Libraries used

* PySpark 3.1.1
* Java (OpenJDK) 11 (Dependency for installing Spark)
* Python 3.9.4
* Faker (Used to generate anonymized data)
* Jupyter Notebook 

## Dataset used
> The data used for demonstrating distributing processing in the project is huge and could not be uploaded on Github. Download the data using the link provided below , rename it as "Records5m.csv" and place the unziped file in the "Dataset" folder.

Here are the details of the dataset used in the project :
* Data Size - 5 million (~ 1.4 GB)
* Download Link - http://eforexcel.com/wp/downloads-16-sample-csv-files-data-sets-for-testing/
* Select the 5m Records zip file (614 MB)
* Unzip the file , rename it as "Records5m.csv" and place it inside "Dataset" folder.

## Project Folder Structure

Anonymize2GB.ipynb --> Jupter Notebook inline code explanation

/PythonFiles       --> Pyspark Project in Object Oriented format for Anonymizing Data

/PythonFiles/AnonymizeDataset.py --->  Pyspark Program to anonymize data

/PythonFiles/AnonymizeDataTests.py ---> Pyspark Unit Testing (TDD)

/PythonFiles/spark_dependency.py ---> Pyspark helper functions

## Code Execution

### Execute Python Script


After downloading and placing the data in "./Dataset" folder , run the following command to run the main file-

`python3  PythonFiles/AnonymizeDataset.py`


Run the command below to run the test script as per TDD approach

`python3  PythonFiles/AnonymizeDatasetTests.py`



### Exceute Jupyter Notebook

Install Jupyter Notebook, import the .ipynb file  and execute all the cells in the code by selecting:

`Kernel -> Restart & Run All `
