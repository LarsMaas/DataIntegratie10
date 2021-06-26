# DataIntegratie10
ETL for observational PGP Canada health data and genomics data to the OMOP common data model. 
This program has been tested using person 10, 20 and 30 from the PGP Canada database.

## Metadata

### Contributors
- Lars Maas
- Ricardo Tolkamp
- Demi van der Pasch
- Pim van Reeuwijk

Last time ran: June 26 2021  
Last code update: June 26 2021  
participant data source: https://personalgenomes.ca/data)
Participants: 10, 20, 30  
Repository location: https://github.com/LarsMaas/DataIntegratie10

## First time setup
Follow the steps to set up everything manually:

1. Ensure you are working on a Linux system.
2. Ensure Python (3.8+) is installed.  
3. Ensure java is installed.
4. Ensure bcftools is installed.  
   `sudo apt-get install bcftools`
5. Ensure [SNPeff](https://snpeff.blob.core.windows.net/versions/snpEff_latest_core.zip) is installed in `DataIntegratie10/SNPeff/`!  
   This is done by downloading SnpEff -> unzip snpEff_latest_core.zip -> go into snpEff_latest_core -> go into snpEff -> Put these files in `DataIntegratie10/SNPeff/`
6. Install python dependencies:
```
tika==1.24
pdfreader==0.1.10
psycopg2_binary==2.8.6
```

## Folder structure
```
├──Metadata  
│  └──run_*.txt  
├──Patient_data  
│  ├──*.pdf  
│  └──*.vcf  
├──Scripts  
│  ├──ConnectPostgre.py  
│  ├──ReadPDF.py  
│  ├──Semantic_health_data.py  
│  └──SNP_mapping.py  
├──snpEff  
│  └──* (SnpEff installation)  
├──temp  
│  ├──chr21_ann.vcf  
│  ├──chr21_ann_10.vcf  
│  └──health_data.csv
├──ExampleData  
│  ├──example_metadata.json
│  ├──P10.tar.xz
│  │  ├──P10.pdf  
│  │  └──P10.vcf
│  ├──P20.tar.xz
│  │  ├──P20.pdf
│  │  └──P20.vcf  
│  └──P30.tar.xz
│     ├──P30.pdf
│     └──P30.vcf    
├──ETL.sh  
└──README.md  
```
### Patient_data
The vcf (extracted) and pdf file from a person should be put here.    

### Metadata
A text file is made here everytime the program is used, the name of the given file is stored along with the date and time and version of the program in JSON format.

### Scripts
All Python scripts used in this program are stored here.

### snpEff
This should be the location of the SNPeff program. 

### temp
All temporary files are stored here. These include the PDF information, and the filtered vfc file.  
This folder is emptied everytime the program runs.

### ExampleData
Contains pdf and vcf data from person 10, 20 and 30. These are zipped. Also contains an example metadata JSON file.

### ETL.sh
This is a Bash script that calls all independent Python scripts.

## Running the program
1. Acquire the .vcf and PDF file from a person in [PGP](https://personalgenomes.ca/data) and store (only) these in `./DataIntegratie10/Patient_data`.  
   <b>Important!</b> Make sure the .vcf file is unzipped.
2. Make sure you have an internet connection.
3. When in the `DataIntegratie10` folder, run ETL.sh using `./ETL.sh`
   
Note: When the temp folder is empty, an error will the given. This happens when no files can be found. This is nothing to worry about.
Note: The first time using snpEff, the GRch37 will be downloaded. This may take a while.

## Example data
The data from person 10, 20 and 30 can be found in the folder `ExampleData`. 
The folder containing this data has been zipped. To use this data:
1. Unzip the folder of the person you would like to use.
2. Place both the PDF and .vcf file in the `Patient_data` folder.
3. Run ETL.sh.

## Program flow
What happens after ETL.sh is called:

### 1. Empty temp folder  
Program tries to empty the temp folder using bash.

### 2. Create metadata  
A JSON file containing the date and time at the start, and folders used, are saved in the Metadata folder. This is done using bash.

### 3. Read PDF file
A Python script called ReadPDF.py is called.  
This script reads the given PDF file and saves this information in the temp folder.

### 4. Read vcf file
1. The vcf file is being zipped.
2. An index is being created using bcftools.
3. The vcf file is being filtered on chromosome 21. This data is saved in the temp folder.
4. snpEff is used annotate the SNPs.
5. The annotated SNPs are filtered on frameshift and missense variants using SnpSifft.
6. the grep command is used to get 10 SNPs.

### 5. Put data in PostgreSQL database
A Python script called ConnectPostgre.py is called. This script puts all data in a PostgreSQL database.
1. Connect to the database.
2. Retrieve health and SNP data.
3. Retrieve the latest `person_id` from the `person` table. Also checks if this person is already in the database.
4. Map standard information (date of birth, person id, ethnicity, gender) from the 'Profile' table in the PDF files. This is done by searching the `concept` view. Here, wildcards before, after and between words are used.  
   This information is then inserted into the `person` table.
5. Map condition information (If a 'Conditions and Symptom' table is present in the PDF file) by searching the `concept` view. Here, wildcards before, after and between words are used.   
   This information is then inserted into the `condition_occurrence` table
6. Map SNP information. This is done by searching `concept.concept_class_id` where `concept_class_id` = 'Genomic Variant'.  
   The query used is `%chr%21%<position>%<reference a.a>%<actual a.a>%`.  
   This information is then inserted into the `measurement` table.
7. Commit all changes made to the database.

### 6. Finish metadata
Add the end time, and date, to the JSON file containing the metadata of this run.

