# DataIntegratie10
ETL for observational health data and genomics data

## Metadata

Contributors
- Lars Maas
- Ricardo Tolkamp
- Demi van der Pasch
- Pim van Reeuwijk

Last time ran: <PLACEHOLDER>
Last code update: <PLACEHOLDER>
participant data source: https://personalgenomes.ca/data)
Participants: 10, 20, 30
Repository location: https://github.com/LarsMaas/DataIntegratie10

## First time setup

Follow the steps to set up everything manually:

Ensure Python (3.8) is installed
Ensure java is installed.
Ensure SNPeff is installed in DataIntegratie10/SNPeff
Ensure bcftools is installed in DataIntegratie10/SNPeff

Install dependencies:
tika v:1.24
pdfreader v:0.1.10
psycopg2_binary v:2.8.6

Folder structure:
Patient_data
    Zipped vcf and pdf for analysis are stored here.    

Metadata
    A text file is made here everytime the program is used, the name of the given file is stored along with the date and time and version of the program

Scripts
    All of the scripts used in this program are stored here.

snpEff
    This is the location of the SNPeff commands

temp
    All temporary files are stored here.
    This folder is emptied everytime the program runs.

bash_script.txt
    This is the bashscript that calls all of the indipendant python scripts.

## After set up
Acquire data from PGP(https://personalgenomes.ca/data) and store it under DataIntegratie10/Patient_data in vcf.gz and PDF format

run the ETL.sh with
./ETL.sh
