#!/bin/bash

# delete /temp
echo "Starting run"
echo "Staring: Remove temporary data from old run"
rm ./temp/*
echo "Done"

echo "Starting: Generating metadata"
RUN_NR=0
# for files in metadata dir
for META_DATA_FILE in $(find ./Metadata -name '*.json')
do
  # Get run nr from file name
  SUBSTRING=$(echo "$META_DATA_FILE" | cut -d'_' -f 2 | cut -d'.' -f 1)
  SUBSTRING=$((SUBSTRING))
  # If run nr from file is higher then RUN_NR, RUN_NR = file run nr
  if [ "$SUBSTRING" -gt "$RUN_NR" ]; then
      RUN_NR="$SUBSTRING"
  fi

done
# run nr + 1
RUN_NR=$(( RUN_NR + 1 ))

echo "{"$'\n' Files [ >> ./Metadata/run_"$RUN_NR".json
for entry in ./Patient_data/*; do
  echo $'\t'$'"'"$entry"$'"',  >> ./Metadata/run_"$RUN_NR".json
done
echo $'\t'],$'\n' Datetime_run "{" >> ./Metadata/run_"$RUN_NR".json
echo $'\t'Start: $'"'"$(date)"$'"', >> ./Metadata/run_"$RUN_NR".json

echo "Done"

# get pdf data
echo "Starting: Reading pdf"
python3 ./Scripts/ReadPDF.py
echo "Done"

## get SNP data
echo "Starting: Zip vcf + create index + filter chromosome 21"
bgzip -c "$(find ./Patient_data -name '*.vcf')" > ./temp/vcf_file.vcf.gz
bcftools index ./temp/vcf_file.vcf.gz
bcftools view ./temp/vcf_file.vcf.gz --regions chr21 > ./temp/chr21.vcf
echo "Done"

echo "Starting: snpEFF annotation"
java -jar ./snpEff/snpEff.jar GRCh37.75 -no-downstream -no-intergenic -no-intron -no-upstream -no-utr -noStats -verbose ./temp/chr21.vcf > ./temp/chr21_ann.vcf
echo "Done"

echo "Starting: Filter missense_variant + frameshift_variant"
cat ./temp/chr21_ann.vcf | java -jar ./snpEff/SnpSift.jar filter "(ANN[*].EFFECT has 'missense_variant' || ANN[*].EFFECT has 'frameshift_variant')" > ./temp/chr21_ann_filtered.vcf
echo "Done"

echo "Starting: Save 10 SNP's"
grep -A 10 CHROM ./temp/chr21_ann_filtered.vcf > ./temp/chr21_ann_10.vcf
echo "Done"

## Put everything in database
echo "Starting: Put everything in database"
python3 ./Scripts/ConnectPostgre.py
echo "Done"

## Complete metadata
echo $'\t'End: $'"'"$(date)"$'"' >> ./Metadata/run_"$RUN_NR".json
echo $'\t'"}"$'\n'"}" >> ./Metadata/run_"$RUN_NR".json

echo "Run complete"
