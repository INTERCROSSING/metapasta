# Metapasta Output files Spec

## Output files description 

A Metapasta analysis generates files related to the following levels (from more general to more specific):

- **Project** files
- **Groups** files
- **Samples** files

In every Metapasta analysis the **Samples** are grouped into **Groups**, and all the groups make up the **Project**.

The folder structure is as follows

- Project
    - Group1
    - Group2
    - GroupN
    - Sample1
    - Sample2
    - SampleN

The files of each category should be placed under the corresponding folder.


### Files common to all the levels

All the following files must be generated for each level.

1. CSV files with all the frequencies (File type A). File name: ItemID.frequencies.csv
2. CSV files with all the frequencies of Kingdom (File type A). File name: ItemID.Kingdom.frequencies.csv
3. CSV files with all the frequencies of Phylum (File type A). File name: ItemID.Phylum.frequencies.csv
4. CSV files with all the frequencies of Class (File type A). File name: ItemID.Class.frequencies.csv
5. CSV files with all the frequencies of Order (File type A). File name: ItemID.Order.frequencies.csv
6. CSV files with all the frequencies of Family (File type A). File name: ItemID.Family.frequencies.csv
7. CSV files with all the frequencies of Genus (File type A). File name: ItemID.Genus.frequencies.csv
8. CSV files with all the frequencies of Species (File type A). File name: ItemID.Species.frequencies.csv


### Levels-specific files

The files described below are specific to each level and should be added to the files described above.

_Project-specific files_

9. viz `*.pdf`. File name: ItemID.frequencies.tree.pdf

10. Clean CSV files with direct absolute frequencies. Modification of file 1 for MetagenAssist usage (File type B). File name: ItemID.direct.absolute.frequencies.clean.csv
11. Clean CSV files with direct relative frequencies. Modification of file 1 for HeatMap viz (File type C). File name: ItemID.direct.relative.frequencies.clean.csv

_Groups-specific files_

12. Group specific CSV file with all the frequencies. Modification of file 1 (File type D). File name: ItemID.grouped.frequencies.csv

_Sample-specific files_

13. FASTA files with the read assignment in the header (File type E). File name: ItemID.reads.fasta
14. Blast/Last output files. File name: ItemID.blast.out
15. PDF reports and charts (File type F). File name: TBD


## File format description

### File type A

CSV files with these **columns**:

- TaxonomyID
- TaxonomyName
- TaxonomyRank
- TaxonomyLevel : @rtobes will provide you with a conversion table between `TaxonomyRank` and `TaxonomyLevel`
- Sample1ID.direct.absolute.freq: This is the number of reads directly assigned to such Taxa for that sample
- Sample1ID.direct.relative.freq: This is the percentage of reads directly assigned to such taxa for that sample. The normalization is done with the total number of reads of that sample
- Sample1ID.cumulative.absolute.freq: This is the cumulative freq of that taxa for that sample
- Sample1ID.cumulative.relative.freq: This is the percentage value of the cumulative frequency of such taxa for that sample. The normalization is done with the total number of reads of that sample
- Sample2ID.direct.absolute.freq: This is the number of reads directly assigned to such Taxa for that sample
- Sample2ID.direct.relative.freq: This is the percentage of reads directly assigned to such taxa for that sample. The normalization is done with the total number of reads of that sample
- Sample2ID.cumulative.absolute.freq: This is the cumulative freq of that taxa for that sample
- Sample2ID.cumulative.relative.freq: This is the percentage value of the cumulative frequency of such taxa for that sample. The normalization is done with the total number of reads of that sample
...
- SampleNID.direct.absolute.freq: This is the number of reads directly assigned to such Taxa for that sample
- SampleNID.direct.relative.freq: This is the percentage of reads directly assigned to such taxa for that sample. The normalization is done with the total number of reads of that sample
- SampleNID.cumulative.absolute.freq: This is the cumulative freq of that taxa for that sample
- SampleNID.cumulative.relative.freq: This is the percentage value of the cumulative frequency of such taxa for that sample. The normalization is done with the total number of reads of that sample

When it comes to **rows**:

- One row per taxa with assigned reads in **any** of the samples in the set
- One additional row with the data of **unassigned** reads
- One additional row with the total values of the frequencies. 


### File type B

This file type is a modification of the file 1 and it is generated only for the _Project_ level.

It is a CSV file with only these **columns**:

- TaxonomyName
- Sample1ID.direct.absolute.freq: This is the number of reads directly assigned to such Taxa for that sample
- Sample2ID.direct.absolute.freq: This is the number of reads directly assigned to such Taxa for that sample
...
- SampleNID.direct.absolute.freq: This is the number of reads directly assigned to such Taxa for that sample

And the **rows** as for the file type A


### File type C

This file type is a modification of the file 1 and it is generated only for the _Project_ level.

It is a CSV file with only these **columns**:

- TaxonomyName
- Sample1ID.direct.relative.freq: This is the percentage of reads directly assigned to such taxa for that sample. The normalization is done with the total number of reads of that sample
- Sample2ID.direct.relative.freq: This is the percentage of reads directly assigned to such taxa for that sample. The normalization is done with the total number of reads of that sample
...
- SampleNID.direct.relative.freq: This is the percentage of reads directly assigned to such taxa for that sample. The normalization is done with the total number of reads of that sample
- Total: this column contains the sum of the direct relative frequencies of all the samples

Only report in this file the **rows** that fulfill any of these 2 criteria:

- `Total` value is larger than the threshold x
- Any of the `direct.relative.freq` values is larger than the threshold x

### File type D

This file type is a modification of the file 1 and it is generated only for the _Group_ level.

It has **all the columns** as the file type A plus these two columns:

- average.direct.relative.freq: This fields has the average value of the direct relative frequencies of all the samples in the set
- average.cumulative.relative.freq: This fields has the average value of the cumulative relative frequencies of all the samples in the set

And about the **rows**, exactly the same as for the file type A



### File type E

FASTA file with the sequences of the reads. 

The header format must be like this

> > ReadID|SampleID|TaxonomyName|TaxonomyID|TaxonomyRank

It is important that you replace the spaces with underscore `_` in the _TaxonomyName_

### File type F

PDF reports and charts: TBD.


## Fixes in the current version

### `out.blast` files

- Add the headers in the files

- Make the file name unique, not `out.blast` for all the files. Maybe adding at the beginning the packet ID 

### viz `supermock3.pdf`

- Add a legend in the PDF with the meaning of both values in the boxes 

### CSV files

- Delete first `#` symbol in the file

- Make sure the fields are separated by commas `,`

- Add a row after the `unassigned` row with **total** values

