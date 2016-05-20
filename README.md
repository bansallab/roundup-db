### Introduction

Livestock markets with an online presence may post data about cattle sales, which the companion [roundup](https://github.com/bansallab/roundup) repository writes to local CSV files. This repository archives that data in a MySQL database, which the Bansal Lab uses to study movement of livestock between animal holding premises, such as a ranch, market or feed lot. The repository also includes post-processing steps that attempt to identify the source and destination counties for every recorded movement and aggregates external datasets relevant to cattle movement (e.g. the USDA Agriculural Census).

### Tables in the Database

#### Tables that describe premises and sales/movements

1. address - As much address information as we obtain from one source about a premises
1. premises - A unique identifier for a physical premises
1. association - An association table linking premises to addresses
1. geoname - Locations (including www.geonames.org identifiers) of premises
1. movement - Representative lots recorded from a market report
1. georef - The latitude and longitude of a verified premises
1. roundup_report - The report from which representative lots were read

#### Tables that list cattle markets, websites or other market related data

1. ag_census
1. ams
1. ams_location
1. ams_quantity
1. ams_receipts
1. aphis
1. gipsa
1. lma
1. loadboard
1. roundup_market
1. roundup_website
1. state_code
1. county

See `db_class.py` for fields within tables and relationships between them.

#### Instructions for adding new markets from project roundup:

+ Do this *before* importing the first market report from the source.
+ Ensure that roundup_website has an entry with the correct URL.
+ Insert an address record for the market, with source = 'roundup_market'.
+ Insert a roundup_market record, referencing the new addres_id and roundup_website_id
+ Insert an association record to any known premises.
+ Insert a new record into georef, after obtaining the market's latitude and longitude.
