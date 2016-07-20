## roundup_report

select *
from roundup_report
where substr(mns_id, -2) != extract(day from date) and mns_id != 'na';

select count(*)
from roundup_report
where mns_id is null;

select * from roundup_report where roundup_report_id = 5352;

## roundup_website

select roundup_website_id, script, mns_id, name, city, state
from roundup_website
join roundup_market using(roundup_website_id)
join address using(address_id)
where script is not null and mns_id is null
order by state;

## report by destination state

select roundup_report.*
from roundup_report
join movement using(roundup_report_id)
join address on movement.to_address_id = address.address_id
where address.state = 'TX' and address.source = 'roundup_market'
group by roundup_report_id
order by date;

## movement by origin and destination state

select movement.*, roundup_report.date
from movement
join roundup_report using (roundup_report_id)
join address on movement.to_address_id = address.address_id
join association on association.address_id = movement.from_address_id and association.to_address_id = movement.to_address_id
join premises using (premises_id)
join geoname using (geoname_id)
where address.state = 'CA' and address.source = 'roundup_market' and geoname.adminCode1 != 'CA' and geoname.fuzzy = 1
order by date;

## origin location by to_address_id

select geoname.*
from geoname
join premises using(geoname_id)
join association using(premises_id)
join movement on (movement.from_address_id = association.address_id and movement.to_address_id = association.to_address_id)
where movement.to_address_id = 52273;

## report by date and day with movement count

select reference, date, receipts, count(movement.movement_id) as 'ct'
from roundup_report
join movement
using(roundup_report_id)
where date > '2015-10-10' and dayofweek(roundup_report.date) = 6
group by roundup_report_id
order by reference, date;

## comparison of roundup_market and aphis market lists

select roundup_market.premises_id, address.name, address.state, aphis.address_id, aphis.foreign_id
from association join aphis using(address_id)
right join (select premises_id, address_id from association natural join roundup_market group by premises_id) as roundup_market using(premises_id)
join address on roundup_market.address_id = address.address_id
order by foreign_id desc;

## AMS Volumes

select ams_location.*
from ams_location
join (
  select *
  from (
    select office, location
    from ams_quantity
    where year(date) = 2009 and species like 'Ca%' and type_of_sale = 'A'
    union all
    select office, location
    from ams_receipts
    where year(date) = 2009 and species = 'C' and type_of_sale = 'A'
    ) as tmp
  group by office, location
  ) as qty
on LS = office and lslocation_id = location
order by LS, lslocation_id;

select ST, count(*)
from ams_location
join (
  select *
  from (
    select office, location
    from ams_quantity
    where year(date) = 2009 and species like 'Ca%' and type_of_sale = 'A'
    union all
    select office, location
    from ams_receipts
    where year(date) = 2009 and species = 'C' and type_of_sale = 'A'
    ) as tmp
  group by office, location
  ) as qty
on LS = office and lslocation_id = location
group by ST;

select adminCode1, count(distinct premises_id)
from address
join association
using(address_id)
join premises using(premises_id)
join geoname using(geoname_id)
where from_address_id is null and to_address_id is null
group by adminCode1;

select premises_id, address.name, address.city, sum(address.source = 'ams') as ams
from address
join association using(address_id)
join premises using(premises_id)
join geoname using(geoname_id)
join state_code using(adminCode1)
join county on adminCode2 = county.county and state_code_id = county.state
where address.source = 'roundup_market' and county.ers_region = 3
group by premises_id;

select premises_id, lslocation_name, count(*)
from ams_location
join (
  select *
  from (
    select office, location
    from ams_quantity
    where year(date) = 2009 and species like 'Ca%' and type_of_sale = 'A'
    union all
    select office, location
    from ams_receipts
    where year(date) = 2009 and species = 'C' and type_of_sale = 'A'
    ) as tmp
  group by office, location
  ) as qty
on LS = office and lslocation_id = location
join ams on lslocation_id = foreign_id
join address using(address_id)
join association using(address_id)
join premises using(premises_id)
join geoname using(geoname_id)
join state_code using(adminCode1)
join county on adminCode2 = county.county and state_code_id = county.state
where county.ers_region = 3
group by premises_id;

select premises_id, address.*
from premises
join association using(premises_id)
join address using(address_id)
join geoname using(geoname_id)
join state_code using(adminCode1)
join (select state as state_code_id from county where county.ers_region = 3 group by state) as ers using(state_code_id)
where address.source != 'roundup';

select premises_id, lat, lng
from georef
right join premises using(premises_id)
join association using(premises_id)
join address using(address_id)
join geoname using(geoname_id)
join state_code using(adminCode1)
join (select state as state_code_id from county where county.ers_region = 3 group by state) as ers using(state_code_id)
where address.source != 'roundup'
group by premises_id;


## unsorted

select movement.roundup_report_id, sum(if(movement.head regexp '^[0-9]+$', movement.head, 1)) as 'head'
from movement
join association on (movement.from_address_id = association.address_id and movement.to_address_id = association.to_address_id)
join premises using(premises_id)
join geoname using(geoname_id)
where movement.to_address_id = 13633
group by roundup_report_id;

select *
from address
where address_id=32203;

select geoname.fuzzy, count(geoname.geoname_id)
from geoname
join premises on geoname.geoname_id = premises.geoname_id

join address on association.to_address_id = address.address_id
where address.source = 'roundup_market'
group by geoname.fuzzy;

select address.*
from movement join address as m on to_address_id = m.address_id
right join address on from_address_id = address.address_id
where from_address_id is null and address.source = 'roundup' and m.source = 'roundup_market';

select address.*, geoname.*
from movement right join address on address_id = movement.from_address_id
left join geoname using(address_id)
where movement.movement_id is null and source = 'roundup' limit 10;

select * from address where name like 'Williams Ranch';

select address_id, roundup_website_id, premises_id, script
from roundup_website
left join roundup_market using(roundup_website_id)
where script is not null
order by script;

select reference, roundup_report.head > 0 as test
from roundup_report
join movement using(roundup_report_id)
join roundup_market on to_address_id = address_id
where year(date) >= 2015
group by address_id, test
order by reference;

select movement.*
from movement join roundup_report using(roundup_report_id)
where reference like 'sterling%' and movement.cattle not regexp('^[0-9]') limit 10;

select *
from roundup_report
where roundup_report_id in (1062, 1041, 989);

select roundup_website_id, script, last_check, website, note
from roundup_website
where website is not null and script is null and last_check is null;

-- CAUTION --
quit;  -- here in case some fool runs the whole sql file

-- roundup market verification and removal

-- extract list for review
select premises_id, lat, lng
into outfile '/tmp/sheet0.csv'
from georef
right join association using(premises_id)
join address using(address_id)
where address.source != 'roundup'
group by premises_id;
select premises_id, name, address, po, city, state, zip, zip_ext
into outfile '/tmp/sheet1.csv'
from association 
join address using(address_id)
where source != 'roundup';

-- load verification in georef_temporary, be sure to run mysql with "--local-infile"
drop table if exists georef_temporary;
create table georef_temporary like georef;
alter table georef_temporary add column verified bool;
load data local infile 'roundup_market_verification.csv'
into table georef_temporary
columns terminated by ","
ignore 1 lines
(premises_id, lat, lng, verified);

begin;
delete address			-- cascades to relevant market table, association
from association		-- if in geoname.address_id:
join georef_temporary using(premises_id) -- also cascades to geoname, premises
join address using(address_id)
where verified = 0;             
delete premises			-- catches premises not in above cascade
from premises
join georef_temporary using(premises_id)
where verified = 0;
# commit;

begin;
insert into georef (premises_id, lat, lng, source)
select gt.premises_id, gt.lat, gt.lng, 1
from georef_temporary as gt
left join georef using(premises_id)
where verified = 1 and georef.georef_id is null;
# commit;

-- roundup_website updates

update roundup_website
set last_check=curdate()
where roundup_website_id=312;

update roundup_website
set note='ranch name, no location'
where roundup_website_id=332;

delete
from roundup_report
where reference like 'billings%' and extract(year from date) > 2012;

-- EXTREME CAUTION --

create database cownet character set utf8;
grant all on cownet.* to 'icarroll'@'localhost';

--delete premises

from premises
join association using(premises_id)
where from_address_id is not null;
