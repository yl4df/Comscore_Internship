--Author: Yunlu Li
--Notes: 
--This is just a prototype, mainly for outlining queries and logic used in python program
--If we have access to most recent month data, use this to get month_id: select (DATE_PART('year', now()) - DATE_PART('year', '2000-01-01'::date))*12 + (DATE_PART('month', now()) - DATE_PART('month', '2000-01-01'::date))
--Select all and press F6 to run pgScript

--get cat_sub_cat id for each web_id in US and on total internet
drop table if exists yunlu_us_total_internet;
create table yunlu_us_total_internet as
 select a.* , b.cat_subcat_id from comscore.mpmmx_web_agg_232m_50000 as a
 inner join Comscore.mm200_cat_subcat_map_232m as b
 on a.web_id = b.web_id
where location_id = 100 and population_id = 840 
distributed randomly;

--table with web_id and category_id for top 10 in each category
DROP TABLE IF EXISTS yunlu_top10_each_category;
CREATE TABLE yunlu_top10_each_category(
web_id bigint,
category_id bigint
)
DISTRIBUTED by(web_id);

--using for loop to pull top from each category
CREATE OR REPLACE FUNCTION for_loop_through_category()
RETURNs void AS $$
DECLARE id bigint;
BEGIN 
FOR id in select cat_subcat_id from Comscore.mm200_cat_subcat_lookup where parent_id = 1
LOOP
INSERT INTO yunlu_top10_each_category(web_id, category_id)
select web_id, cat_subcat_id
from yunlu_us_total_internet where cat_subcat_id = id order by visitors_proj desc LIMIT 10;
end LOOP;
END $$ LANGUAGE plpgsql;

--you have to run the function first, and then get the table
select for_loop_through_category();
select * from yunlu_top10_each_category ;

--time-series table with 3 metrices for each web_id
DROP TABLE IF EXISTS yunlu_proj2_gender_age;
CREATE TABLE yunlu_proj2_gender_age
( web_id bigint,
  month_id int,
  gender_age_id bigint,
  UV numeric(20,6),
  Page_Views numeric(20,6),
  Duration numeric(20,6)
)
DISTRIBUTED BY (web_id);

--using while loop to pull out the entity level data
SET OPTIMIZER = OFF;
SET @startdate = 230;
SET @enddate = 201;
SET @month_id = @startdate;
SET @loc_id = 100;
SET @pop_id = 840;
SET @tablename= 'yunlu_proj2_gender_age';
SET @ltt_table = 'comscore.Mpmmx_ltt_'+ cast(@month_id as string) +'m_50000';

WHILE @month_id > @enddate
   BEGIN
    SET @month_id = cast(@month_id as string);
    SET @ltt_table = 'comscore.Mpmmx_ltt_'+ cast(@month_id as string) +'m_50000';
    INSERT INTO @tablename select 
    web_id, 
    month_id, 
    gender_age_id,
    sum(visitors_proj) as UV, 
    sum(pages_proj) as Page_Views, 
    sum(minutes_proj) as Duration
    
    from @ltt_table where location_id = @loc_id and population_id = @pop_id and web_id in (select web_id from yunlu_top10_each_category)
    group by web_id, month_id, gender_age_id;
    SET @month_id = cast(@month_id as integer)- 1;
 
  END

--using while loop to pull out category_level data
SET OPTIMIZER = OFF;
SET @startdate = 230;
SET @enddate = 201;
SET @month_id = @startdate;
SET @loc_id = 100;
SET @pop_id = 840;
SET @tablename= 'yunlu_proj2_gender_age';
SET @ltt_table = 'comscore.Mpmmx_ltt_'+ cast(@month_id as string) +'m_50000';
WHILE @month_id > @enddate
   BEGIN
    SET @month_id = cast(@month_id as string);
    SET @ltt_table = 'comscore.Mpmmx_ltt_'+ cast(@month_id as string) +'m_50000';
    INSERT INTO @tablename select 
    web_id, 
    month_id, 
    gender_age_id,
    sum(visitors_proj) as UV, 
    sum(pages_proj) as Page_Views, 
    sum(minutes_proj) as Duration
    
    from @ltt_table where location_id = @loc_id and population_id = @pop_id and web_id in (select cat_subcat_id from Comscore.mm200_cat_subcat_lookup where parent_id = 1)
    group by web_id, month_id, gender_age_id;
    SET @month_id = cast(@month_id as integer)- 1;
 
  END

--append web_name
DROP TABLE IF EXISTS yunlu_proj2_gender_age_name_noncat;
CREATE TABLE yunlu_proj2_gender_age_name_noncat AS
select h.web_name, i.* from yunlu_proj2_gender_age as i
left join comScore.mm200_hierarchy_web_lookup_232m as h
on i.web_id = h.web_id
DISTRIBUTED RANDOMLY;

--append category name
DROP TABLE IF EXISTS yunlu_proj2_gender_age_name_cat;
CREATE TABLE yunlu_proj2_gender_age_name_cat AS
select b.cat_subcat_name as web_name, a .web_id, a.month_id, a.gender_age_id, a.uv, a.page_views, a.duration from (select * from yunlu_proj2_gender_age_name_noncat where web_name isnull) as a
inner join Comscore.mm200_cat_subcat_lookup as b
on a.web_id = b.cat_subcat_id
DISTRIBUTED RANDOMLY;

--union two tables above
DROP TABLE IF EXISTS yunlu_proj2_gender_age_name;
CREATE TABLE yunlu_proj2_gender_age_name AS
select * from yunlu_proj2_gender_age_name_noncat where web_name is not null
union
select * from yunlu_proj2_gender_age_name_cat
DISTRIBUTED RANDOMLY;

--we want to make sure time-series data has no gap in betwee, so we only choose whose data is consecutive for 29 months
--for automation, this number should be set as start_month-end_month+1
DROP TABLE IF EXISTS yunlu_proj2_gender_age_filtered;
CREATE TABLE yunlu_proj2_gender_age_filtered AS
SELECT *
from yunlu_proj2_gender_age_name where web_id in (select web_id from yunlu_proj2_gender_age_name group by web_id, gender_age_id having count(*)=29) 
DISTRIBUTED RANDOMLY;

--append descriptive text
DROP TABLE IF EXISTS yunlu_proj2_gender_age_text;
CREATE TABLE yunlu_proj2_gender_age_text AS
SELECT a.web_name, a.web_id, a.month_id, a.gender_age_id, b.gender_id, b.desc_text, a.uv, a.page_views, a.duration 
from yunlu_proj2_gender_age_filtered as a
inner join (select distinct value, desc_text, gender_id from comScore.mm200_gender_age_lookup where population_id = 840) as b
on a.gender_age_id = b.value
DISTRIBUTED RANDOMLY;


--append category_name
DROP TABLE IF EXISTS yunlu_proj2_gender_age_category;
CREATE TABLE yunlu_proj2_gender_age_category AS
select a.*, b.category_id, c.cat_subcat_name from yunlu_proj2_gender_age_text as a
left join yunlu_top10_each_category as b
on a.web_id = b.web_id
left join Comscore.mm200_cat_subcat_lookup as c
on b.category_id = c.cat_subcat_id
DISTRIBUTED RANDOMLY;

--for category level data, set their category_name as web_name and same for id
Update yunlu_proj2_gender_age_category
SET category_id = web_id
where category_id isnull ;

Update yunlu_proj2_gender_age_category
SET cat_subcat_name = web_name
where cat_subcat_name isnull ;

--export data
DROP EXTERNAL TABLE IF EXISTS yunlu_proj2_gender_age_result;
CREATE WRITABLE EXTERNAL TABLE yunlu_proj2_gender_age_result
(
LIKE yunlu_proj2_gender_age_category
)
LOCATION
(
'gpfdist://csia2gpl06-1:8081/yuli/DATA/yunlu_gender_age_prototype.txt'
)
FORMAT 'text' (delimiter E'\t' null 'null' escape 'off')
ENCODING 'UTF8';

INSERT INTO yunlu_proj2_gender_age_result
SELECT *
from yunlu_proj2_gender_age_category;



