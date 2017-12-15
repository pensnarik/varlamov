\set ON_ERROR_STOP 1

create database :database owner :user;

\connect :database

/* Write your SQL code here. You may include scripts from directories "data" and "schema"

   CREATE TABLE test (id serial, value text);

   \i data/public.test.sql
   \i schema/public/tables/test.sql
*/

create table public.post
(
    id serial primary key,
    url text not null unique,
    title text,
    date_published timestamptz,
    date_modified timestamptz,
    tags text[]
);

grant select, insert, update, delete on public.post to :user;
grant select, usage on sequence public.post_id_seq to :user;

create table public.image
(
    id serial primary key,
    post_id integer not null references public.post(id),
    url text not null,
    file_size integer,
    width integer,
    height integer,
    exif_camera_model text,
    exif_focal_length text,
    exif_exposure_time text,
    exif_date_time timestamptz,
    exif_aperture_value text,
    exif_iso text
);

grant select, insert, update, delete on public.image to :user;
grant select, usage on sequence public.image_id_seq to :user;

create unique index on image (post_id, url);

comment on column public.image.file_size is 'File size in bytes';

drop view if exists cameras_stat;
create view cameras_stat as
select exif_camera_model,
       sum(case when date_trunc('y', date_published) = '2008-01-01' then 1 else 0 end) as "2008",
       sum(case when date_trunc('y', date_published) = '2009-01-01' then 1 else 0 end) as "2009",
       sum(case when date_trunc('y', date_published) = '2010-01-01' then 1 else 0 end) as "2010",
       sum(case when date_trunc('y', date_published) = '2011-01-01' then 1 else 0 end) as "2011",
       sum(case when date_trunc('y', date_published) = '2012-01-01' then 1 else 0 end) as "2012",
       sum(case when date_trunc('y', date_published) = '2013-01-01' then 1 else 0 end) as "2013",
       sum(case when date_trunc('y', date_published) = '2014-01-01' then 1 else 0 end) as "2014",
       sum(case when date_trunc('y', date_published) = '2015-01-01' then 1 else 0 end) as "2015",
       sum(case when date_trunc('y', date_published) = '2016-01-01' then 1 else 0 end) as "2016",
       sum(case when date_trunc('y', date_published) = '2017-01-01' then 1 else 0 end) as "2017",
       count(*) as "total"
  from image i
  join post p on p.id = i .post_id
 where i.exif_camera_model is not null
  group by 1
  having count(*) >= 500
  order by 1;

create view iso_stat as
select exif_iso,
       sum(case when date_trunc('y', date_published) = '2008-01-01' then 1 else 0 end) as "2008",
       sum(case when date_trunc('y', date_published) = '2009-01-01' then 1 else 0 end) as "2009",
       sum(case when date_trunc('y', date_published) = '2010-01-01' then 1 else 0 end) as "2010",
       sum(case when date_trunc('y', date_published) = '2011-01-01' then 1 else 0 end) as "2011",
       sum(case when date_trunc('y', date_published) = '2012-01-01' then 1 else 0 end) as "2012",
       sum(case when date_trunc('y', date_published) = '2013-01-01' then 1 else 0 end) as "2013",
       sum(case when date_trunc('y', date_published) = '2014-01-01' then 1 else 0 end) as "2014",
       sum(case when date_trunc('y', date_published) = '2015-01-01' then 1 else 0 end) as "2015",
       sum(case when date_trunc('y', date_published) = '2016-01-01' then 1 else 0 end) as "2016",
       sum(case when date_trunc('y', date_published) = '2017-01-01' then 1 else 0 end) as "2017",
       count(*) as "total"
  from image i
  join post p on p.id = i .post_id
 where i.exif_iso is not null
  group by 1
  having count(*) >= 100
  order by exif_iso::integer;

drop view if exists posts_stat;
create view posts_stat as
select 'Posts count' as metric,
       sum(case when date_trunc('y', date_published) = '2008-01-01' then 1 else 0 end) as "2008",
       sum(case when date_trunc('y', date_published) = '2009-01-01' then 1 else 0 end) as "2009",
       sum(case when date_trunc('y', date_published) = '2010-01-01' then 1 else 0 end) as "2010",
       sum(case when date_trunc('y', date_published) = '2011-01-01' then 1 else 0 end) as "2011",
       sum(case when date_trunc('y', date_published) = '2012-01-01' then 1 else 0 end) as "2012",
       sum(case when date_trunc('y', date_published) = '2013-01-01' then 1 else 0 end) as "2013",
       sum(case when date_trunc('y', date_published) = '2014-01-01' then 1 else 0 end) as "2014",
       sum(case when date_trunc('y', date_published) = '2015-01-01' then 1 else 0 end) as "2015",
       sum(case when date_trunc('y', date_published) = '2016-01-01' then 1 else 0 end) as "2016",
       sum(case when date_trunc('y', date_published) = '2017-01-01' then 1 else 0 end) as "2017"
from post group by 1 order by 1;

drop view if exists most_popular_dimensions;
create view most_popular_dimensions as
with data as (
    select date_trunc('y', date_published) as date_published,
           format('%s x %s', greatest(width, height), least(width, height)) as image_size,
           count(*)
      from image i
      join post p on p.id = i.post_id
      group by 1, 2 order by 1 asc, 3 desc
),
sorted_data as (
    select t.*,
           row_number() over (partition by date_published order by "count" desc)
      from data t
    order by 1, 4
) select *
    from sorted_data
   where "row_number" = 1;

/* Самые популярные теги за всё время */

with
raw_tags as (
  select unnest(tags) tag, to_char(date_published, 'yyyy') as year
    from post
),
all_tags as (
  select tag, year, count(*),
         rank() over(partition by year order by count(*) desc)
    from raw_tags
   where tag != 'p-news'
    group by 1, 2
),
top_tags as (
  select * from all_tags where "rank" <= 5 order by 2, 4
),
uniq_tags as (
  select distinct tag from top_tags
),
uniq_years as (
  select distinct year from top_tags
),
flat_table as (
select ut.tag,
       uy.year,
       coalesce("count", 0) as tags_count,
       t."rank"
  from uniq_tags ut
  join uniq_years uy on true
  left join top_tags t on t.tag = ut.tag and t.year = uy.year
)
select tag,
       sum(case when year = '2008' then tags_count else 0 end) as "2008",
       sum(case when year = '2009' then tags_count else 0 end) as "2009",
       sum(case when year = '2010' then tags_count else 0 end) as "2010",
       sum(case when year = '2011' then tags_count else 0 end) as "2011",
       sum(case when year = '2012' then tags_count else 0 end) as "2012",
       sum(case when year = '2013' then tags_count else 0 end) as "2013",
       sum(case when year = '2014' then tags_count else 0 end) as "2014",
       sum(case when year = '2015' then tags_count else 0 end) as "2015",
       sum(case when year = '2016' then tags_count else 0 end) as "2016",
       sum(case when year = '2017' then tags_count else 0 end) as "2017"
  from flat_table
  group by 1;
