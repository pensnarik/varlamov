\set ON_ERROR_STOP 1

create database :database;

\connect :database

create extension plpythonu;

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
    date_modified timestamptz
);

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

create unique index on image (post_id, url);

comment on column public.image.file_size is 'File size in bytes';

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
  group by 1
  having count(*) >= 100
  order by 1;

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
   where "row_number" < 3;
