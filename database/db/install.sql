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
    width integer,
    height integer,
    exif_camera_model text,
    exif_focal_length text,
    exif_exposure_time text,
    exif_date_time timestamptz,
    exif_aperture_value text,
    exif_iso text
);
