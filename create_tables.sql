use role sysadmin;
create or replace database millionSongs_ETL;
use database millionSongs_ETL;
create or replace schema millionSongs_ETL_DWH;
use schema millionSongs_ETL_DWH;

create or replace table songplays
(
	songplay_id text, 
	start_time text, 
	user_id text, 
	level text, 
	song_id text, 
	artist_id text, 
	session_id text, 
	location text, 
	user_agent text
);

create or replace table users
(
	user_id text, 
	first_name text, 
	last_name text, 
	gender text, 
	level text
);

create or replace table songs
(
	song_id text, 
	title text,
	artist_id text, 
	year text, 
	duration text
);

create or replace table artists
(
	artist_id text, 
	name text,
	location text, 
	latitude text, 
	longitude text
);

create or replace table time
(
	start_time text, 
	hour text,
	day text, 
	week text, 
	month text,
	year text, 
	weekday text
);

