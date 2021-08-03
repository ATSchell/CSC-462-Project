CREATE DATABASE lodcaldb

drop TABLE overlays

CREATE TABLE overlays (
    overlay_id int PRIMARY KEY,
    data_name varchar(25),
    data_description varchar(100),
    ul_lat float(32),
    ul_lng float(32),
    lr_lat float(32),
    lr_lng float(32),
    resolution int,
    file_path varchar(255),
    created timestamp,
    creator varchar(25),
    is_earth_daily boolean NOT NULL,
);

insert into overlays values (1, 'klinaklini', 'ndvi', 51.4464, -125.9569, 51.7711, -125.7711, 10, NULL, CURRENT_TIMESTAMP, 'owner', true);