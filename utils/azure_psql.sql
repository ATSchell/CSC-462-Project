-- Table: public.overlays

-- DROP TABLE public.overlays;

CREATE TABLE public.overlays
(
    created timestamp without time zone,
    creator character varying(25) COLLATE pg_catalog."default" NOT NULL,
    data_description character varying(100) COLLATE pg_catalog."default",
    data_name character varying(25) COLLATE pg_catalog."default",
    file_path character varying(255) COLLATE pg_catalog."default",
    lr_lat numeric,
    lr_lng numeric,
    overlay_id integer NOT NULL,
    resolution integer,
    ul_lat numeric,
    ul_lng numeric,
    is_earth_daily boolean NOT NULL,
    vector boolean NOT NULL DEFAULT false,
    is_point_entry boolean NOT NULL DEFAULT false,
    CONSTRAINT overlays_pkey PRIMARY KEY (overlay_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.overlays
    OWNER to azure_user;