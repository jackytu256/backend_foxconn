CREATE TABLE img(
    id serial NOT NULL,
    path character(65),
    ts character(18)
)WITH (OIDS = FALSE);
CREATE TABLE lbl(
    id serial NOT NULL,
    name character(32)
) WITH (OIDS = FALSE);
CREATE TABLE attribs(
    id serial NOT NULL,
    did integer,
    iid integer,
    age integer,
    arched_eyebrows integer,
    attractive float,
    bags_under_eyes integer,
    beard_style integer,
    bushy_eyebrows integer,
    double_chin integer,
    eyeglasses integer,
    gender integer,
    hair_color integer,
    hair_length integer,
    hair_style integer,
    heavy_makeup integer,
    race integer,
    wearing_hat integer,
    interest integer,
    intent integer,
    angry float,
    confused float,
    contempt float,
    disgust float,
    fear float,
    happy float,
    neutral float, 
    sad float,
    surprise float,
    x float, 
    y float, 
    w float, 
    h float
)WITH (OIDS = FALSE);

ALTER TABLE img
  OWNER TO postgres;
ALTER TABLE lbl
  OWNER TO postgres;
ALTER TABLE attribs
  OWNER TO postgres;
