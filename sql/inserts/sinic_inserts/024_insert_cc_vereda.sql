-- Insert para la tabla CC_Vereda

INSERT INTO {schema}.cc_vereda (
    t_id,
    t_ili_tid,
    codigo,
    nombre,
    geometria
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    uuid_generate_v4(),
    codigo,
    nombre,
    geometria
FROM tmp_cc_vereda;
