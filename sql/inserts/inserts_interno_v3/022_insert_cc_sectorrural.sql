-- Insert para la tabla CC_SectorRural

INSERT INTO {schema}.cc_sectorrural (
    t_id,
    t_ili_tid,
    codigo,
    geometria
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    uuid_generate_v4(),
    codigo,
    geometria
FROM tmp_cc_sectorrural;
