-- Insert para la tabla CC_PerimetroUrbano

INSERT INTO {schema}.cc_perimetrourbano (
    t_id,
    t_ili_tid,
    codigo_departamento,
    codigo_municipio,
    nombre_geografico,
    codigo_nombre,
    geometria
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    uuid_generate_v4(),
    codigo_departamento,
    codigo_municipio,
    nombre_geografico,
    codigo_nombre,
    geometria
FROM tmp_cc_perimetrourbano;
