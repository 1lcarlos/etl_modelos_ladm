-- INSERT para tabla ric_datosphcondominio
-- Fecha: 2025-12-19
INSERT INTO
    ric.ric_datosphcondominio (
        t_id,
        t_ili_tid,
        area_total_terreno,
        area_total_terreno_privada,
        area_total_terreno_comun,
        area_total_construida,
        area_total_construida_privada,
        area_total_construida_comun,
        numero_torres,
        total_unidades_privadas,
        ric_predio
    )
SELECT
    --nextval('ric.t_ili2db_seq'::regclass),
    dc.id::bigint,
    uuid_generate_v4 (),
    dc.area_total_terreno::numeric(15,2),
    dc.area_total_terreno_privada::numeric(15,2),
    dc.area_total_terreno_comun::numeric(15,2),
    dc.area_total_construida::numeric(15,2),
    dc.area_total_construida_privada::numeric(15,2),
    dc.area_total_construida_comun::numeric(15,2),
    dc.numero_torres::integer,
    dc.total_unidades_privadas::integer,
    (
        SELECT
            rp.t_id
        FROM
            ric.ric_predio rp
        WHERE
            rp.numero_predial = dc.numero_predial
        LIMIT
            1
    )
FROM
    tmp_datosphcondominio dc
WHERE
    dc.numero_predial IS NOT NULL
    AND EXISTS (
        SELECT
            1
        FROM
            ric.ric_predio rp
        WHERE
            rp.numero_predial = dc.numero_predial and SUBSTRING(rp.numero_predial FROM 22 FOR 9) in ('900000000','800000000')
    );