-- INSERT para tabla col_uebaunit (relacion construccion-predio)
-- Fecha: 2025-12-19
INSERT INTO
    ric.col_uebaunit (
        t_id,
        ue_ric_terreno,
        ue_ric_unidadconstruccion,
        ue_ric_construccion,
        ue_ric_nu_espaciojuridicoredservicios,
        ue_ric_nu_espaciojuridicounidadedificacion,
        baunit
    )
SELECT
    nextval('ric.t_ili2db_seq'::regclass),
    --cu.id::bigint,
    NULL,
    NULL,
    (
        SELECT
            rc.t_id
        FROM
            ric.ric_construccion rc
        WHERE
            rc.local_id::integer = cu.ue_gc_construccion::integer
        LIMIT
            1
    ),
    NULL,
    NULL,
    (
        SELECT
            rp.t_id
        FROM
            ric.ric_predio rp
        WHERE
            rp.local_id::integer = cu.unidad::integer
        LIMIT
            1
    )
FROM
    tmp_col_uebaunit_predio_construccion cu
WHERE
    cu.ue_gc_construccion IS NOT NULL
    AND cu.unidad IS NOT NULL
    AND EXISTS (
        SELECT
            1
        FROM
            ric.ric_construccion rc
        WHERE
            rc.local_id::integer = cu.ue_gc_construccion::integer
    )
    AND EXISTS (
        SELECT
            1
        FROM
            ric.ric_predio rp
        WHERE
            rp.local_id::integer = cu.unidad::integer
    );