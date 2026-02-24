-- ============================================================================
-- ARCHIVO 04: Verificacion de Integridad Referencial (Nivel 4)
-- ============================================================================
-- Ejecutar desde: pgAdmin conectado a base DESTINO (puerto 5432, db actualizacion)
-- Proposito: Detectar registros huerfanos en cada relacion FK
-- Fecha: 2026-02-08
-- ============================================================================

-- ============================================================================
-- SECCION A: Verificacion de FKs - Registros huerfanos
-- ============================================================================

WITH fk_checks AS (

    -- 1. extdireccion.gc_predio_direccion -> gc_predio.id
    SELECT
        'extdireccion.gc_predio_direccion -> gc_predio.id' AS relacion,
        (SELECT count(*) FROM cun25436.extdireccion) AS total_registros,
        (SELECT count(*) FROM cun25436.extdireccion ed
         WHERE EXISTS (SELECT 1 FROM cun25436.gc_predio gp WHERE gp.id = ed.gc_predio_direccion)) AS fk_validas,
        (SELECT count(*) FROM cun25436.extdireccion ed
         WHERE ed.gc_predio_direccion IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.gc_predio gp WHERE gp.id = ed.gc_predio_direccion)) AS fk_huerfanas

    UNION ALL

    -- 2. dlc_datosadicionaleslevantamientocatastral.gc_predio -> gc_predio.id
    SELECT
        'dlc_datos.gc_predio -> gc_predio.id',
        (SELECT count(*) FROM cun25436.dlc_datosadicionaleslevantamientocatastral),
        (SELECT count(*) FROM cun25436.dlc_datosadicionaleslevantamientocatastral dlc
         WHERE EXISTS (SELECT 1 FROM cun25436.gc_predio gp WHERE gp.id = dlc.gc_predio)),
        (SELECT count(*) FROM cun25436.dlc_datosadicionaleslevantamientocatastral dlc
         WHERE dlc.gc_predio IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.gc_predio gp WHERE gp.id = dlc.gc_predio))

    UNION ALL

    -- 3. gc_estructuranovedadnumeropredial.gc_predio_novedad_numeros_prediales -> dlc_datos.id
    SELECT
        'gc_estructuranovedad -> dlc_datos.id',
        (SELECT count(*) FROM cun25436.gc_estructuranovedadnumeropredial),
        (SELECT count(*) FROM cun25436.gc_estructuranovedadnumeropredial en
         WHERE EXISTS (SELECT 1 FROM cun25436.dlc_datosadicionaleslevantamientocatastral dlc
                       WHERE dlc.id = en.gc_predio_novedad_numeros_prediales)),
        (SELECT count(*) FROM cun25436.gc_estructuranovedadnumeropredial en
         WHERE en.gc_predio_novedad_numeros_prediales IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.dlc_datosadicionaleslevantamientocatastral dlc
                           WHERE dlc.id = en.gc_predio_novedad_numeros_prediales))

    UNION ALL

    -- 4. gc_unidadconstruccion.gc_construccion -> gc_construccion.id
    SELECT
        'gc_unidadconst.gc_construccion -> gc_construccion.id',
        (SELECT count(*) FROM cun25436.gc_unidadconstruccion),
        (SELECT count(*) FROM cun25436.gc_unidadconstruccion uc
         WHERE EXISTS (SELECT 1 FROM cun25436.gc_construccion gc WHERE gc.id = uc.gc_construccion)),
        (SELECT count(*) FROM cun25436.gc_unidadconstruccion uc
         WHERE uc.gc_construccion IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.gc_construccion gc WHERE gc.id = uc.gc_construccion))

    UNION ALL

    -- 5. gc_unidadconstruccion.gc_caracteristicasunidadconstruccion -> gc_caract.id
    SELECT
        'gc_unidadconst.gc_caract -> gc_caract.id',
        (SELECT count(*) FROM cun25436.gc_unidadconstruccion),
        (SELECT count(*) FROM cun25436.gc_unidadconstruccion uc
         WHERE EXISTS (SELECT 1 FROM cun25436.gc_caracteristicasunidadconstruccion gc
                       WHERE gc.id = uc.gc_caracteristicasunidadconstruccion)),
        (SELECT count(*) FROM cun25436.gc_unidadconstruccion uc
         WHERE uc.gc_caracteristicasunidadconstruccion IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.gc_caracteristicasunidadconstruccion gc
                           WHERE gc.id = uc.gc_caracteristicasunidadconstruccion))

    UNION ALL

    -- 6. col_uebaunit.unidad -> gc_predio.id (terreno)
    SELECT
        'col_uebaunit(terreno).unidad -> gc_predio.id',
        (SELECT count(*) FROM cun25436.col_uebaunit WHERE ue_gc_terreno IS NOT NULL),
        (SELECT count(*) FROM cun25436.col_uebaunit cu
         WHERE cu.ue_gc_terreno IS NOT NULL
           AND EXISTS (SELECT 1 FROM cun25436.gc_predio gp WHERE gp.id = cu.unidad)),
        (SELECT count(*) FROM cun25436.col_uebaunit cu
         WHERE cu.ue_gc_terreno IS NOT NULL
           AND cu.unidad IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.gc_predio gp WHERE gp.id = cu.unidad))

    UNION ALL

    -- 7. col_uebaunit.ue_gc_terreno -> gc_terreno.id
    SELECT
        'col_uebaunit.ue_gc_terreno -> gc_terreno.id',
        (SELECT count(*) FROM cun25436.col_uebaunit WHERE ue_gc_terreno IS NOT NULL),
        (SELECT count(*) FROM cun25436.col_uebaunit cu
         WHERE cu.ue_gc_terreno IS NOT NULL
           AND EXISTS (SELECT 1 FROM cun25436.gc_terreno gt WHERE gt.id = cu.ue_gc_terreno)),
        (SELECT count(*) FROM cun25436.col_uebaunit cu
         WHERE cu.ue_gc_terreno IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.gc_terreno gt WHERE gt.id = cu.ue_gc_terreno))

    UNION ALL

    -- 8. col_uebaunit.ue_gc_construccion -> gc_construccion.id
    SELECT
        'col_uebaunit.ue_gc_construccion -> gc_construccion.id',
        (SELECT count(*) FROM cun25436.col_uebaunit WHERE ue_gc_construccion IS NOT NULL),
        (SELECT count(*) FROM cun25436.col_uebaunit cu
         WHERE cu.ue_gc_construccion IS NOT NULL
           AND EXISTS (SELECT 1 FROM cun25436.gc_construccion gc WHERE gc.id = cu.ue_gc_construccion)),
        (SELECT count(*) FROM cun25436.col_uebaunit cu
         WHERE cu.ue_gc_construccion IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.gc_construccion gc WHERE gc.id = cu.ue_gc_construccion))

    UNION ALL

    -- 9. col_uebaunit.ue_gc_unidadconstruccion -> gc_unidadconstruccion.id
    SELECT
        'col_uebaunit.ue_gc_unidadconst -> gc_unidadconst.id',
        (SELECT count(*) FROM cun25436.col_uebaunit WHERE ue_gc_unidadconstruccion IS NOT NULL),
        (SELECT count(*) FROM cun25436.col_uebaunit cu
         WHERE cu.ue_gc_unidadconstruccion IS NOT NULL
           AND EXISTS (SELECT 1 FROM cun25436.gc_unidadconstruccion gu WHERE gu.id = cu.ue_gc_unidadconstruccion)),
        (SELECT count(*) FROM cun25436.col_uebaunit cu
         WHERE cu.ue_gc_unidadconstruccion IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.gc_unidadconstruccion gu WHERE gu.id = cu.ue_gc_unidadconstruccion))

    UNION ALL

    -- 10. col_uebaunit(construccion).unidad -> gc_predio.id
    SELECT
        'col_uebaunit(constr).unidad -> gc_predio.id',
        (SELECT count(*) FROM cun25436.col_uebaunit WHERE ue_gc_construccion IS NOT NULL),
        (SELECT count(*) FROM cun25436.col_uebaunit cu
         WHERE cu.ue_gc_construccion IS NOT NULL
           AND EXISTS (SELECT 1 FROM cun25436.gc_predio gp WHERE gp.id = cu.unidad)),
        (SELECT count(*) FROM cun25436.col_uebaunit cu
         WHERE cu.ue_gc_construccion IS NOT NULL
           AND cu.unidad IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.gc_predio gp WHERE gp.id = cu.unidad))

    UNION ALL

    -- 11. col_uebaunit(unidadconst).unidad -> gc_predio.id
    SELECT
        'col_uebaunit(unidconst).unidad -> gc_predio.id',
        (SELECT count(*) FROM cun25436.col_uebaunit WHERE ue_gc_unidadconstruccion IS NOT NULL),
        (SELECT count(*) FROM cun25436.col_uebaunit cu
         WHERE cu.ue_gc_unidadconstruccion IS NOT NULL
           AND EXISTS (SELECT 1 FROM cun25436.gc_predio gp WHERE gp.id = cu.unidad)),
        (SELECT count(*) FROM cun25436.col_uebaunit cu
         WHERE cu.ue_gc_unidadconstruccion IS NOT NULL
           AND cu.unidad IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.gc_predio gp WHERE gp.id = cu.unidad))

    UNION ALL

    -- 12. cuc_calificacionconvencional.gc_caracteristicasunidadconstruccion -> gc_caract.id
    SELECT
        'cuc_califconv.gc_caract -> gc_caract.id',
        (SELECT count(*) FROM cun25436.cuc_calificacionconvencional),
        (SELECT count(*) FROM cun25436.cuc_calificacionconvencional cc
         WHERE EXISTS (SELECT 1 FROM cun25436.gc_caracteristicasunidadconstruccion gc
                       WHERE gc.id = cc.gc_caracteristicasunidadconstruccion)),
        (SELECT count(*) FROM cun25436.cuc_calificacionconvencional cc
         WHERE cc.gc_caracteristicasunidadconstruccion IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.gc_caracteristicasunidadconstruccion gc
                           WHERE gc.id = cc.gc_caracteristicasunidadconstruccion))

    UNION ALL

    -- 13. cuc_grupocalificacion.cuc_calificacion_convencional -> cuc_calificacionconvencional.id
    SELECT
        'cuc_grupo.cuc_calif_conv -> cuc_califconv.id',
        (SELECT count(*) FROM cun25436.cuc_grupocalificacion),
        (SELECT count(*) FROM cun25436.cuc_grupocalificacion gp
         WHERE EXISTS (SELECT 1 FROM cun25436.cuc_calificacionconvencional cc
                       WHERE cc.id = gp.cuc_calificacion_convencional)),
        (SELECT count(*) FROM cun25436.cuc_grupocalificacion gp
         WHERE gp.cuc_calificacion_convencional IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.cuc_calificacionconvencional cc
                           WHERE cc.id = gp.cuc_calificacion_convencional))

    UNION ALL

    -- 14. cuc_objetoconstruccion.cuc_grupo_calificacion -> cuc_grupocalificacion.id
    SELECT
        'cuc_objeto.cuc_grupo -> cuc_grupo.id',
        (SELECT count(*) FROM cun25436.cuc_objetoconstruccion),
        (SELECT count(*) FROM cun25436.cuc_objetoconstruccion oc
         WHERE EXISTS (SELECT 1 FROM cun25436.cuc_grupocalificacion gp
                       WHERE gp.id = oc.cuc_grupo_calificacion)),
        (SELECT count(*) FROM cun25436.cuc_objetoconstruccion oc
         WHERE oc.cuc_grupo_calificacion IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.cuc_grupocalificacion gp
                           WHERE gp.id = oc.cuc_grupo_calificacion))

    UNION ALL

    -- 15. cuc_calificacionnoconvencional.gc_caracteristicasunidadconstruccion -> gc_caract.id
    SELECT
        'cuc_califnoconv.gc_caract -> gc_caract.id',
        (SELECT count(*) FROM cun25436.cuc_calificacionnoconvencional),
        (SELECT count(*) FROM cun25436.cuc_calificacionnoconvencional cn
         WHERE EXISTS (SELECT 1 FROM cun25436.gc_caracteristicasunidadconstruccion gc
                       WHERE gc.id = cn.gc_caracteristicasunidadconstruccion)),
        (SELECT count(*) FROM cun25436.cuc_calificacionnoconvencional cn
         WHERE cn.gc_caracteristicasunidadconstruccion IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.gc_caracteristicasunidadconstruccion gc
                           WHERE gc.id = cn.gc_caracteristicasunidadconstruccion))

    UNION ALL

    -- 16. col_miembros.interesado_gc_interesado -> gc_interesado.id
    SELECT
        'col_miembros.interesado -> gc_interesado.id',
        (SELECT count(*) FROM cun25436.col_miembros),
        (SELECT count(*) FROM cun25436.col_miembros cm
         WHERE EXISTS (SELECT 1 FROM cun25436.gc_interesado gi WHERE gi.id = cm.interesado_gc_interesado)),
        (SELECT count(*) FROM cun25436.col_miembros cm
         WHERE cm.interesado_gc_interesado IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.gc_interesado gi WHERE gi.id = cm.interesado_gc_interesado))

    UNION ALL

    -- 17. col_miembros.agrupacion -> gc_agrupacioninteresados.id
    SELECT
        'col_miembros.agrupacion -> gc_agrupacion.id',
        (SELECT count(*) FROM cun25436.col_miembros),
        (SELECT count(*) FROM cun25436.col_miembros cm
         WHERE EXISTS (SELECT 1 FROM cun25436.gc_agrupacioninteresados ga WHERE ga.id = cm.agrupacion)),
        (SELECT count(*) FROM cun25436.col_miembros cm
         WHERE cm.agrupacion IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.gc_agrupacioninteresados ga WHERE ga.id = cm.agrupacion))

    UNION ALL

    -- 18. gc_derecho.baunit -> gc_predio.id
    SELECT
        'gc_derecho.baunit -> gc_predio.id',
        (SELECT count(*) FROM cun25436.gc_derecho),
        (SELECT count(*) FROM cun25436.gc_derecho gd
         WHERE EXISTS (SELECT 1 FROM cun25436.gc_predio gp WHERE gp.id = gd.baunit)),
        (SELECT count(*) FROM cun25436.gc_derecho gd
         WHERE gd.baunit IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.gc_predio gp WHERE gp.id = gd.baunit))

    UNION ALL

    -- 19. gc_derecho.interesado_gc_interesado -> gc_interesado.id
    SELECT
        'gc_derecho.interesado -> gc_interesado.id',
        (SELECT count(*) FROM cun25436.gc_derecho WHERE interesado_gc_interesado IS NOT NULL),
        (SELECT count(*) FROM cun25436.gc_derecho gd
         WHERE gd.interesado_gc_interesado IS NOT NULL
           AND EXISTS (SELECT 1 FROM cun25436.gc_interesado gi WHERE gi.id = gd.interesado_gc_interesado)),
        (SELECT count(*) FROM cun25436.gc_derecho gd
         WHERE gd.interesado_gc_interesado IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.gc_interesado gi WHERE gi.id = gd.interesado_gc_interesado))

    UNION ALL

    -- 20. gc_derecho.interesado_gc_agrupacioninteresados -> gc_agrupacion.id
    SELECT
        'gc_derecho.agrupacion -> gc_agrupacion.id',
        (SELECT count(*) FROM cun25436.gc_derecho WHERE interesado_gc_agrupacioninteresados IS NOT NULL),
        (SELECT count(*) FROM cun25436.gc_derecho gd
         WHERE gd.interesado_gc_agrupacioninteresados IS NOT NULL
           AND EXISTS (SELECT 1 FROM cun25436.gc_agrupacioninteresados ga
                       WHERE ga.id = gd.interesado_gc_agrupacioninteresados)),
        (SELECT count(*) FROM cun25436.gc_derecho gd
         WHERE gd.interesado_gc_agrupacioninteresados IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.gc_agrupacioninteresados ga
                           WHERE ga.id = gd.interesado_gc_agrupacioninteresados))

    UNION ALL

    -- 21. col_rrrfuente.rrr_gc_derecho -> gc_derecho.id
    SELECT
        'col_rrrfuente.rrr_gc_derecho -> gc_derecho.id',
        (SELECT count(*) FROM cun25436.col_rrrfuente),
        (SELECT count(*) FROM cun25436.col_rrrfuente rf
         WHERE EXISTS (SELECT 1 FROM cun25436.gc_derecho gd WHERE gd.id = rf.rrr_gc_derecho)),
        (SELECT count(*) FROM cun25436.col_rrrfuente rf
         WHERE rf.rrr_gc_derecho IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.gc_derecho gd WHERE gd.id = rf.rrr_gc_derecho))

    UNION ALL

    -- 22. col_rrrfuente.fuente_administrativa -> gc_fuenteadministrativa.id
    SELECT
        'col_rrrfuente.fuente_adm -> gc_fuenteadm.id',
        (SELECT count(*) FROM cun25436.col_rrrfuente),
        (SELECT count(*) FROM cun25436.col_rrrfuente rf
         WHERE EXISTS (SELECT 1 FROM cun25436.gc_fuenteadministrativa gf
                       WHERE gf.id = rf.fuente_administrativa)),
        (SELECT count(*) FROM cun25436.col_rrrfuente rf
         WHERE rf.fuente_administrativa IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.gc_fuenteadministrativa gf
                           WHERE gf.id = rf.fuente_administrativa))

    UNION ALL

    -- 23. col_unidadfuente.unidad -> gc_predio.id
    SELECT
        'col_unidadfuente.unidad -> gc_predio.id',
        (SELECT count(*) FROM cun25436.col_unidadfuente),
        (SELECT count(*) FROM cun25436.col_unidadfuente uf
         WHERE EXISTS (SELECT 1 FROM cun25436.gc_predio gp WHERE gp.id = uf.unidad)),
        (SELECT count(*) FROM cun25436.col_unidadfuente uf
         WHERE uf.unidad IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.gc_predio gp WHERE gp.id = uf.unidad))

    UNION ALL

    -- 24. col_unidadfuente.fuente_administrativa -> gc_fuenteadministrativa.id
    SELECT
        'col_unidadfuente.fuente_adm -> gc_fuenteadm.id',
        (SELECT count(*) FROM cun25436.col_unidadfuente),
        (SELECT count(*) FROM cun25436.col_unidadfuente uf
         WHERE EXISTS (SELECT 1 FROM cun25436.gc_fuenteadministrativa gf
                       WHERE gf.id = uf.fuente_administrativa)),
        (SELECT count(*) FROM cun25436.col_unidadfuente uf
         WHERE uf.fuente_administrativa IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM cun25436.gc_fuenteadministrativa gf
                           WHERE gf.id = uf.fuente_administrativa))
)
SELECT
    relacion,
    total_registros,
    fk_validas,
    fk_huerfanas,
    CASE
        WHEN fk_huerfanas = 0 THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM fk_checks
ORDER BY relacion;

-- ============================================================================
-- SECCION B: Cobertura inversa - Entidades sin relaciones esperadas
-- ============================================================================

SELECT
    'Predios sin terreno' AS cobertura,
    count(*) AS total_predios,
    count(*) FILTER (WHERE NOT EXISTS (
        SELECT 1 FROM cun25436.col_uebaunit cu
        WHERE cu.unidad = gp.id AND cu.ue_gc_terreno IS NOT NULL
    )) AS sin_terreno,
    CASE
        WHEN count(*) FILTER (WHERE NOT EXISTS (
            SELECT 1 FROM cun25436.col_uebaunit cu
            WHERE cu.unidad = gp.id AND cu.ue_gc_terreno IS NOT NULL
        )) = 0 THEN 'OK'
        ELSE 'REVISAR'
    END AS estado
FROM cun25436.gc_predio gp

UNION ALL

SELECT
    'Predios sin derecho',
    count(*),
    count(*) FILTER (WHERE NOT EXISTS (
        SELECT 1 FROM cun25436.gc_derecho gd WHERE gd.baunit = gp.id
    )),
    CASE
        WHEN count(*) FILTER (WHERE NOT EXISTS (
            SELECT 1 FROM cun25436.gc_derecho gd WHERE gd.baunit = gp.id
        )) = 0 THEN 'OK'
        ELSE 'REVISAR'
    END
FROM cun25436.gc_predio gp

UNION ALL

SELECT
    'Predios sin direccion',
    count(*),
    count(*) FILTER (WHERE NOT EXISTS (
        SELECT 1 FROM cun25436.extdireccion ed WHERE ed.gc_predio_direccion = gp.id
    )),
    CASE
        WHEN count(*) FILTER (WHERE NOT EXISTS (
            SELECT 1 FROM cun25436.extdireccion ed WHERE ed.gc_predio_direccion = gp.id
        )) = 0 THEN 'OK'
        ELSE 'REVISAR'
    END
FROM cun25436.gc_predio gp

UNION ALL

SELECT
    'Predios sin datos adicionales',
    count(*),
    count(*) FILTER (WHERE NOT EXISTS (
        SELECT 1 FROM cun25436.dlc_datosadicionaleslevantamientocatastral dlc WHERE dlc.gc_predio = gp.id
    )),
    CASE
        WHEN count(*) FILTER (WHERE NOT EXISTS (
            SELECT 1 FROM cun25436.dlc_datosadicionaleslevantamientocatastral dlc WHERE dlc.gc_predio = gp.id
        )) = 0 THEN 'OK'
        ELSE 'REVISAR'
    END
FROM cun25436.gc_predio gp

UNION ALL

SELECT
    'Construcciones sin unidad de construccion',
    count(*),
    count(*) FILTER (WHERE NOT EXISTS (
        SELECT 1 FROM cun25436.gc_unidadconstruccion uc WHERE uc.gc_construccion = gc.id
    )),
    CASE
        WHEN count(*) FILTER (WHERE NOT EXISTS (
            SELECT 1 FROM cun25436.gc_unidadconstruccion uc WHERE uc.gc_construccion = gc.id
        )) = 0 THEN 'OK'
        ELSE 'REVISAR'
    END
FROM cun25436.gc_construccion gc

UNION ALL

SELECT
    'Derechos sin fuente administrativa',
    count(*),
    count(*) FILTER (WHERE NOT EXISTS (
        SELECT 1 FROM cun25436.col_rrrfuente rf WHERE rf.rrr_gc_derecho = gd.id
    )),
    CASE
        WHEN count(*) FILTER (WHERE NOT EXISTS (
            SELECT 1 FROM cun25436.col_rrrfuente rf WHERE rf.rrr_gc_derecho = gd.id
        )) = 0 THEN 'OK'
        ELSE 'REVISAR'
    END
FROM cun25436.gc_derecho gd;
