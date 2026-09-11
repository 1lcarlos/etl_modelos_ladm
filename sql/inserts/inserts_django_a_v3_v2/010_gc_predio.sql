-- =============================================================================
-- Destino: gc_predio                       Origen: tmp_predio (predio.sql)
-- Reglas:
--   * t_id por secuencia; local_id = id de Django; espacio_de_nombres = 'DJANGO.GC_PREDIO'.
--   * matricula_inmobiliaria: numerica entre 1 y 9.999.999.999; si no, NULL + incidencia.
--   * codigo_orip se copia tal cual (sin rellenar con ceros).
--   * tipo_predio: sin valor en origen -> Predio.Privado (decision 2026-09-11).
--   * condicion_predio: Mejora -> Informal (relacion en 018_gc_predio_informalidad).
--   * estado: Activo si no tiene fin_vida_util_version; si la tiene, Cancelado.
--   * tipo (UAB): Informacion_Estadistica, igual que el flujo original.
--   * codigo_homologado = nupre.
--   * numero_predial_anterior: solo si tiene 20 digitos.
--   * Sin dato en origen -> NULL (ver 002_relajar_obligatorios.sql).
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_predio', p.id, x.campo, x.valor, x.accion, x.motivo
FROM tmp_predio AS p
CROSS JOIN LATERAL (VALUES
    ('matricula_inmobiliaria', p.matricula_inmobiliaria, 'NULIFICADO',
     'Matricula no numerica, en ceros o fuera de rango 1..9999999999',
     p.matricula_inmobiliaria IS NOT NULL AND etl_control.matricula(p.matricula_inmobiliaria) IS NULL),
    ('numero_predial_anterior', p.numero_predial_anterior, 'NULIFICADO',
     'No tiene 20 digitos',
     p.numero_predial_anterior IS NOT NULL AND p.numero_predial_anterior !~ '^[0-9]{20}$'),
    ('tipo_predio', p.tipo_predio, 'NULIFICADO',
     'Valor sin equivalente en el dominio destino',
     p.tipo_predio IS NOT NULL AND etl_control.dominio('{schema}', 'gc_prediotipo', p.tipo_predio) IS NULL),
    ('condicion_predio', p.condicion_predio, 'NULIFICADO',
     'Valor sin equivalente en el dominio destino',
     p.condicion_predio IS NOT NULL AND etl_control.dominio('{schema}', 'gc_condicionprediotipo', p.condicion_predio) IS NULL),
    ('destinacion_economica', p.destinacion_economica, 'NULIFICADO',
     'Valor sin equivalente en el dominio destino',
     p.destinacion_economica IS NOT NULL AND etl_control.dominio('{schema}', 'gc_destinacioneconomicatipo', p.destinacion_economica) IS NULL),
    ('clase_suelo', p.clase_suelo, 'NULIFICADO',
     'Valor sin equivalente en el dominio destino',
     p.clase_suelo IS NOT NULL AND etl_control.dominio('{schema}', 'gc_clasesuelotipo', p.clase_suelo) IS NULL),
    ('categoria_suelo', p.categoria_suelo, 'NULIFICADO',
     'Valor sin equivalente en el dominio destino',
     p.categoria_suelo IS NOT NULL AND etl_control.dominio('{schema}', 'gc_categoriasuelotipo', p.categoria_suelo) IS NULL),
    ('vigencia_actualizacion_catastral', NULL, 'NULIFICADO',
     'Sin vigencia en public.cadaster_information para el municipio y la clase de suelo',
     p.vigencia_actualizacion_catastral IS NULL),
    ('codigo_homologado', NULL, 'NULIFICADO',
     'Predio sin NUPRE',
     etl_control.texto(p.nupre) IS NULL)
) AS x(campo, valor, accion, motivo, aplica)
WHERE NOT p.es_centinela
  AND x.aplica
UNION ALL
-- Reglas generales aplicadas a muchas filas: una incidencia agregada por regla
SELECT '{schema}', 'gc_predio', NULL, 'tipo_predio', count(*)::text || ' predios', 'DERIVADO',
       'Sin tipo de predio en origen: se asigna Predio.Privado'
FROM tmp_predio AS p
WHERE NOT p.es_centinela AND p.tipo_predio IS NULL
HAVING count(*) > 0
UNION ALL
SELECT '{schema}', 'gc_predio', NULL, 'condicion_predio', count(*)::text || ' predios', 'DERIVADO',
       'Condicion Mejora registrada como Informal'
FROM tmp_predio AS p
WHERE NOT p.es_centinela AND p.condicion_predio = 'Mejora'
HAVING count(*) > 0;

INSERT INTO {schema}.gc_predio (
    t_basket,
    departamento,
    municipio,
    codigo_orip,
    matricula_inmobiliaria,
    numero_predial_nacional,
    codigo_homologado,
    nupre,
    tipo_predio,
    condicion_predio,
    destinacion_economica,
    area_catastral_terreno,
    area_registral_m2,
    vigencia_actualizacion_catastral,
    estado,
    categoria_suelo,
    clase_suelo,
    numero_predial_anterior,
    area_construida,
    nombre,
    tipo,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    p.departamento,
    p.municipio,
    etl_control.texto(p.codigo_orip),
    etl_control.matricula(p.matricula_inmobiliaria),
    p.numero_predial,
    etl_control.texto(p.nupre),
    etl_control.texto(p.nupre),
    etl_control.dominio('{schema}', 'gc_prediotipo', COALESCE(p.tipo_predio, 'Privado')),
    etl_control.dominio('{schema}', 'gc_condicionprediotipo', p.condicion_predio),
    etl_control.dominio('{schema}', 'gc_destinacioneconomicatipo', p.destinacion_economica),
    p.area::numeric,
    p.area_registral_m2::numeric,
    p.vigencia_actualizacion_catastral::date,
    etl_control.dominio('{schema}', 'gc_estadotipo',
                        CASE WHEN p.fin_vida_util_version IS NULL THEN 'Activo' ELSE 'Cancelado' END),
    etl_control.dominio('{schema}', 'gc_categoriasuelotipo', p.categoria_suelo),
    etl_control.dominio('{schema}', 'gc_clasesuelotipo', p.clase_suelo),
    CASE WHEN p.numero_predial_anterior ~ '^[0-9]{20}$' THEN p.numero_predial_anterior END,
    p.area_construida::numeric,
    etl_control.texto(p.nombre),
    etl_control.dominio('{schema}', 'col_unidadadministrativabasicatipo', 'Informacion_Estadistica'),
    COALESCE(p.comienzo_vida_util_version::timestamp, now()::timestamp),
    p.fin_vida_util_version::timestamp,
    'DJANGO.GC_PREDIO',
    p.id
FROM tmp_predio AS p
WHERE NOT p.es_centinela;
