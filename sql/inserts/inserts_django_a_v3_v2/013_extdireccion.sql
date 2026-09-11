-- =============================================================================
-- Destino: extdireccion (estructura de gc_predio)   Origen: tmp_extdireccion
-- Reglas:
--   * Sin localizacion en origen -> NULL (no se usan coordenadas fijas).
--   * codigo_postal: v3 admite 6 caracteres; si excede -> NULL + incidencia.
--   * t_seq: orden por seq de Django dentro de cada predio.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'extdireccion', e.id, 'gc_predio_direccion', e.gc_predio_direccion, 'RECHAZADO',
       'Predio no migrado'
FROM tmp_extdireccion AS e
LEFT JOIN {schema}.gc_predio AS p ON p.local_id = e.gc_predio_direccion AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
WHERE NOT e.es_centinela AND p.t_id IS NULL
UNION ALL
SELECT '{schema}', 'extdireccion', e.id, x.campo, x.valor, 'NULIFICADO', x.motivo
FROM tmp_extdireccion AS e
CROSS JOIN LATERAL (VALUES
    ('codigo_postal', e.codigo_postal, 'Excede 6 caracteres',
     length(etl_control.texto(e.codigo_postal)) > 6),
    ('tipo_direccion', e.tipo_direccion, 'Valor sin equivalente en el dominio destino',
     e.tipo_direccion IS NOT NULL AND etl_control.dominio('{schema}', 'extdireccion_tipo_direccion', e.tipo_direccion) IS NULL),
    ('clase_via_principal', e.clase_via_principal, 'Valor sin equivalente en el dominio destino',
     e.clase_via_principal IS NOT NULL AND etl_control.dominio('{schema}', 'extdireccion_clase_via_principal', e.clase_via_principal) IS NULL),
    ('sector_ciudad', e.sector_ciudad, 'Valor sin equivalente en el dominio destino',
     e.sector_ciudad IS NOT NULL AND etl_control.dominio('{schema}', 'extdireccion_sector_ciudad', e.sector_ciudad) IS NULL),
    ('sector_predio', e.sector_predio, 'Valor sin equivalente en el dominio destino',
     e.sector_predio IS NOT NULL AND etl_control.dominio('{schema}', 'extdireccion_sector_predio', e.sector_predio) IS NULL)
) AS x(campo, valor, motivo, aplica)
WHERE NOT e.es_centinela AND x.aplica;

INSERT INTO {schema}.extdireccion (
    t_basket,
    t_seq,
    tipo_direccion,
    es_direccion_principal,
    localizacion,
    codigo_postal,
    clase_via_principal,
    valor_via_principal,
    letra_via_principal,
    sector_ciudad,
    valor_via_generadora,
    letra_via_generadora,
    numero_predio,
    sector_predio,
    complemento,
    nombre_predio,
    gc_predio_direccion
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    row_number() OVER (PARTITION BY p.t_id ORDER BY e.seq::bigint NULLS LAST, e.id::bigint) - 1,
    etl_control.dominio('{schema}', 'extdireccion_tipo_direccion', e.tipo_direccion),
    e.es_direccion_principal::boolean,
    e.localizacion::geometry,
    CASE WHEN length(etl_control.texto(e.codigo_postal)) <= 6 THEN etl_control.texto(e.codigo_postal) END,
    etl_control.dominio('{schema}', 'extdireccion_clase_via_principal', e.clase_via_principal),
    etl_control.texto(e.valor_via_principal),
    etl_control.texto(e.letra_via_principal),
    etl_control.dominio('{schema}', 'extdireccion_sector_ciudad', e.sector_ciudad),
    etl_control.texto(e.valor_via_generadora),
    etl_control.texto(e.letra_via_generadora),
    etl_control.texto(e.numero_predio),
    etl_control.dominio('{schema}', 'extdireccion_sector_predio', e.sector_predio),
    etl_control.texto(e.complemento),
    etl_control.texto(e.nombre_predio),
    p.t_id
FROM tmp_extdireccion AS e
JOIN {schema}.gc_predio AS p ON p.local_id = e.gc_predio_direccion AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
WHERE NOT e.es_centinela;
