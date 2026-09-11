-- =============================================================================
-- Destino: gc_interesado                   Origen: tmp_interesado
-- Reglas:
--   * local_id = id de Django; espacio_de_nombres = 'DJANGO.GC_INTERESADO'.
--   * Nombres: se recortan y se colapsan espacios repetidos.
--   * numero_documento compuesto solo por ceros -> NULL.
--   * razon_social 'nan' (residuo de carga desde pandas) -> NULL.
--   * tipo_documento sin dato -> Sin_Informacion (valor de "desconocido" del dominio).
--   * sexo: persona juridica -> No_Aplica; natural sin dato -> Sin_Determinar.
--   * autoreconocimientocampesino: Django no lo tiene -> NULL (antes se forzaba false).
--   * verificado: no tiene columna en v3.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_interesado', i.id, x.campo, x.valor, 'NULIFICADO', x.motivo
FROM tmp_interesado AS i
CROSS JOIN LATERAL (VALUES
    ('numero_documento', i.documento_identidad, 'Documento compuesto solo por ceros',
     btrim(i.documento_identidad) ~ '^0+$'),
    ('razon_social', i.razon_social, 'Valor nan (residuo de carga)',
     lower(etl_control.texto(i.razon_social)) = 'nan'),
    ('tipo_documento', i.tipo_documento, 'Valor sin equivalente en el dominio destino',
     i.tipo_documento IS NOT NULL AND etl_control.dominio('{schema}', 'col_documentotipo', i.tipo_documento) IS NULL),
    ('sexo', i.sexo, 'Valor sin equivalente en el dominio destino',
     i.sexo IS NOT NULL AND COALESCE(i.tipo_interesado, '') <> 'Persona_Juridica'
     AND etl_control.dominio('{schema}', 'gc_sexotipo', i.sexo) IS NULL),
    ('autoreconocimientoetnico', i.grupo_etnico, 'Valor sin equivalente en el dominio destino',
     i.grupo_etnico IS NOT NULL AND etl_control.dominio('{schema}', 'gc_autoreconocimientoetnicotipo', i.grupo_etnico) IS NULL),
    ('tipo_interesado', i.tipo_interesado, 'Valor sin equivalente en el dominio destino',
     i.tipo_interesado IS NOT NULL AND etl_control.dominio('{schema}', 'col_interesadotipo', i.tipo_interesado) IS NULL),
    ('estado_civil', i.estado_civil, 'Valor sin equivalente en el dominio destino',
     i.estado_civil IS NOT NULL AND etl_control.dominio('{schema}', 'gc_estadociviltipo', i.estado_civil) IS NULL)
) AS x(campo, valor, motivo, aplica)
WHERE NOT i.es_centinela AND x.aplica
UNION ALL
SELECT '{schema}', 'gc_interesado', NULL, 'tipo_documento', count(*)::text || ' interesados', 'DERIVADO',
       'Sin tipo de documento en origen: Sin_Informacion'
FROM tmp_interesado AS i
WHERE NOT i.es_centinela AND i.tipo_documento IS NULL
HAVING count(*) > 0
UNION ALL
SELECT '{schema}', 'gc_interesado', NULL, 'sexo', count(*)::text || ' interesados', 'DERIVADO',
       'Sin sexo en origen: Sin_Determinar (persona natural) o No_Aplica (persona juridica)'
FROM tmp_interesado AS i
WHERE NOT i.es_centinela AND (i.sexo IS NULL OR i.tipo_interesado = 'Persona_Juridica')
HAVING count(*) > 0
UNION ALL
SELECT '{schema}', 'gc_interesado', NULL, 'verificado', count(*)::text || ' interesados con valor', 'NO_MIGRADO',
       'El modelo destino no tiene este atributo'
FROM tmp_interesado AS i
WHERE NOT i.es_centinela AND i.verificado IS NOT NULL
HAVING count(*) > 0;

INSERT INTO {schema}.gc_interesado (
    t_basket,
    tipo_documento,
    primer_nombre,
    segundo_nombre,
    primer_apellido,
    segundo_apellido,
    sexo,
    autoreconocimientoetnico,
    razon_social,
    estado_civil,
    nombre,
    tipo_interesado,
    numero_documento,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    etl_control.dominio('{schema}', 'col_documentotipo', COALESCE(i.tipo_documento, 'Sin_Informacion')),
    etl_control.texto(i.primer_nombre),
    etl_control.texto(i.segundo_nombre),
    etl_control.texto(i.primer_apellido),
    etl_control.texto(i.segundo_apellido),
    etl_control.dominio('{schema}', 'gc_sexotipo',
        CASE
            WHEN i.tipo_interesado = 'Persona_Juridica' THEN 'No_Aplica'
            ELSE COALESCE(i.sexo, 'Sin_Determinar')
        END),
    etl_control.dominio('{schema}', 'gc_autoreconocimientoetnicotipo', i.grupo_etnico),
    CASE WHEN lower(etl_control.texto(i.razon_social)) = 'nan' THEN NULL
         ELSE etl_control.texto(i.razon_social) END,
    etl_control.dominio('{schema}', 'gc_estadociviltipo', i.estado_civil),
    etl_control.texto(i.nombre),
    etl_control.dominio('{schema}', 'col_interesadotipo', i.tipo_interesado),
    CASE WHEN btrim(i.documento_identidad) ~ '^0+$' THEN NULL
         ELSE etl_control.texto(i.documento_identidad) END,
    COALESCE(i.comienzo_vida_util_version::timestamp, now()::timestamp),
    i.fin_vida_util_version::timestamp,
    'DJANGO.GC_INTERESADO',
    i.id
FROM tmp_interesado AS i
WHERE NOT i.es_centinela;
