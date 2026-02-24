-- INSERT para tabla gc_interesado (Modelo Interno Django)
-- Migra interesados desde modelo CCA a modelo interno Django
-- Fecha: 2026-02-06
--
-- Origen: tmp_cca_interesado (query cca_interesado.sql)
-- Destino: gc_interesado (modelo interno Django)
--
-- Diferencias clave con modelo CCA:
--   - PK es 'id' (se usa cca_interesado_id)
--   - Dominios usan text_code (no ilicode) y FK apunta a 'id' (no t_id)
--   - CCA tiene campos no presentes en Django: departamento, municipio,
--     direccion_residencia, telefono, correo_electronico, autoriza_notificacion_correo
--   - Django tiene campo 'verificado' (FK gc_verificaciontipo) que no existe en CCA
--   - grupo_etnico tiene diferencias de nombres:
--     CCA Rrom -> Django Gitano
--     CCA Negro_Afrocolombiano -> Django Afrocolombiano
--     CCA Palenquero -> Django Palanquero
--
-- Dependencias:
--   - Las tablas de dominio deben estar pobladas
--   - No depende de otras tablas de datos
--
-- IMPORTANTE: Este insert puede ejecutarse en paralelo con gc_predio

INSERT INTO {schema}.gc_interesado (
    id,
    espacio_de_nombres,
    local_id,
    comienzo_vida_util_version,
    fin_vida_util_version,
    nombre,
    documento_identidad,
    primer_nombre,
    segundo_nombre,
    primer_apellido,
    segundo_apellido,
    razon_social,
    tipo,
    tipo_documento,
    sexo,
    grupo_etnico,
    estado_civil,
    verificado
)
SELECT
    -- id: Usar cca_interesado_id como id en Django
    i.cca_interesado_id,

    -- espacio_de_nombres
    'GC_INTERESADO_CCA',

    -- local_id
    i.cca_interesado_id::varchar,

    -- comienzo_vida_util_version
    NOW(),

    -- fin_vida_util_version
    NULL,

    -- nombre
    i.nombre,

    -- documento_identidad
    i.documento_identidad,

    -- primer_nombre
    i.primer_nombre,

    -- segundo_nombre
    i.segundo_nombre,

    -- primer_apellido
    i.primer_apellido,

    -- segundo_apellido
    i.segundo_apellido,

    -- razon_social
    i.razon_social,

    -- tipo: Mapeo ilicode (CCA) -> text_code (Django) -> id
    COALESCE(
        (SELECT id FROM {schema}.gc_interesadotipo
         WHERE text_code = i.tipo LIMIT 1),
        (SELECT id FROM {schema}.gc_interesadotipo
         WHERE text_code ILIKE '%' || i.tipo || '%' LIMIT 1),
        NULL
    ),

    -- tipo_documento: Mapeo ilicode (CCA) -> text_code (Django) -> id
    COALESCE(
        (SELECT id FROM {schema}.gc_interesadodocumentotipo
         WHERE text_code = i.tipo_documento LIMIT 1),
        (SELECT id FROM {schema}.gc_interesadodocumentotipo
         WHERE text_code ILIKE '%' || i.tipo_documento || '%' LIMIT 1),
        NULL
    ),

    -- sexo: Mapeo ilicode (CCA) -> text_code (Django) -> id
    COALESCE(
        (SELECT id FROM {schema}.gc_sexotipo
         WHERE text_code = i.sexo LIMIT 1),
        (SELECT id FROM {schema}.gc_sexotipo
         WHERE text_code ILIKE '%' || i.sexo || '%' LIMIT 1),
        NULL
    ),

    -- grupo_etnico: Mapeo ilicode (CCA) -> text_code (Django) -> id
    -- NOTA: Diferencias de nombres entre CCA y Django requieren mapeo especial
    COALESCE(
        (SELECT id FROM {schema}.gc_grupoetnicotipo
         WHERE text_code = i.grupo_etnico LIMIT 1),
        (SELECT id FROM {schema}.gc_grupoetnicotipo
         WHERE text_code ILIKE '%' || i.grupo_etnico || '%' LIMIT 1),
        CASE
            WHEN i.grupo_etnico = 'Rrom' THEN
                (SELECT id FROM {schema}.gc_grupoetnicotipo WHERE text_code = 'Gitano' LIMIT 1)
            WHEN i.grupo_etnico = 'Negro_Afrocolombiano' THEN
                (SELECT id FROM {schema}.gc_grupoetnicotipo WHERE text_code = 'Afrocolombiano' LIMIT 1)
            WHEN i.grupo_etnico = 'Palenquero' THEN
                (SELECT id FROM {schema}.gc_grupoetnicotipo WHERE text_code = 'Palanquero' LIMIT 1)
            ELSE NULL
        END
    ),

    -- estado_civil: Mapeo ilicode (CCA) -> text_code (Django) -> id
    COALESCE(
        (SELECT id FROM {schema}.gc_estadociviltipo
         WHERE text_code = i.estado_civil LIMIT 1),
        (SELECT id FROM {schema}.gc_estadociviltipo
         WHERE text_code ILIKE '%' || i.estado_civil || '%' LIMIT 1),
        NULL
    ),

    -- verificado: No existe en CCA, se deja NULL
    NULL

FROM tmp_cca_interesado i;
