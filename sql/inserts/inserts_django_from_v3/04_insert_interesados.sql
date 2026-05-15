-- Inserción de interesados en modelo Django desde interno_v3
-- Dirección: interno_v3 → modelo interno Django
--
-- Mapeo de dominios: ilicode (interno_v3) → text_code (Django)
-- Tablas de dominio cambian de nombre entre modelos:
--   col_documentotipo (v3) → gc_interesadodocumentotipo (Django)
--   col_interesadotipo (v3) → gc_interesadotipo (Django)
--   gc_autoreconocimientoetnicotipo (v3) → gc_grupoetnicotipo (Django)
-- El id se genera automáticamente por la secuencia gc_interesado_id_seq de Django

INSERT INTO {schema}.gc_interesado (
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
    estado_civil,
    grupo_etnico,
    sexo,
    tipo,
    tipo_documento
)
SELECT
    i.espacio_de_nombres,
    COALESCE(i.local_id, i.id_interesado::varchar),
    i.comienzo_vida_util_version,
    i.fin_vida_util_version,
    i.nombre,
    i.documento_identidad,
    i.primer_nombre,
    i.segundo_nombre,
    i.primer_apellido,
    i.segundo_apellido,
    i.razon_social,

    -- estado_civil (gc_estadociviltipo): ilicode → text_code
    COALESCE(
        (SELECT id FROM {schema}.gc_estadociviltipo WHERE text_code = i.estado_civil LIMIT 1),
        (SELECT id FROM {schema}.gc_estadociviltipo WHERE i.estado_civil ILIKE '%' || text_code || '%' LIMIT 1)
    ) AS estado_civil,

    -- grupo_etnico (gc_grupoetnicotipo): ilicode de gc_autoreconocimientoetnicotipo → text_code
    COALESCE(
        (SELECT id FROM {schema}.gc_grupoetnicotipo WHERE text_code = i.grupo_etnico LIMIT 1),
        (SELECT id FROM {schema}.gc_grupoetnicotipo WHERE i.grupo_etnico ILIKE '%' || text_code || '%' LIMIT 1)
    ) AS grupo_etnico,

    -- sexo (gc_sexotipo): ilicode → text_code
    COALESCE(
        (SELECT id FROM {schema}.gc_sexotipo WHERE text_code = i.sexo LIMIT 1),
        (SELECT id FROM {schema}.gc_sexotipo WHERE i.sexo ILIKE '%' || text_code || '%' LIMIT 1),
        (SELECT id FROM {schema}.gc_sexotipo WHERE text_code = 'Sin_Determinar' LIMIT 1)
    ) AS sexo,

    -- tipo (gc_interesadotipo): ilicode de col_interesadotipo → text_code
    COALESCE(
        (SELECT id FROM {schema}.gc_interesadotipo WHERE text_code = i.interesadotipo LIMIT 1),
        (SELECT id FROM {schema}.gc_interesadotipo WHERE i.interesadotipo ILIKE '%' || text_code || '%' LIMIT 1)
    ) AS tipo,

    -- tipo_documento (gc_interesadodocumentotipo): ilicode de col_documentotipo → text_code
    COALESCE(
        (SELECT id FROM {schema}.gc_interesadodocumentotipo WHERE text_code = i.tipo_documento LIMIT 1),
        (SELECT id FROM {schema}.gc_interesadodocumentotipo WHERE i.tipo_documento ILIKE '%' || text_code || '%' LIMIT 1),
        (SELECT id FROM {schema}.gc_interesadodocumentotipo WHERE text_code = 'Sin_Informacion' LIMIT 1)
    ) AS tipo_documento

FROM tmp_interesado i;
