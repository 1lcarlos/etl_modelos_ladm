-- INSERT para tabla dlc_datosadicionaleslevantamientocatastral (Modelo Interno Django)
-- Migra datos adicionales del levantamiento catastral desde modelo CCA
-- Fecha: 2026-02-06
--
-- Origen: tmp_cca_datosadicionaleslevantamientocatastral (query cca_datosadicionaleslevantamientocatastral.sql)
-- Destino: dlc_datosadicionaleslevantamientocatastral (modelo interno Django)
--
-- Diferencias clave con modelo INTERLIS:
--   - PK es 'id' auto-generado (no t_id)
--   - No existe t_ili_tid
--   - 'document_type_recognizer' es varchar (no FK) - se inserta text_code como texto
--   - 'resultado_visita' es varchar (no FK) - se inserta text_code como texto
--   - 'estrato' es varchar (no FK) - se inserta text_code como texto
--   - Tiene campos adicionales: tiene_area_registral, area_registral_m2,
--     procedimiento_catastral_registral
--   - No existe metodo_levantamiento en esta tabla Django
--
-- Dependencias:
--   - Requiere que gc_predio ya este migrado
--
-- IMPORTANTE: Este insert debe ejecutarse DESPUES de gc_predio

INSERT INTO {schema}.dlc_datosadicionaleslevantamientocatastral (
    tiene_area_registral,
    area_registral_m2,
    procedimiento_catastral_registral,
    observaciones,
    fecha_visita_predial,
    document_type_recognizer,
    numero_documento_reconocedor,
    primer_nombre_reconocedor,
    segundo_nombre_reconocedor,
    primer_apellido_reconocedor,
    segundo_apellido_reconocedor,
    resultado_visita,
    otro_cual_resultado_visita,
    suscribe_acta_colindancia,
    despojo_abandono,
    estrato,
    otro_cual_estrato,
    gc_predio
)
SELECT
    -- tiene_area_registral (NOT NULL): Verificar si el predio tiene area registral
    CASE
        WHEN gp.area IS NOT NULL AND gp.area > 0 THEN true
        ELSE false
    END,

    -- area_registral_m2: Obtener del predio migrado
    gp.area,

    -- procedimiento_catastral_registral: No existe en CCA
    NULL,

    -- observaciones
    d.observaciones,

    -- fecha_visita_predial
    d.fecha_visita_predial::date,

    -- document_type_recognizer (varchar): Mapeo ilicode (CCA) -> text_code (Django) como texto
    COALESCE(
        (SELECT text_code FROM {schema}.gc_interesadodocumentotipo
         WHERE text_code = d.tipo_documento_reconocedor LIMIT 1),
        d.tipo_documento_reconocedor
    ),

    -- numero_documento_reconocedor (NOT NULL)
    COALESCE(d.numero_documento_reconocedor, ''),

    -- primer_nombre_reconocedor (NOT NULL)
    COALESCE(d.primer_nombre_reconocedor, ''),

    -- segundo_nombre_reconocedor
    d.segundo_nombre_reconocedor,

    -- primer_apellido_reconocedor (NOT NULL)
    COALESCE(d.primer_apellido_reconocedor, ''),

    -- segundo_apellido_reconocedor
    d.segundo_apellido_reconocedor,

    -- resultado_visita (varchar): Mapeo ilicode (CCA) -> text_code (Django) como texto
    /* COALESCE(
        (SELECT text_code FROM {schema}.gc_resultadovisitatipo
         WHERE text_code = d.resultado_visita LIMIT 1),
        d.resultado_visita
    ), */
    d.resultado_visita,


    -- otro_cual_resultado_visita
    d.otro_cual_resultado_visita,

    -- suscribe_acta_colindancia
    d.suscribe_acta_colindancia::boolean,

    -- despojo_abandono
    d.despojo_abandono::boolean,

    -- estrato (varchar): Mapeo ilicode (CCA) -> text_code (Django) como texto
    /* COALESCE(
        (SELECT text_code FROM {schema}.gc_estratotipo
         WHERE text_code = d.estrato LIMIT 1),
        d.estrato
    ), */
    d.estrato,
    -- otro_cual_estrato
    d.otro_cual_estrato,

    -- gc_predio: Buscar el id del predio migrado usando numero_predial
    gp.id

FROM tmp_cca_datosadicionaleslevantamientocatastral d
INNER JOIN {schema}.gc_predio gp ON gp.numero_predial = d.numero_predial
WHERE d.numero_predial IS NOT NULL;
