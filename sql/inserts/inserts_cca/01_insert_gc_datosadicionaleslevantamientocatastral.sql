-- Insert para gc_datosadicionaleslevantamientocatastral
-- Origen: tmp_cca_datosadicionaleslevantamientocatastral (datos de cca_predio + cca_usuario)
-- Destino: gc_datosadicionaleslevantamientocatastral
-- Versión: 1.0
-- Fecha: 2026-02-04
--
-- Notas:
-- 1. Este insert debe ejecutarse DESPUES de migrar gc_predio
-- 2. Los dominios se mapean usando ilicode con coincidencia exacta o parcial
-- 3. El campo metodo_levantamiento no existe en CCA, se usa valor por defecto

INSERT INTO {schema}.gc_datosadicionaleslevantamientocatastral (
    t_id,
    t_ili_tid,
    observaciones,
    fecha_visita_predial,
    tipo_documento_reconocedor,
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
    metodo_levantamiento,
    gc_predio
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    uuid_generate_v4(),
    d.observaciones,
    d.fecha_visita_predial,

    -- tipo_documento_reconocedor: Mapeo de CCA a COL_DocumentoTipo
    -- CCA usa cca_interesadodocumentotipo, GC usa col_documentotipo
    COALESCE(
        (SELECT t_id FROM {schema}.col_documentotipo
         WHERE ilicode = d.tipo_documento_reconocedor LIMIT 1),
        (SELECT t_id FROM {schema}.col_documentotipo
         WHERE ilicode ILIKE '%' || d.tipo_documento_reconocedor || '%' LIMIT 1),
        (SELECT t_id FROM {schema}.col_documentotipo
         WHERE ilicode = 'Sin_Informacion' LIMIT 1)
    ) AS tipo_documento_reconocedor,

    d.numero_documento_reconocedor,
    d.primer_nombre_reconocedor,
    d.segundo_nombre_reconocedor,
    d.primer_apellido_reconocedor,
    d.segundo_apellido_reconocedor,

    -- resultado_visita: Mapeo de CCA a gc_resultadovisitatipo
    COALESCE(
        (SELECT t_id FROM {schema}.gc_resultadovisitatipo
         WHERE ilicode = d.resultado_visita LIMIT 1),
        (SELECT t_id FROM {schema}.gc_resultadovisitatipo
         WHERE ilicode ILIKE '%' || d.resultado_visita || '%' LIMIT 1),
        (SELECT t_id FROM {schema}.gc_resultadovisitatipo
         WHERE ilicode = 'Sin_Visita' LIMIT 1)
    ) AS resultado_visita,

    d.otro_cual_resultado_visita,
    d.suscribe_acta_colindancia,
    d.despojo_abandono,

    -- estrato: Mapeo de CCA a gc_estratotipo
    COALESCE(
        (SELECT t_id FROM {schema}.gc_estratotipo
         WHERE ilicode = d.estrato LIMIT 1),
        (SELECT t_id FROM {schema}.gc_estratotipo
         WHERE ilicode ILIKE '%' || d.estrato || '%' LIMIT 1),
        NULL
    ) AS estrato,

    d.otro_cual_estrato,

    -- metodo_levantamiento: No existe en CCA, usar valor por defecto 'Metodo_Directo'
    (SELECT t_id FROM {schema}.gc_metodolevantamientotipo
     WHERE ilicode = 'Metodo_Directo' LIMIT 1) AS metodo_levantamiento,

    -- gc_predio: Buscar el t_id del predio migrado usando numero_predial
    (SELECT gp.t_id FROM {schema}.gc_predio gp
     WHERE gp.numero_predial_nacional = d.numero_predial LIMIT 1) AS gc_predio

FROM tmp_cca_datosadicionaleslevantamientocatastral d
WHERE EXISTS (
    -- Solo insertar si el predio ya fue migrado
    SELECT 1 FROM {schema}.gc_predio gp
    WHERE gp.numero_predial_nacional = d.numero_predial
);
