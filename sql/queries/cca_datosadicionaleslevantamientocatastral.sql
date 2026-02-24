-- Consulta para extraer datos de cca_predio y cca_usuario para gc_datosadicionaleslevantamientocatastral
-- Origen: Modelo CCA (cca_predio + cca_usuario)
-- Destino: gc_datosadicionaleslevantamientocatastral

SELECT
    p.t_id as cca_predio_id,
    p.numero_predial,
    p.observaciones,
    p.fecha_visita_predial,
    -- Datos del reconocedor desde cca_usuario
    idt.ilicode as tipo_documento_reconocedor,
    u.numero_documento as numero_documento_reconocedor,
    -- El nombre en CCA viene completo, lo separamos
    CASE
        WHEN u.nombre IS NOT NULL THEN split_part(u.nombre, ' ', 1)
        ELSE NULL
    END as primer_nombre_reconocedor,
    CASE
        WHEN u.nombre IS NOT NULL AND array_length(string_to_array(u.nombre, ' '), 1) > 2
        THEN split_part(u.nombre, ' ', 2)
        ELSE NULL
    END as segundo_nombre_reconocedor,
    CASE
        WHEN u.nombre IS NOT NULL AND array_length(string_to_array(u.nombre, ' '), 1) >= 2
        THEN
            CASE
                WHEN array_length(string_to_array(u.nombre, ' '), 1) > 2
                THEN split_part(u.nombre, ' ', 3)
                ELSE split_part(u.nombre, ' ', 2)
            END
        ELSE NULL
    END as primer_apellido_reconocedor,
    CASE
        WHEN u.nombre IS NOT NULL AND array_length(string_to_array(u.nombre, ' '), 1) >= 4
        THEN split_part(u.nombre, ' ', 4)
        ELSE NULL
    END as segundo_apellido_reconocedor,
    -- Resultado visita con mapeo de dominio
    rvt.ilicode as resultado_visita,
    p.otro_cual_resultado_visita,
    -- Suscribe acta colindancia (convertir bigint a boolean)
    CASE
        WHEN bat.ilicode = 'Si' THEN true
        WHEN bat.ilicode = 'No' THEN false
        ELSE NULL
    END as suscribe_acta_colindancia,
    p.despojo_abandono,
    -- Estrato con mapeo de dominio
    et.ilicode as estrato,
    p.otro_cual_estrato
FROM {schema}.cca_predio p
LEFT JOIN {schema}.cca_usuario u ON p.usuario = u.t_id
LEFT JOIN {schema}.cca_interesadodocumentotipo idt ON u.tipo_documento = idt.t_id
LEFT JOIN {schema}.cca_resultadovisitatipo rvt ON p.resultado_visita = rvt.t_id
LEFT JOIN {schema}.cca_booleanotipo bat ON p.suscribe_acta_colindancia = bat.t_id
LEFT JOIN {schema}.cca_estratotipo et ON p.estrato = et.t_id;
