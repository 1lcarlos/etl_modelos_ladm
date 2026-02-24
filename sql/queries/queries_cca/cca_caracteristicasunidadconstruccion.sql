-- Consulta para extraer datos de cca_caracteristicasunidadconstruccion
-- Origen: Modelo CCA (cca_caracteristicasunidadconstruccion)
-- Destino: gc_caracteristicasunidadconstruccion (modelo interno Django)
-- Fecha: 2026-02-06
--
-- Notas:
-- 1. Los dominios se extraen con ilicode para mapeo con text_code en destino
-- 2. El campo calificacion_convencional es FK a cca_calificacionconvencional
--    cuyo total_calificacion se mapea al campo 'puntaje' en destino
-- 3. tipo_anexo y tipo_tipologia no existen directamente en gc destino,
--    se manejan en tablas separadas (cuc_calificacionnoconvencional, cuc_tipologiaconstruccion)

SELECT
    cu.t_id as cca_caracteristicas_id,
    cu.t_ili_tid,
    cu.identificador,
    cu.total_habitaciones,
    cu.total_banios,
    cu.total_locales,
    cu.total_plantas,
    cu.anio_construccion,
    cu.area_construida,
    cu.area_privada_construida,
    cu.observaciones,

    -- Puntaje desde calificacion convencional
    cc.total_calificacion as puntaje,

    -- Dominios con ilicode para mapeo en destino
    tct.ilicode as tipo_construccion,
    tdt.ilicode as tipo_dominio,
    tpt.ilicode as tipo_planta,
    tuct.ilicode as tipo_unidad_construccion,
    tut.ilicode as uso,
    tat.ilicode as tipo_anexo,
    ttt.ilicode as tipo_tipologia

FROM {schema}.cca_caracteristicasunidadconstruccion cu
LEFT JOIN {schema}.cca_calificacionconvencional cc ON cu.calificacion_convencional = cc.t_id
LEFT JOIN {schema}.cca_construcciontipo tct ON cu.tipo_construccion = tct.t_id
LEFT JOIN {schema}.cca_dominioconstrucciontipo tdt ON cu.tipo_dominio = tdt.t_id
LEFT JOIN {schema}.cca_construccionplantatipo tpt ON cu.tipo_planta = tpt.t_id
LEFT JOIN {schema}.cca_unidadconstrucciontipo tuct ON cu.tipo_unidad_construccion = tuct.t_id
LEFT JOIN {schema}.cca_usouconstipo tut ON cu.uso = tut.t_id
LEFT JOIN {schema}.cca_anexotipo tat ON cu.tipo_anexo = tat.t_id
LEFT JOIN {schema}.cca_tipologiatipo ttt ON cu.tipo_tipologia = ttt.t_id;
