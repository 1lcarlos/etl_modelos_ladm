-- Consulta para extraer datos de cca_calificacionconvencional
-- Origen: Modelo CCA (cca_calificacionconvencional + dominios)
-- Destino: cuc_calificacionconvencional, cuc_grupocalificacion, cuc_objetoconstruccion (modelo Django)
-- Fecha: 2026-02-06
--
-- Notas:
-- 1. En CCA la calificacion es una tabla plana con todos los campos
-- 2. En Django se normaliza en 3 tablas: calificacion, grupo, objeto
-- 3. Se extrae ilicode de cada dominio para mapeo con text_code en destino
-- 4. La relacion con caracteristicas es inversa: cca_caracteristicasunidadconstruccion.calificacion_convencional
-- 5. Los dominios del destino usan prefijo: 'Armazon.' + ilicode, 'Muros.' + ilicode, etc.

SELECT
    distinct on (cc.t_id, cu.t_id)
    cc.t_id as cca_calificacion_id,
    cc.t_ili_tid,
    cc.total_calificacion,

    -- Relacion inversa: obtener el t_id de caracteristicasunidadconstruccion
    cu.t_id as cca_caracteristicas_id,

    -- Tipo calificar
    tct.ilicode as tipo_calificar,

    -- Clase calificacion (concepto CCA, diferente al grupo Django)
    ccl.ilicode as clase_calificacion,

    -- ===== GRUPO ESTRUCTURA =====
    cc.subtotal_estructura,
    arm.ilicode as armazon,
    mur.ilicode as muros,
    cub.ilicode as cubierta,
    ce.ilicode as conservacion_estructura,

    -- ===== GRUPO ACABADOS =====
    cc.subtotal_acabados,
    fac.ilicode as fachada,
    cmur.ilicode as cubrimiento_muros,
    pis.ilicode as piso,
    ca.ilicode as conservacion_acabados,

    -- ===== GRUPO BANIO =====
    cc.subtotal_banio,
    tb.ilicode as tamanio_banio,
    eb.ilicode as enchape_banio,
    mb.ilicode as mobiliario_banio,
    cb.ilicode as conservacion_banio,

    -- ===== GRUPO COCINA =====
    cc.subtotal_cocina,
    tc.ilicode as tamanio_cocina,
    ec.ilicode as enchape_cocina,
    mc.ilicode as mobiliario_cocina,
    cco.ilicode as conservacion_cocina,

    -- ===== GRUPO CERCHAS =====
    cc.subtotal_cerchas,
    cer.ilicode as cerchas

FROM {schema}.cca_calificacionconvencional cc
-- Relacion inversa con caracteristicas
LEFT JOIN {schema}.cca_caracteristicasunidadconstruccion cu ON cu.calificacion_convencional = cc.t_id
-- Dominios generales
LEFT JOIN {schema}.cca_calificartipo tct ON cc.tipo_calificar = tct.t_id
LEFT JOIN {schema}.cca_clasecalificaciontipo ccl ON cc.clase_calificacion = ccl.t_id
-- Estructura
LEFT JOIN {schema}.cca_armazontipo arm ON cc.armazon = arm.t_id
LEFT JOIN {schema}.cca_murostipo mur ON cc.muros = mur.t_id
LEFT JOIN {schema}.cca_cubiertatipo cub ON cc.cubierta = cub.t_id
LEFT JOIN {schema}.cca_estadoconservaciontipo ce ON cc.conservacion_estructura = ce.t_id
-- Acabados
LEFT JOIN {schema}.cca_fachadatipo fac ON cc.fachada = fac.t_id
LEFT JOIN {schema}.cca_cubrimientomurostipo cmur ON cc.cubrimiento_muros = cmur.t_id
LEFT JOIN {schema}.cca_pisotipo pis ON cc.piso = pis.t_id
LEFT JOIN {schema}.cca_estadoconservaciontipo ca ON cc.conservacion_acabados = ca.t_id
-- Banio
LEFT JOIN {schema}.cca_tamaniobaniotipo tb ON cc.tamanio_banio = tb.t_id
LEFT JOIN {schema}.cca_enchapetipo eb ON cc.enchape_banio = eb.t_id
LEFT JOIN {schema}.cca_mobiliariotipo mb ON cc.mobiliario_banio = mb.t_id
LEFT JOIN {schema}.cca_estadoconservaciontipo cb ON cc.conservacion_banio = cb.t_id
-- Cocina
LEFT JOIN {schema}.cca_tamaniococinatipo tc ON cc.tamanio_cocina = tc.t_id
LEFT JOIN {schema}.cca_enchapetipo ec ON cc.enchape_cocina = ec.t_id
LEFT JOIN {schema}.cca_mobiliariotipo mc ON cc.mobiliario_cocina = mc.t_id
LEFT JOIN {schema}.cca_estadoconservaciontipo cco ON cc.conservacion_cocina = cco.t_id
-- Cerchas
LEFT JOIN {schema}.cca_cerchastipo cer ON cc.cerchas = cer.t_id;
