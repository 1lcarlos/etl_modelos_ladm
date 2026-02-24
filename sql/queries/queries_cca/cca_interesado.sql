-- Consulta para extraer datos de cca_interesado
-- Origen: Modelo CCA (cca_interesado + dominios)
-- Destino: gc_interesado (modelo Django)
-- Fecha: 2026-02-06
--
-- Notas:
-- 1. Los dominios se extraen con ilicode para mapeo con text_code en destino
-- 2. CCA tiene campos adicionales no presentes en Django:
--    departamento, municipio, direccion_residencia, telefono,
--    correo_electronico, autoriza_notificacion_correo
-- 3. Django tiene campo 'verificado' que no existe en CCA
-- 4. grupo_etnico tiene diferencias de nombres entre CCA y Django:
--    Rrom -> Gitano, Negro_Afrocolombiano -> Afrocolombiano, Palenquero -> Palanquero

SELECT
    i.t_id as cca_interesado_id,
    i.t_ili_tid,
    i.documento_identidad,
    i.primer_nombre,
    i.segundo_nombre,
    i.primer_apellido,
    i.segundo_apellido,
    i.razon_social,
    i.nombre,

    -- Dominios con ilicode para mapeo en destino
    tit.ilicode as tipo,
    tdt.ilicode as tipo_documento,
    st.ilicode as sexo,
    get.ilicode as grupo_etnico,
    ect.ilicode as estado_civil

FROM {schema}.cca_interesado i
LEFT JOIN {schema}.cca_interesadotipo tit ON i.tipo = tit.t_id
LEFT JOIN {schema}.cca_interesadodocumentotipo tdt ON i.tipo_documento = tdt.t_id
LEFT JOIN {schema}.cca_sexotipo st ON i.sexo = st.t_id
LEFT JOIN {schema}.cca_grupoetnicotipo get ON i.grupo_etnico = get.t_id
LEFT JOIN {schema}.cca_estadociviltipo ect ON i.estado_civil = ect.t_id;
