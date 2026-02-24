-- Consulta para extraer datos de cca_fuenteadministrativa_derecho
-- Origen: Modelo CCA (cca_fuenteadministrativa_derecho)
-- Destino: col_rrrfuente (modelo Django)
-- Fecha: 2026-02-06
--
-- Notas:
-- 1. Tabla de relacion entre fuente administrativa y derecho
-- 2. CCA derecho -> Django rrr_gc_derecho
-- 3. CCA fuente_administrativa -> Django fuente_administrativa
-- 4. Django tiene rrr_gc_restriccion que no aplica en esta relacion (NULL)

SELECT
    fd.t_id as cca_fuenteadm_derecho_id,
    fd.t_ili_tid,
    fd.derecho as cca_derecho_id,
    fd.fuente_administrativa as cca_fuenteadministrativa_id

FROM {schema}.cca_fuenteadministrativa_derecho fd;
