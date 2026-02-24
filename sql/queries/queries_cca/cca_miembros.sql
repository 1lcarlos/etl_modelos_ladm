-- Consulta para extraer datos de cca_miembros
-- Origen: Modelo CCA (cca_miembros)
-- Destino: col_miembros (modelo Django)
-- Fecha: 2026-02-06
--
-- Notas:
-- 1. Relaciona interesados con agrupaciones
-- 2. participacion es valor numerico entre 0.0 y 1.0
-- 3. En Django el FK a interesado se llama interesado_gc_interesado

SELECT
    m.t_id as cca_miembro_id,
    m.t_ili_tid,
    m.interesado as cca_interesado_id,
    m.agrupacion as cca_agrupacion_id,
    m.participacion

FROM {schema}.cca_miembros m;
