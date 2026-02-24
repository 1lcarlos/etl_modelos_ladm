-- Consulta para extraer datos de col_unidadfuente
-- Origen: Modelo CCA (cca_fuenteadministrativa_derecho + cca_derecho + cca_predio)
-- Destino: col_unidadfuente (modelo Django)
-- Fecha: 2026-02-06
--
-- Notas:
-- 1. En CCA la relacion fuente->predio es indirecta: fuente -> derecho -> predio
-- 2. Se hace JOIN de las 3 tablas para obtener el par (fuente_administrativa, predio)
-- 3. Se usa DISTINCT para evitar duplicados cuando multiples derechos del mismo predio
--    comparten la misma fuente administrativa

SELECT DISTINCT
    fd.fuente_administrativa as cca_fuenteadministrativa_id,
    d.predio as cca_predio_id

FROM {schema}.cca_fuenteadministrativa_derecho fd
INNER JOIN {schema}.cca_derecho d ON fd.derecho = d.t_id
INNER JOIN {schema}.cca_predio p ON d.predio = p.t_id;
