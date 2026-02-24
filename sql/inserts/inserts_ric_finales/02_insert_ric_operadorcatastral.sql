-- INSERT para tabla ric_operadorcatastral
-- Crea el operador catastral por defecto necesario para los predios RIC
-- Fecha: 2025-12-18

-- Insertar operador catastral por defecto (IGAC)
-- Solo se inserta si no existe ya un registro
INSERT INTO ric.ric_operadorcatastral (
    t_id,
    t_ili_tid,
    nombre_operador,
    nit_operador_catastral
)
SELECT
    nextval('ric.t_ili2db_seq'::regclass),
    uuid_generate_v4(),
    'Agencia Catastral de Cundinamarca ACC',
    '901421041-7'
WHERE NOT EXISTS (
    SELECT 1 FROM ric.ric_operadorcatastral WHERE nombre_operador = 'Agencia Catastral de Cundinamarca ACC'
);
