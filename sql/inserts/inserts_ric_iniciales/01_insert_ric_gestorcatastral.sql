-- INSERT para tabla ric_gestorcatastral
-- Crea el gestor catastral por defecto necesario para los predios RIC
-- Fecha: 2025-12-18

-- Insertar gestor catastral por defecto (IGAC)
-- Solo se inserta si no existe ya un registro
INSERT INTO ric.ric_gestorcatastral (
    t_id,
    t_ili_tid,
    nombre_gestor,
    nit_gestor_catastral,
    fecha_inicio_prestacion_servicio
)
SELECT
    nextval('ric.t_ili2db_seq'::regclass),
    uuid_generate_v4(),
    'IGAC',
    '899999004-9',
    '2000-01-01'::date
WHERE NOT EXISTS (
    SELECT 1 FROM ric.ric_gestorcatastral WHERE nombre_gestor = 'IGAC'
);
