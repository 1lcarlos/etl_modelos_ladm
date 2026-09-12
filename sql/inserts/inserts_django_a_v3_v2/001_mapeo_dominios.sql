-- =============================================================================
-- 001 - Mapeo de dominios Django -> v3 (reglas de negocio en un solo lugar)
-- Solo se listan los valores que NO coinciden textualmente con un ilicode de v3;
-- el resto se resuelve por coincidencia exacta en etl_control.dominio().
--   ilicode_destino NULL = sin equivalente aprobado: el dato queda NULL y se
--   registra una incidencia NULIFICADO. Para aprobar un mapeo, llenar ese valor.
-- =============================================================================

DELETE FROM "etl_control"."mapeo_dominio";

INSERT INTO "etl_control"."mapeo_dominio" (dominio, valor_origen, ilicode_destino, observacion) VALUES
    -- Tipo de predio
    ('gc_prediotipo', 'Privado',              'Predio.Privado',                    'Equivalente'),
    ('gc_prediotipo', 'Publico.Baldio',       'Predio.Publico.Baldio',             'Equivalente'),
    ('gc_prediotipo', 'Publico.Fiscal',       'Predio.Publico.Fiscal_Patrimonial', 'v3 une Fiscal y Patrimonial'),
    ('gc_prediotipo', 'Publico.Patrimonial',  'Predio.Publico.Fiscal_Patrimonial', 'v3 une Fiscal y Patrimonial'),
    ('gc_prediotipo', 'Publico.Uso_Publico',  'Predio.Publico.Uso_Publico',        'Equivalente'),
    ('gc_prediotipo', 'Publico.Ejido',        NULL, 'PENDIENTE: sin equivalente en v3'),
    ('gc_prediotipo', 'Territorio_Colectivo', NULL, 'PENDIENTE: sin equivalente en v3'),
    ('gc_prediotipo', 'Vacante',              NULL, 'PENDIENTE: sin equivalente en v3'),

    -- Condicion del predio
    ('gc_condicionprediotipo', 'Mejora', 'Informal', 'Decision 2026-09-11: relacion en gc_predio_informalidad'),

    -- Destinacion economica
    ('gc_destinacioneconomicatipo', 'Forestal',             NULL, 'PENDIENTE: v3 tiene Forestal_Productor y Conservacion_Proteccion_Ambiental'),
    ('gc_destinacioneconomicatipo', 'Servicios_Especiales', NULL, 'PENDIENTE: sin equivalente en v3'),

    -- Tipo de documento (interesado y reconocedor)
    ('col_documentotipo', 'NUIP',                 NULL,                'PENDIENTE: sin equivalente en v3'),
    ('col_documentotipo', 'Cédula de ciudadanía', 'Cedula_Ciudadania', 'Texto libre del reconocedor (dlc), regla del flujo original'),

    -- Sexo
    ('gc_sexotipo', 'Otro', NULL, 'PENDIENTE: v3 tiene No_Binario, no es equivalente directo'),

    -- Grupo etnico -> autorreconocimiento etnico
    ('gc_autoreconocimientoetnicotipo', 'Indigena',             'Etnico.Indigena',    'Equivalente'),
    ('gc_autoreconocimientoetnicotipo', 'Rrom',                 'Etnico.Gitano_Rrom', 'Equivalente'),
    ('gc_autoreconocimientoetnicotipo', 'Raizal',               'Etnico.Raizal',      'Equivalente'),
    ('gc_autoreconocimientoetnicotipo', 'Palenquero',           'Etnico.Palenquero',  'Equivalente'),
    ('gc_autoreconocimientoetnicotipo', 'Negro_Afrocolombiano', NULL, 'PENDIENTE: v3 separa Etnico.Negro y Etnico.Afrocolombiano'),

    -- Uso de la unidad de construccion
    ('gc_usouconstipo', 'Residencial.Apartamentos_Mas_De_4_Pisos',                   'Residencial.Apartamentos_4_y_mas_pisos', 'Equivalente'),
    ('gc_usouconstipo', 'Residencial.Apartamentos_En_Edificio_4_y_5_Pisos_Cartagena', NULL, 'PENDIENTE: sin equivalente en v3'),
    ('gc_usouconstipo', 'Anexo.Unidad_Predial_por_Construir',                          NULL, 'PENDIENTE: sin equivalente en v3'),

    -- Tipo de fuente espacial
    ('col_fuenteespacialtipo', 'Acta_Colindacia',     NULL, 'PENDIENTE: sin equivalente en v3'),
    ('col_fuenteespacialtipo', 'Acto_Administrativo', NULL, 'PENDIENTE: sin equivalente en v3'),
    ('col_fuenteespacialtipo', 'Plano',               NULL, 'PENDIENTE: sin equivalente en v3'),

    -- Tipo de punto (gc_puntotipo en Django -> col_puntotipo en v3)
    ('col_puntotipo', 'Poste',               'Catastro.Poste',               'Equivalente'),
    ('col_puntotipo', 'Construccion',        'Catastro.Construccion',        'Equivalente'),
    ('col_puntotipo', 'Punto_Dinamico',      'Catastro.Punto_Dinamico',      'Equivalente'),
    ('col_puntotipo', 'Elemento_Natural',    'Catastro.Elemento_Natural',    'Equivalente'),
    ('col_puntotipo', 'Piedra',              'Catastro.Piedra',              'Equivalente'),
    ('col_puntotipo', 'Sin_Materializacion', 'Catastro.Sin_Materializacion', 'Equivalente'),
    ('col_puntotipo', 'Mojon',               'Catastro.Mojon',               'Equivalente'),
    ('col_puntotipo', 'Incrustacion',        'Catastro.Incrustacion',        'Equivalente'),
    ('col_puntotipo', 'Pilastra',            'Catastro.Pilastra',            'Equivalente'),

    -- Resultado de visita (texto libre en dlc, regla del flujo original)
    ('gc_resultadovisitatipo', 'No hay nadie',               'No_Hay_Nadie',            'Flujo original'),
    ('gc_resultadovisitatipo', 'Zona de difícil acceso',     'Zona_Dificil_Acceso',     'Flujo original'),
    ('gc_resultadovisitatipo', 'No permitieron acceso',      'No_Permitieron_Acceso',   'Flujo original'),
    ('gc_resultadovisitatipo', 'No permitieron accesotro',   'No_Permitieron_Acceso',   'Flujo original (valor mal digitado en origen)'),
    ('gc_resultadovisitatipo', 'Menor de edad',              'Menor_Edad',              'Flujo original'),
    ('gc_resultadovisitatipo', 'Situacion de orden publico', 'Situacion_Orden_Publico', 'Flujo original'),

    -- Estrato (texto en dlc)
    ('gc_estratotipo', '1', 'Bajo_Bajo_1',  'Estrato numerico'),
    ('gc_estratotipo', '2', 'Bajo_2',       'Estrato numerico'),
    ('gc_estratotipo', '3', 'Medio_Bajo_3', 'Estrato numerico'),
    ('gc_estratotipo', '4', 'Medio_4',      'Estrato numerico'),
    ('gc_estratotipo', '5', 'Medio_Alto_5', 'Estrato numerico'),
    ('gc_estratotipo', '6', 'Alto_6',       'Estrato numerico'),

    -- Tipo de tramite (public.tmp_tramite) -> clasificacion de mutacion en v3.
    -- Decisiones 2026-09-11: el origen no distingue mas detalle que este texto.
    ('gc_mutaciontipo', 'Mutación de segunda - Englobe',                      'Mutacion_Segunda_Clase.Agregacion',             'Englobe = agregacion de predios'),
    ('gc_mutaciontipo', 'Mutación de segunda - Desenglobe total',             'Mutacion_Segunda_Clase.Segregacion',            'Desenglobe = segregacion'),
    ('gc_mutaciontipo', 'Mutación de segunda - Desenglobe parcial',           'Mutacion_Segunda_Clase.Segregacion',            'Desenglobe = segregacion'),
    ('gc_mutaciontipo', 'Mutación de quinta - Inscripción de predio',         'Mutacion_Quinta_Clase.Predios_Nuevos_Formales', 'Incorporacion de predio nuevo formal'),
    ('gc_mutaciontipo', 'Rectificación de datos que modifican avalúo',        'Rectificaciones.Errores_Inscripcion_Catastral', 'Decision: se rectifican datos mal inscritos; el cambio de avaluo es la consecuencia'),
    ('gc_mutaciontipo', 'Rectificación de datos que no modifican avalúo',     'Rectificaciones.Errores_Inscripcion_Catastral', 'Decision: se rectifican datos mal inscritos'),
    ('gc_mutaciontipo', 'Procedimientos catastrales con efectos registrales', 'Rectificaciones.Errores_Inscripcion_Catastral', 'Decision: correccion de area o linderos con efecto registral'),
    ('gc_mutaciontipo', 'Cancelación de predios',                             'Cancelaciones.Fuerza_Mayor',                    'Decision: el origen no distingue la causa de la cancelacion');
