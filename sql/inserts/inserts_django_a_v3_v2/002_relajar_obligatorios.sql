-- =============================================================================
-- 002 - Obligatoriedad temporal (decision 2026-09-11: "NULL cuando no exista el dato")
-- El modelo v3 marca como obligatorios atributos que Django no tiene o permite vacios.
-- Para migrar sin inventar valores, se quita el NOT NULL de estas columnas.
-- Consecuencias:
--   * ilivalidator reportara esos atributos faltantes: es la deuda de datos real.
--   * manual/reporte_migracion.sql cuenta los NULL de cada columna de esta lista.
--   * manual/restaurar_obligatorios.sql vuelve a exigirlos cuando los datos esten completos.
-- =============================================================================

ALTER TABLE {schema}.gc_predio
    ALTER COLUMN tipo_predio DROP NOT NULL,
    ALTER COLUMN condicion_predio DROP NOT NULL,
    ALTER COLUMN destinacion_economica DROP NOT NULL,
    ALTER COLUMN area_catastral_terreno DROP NOT NULL,
    ALTER COLUMN vigencia_actualizacion_catastral DROP NOT NULL,
    ALTER COLUMN clase_suelo DROP NOT NULL,
    ALTER COLUMN codigo_homologado DROP NOT NULL;

ALTER TABLE {schema}.gc_interesado
    ALTER COLUMN tipo_documento DROP NOT NULL,
    ALTER COLUMN sexo DROP NOT NULL;

ALTER TABLE {schema}.gc_agrupacioninteresados
    ALTER COLUMN tipo DROP NOT NULL;

ALTER TABLE {schema}.gc_derechocatastral
    ALTER COLUMN tipo DROP NOT NULL;

ALTER TABLE {schema}.gc_fuenteadministrativa
    ALTER COLUMN tipo DROP NOT NULL;

ALTER TABLE {schema}.extdireccion
    ALTER COLUMN tipo_direccion DROP NOT NULL;

ALTER TABLE {schema}.gc_caracteristicasunidadconstruccion
    ALTER COLUMN tipo_unidad_construccion DROP NOT NULL,
    ALTER COLUMN total_plantas DROP NOT NULL,
    ALTER COLUMN uso DROP NOT NULL,
    ALTER COLUMN anio_construccion DROP NOT NULL,
    ALTER COLUMN area_construida DROP NOT NULL,
    ALTER COLUMN estado_conservacion DROP NOT NULL;

ALTER TABLE {schema}.gc_unidadconstruccion
    ALTER COLUMN tipo_planta DROP NOT NULL,
    ALTER COLUMN planta_ubicacion DROP NOT NULL,
    ALTER COLUMN cr_caracteristicasunidadconstruccion DROP NOT NULL;

ALTER TABLE {schema}.gc_estructuraavaluo
    ALTER COLUMN fecha_avaluo DROP NOT NULL,
    ALTER COLUMN avaluo_catastral DROP NOT NULL;

ALTER TABLE {schema}.gc_estructuraavaluocaracterizacion
    ALTER COLUMN fecha_avaluo DROP NOT NULL,
    ALTER COLUMN avaluo_catastral DROP NOT NULL;

ALTER TABLE {schema}.gc_calificacionconvencional
    ALTER COLUMN tipo_calificar DROP NOT NULL,
    ALTER COLUMN total_calificacion DROP NOT NULL;

ALTER TABLE {schema}.gc_calificacionnoconvencional
    ALTER COLUMN tipo_anexo DROP NOT NULL;

ALTER TABLE {schema}.gc_grupocalificacion
    ALTER COLUMN clase_calificacion DROP NOT NULL,
    ALTER COLUMN subtotal DROP NOT NULL;

ALTER TABLE {schema}.gc_objetoconstruccion
    ALTER COLUMN tipo_objeto_construccion DROP NOT NULL,
    ALTER COLUMN puntos DROP NOT NULL;

ALTER TABLE {schema}.gc_puntolindero
    ALTER COLUMN puntotipo DROP NOT NULL,
    ALTER COLUMN desacuerdo DROP NOT NULL,
    ALTER COLUMN metodoproduccion DROP NOT NULL;

ALTER TABLE {schema}.gc_puntocontrol
    ALTER COLUMN puntotipo DROP NOT NULL,
    ALTER COLUMN metodoproduccion DROP NOT NULL;

ALTER TABLE {schema}.gc_fuenteespacial
    ALTER COLUMN tipo DROP NOT NULL;

ALTER TABLE {schema}.gc_predio_copropiedad
    ALTER COLUMN coeficiente DROP NOT NULL,
    ALTER COLUMN area_catastral_terreno_coeficiente DROP NOT NULL;

ALTER TABLE {schema}.gc_predio_informalidad
    ALTER COLUMN area_terreno_interseccion DROP NOT NULL;

ALTER TABLE {schema}.gc_estructurareferenciaregistralsistemaantiguo
    ALTER COLUMN tipo_referencia DROP NOT NULL,
    ALTER COLUMN matricula DROP NOT NULL;

ALTER TABLE {schema}.gc_estructuraalertapredio
    ALTER COLUMN descripcion_alerta DROP NOT NULL,
    ALTER COLUMN entidad_emisora_alerta DROP NOT NULL;
