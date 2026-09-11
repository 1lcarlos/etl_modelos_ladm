-- =============================================================================
-- Vuelve a exigir los atributos obligatorios relajados en 002_relajar_obligatorios.sql.
-- Ejecutar a mano cuando la deuda de datos este resuelta (reporte_migracion.sql, seccion 4).
-- Si alguna columna aun tiene NULL, PostgreSQL rechaza el cambio: es la senal esperada.
-- Reemplazar cun25123 por el esquema a restaurar.
-- =============================================================================

ALTER TABLE cun25123.gc_predio
    ALTER COLUMN tipo_predio SET NOT NULL,
    ALTER COLUMN condicion_predio SET NOT NULL,
    ALTER COLUMN destinacion_economica SET NOT NULL,
    ALTER COLUMN area_catastral_terreno SET NOT NULL,
    ALTER COLUMN vigencia_actualizacion_catastral SET NOT NULL,
    ALTER COLUMN clase_suelo SET NOT NULL,
    ALTER COLUMN codigo_homologado SET NOT NULL;

ALTER TABLE cun25123.gc_interesado
    ALTER COLUMN tipo_documento SET NOT NULL,
    ALTER COLUMN sexo SET NOT NULL;

ALTER TABLE cun25123.gc_agrupacioninteresados ALTER COLUMN tipo SET NOT NULL;
ALTER TABLE cun25123.gc_derechocatastral ALTER COLUMN tipo SET NOT NULL;
ALTER TABLE cun25123.gc_fuenteadministrativa ALTER COLUMN tipo SET NOT NULL;
ALTER TABLE cun25123.extdireccion ALTER COLUMN tipo_direccion SET NOT NULL;

ALTER TABLE cun25123.gc_caracteristicasunidadconstruccion
    ALTER COLUMN tipo_unidad_construccion SET NOT NULL,
    ALTER COLUMN total_plantas SET NOT NULL,
    ALTER COLUMN uso SET NOT NULL,
    ALTER COLUMN anio_construccion SET NOT NULL,
    ALTER COLUMN area_construida SET NOT NULL,
    ALTER COLUMN estado_conservacion SET NOT NULL;

ALTER TABLE cun25123.gc_unidadconstruccion
    ALTER COLUMN tipo_planta SET NOT NULL,
    ALTER COLUMN planta_ubicacion SET NOT NULL,
    ALTER COLUMN cr_caracteristicasunidadconstruccion SET NOT NULL;

ALTER TABLE cun25123.gc_estructuraavaluo
    ALTER COLUMN fecha_avaluo SET NOT NULL,
    ALTER COLUMN avaluo_catastral SET NOT NULL;

ALTER TABLE cun25123.gc_estructuraavaluocaracterizacion
    ALTER COLUMN fecha_avaluo SET NOT NULL,
    ALTER COLUMN avaluo_catastral SET NOT NULL;

ALTER TABLE cun25123.gc_calificacionconvencional
    ALTER COLUMN tipo_calificar SET NOT NULL,
    ALTER COLUMN total_calificacion SET NOT NULL;

ALTER TABLE cun25123.gc_calificacionnoconvencional ALTER COLUMN tipo_anexo SET NOT NULL;

ALTER TABLE cun25123.gc_grupocalificacion
    ALTER COLUMN clase_calificacion SET NOT NULL,
    ALTER COLUMN subtotal SET NOT NULL;

ALTER TABLE cun25123.gc_objetoconstruccion
    ALTER COLUMN tipo_objeto_construccion SET NOT NULL,
    ALTER COLUMN puntos SET NOT NULL;

ALTER TABLE cun25123.gc_puntolindero
    ALTER COLUMN puntotipo SET NOT NULL,
    ALTER COLUMN desacuerdo SET NOT NULL,
    ALTER COLUMN metodoproduccion SET NOT NULL;

ALTER TABLE cun25123.gc_puntocontrol
    ALTER COLUMN puntotipo SET NOT NULL,
    ALTER COLUMN metodoproduccion SET NOT NULL;

ALTER TABLE cun25123.gc_fuenteespacial ALTER COLUMN tipo SET NOT NULL;

ALTER TABLE cun25123.gc_predio_copropiedad
    ALTER COLUMN coeficiente SET NOT NULL,
    ALTER COLUMN area_catastral_terreno_coeficiente SET NOT NULL;

ALTER TABLE cun25123.gc_predio_informalidad ALTER COLUMN area_terreno_interseccion SET NOT NULL;

ALTER TABLE cun25123.gc_estructurareferenciaregistralsistemaantiguo
    ALTER COLUMN tipo_referencia SET NOT NULL,
    ALTER COLUMN matricula SET NOT NULL;

ALTER TABLE cun25123.gc_estructuraalertapredio
    ALTER COLUMN descripcion_alerta SET NOT NULL,
    ALTER COLUMN entidad_emisora_alerta SET NOT NULL;
