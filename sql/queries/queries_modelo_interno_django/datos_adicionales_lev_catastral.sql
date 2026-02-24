SELECT
    id,    
    observaciones,
    fecha_visita_predial::date,
    case
        when document_type_recognizer = 'Cédula de ciudadanía'
        then'Cedula_Ciudadania'
        when document_type_recognizer = 'Cedula_Ciudadania'
        then'Cedula_Ciudadania'
        else 'Sin_Informacion'
    end as tipo_documento_reconocedor,
    numero_documento_reconocedor,
    primer_nombre_reconocedor,
    segundo_nombre_reconocedor,
    primer_apellido_reconocedor,
    segundo_apellido_reconocedor,
    case
        when resultado_visita = 'No hay nadie'
        then 'No_Hay_Nadie'
        when resultado_visita = 'Zona de difícil acceso'
        then  'Zona_Dificil_Acceso'
        when resultado_visita = 'Otro'
        then  'Otro'
        when resultado_visita = 'No permitieron accesotro'
        then  'No_Permitieron_Acceso'
        when resultado_visita = 'Menor de edad'
        then  'Menor_Edad'
        when resultado_visita = 'Incompleto'
        then  'Incompleto'
        when resultado_visita = 'Exitoso'
        then  'Exitoso'
        when resultado_visita = 'Situacion de orden publico'
        then  'Situacion_Orden_Publico'        
    else 'Sin_Visita'
    end as resultado_visita,
    otro_cual_resultado_visita,
    suscribe_acta_colindancia::bool,
    despojo_abandono::bool,
    estrato::integer,
    otro_cual_estrato,
    gc_predio
FROM
    {schema}.dlc_datosadicionaleslevantamientocatastral;