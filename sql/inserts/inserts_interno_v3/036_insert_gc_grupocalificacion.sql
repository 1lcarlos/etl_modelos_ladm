INSERT INTO {schema}.gc_grupocalificacion
(t_id
, t_ili_tid
, clase_calificacion
, conservacion
, subtotal
, gc_calificacion_convencional)
select 
 id_grupo_calificacion 
, uuid_generate_v4()
, (SELECT t_id FROM {schema}.gc_clasecalificaciontipo WHERE ilicode = tgc.clase_calificacion LIMIT 1)
 -- clase_calificacion: Mapeo a gc_clasecalificaciontipo
   /*  COALESCE(
        (SELECT t_id FROM {schema}.gc_clasecalificaciontipo WHERE ilicode = tgc.clase_calificacion LIMIT 1),
        (SELECT t_id FROM {schema}.gc_clasecalificaciontipo WHERE ilicode = 'NPH' LIMIT 1)
    ), */
, (SELECT t_id FROM {schema}.gc_estadoconservaciontipo WHERE ilicode = tgc.conservacion LIMIT 1)
, subtotal
, cc.t_id as id_gc_calificacion_convencional
from tmp_grupocalificacion tgc
left join {schema}.gc_calificacionconvencional cc on cc.t_id = tgc.id_calificacion_convencional
