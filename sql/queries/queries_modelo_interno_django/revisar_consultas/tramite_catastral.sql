SELECT
    t.id as id_tramite,    
    numero_radicado,
    fecha_radicado,
    resolution_number,    
    tt.dispname  as tramite_tipo,    
    resolution_date,
    p.numero_predial
FROM
    {schema}.tramite t
JOIN {schema}.tramite_tipo tt 
    ON tt.id = t.tramite_tipo
    AND t.town in     
        (substring('{schema}',4,5))
        and estado_actual = 8
        and fecha_radicado is not null
        and resolution_number is not null
        and resolution_date is not null
JOIN {schema}.predio p 
    ON t.id = p.tramite_act_asociado
    AND p.tramite_act_asociado is not null;
