select
    t.numero_radicado,
    t.fecha_radicado,
    t.resolution_number,
    t.tramite_tipo,
    t.resolution_date,
    t.numero_predial
from
    tmp_tramite t
join {schema}.gc_predio p on t.numero_predial = p.numero_predial 
where
    /* extract(
        year
        from
            resolution_date
    ) = '2025'
    and extract(
        month
        from
            resolution_date
    ) in ('08', '07')
    and */ 
substring(t.numero_predial, 1, 5) = substring('{schema}', 4, 5)