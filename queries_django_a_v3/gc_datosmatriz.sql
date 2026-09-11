SELECT
    dph.area_total_terreno,
    dph.area_total_terreno_privada,
    dph.area_total_terreno_comun,
    dph.area_total_construida,
    dph.area_total_construida_privada,
    dph.area_total_construida_comun,
    dph.numero_torres,
    dph.total_unidades_privadas,
    p.local_id  AS predio_local_id
FROM {schema}.gc_datosphcondominio dph
JOIN {schema}.gc_predio p ON p.id = dph.gc_predio;
