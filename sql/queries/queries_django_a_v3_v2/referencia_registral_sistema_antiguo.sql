-- Destino: gc_estructurareferenciaregistralsistemaantiguo
-- Origen : extreferenciaregistralsistemaantiguo
WITH datos AS (
    SELECT
        r.id,
        r.seq,
        r.gc_predio_referencia_registral_sistema_antiguo AS id_predio,
        r.oficina,
        r.libro,
        r.tomo,
        r.pagina,
        r.numero,
        r.dia,
        r.mes,
        r.anio,
        r.matricula,
        tr.text_code AS tipo_referencia
    FROM {schema}.extreferenciaregistralsistemaantiguo AS r
    LEFT JOIN {schema}.extreferenciaregistralsistemaantiguo_tipo_referencia AS tr ON tr.id = r.tipo_referencia
    WHERE r.gc_predio_referencia_registral_sistema_antiguo IS NOT NULL
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
