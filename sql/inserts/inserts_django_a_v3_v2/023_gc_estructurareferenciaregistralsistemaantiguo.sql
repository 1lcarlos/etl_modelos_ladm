-- =============================================================================
-- Destino: gc_estructurareferenciaregistralsistemaantiguo
-- Origen : tmp_referencia_registral_sistema_antiguo (extreferenciaregistralsistemaantiguo)
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_estructurareferenciaregistralsistemaantiguo', r.id, x.campo, x.valor, x.accion, x.motivo
FROM tmp_referencia_registral_sistema_antiguo AS r
LEFT JOIN {schema}.gc_predio AS p ON p.local_id = r.id_predio AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
CROSS JOIN LATERAL (VALUES
    ('gc_predio_referencia_registral_sistema_antiguo', r.id_predio, 'RECHAZADO', 'Predio no migrado',
     p.t_id IS NULL),
    ('tipo_referencia', r.tipo_referencia, 'NULIFICADO', 'Valor sin equivalente en el dominio destino',
     r.tipo_referencia IS NOT NULL
     AND etl_control.dominio('{schema}', 'gc_estructurareferenciaregistralsistemaantiguo_tipo_refrncia', r.tipo_referencia) IS NULL)
) AS x(campo, valor, accion, motivo, aplica)
WHERE NOT r.es_centinela AND x.aplica;

INSERT INTO {schema}.gc_estructurareferenciaregistralsistemaantiguo (
    t_basket,
    t_seq,
    tipo_referencia,
    oficina,
    libro,
    tomo,
    pagina,
    numero,
    dia,
    mes,
    anio,
    matricula,
    gc_predio_referencia_registral_sistema_antiguo
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    row_number() OVER (PARTITION BY p.t_id ORDER BY r.seq::bigint NULLS LAST, r.id::bigint) - 1,
    etl_control.dominio('{schema}', 'gc_estructurareferenciaregistralsistemaantiguo_tipo_refrncia', r.tipo_referencia),
    etl_control.texto(r.oficina),
    etl_control.texto(r.libro),
    etl_control.texto(r.tomo),
    etl_control.texto(r.pagina),
    etl_control.texto(r.numero),
    etl_control.texto(r.dia),
    etl_control.texto(r.mes),
    etl_control.texto(r.anio),
    etl_control.texto(r.matricula),
    p.t_id
FROM tmp_referencia_registral_sistema_antiguo AS r
JOIN {schema}.gc_predio AS p ON p.local_id = r.id_predio AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
WHERE NOT r.es_centinela;
