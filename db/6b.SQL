-- analisa preco medio e volume de registros por commodity e regiao no ultimo ano
SELECT
        c.nome AS commodity
        ,r.nome AS regiao
        ,COUNT(*) AS total_registros
        ,ROUND(AVG(f.val_brl)::numeric, 2) AS preco_medio_brl

FROM analytics.fato_preco f
-- relaciona tabelas de dimensao
JOIN analytics.dim_commodity c  
        ON c.id_commodity = f.id_commodity
JOIN analytics.dim_regiao r  
        ON r.id_regiao    = f.id_regiao
-- filtra periodo recente e valores validos
WHERE f.dt_ref >= (CURRENT_DATE - INTERVAL '1 year')
  AND f.val_brl > 0
-- agrupa por commodity e regiao
GROUP BY c.nome, r.nome
-- ordena pelos maiores volumes
ORDER BY total_registros DESC
-- limita aos principais resultados
LIMIT 5;