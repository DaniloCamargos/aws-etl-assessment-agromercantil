-- calcula preco medio mensal por commodity
WITH media_mensal AS (
  SELECT
		c.nome AS commodity
		,DATE_TRUNC('month', f.dt_ref) AS mes
		,ROUND(AVG(f.val_brl)::numeric, 2) AS preco_medio_brl
  FROM analytics.fato_preco f
  -- relaciona fato com dimensao
  JOIN analytics.dim_commodity c
  		ON c.id_commodity = f.id_commodity
  -- filtra valores validos
  WHERE f.val_brl > 0
  -- agrupa por commodity e mes
  GROUP BY 
		c.nome
		,DATE_TRUNC('month', f.dt_ref)
)
-- calcula variacao em relacao ao mes anterior
SELECT
		commodity
		,TO_CHAR(mes, 'YYYY-MM') AS mes
		,preco_medio_brl

		-- recupera valor do mes anterior
		,LAG(preco_medio_brl) OVER (
PARTITION BY commodity
ORDER BY     mes
) AS preco_mes_anterior
		-- calcula variacao percentual mes a mes
		,ROUND(
        (preco_medio_brl - LAG(preco_medio_brl) OVER (
            PARTITION BY commodity ORDER BY mes)
        ) / NULLIF(LAG(preco_medio_brl) OVER (
            PARTITION BY commodity ORDER BY mes), 0) * 100
      	, 2
    	) AS var_pct_mes_anterior
		
FROM  media_mensal
-- ordena resultado final
ORDER BY commodity, mes DESC;