-- ===========================================================================
--  PARTIE 5.4 - AGREGATIONS sur la couche GOLD (GROUP BY, moyennes, tendance)
--  Met en evidence l'analyse temporelle sur ~6 mois d'historique reconstitue.
-- ===========================================================================

-- 1) Comptage / moyenne simples sur une table Gold
SELECT
    category,
    COUNT(*)                       AS nb_produits,
    ROUND(AVG(price), 2)           AS prix_moyen,
    ROUND(AVG(avg_rating_6m), 2)   AS note_moyenne_6m,
    MIN(price)                     AS prix_min,
    MAX(price)                     AS prix_max
FROM nessie.gold.dim_products
GROUP BY category
ORDER BY prix_moyen DESC;

-- 2) Tendance du chiffre d'affaires par mois (timeline reconstituee)
SELECT
    snapshot_month,
    SUM(revenue)                                   AS ca_mensuel,
    SUM(units_sold)                                AS unites,
    SUM(n_carts)                                   AS paniers,
    ROUND(SUM(revenue) / NULLIF(SUM(n_carts), 0), 2) AS panier_moyen
FROM nessie.gold.agg_sales_by_category_month
GROUP BY snapshot_month
ORDER BY snapshot_month;

-- 3) Evolution mensuelle du CA par categorie + variation vs mois precedent
SELECT
    snapshot_month,
    category,
    revenue,
    LAG(revenue) OVER (PARTITION BY category ORDER BY snapshot_month)          AS revenue_mois_prec,
    ROUND(revenue - LAG(revenue) OVER (PARTITION BY category ORDER BY snapshot_month), 2) AS delta
FROM nessie.gold.agg_sales_by_category_month
ORDER BY category, snapshot_month;

-- 4) Repartition du CA par zone client et par mois (jointure deja faite en Gold)
SELECT snapshot_month, us_region, revenue, units_sold, n_customers
FROM nessie.gold.agg_sales_by_region_month
ORDER BY snapshot_month, revenue DESC;

-- 5) Produits dont le prix moyen a le plus bouge sur la periode
SELECT product_id,
       MIN(avg_price)                    AS prix_min_mensuel,
       MAX(avg_price)                    AS prix_max_mensuel,
       ROUND(MAX(avg_price) - MIN(avg_price), 2) AS amplitude,
       MAX(ABS(price_change_pct))        AS variation_max_pct
FROM nessie.gold.agg_product_price_history
GROUP BY product_id
HAVING MAX(avg_price) <> MIN(avg_price)
ORDER BY amplitude DESC
LIMIT 15;
