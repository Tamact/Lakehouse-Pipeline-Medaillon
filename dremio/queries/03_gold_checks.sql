-- ===========================================================================
--  PARTIE 5 - Lecture de la couche GOLD depuis Dremio
-- ===========================================================================

-- 1) Dimension produits (etat courant + fenetre de vie sur 6 mois)
SELECT product_id, title, category, price, avg_price_6m, avg_rating_6m,
       first_seen_date, last_seen_date
FROM nessie.gold.dim_products
ORDER BY category, product_id
LIMIT 25;

-- 2) Dimension clients
SELECT user_id, full_name, city, us_region, email_is_valid
FROM nessie.gold.dim_users
ORDER BY us_region, user_id;

-- 3) Table de faits (grain : panier x produit x snapshot)
SELECT snapshot_month, cart_id, user_id, us_region, product_title, category,
       quantity, unit_price, line_revenue
FROM nessie.gold.fact_cart_items
ORDER BY snapshot_month DESC, cart_id
LIMIT 30;

-- 4) Agregat pret a l'emploi : revenu par categorie et par mois
SELECT snapshot_month, category, n_carts, units_sold, revenue, revenue_per_cart
FROM nessie.gold.agg_sales_by_category_month
ORDER BY snapshot_month, category;
