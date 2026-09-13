-- ===========================================================================
--  PARTIE 5 - Lecture de la couche SILVER depuis Dremio
--  Silver = donnees typees, nettoyees, deduupliquees, structures aplaties.
-- ===========================================================================

-- 1) Produits Silver : rating.rate / rating.count aplatis, price_band derive
SELECT product_id, title, category, price, price_band, rating_rate, rating_count, snapshot_date
FROM nessie.silver.products
ORDER BY snapshot_date DESC, product_id
LIMIT 25;

-- 2) Utilisateurs Silver : name/address aplatis, full_name, us_region, email_is_valid
SELECT user_id, full_name, email, email_is_valid, city, zipcode, us_region, snapshot_date
FROM nessie.silver.users
ORDER BY snapshot_date DESC, user_id
LIMIT 25;

-- 3) Detail des paniers Silver : 1 ligne par article (tableau "products" eclate)
SELECT cart_id, user_id, product_id, quantity, cart_date, snapshot_date
FROM nessie.silver.cart_items
ORDER BY snapshot_date DESC, cart_id, product_id
LIMIT 30;

-- 4) Controle qualite : aucune ligne ne doit violer les regles Silver
SELECT
  (SELECT COUNT(*) FROM nessie.silver.products   WHERE price < 0 OR product_id IS NULL) AS bad_products,
  (SELECT COUNT(*) FROM nessie.silver.cart_items WHERE quantity <= 0 OR product_id IS NULL) AS bad_items,
  (SELECT COUNT(*) FROM nessie.silver.users      WHERE user_id IS NULL) AS bad_users;
