MATCH (p:product {id:$ID})-[:also_view*1..3]-(product:product)
WHERE product.main_cat = $CATALOG 
RETURN DISTINCT
	product.id,
	product.brand,
	product.title,
	product.price
ORDER BY
	product.price ASC,
	product.id ASC
LIMIT 20
