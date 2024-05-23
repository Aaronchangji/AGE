MATCH (n:product {id:$ID})-[:also_buy]->(m:product)
RETURN
	m.id as productId,
	m.title as productTitle,
	m.brand as productBrand
ORDER BY
	m.id ASC
