MATCH (u:user {id:$ID})<-[:has_creator]-(r:review)-[:has_review]-(p:product)-[:also_buy*1..2]-(m:product)
WHERE r.rating > $RATE
RETURN
	m.id,
	m.title,
	m.brand
ORDER BY
	r.rating DESC,
	m.id ASC
LIMIT 20