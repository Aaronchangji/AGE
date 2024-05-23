MATCH (u:user {id:$ID})<-[:has_creator]-(r:review)<-[:has_review]-(p:product)-[:has_review]->(r2:review)-[:has_creator]->(u2:user)
RETURN
	u2.user_id as user_id,
	p.title as product_title,
	p.brand as product_brand
ORDER BY
	u2.id ASC
LIMIT 20