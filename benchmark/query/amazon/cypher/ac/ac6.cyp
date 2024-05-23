MATCH (u:user {id:$ID})<-[:has_creator]-(r:review)<-[:has_review]-(p:product)-[:also_view*1..2]-(rec:product)
WHERE r.rating > $RATE AND (p)-[:also_buy]->(rec)
RETURN
	p.title AS ProductID,
	rec.id AS RecommendationID,
	rec.brand AS RecommendationBrand
ORDER BY
	p.id ASC,
	rec.id ASC
LIMIT 20