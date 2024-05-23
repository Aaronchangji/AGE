MATCH (n:product {id:$ID})-[:also_buy*1..3]->(m:product)
WITH DISTINCT m
MATCH (m)-[:has_review]->(r:review)
WHERE (r.review_time > $TIME1 AND r.review_time < $TIME2) AND (r.rating = $RATE1 OR r.rating = $RATE2)
RETURN
	m.id,
	m.title,
	m.brand,
	r.asin
ORDER BY
	m.id ASC
LIMIT 20