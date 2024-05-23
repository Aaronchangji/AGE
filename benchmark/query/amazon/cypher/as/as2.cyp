MATCH (p:product {id:$ID})-[:also_view]-(:product)-[:has_review]->(r:review)-[:has_creator]->(u:user)
RETURN
	r.id as rid,
	r.rating as rating,
	r.review_time as rtime,
	u.id as uid
