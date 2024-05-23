MATCH (:product {id:$ID})-[:also_view]->(p:product)-[:has_review]->(review:review)
WHERE review.review_time < $TIME
RETURN
	p.id,
	p.title,
	p.brand,
	review.id,
	review.rating,
	review.review_time
ORDER BY
	review.review_time DESC,
	review.id ASC
LIMIT 20
