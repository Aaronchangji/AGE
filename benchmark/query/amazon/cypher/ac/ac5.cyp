MATCH (:product {id:$ID})-[:also_view*1..3]-(product:product)
WITH DISTINCT product
MATCH (product)-[:has_review]->(goodreview:review)
WHERE
	goodreview.rating >= $RATE1
WITH DISTINCT product
MATCH (product)-[:has_review]->(badreview:review)
WHERE
	badreview.rating <= $RATE2
RETURN
	product.brand,
	count(DISTINCT badreview)
ORDER BY
	product.brand
LIMIT 20