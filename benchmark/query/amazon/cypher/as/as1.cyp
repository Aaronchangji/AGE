MATCH (r:review {id:$ID})<-[:has_review]-(p:product)
RETURN
	r.rating as rating,
	r.review_time as rtime,
	r.asin as productName,
	p.id as productId
