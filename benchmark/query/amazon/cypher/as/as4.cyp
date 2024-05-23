MATCH (r:review {id:$ID})
RETURN
	r.review_time as review_time,
	r.rating as rating
