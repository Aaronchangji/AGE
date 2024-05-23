MATCH (r:review {id:$ID})<-[:has_review]-(p:product)
RETURN
	p.id as productId,
	p.title as productTitle,
	p.brand as productBrand
