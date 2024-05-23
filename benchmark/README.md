# Benchmarks

## LDBC-SNB

The query templates of LDBC-SNB can be seen in https://github.com/ldbc/ldbc_snb_interactive_v1_impls. 

## Amazon

### Data Schama

Vertices:

* Products: label:LABEL | node_id:ID | asin:STRING | main_cat:STRING | brand:STRING | title:STRING | price:STRING | description:STRING
* Reviews: label:LABEL | node_id:ID | asin:STRING | user:STRING | rating:FLOAT | review_time:INT
* Users: label:LABEL | node_id:ID | user_id:STRING

Edges:
* also_view/also_buy: (n:product)-[e:also_view]->(m:product)
* has_review: (n:product)-[e:has_review]->(m:review)
* has_creator: (r:review)-[:has_creator]->(u:user)

### Queries

Amazon Complex Queries:

| ID | Name | Description |
|---|---|---|
|AC1| Product Relationship w/ also_view | Given a product, find the products that are also viewed within three relationships. |
|AC2| Product Analysis | Given a product, find the products that are also viewed. Return all the reviews of those products. |
|AC3| Product Relationship w/ also_buy | Given a product, find the products that are also bought within three relationships. Find the reviews of those products. Reviews should be generated within given period and with given rating. |
|AC4| Recommend Product | Given a user, recommend products based on the products the user has reviewed. The recommended products should be also bought within two relationships, and the overall rating is larger than given rate.|
|AC5| Good and Bad Reviews | Given a product, find the products that are also viewed by users. Based on the given rate threshold, find all bad reviews (rating is lower than the rate threshold), of products with good reviews (rating is large than the rate threshold). |
|AC6| Recommend Product | Given a user, recommend products that are also viewed and bought by other users. The products should be also viewed with products the given user has reviewed. The overall rating of the product should be larger than given rate.|
|AC7| Recommend User | Given a user, recommend users that have similar interests. The recommended users should create reviews on the same products. |

Amazon Short Queries:

| ID | Name | Description |
|---|---|---|
|AS1| Find Review | Given a review, retrieve its rating, review time, and the product information.  |
|AS2| User Profile | Given a product, find the products that are also viewed. Return all users that reviewed those products. |
|AS3| Product AlsoBuy | Given a product, find all products that are also bought by users. |
|AS4| Review Access | Given a review, return its rating and creation time. |
|AS5| Review Product | Given a review, return the associated product. |
