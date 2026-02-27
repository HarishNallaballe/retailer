SELECT * FROM `project-b4eb31f9-d0fb-422f-8d7.silver_dataset.customer_reviews`;

CREATE TABLE IF NOT EXISTS `project-b4eb31f9-d0fb-422f-8d7.silver_dataset.customers`
(
    customer_id INT64,
    name STRING,
    email STRING,
    updated_at STRING,
    is_quarantined BOOL,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOL
);

MERGE `project-b4eb31f9-d0fb-422f-8d7.silver_dataset.customers` target
USING (select DISTINCT customer_id,
name,
email,
updated_at,
CASE 
   WHEN customer_id IS NULL or email IS NULL or name IS NULL THEN TRUE
   ELSE FALSE
END AS is_quarantined
FROM `project-b4eb31f9-d0fb-422f-8d7.bronze_dataset.customers`
) source
ON target.customer_id = source.customer_id
AND target.is_active=TRUE

WHEN MATCHED AND (
  target.email != source.email OR
  target.name != source.name OR
  target.updated_at != source.updated_at
)
THEN UPDATE SET
       target.is_active = FALSE,
       target.effective_end_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
INSERT(
  customer_id ,
    name ,
    email ,
    updated_at ,
    is_quarantined ,
    effective_start_date ,
    effective_end_date ,
    is_active 
)VALUES(
  source.customer_id,
  source.name,
  source.email,
  source.updated_at,
  source.is_quarantined,
  CURRENT_TIMESTAMP(),
  NULL,
  TRUE
);

CREATE TABLE IF NOT EXISTS `project-b4eb31f9-d0fb-422f-8d7.silver_dataset.orders`
(
    order_id INT64,
    customer_id INT64,
    order_date STRING,
    total_amount FLOAT64,
    updated_at STRING,
    is_quarantined BOOL,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOL
);

MERGE INTO `project-b4eb31f9-d0fb-422f-8d7.silver_dataset.orders` target
USING (
SELECT DISTINCT order_id,
customer_id,
order_date,
total_amount,
updated_at,
CASE
  WHEN order_id IS NULL OR customer_id IS NULL OR order_date IS NULL OR total_amount < 0 THEN TRUE
  ELSE FALSE
END AS is_quarantined
FROM `project-b4eb31f9-d0fb-422f-8d7.bronze_dataset.orders`) source
ON target.order_id = source.order_id AND target.is_active = TRUE
WHEN MATCHED AND(
    target.customer_id != source.customer_id OR
    target.order_date != source.order_date OR
    target.total_amount != source.total_amount OR
    target.updated_at != source.updated_at
)
THEN UPDATE SET
    target.is_active = FALSE,
    target.effective_end_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT(
order_id,
customer_id,
order_date,
total_amount,
updated_at,
is_quarantined,
effective_start_date,
effective_end_date,
is_active
)
VALUES(
source.order_id,
source.customer_id,
source.order_date,
source.total_amount,
source.updated_at,
source.is_quarantined,
CURRENT_TIMESTAMP(),
NULL,
TRUE
);

------ STEP 3 --------------------------
CREATE TABLE IF NOT EXISTS `project-b4eb31f9-d0fb-422f-8d7.silver_dataset.order_items`
(
    order_item_id INT64,
    order_id INT64,
    product_id INT64,
    quantity INT64,
    price FLOAT64,
    updated_at STRING,
    is_quarantined BOOL,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOL
);

MERGE INTO `project-b4eb31f9-d0fb-422f-8d7.silver_dataset.order_items` target
USING (
SELECT DISTINCT order_item_id,
order_id,
product_id,
quantity,
price,
updated_at,
CASE 
  WHEN order_item_id IS NULL OR order_id IS NULL OR product_id IS NULL OR quantity < 0 OR price<0 THEN TRUE
  ELSE FALSE
END AS is_quarantined
FROM `project-b4eb31f9-d0fb-422f-8d7.bronze_dataset.order_items`) source
ON target.order_item_id = source.order_item_id AND is_active = TRUE
WHEN MATCHED AND(
  target.order_id != source.order_id OR
  target.product_id != source.product_id OR
  target.quantity != source.quantity OR
  target.price != source.price OR
  target.updated_at != source.updated_at
)
THEN UPDATE SET
  target.effective_end_date = CURRENT_TIMESTAMP(),
  target.is_active = FALSE
WHEN NOT MATCHED THEN INSERT(
  order_item_id ,
    order_id ,
    product_id ,
    quantity ,
    price ,
    updated_at ,
    is_quarantined ,
    effective_start_date ,
    effective_end_date ,
    is_active 
)
VALUES(
  source.order_item_id,
  source.order_id,
  source.product_id,
  source.quantity,
  source.price,
  source.updated_at,
  source.is_quarantined,
  CURRENT_TIMESTAMP(),
  NULL,
  TRUE
);

-------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `project-b4eb31f9-d0fb-422f-8d7.silver_dataset.categories`
(
    category_id INT64,
    name STRING,
    updated_at STRING,
    is_quarantined BOOL
);

TRUNCATE TABLE `project-b4eb31f9-d0fb-422f-8d7.silver_dataset.categories`;

INSERT INTO `project-b4eb31f9-d0fb-422f-8d7.silver_dataset.categories`
SELECT *,
CASE
  WHEN category_id IS NULL OR name IS NULL THEN TRUE
  ELSE FALSE
END AS is_quarantined
FROM `project-b4eb31f9-d0fb-422f-8d7.bronze_dataset.categories`;

CREATE TABLE IF NOT EXISTS `project-b4eb31f9-d0fb-422f-8d7.silver_dataset.products`
(
  product_id INT64,
  name STRING,
  category_id INT64,
  price FLOAT64,
  updated_at STRING,
  is_quarantined BOOL
);

TRUNCATE TABLE `project-b4eb31f9-d0fb-422f-8d7.silver_dataset.products`;

INSERT INTO `project-b4eb31f9-d0fb-422f-8d7.silver_dataset.products`
SELECT 
  *,
  CASE 
    WHEN category_id IS NULL OR name IS NULL THEN TRUE
    ELSE FALSE
  END AS is_quarantined
  
FROM `project-b4eb31f9-d0fb-422f-8d7.bronze_dataset.products`;

-------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `project-b4eb31f9-d0fb-422f-8d7.silver_dataset.product_suppliers`
(
    supplier_id INT64,
    product_id INT64,
    supply_price FLOAT64,
    last_updated STRING,
    is_quarantined BOOL,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOL
);

MERGE INTO `project-b4eb31f9-d0fb-422f-8d7.silver_dataset.product_suppliers` target
USING (
select DISTINCT supplier_id,
product_id,
supply_price,
last_updated,
CASE
  WHEN product_id IS NULL OR supply_price IS NULL THEN TRUE
  ELSE FALSE
END AS is_quarantined
from `project-b4eb31f9-d0fb-422f-8d7.bronze_dataset.product_suppliers`) source
ON source.supplier_id = target.supplier_id AND target.is_active = TRUE
WHEN MATCHED AND(
  target.supply_price != source.supply_price OR
  target.last_updated = source.last_updated
)
THEN UPDATE SET
  target.effective_end_date = NULL,
  target.is_active = FALSE
WHEN NOT MATCHED THEN INSERT(
    supplier_id ,
    product_id ,
    supply_price ,
    last_updated ,
    is_quarantined ,
    effective_start_date ,
    effective_end_date ,
    is_active 
)VALUES(
  source.supplier_id,
  source.product_id,
  source.supply_price,
  source.last_updated,
  source.is_quarantined,
  CURRENT_TIMESTAMP(),
  NULL,
  TRUE
);

--------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `project-b4eb31f9-d0fb-422f-8d7.silver_dataset.suppliers`
(
  supplier_id INT64,
  supplier_name STRING,
  contact_name STRING,
  phone STRING,
  email STRING,
  address STRING,
  city STRING,
  country STRING,
  created_at STRING,
  is_quarantined BOOL
);

TRUNCATE TABLE `project-b4eb31f9-d0fb-422f-8d7.silver_dataset.suppliers`;

--Step 3: Insert New or Updated Records
INSERT INTO `project-b4eb31f9-d0fb-422f-8d7.silver_dataset.suppliers`
SELECT 
  *,
  CASE 
    WHEN supplier_id IS NULL OR supplier_name IS NULL THEN TRUE
    ELSE FALSE
  END AS is_quarantined
  
FROM `project-b4eb31f9-d0fb-422f-8d7.bronze_dataset.suppliers`;

---------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `project-b4eb31f9-d0fb-422f-8d7.silver_dataset.customer_reviews`
(
    id STRING,
    customer_id INT64,
    product_id INT64,
    rating INT64,
    review_text STRING,
    review_date STRING,
    is_quarantined BOOL,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOL
);


MERGE INTO `project-b4eb31f9-d0fb-422f-8d7.silver_dataset.customer_reviews` target
USING 
  (SELECT 
    *, 
    CASE 
      WHEN customer_id IS NULL OR product_id IS NULL OR rating IS NULL THEN TRUE
      ELSE FALSE
    END AS is_quarantined
  FROM `project-b4eb31f9-d0fb-422f-8d7.bronze_dataset.customer_reviews`) source
ON target.id = source.id AND target.is_active = true
WHEN MATCHED AND 
            (
             target.customer_id != source.customer_id OR
             target.product_id != source.product_id OR
             target.rating != source.rating OR
             target.review_text != source.review_text OR
             target.review_date != source.review_date
            ) 
THEN UPDATE SET 
        target.is_active = FALSE,
        target.effective_end_date = current_timestamp()
WHEN NOT MATCHED THEN 
    INSERT (id, customer_id, product_id, rating, review_text, review_date, effective_start_date, effective_end_date, is_active)
    VALUES (source.id, source.customer_id, source.product_id, source.rating, source.review_text, source.review_date, CURRENT_TIMESTAMP(), NULL, TRUE);
-------------------------------------------------------------------------------------------------------------

