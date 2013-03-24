-- Dumps all the keys into the tapioca log file -- be careful!
CHECK TABLE customer       QUICK;
CHECK TABLE district       QUICK;
CHECK TABLE history        QUICK;
CHECK TABLE item           QUICK;
CHECK TABLE new_orders     QUICK;
CHECK TABLE order_line     QUICK;
CHECK TABLE orders         QUICK;
CHECK TABLE stock          QUICK;
CHECK TABLE warehouse      QUICK;

-- Does a bptree_next() based ordering validation of the table indexes 
CHECK TABLE warehouse, district MEDIUM;
CHECK TABLE new_orders MEDIUM;
CHECK TABLE item MEDIUM;
CHECK TABLE customer MEDIUM;
CHECK TABLE stock MEDIUM;
CHECK TABLE history MEDIUM;
CHECK TABLE orders MEDIUM;
CHECK TABLE order_line MEDIUM;


-- Load all non-leaf B+Tree nodes into the cache of the current tapioca node
CHECK TABLE warehouse, district FAST;
CHECK TABLE new_orders FAST;
CHECK TABLE item FAST;
CHECK TABLE customer FAST;
CHECK TABLE stock FAST;
CHECK TABLE history FAST;
CHECK TABLE orders FAST;
CHECK TABLE order_line FAST;

-- Does a more thorough recursive traversal of the tree, verifying 
-- that key values are not out of order
CHECK TABLE warehouse, district EXTENDED;
CHECK TABLE item EXTENDED;
CHECK TABLE stock EXTENDED;
CHECK TABLE customer EXTENDED; 

-- Tables which are INSERTed to as part of TPC-C
CHECK TABLE history EXTENDED; 
CHECK TABLE order_line EXTENDED; 
CHECK TABLE orders EXTENDED; 
CHECK TABLE new_orders EXTENDED; 


SELECT COUNT(*) FROM  customer       ;
SELECT COUNT(*) FROM  district       ;
SELECT COUNT(*) FROM  history        ;
SELECT COUNT(*) FROM  item           ;
SELECT COUNT(*) FROM  new_orders     ;
SELECT COUNT(*) FROM  order_line     ;
SELECT COUNT(*) FROM  orders         ;
SELECT COUNT(*) FROM  stock          ;
SELECT COUNT(*) FROM  warehouse      ;
