# LinearDB
Linear DB is convenient for many consecutive additions of records and no many reads.

Small linear db, has log file and index.

Used for not big count of elements.

Db not have internal cache - read data operation always read file.

Full index resident in memory.

Used log file for operations. It increases speed:
 * add elements
 * update elements
 * delete first n elements
 * delete element
 
Search operations slow down as the number of items increases.

Each element mast have id (long) and date (long).

`