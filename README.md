# LinearDB
Linear DB for many consecutive additions of records and no many reads

small linear db.

has log file.

used for not big count of elements.

db not have internal cache - read data operation always read file.

full index resident in memory.

used log file for operations. it increase speed:
 * add elements
 * update elements
 * delete first n elements
 * delete element
 
search operations slow down as the number of items increases.

each element mast have id (long) and date (long).

