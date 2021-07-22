-- use CSV module
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage; 

-- Load orders.csv from the user directory 
orderList = LOAD '/user/maria_dev/diplomacy/orders.csv' USING CSVExcelStorage() AS
(game_id:int,
unit_id:int,
unit_order:chararray,
location:chararray,
target:chararray,
target_dest:chararray,
success:int,
reason:int,
turn_num:int);

-- Filter all the entries that contain Holland 
filteredResults = FILTER orderList BY target == 'Holland';

-- Group the entries by the location and target being Holland 
groupedAndFilteredResults = GROUP filteredResults BY (location, target);

-- Count the grouped rows 
countedResults = FOREACH groupedAndFilteredResults GENERATE group, COUNT(filteredResults);

-- Order the final locations list to ascending 
orderedResults = ORDER countedResults BY $0 ASC;

DUMP orderedResults;