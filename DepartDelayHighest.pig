/*Top ten Airports that have highest Departure Delay*/

REGISTER '~/piggybank-0.17.0.jar';
Data1 = load '/data/airlines/' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');
Data2 = foreach Data1 generate (int)$7 as dep_delay, (chararray)$3 as origin;
Data3 = filter Data2 by (dep_delay is not null) AND (origin is not null);
Data4 = group Data3 by origin;
Data5 = foreach Data4 generate group, AVG(Data3.dep_delay);
Data6 = order Data5 by $1 DESC;
Result = limit Data6 10;


Data7 = load '/data/airport-codes/' USING PigStorage(',');
Data8 = foreach Data7 generate (chararray)$2 as origin, (chararray)$1 as city, (chararray)$0 as country;
Data9 = join Data8 by origin, Result by $0;
Data10 = foreach Data9 generate $0,$1,$2,$4;
Final_Result = ORDER Data10 by $3 DESC;
dump Final_Result;
