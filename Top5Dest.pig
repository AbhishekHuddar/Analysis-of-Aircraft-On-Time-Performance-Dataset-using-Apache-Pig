*/ Most visited Destination /*

REGISTER '~/piggybank-0.17.0.jar';
Data1 = load '/data/airlines/' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');
Data2 = foreach Data1 generate (chararray)$0 as year, (int)$2 as flight_num, (chararray)$3 as origin,(chararray) $4 as dest;
Data3 = filter Data2 by dest is not null;
Data4 = group Data3 by dest;
Data5 = foreach Data4 generate group, COUNT(Data3.dest);
Data6 = order Data5 by $1 DESC;
Result = LIMIT Data6 5;

Data7 = load '/data/airport-codes/' USING PigStorage(',');
Data8 = foreach Data7 generate (chararray)$2 as dest, (chararray)$1 as city, (chararray)$0 as country;
Result2 = join Result by $0, Data8 by dest;
dump Result2;
