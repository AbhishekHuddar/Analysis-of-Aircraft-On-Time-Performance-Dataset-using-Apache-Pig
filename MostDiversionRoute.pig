/*Which route has seen the most diversion?*/

REGISTER '~/piggybank-0.17.0.jar';

Data1 = load '/data/airlines/' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');
Data2 = FOREACH Data1 GENERATE (chararray)$3 as origin, (chararray)$4 as dest, (int)$17 as diversion;
Data3 = FILTER Data2 BY (origin is not null) AND (dest is not null) AND (diversion == 1);
Data4 = GROUP Data3 by (origin,dest);
Data5 = FOREACH Data4 generate group, COUNT(Data3.diversion);
Data6 = ORDER Data5 BY $1 DESC;
Result = limit Data6 10;
dump Result;