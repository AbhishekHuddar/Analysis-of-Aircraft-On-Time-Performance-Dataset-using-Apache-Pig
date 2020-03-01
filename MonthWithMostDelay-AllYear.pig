/*Month with most Cancellations - all years*/

REGISTER '~/piggybank-0.17.0.jar';
Data1 = load '/data/airlines_no_header/' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX');
Data2 = foreach Data1 generate GetYear(ToDate((chararray)$0, 'yyyy-MM-dd')) as year, GetMonth(ToDate((chararray)$0, 'yyyy-MM-dd')) as month, (int)$2 as flight_num, (int)$15 as cancelled, (chararray)$16 as cancel_code;

Data3 = filter Data2 by cancelled == 1 AND cancel_code =='B';
Data4 = group Data3 by (year, month);
Data5 = foreach Data4 generate group, COUNT(Data3.cancelled);
Data6= order Data5 by $1 DESC;
Result = limit Data6 1;
dump Result;