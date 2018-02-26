lines = load 'eBooks/*.txt' as (line:chararray);
words1 = foreach lines generate TOKENIZE(line) as word;
words = foreach lines generate FLATTEN(TOKENIZE(line)) as word;
grouped_by_len = group words by SIZE(word);
count_gr_by_len = foreach grouped_by_len generate group as num_char, COUNT(words) as count;
DUMP count_gr_by_len;


