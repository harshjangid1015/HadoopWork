lines = load 'eBooks/*.txt' as (line:chararray);
words1 = foreach lines generate TOKENIZE(line) as word;
words = foreach lines generate FLATTEN(TOKENIZE(line)) as word;
grouped = group words by word;
wordcount = foreach grouped generate group, COUNT(words);
wordcount = foreach grouped generate group as word_group, COUNT(words) as count;
freq_words = FILTER wordcount by count > 50000;
freq_words_desc = ORDER freq_words BY count DESC;
DUMP freq_words_desc;