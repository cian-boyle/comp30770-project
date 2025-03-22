-- most favourited tweets
SELECT screen_name, text, favourites_count FROM tweets ORDER BY favourites_count DESC LIMIT 3;

-- most retweeted tweets
SELECT screen_name, text, retweet_count FROM tweets ORDER BY retweet_count DESC LIMIT 3;

-- most common tweet languages
SELECT lang, COUNT(*) FROM tweets GROUP BY lang ORDER BY COUNT(*) DESC LIMIT 5;

-- most active account
SELECT screen_name, COUNT(*) FROM tweets GROUP BY screen_name ORDER BY COUNT(*) DESC LIMIT 5;

-- average tweet Engagement
SELECT AVG(favourites_count + retweet_count) AS avg_tweet_engagement FROM tweets;


-- difference of tweet performance from verified and unverified
SELECT verified, AVG(favourites_count + retweet_count) AS avg_engagement FROM tweets  GROUP BY verified;


-- account with the most engagement
SELECT screen_name, SUM(favourites_count + retweet_count) AS total_engagement
FROM tweets 
GROUP BY screen_name 
ORDER BY total_engagement DESC LIMIT 5;

-- most common hashtags
WITH RECURSIVE hashtag_extractor AS (
  SELECT text, LOCATE('#', text) AS hash_pos
  FROM tweets
  WHERE text LIKE '%#%'

  UNION ALL

  SELECT text, LOCATE('#', text, hash_pos + 1) AS hash_pos
  FROM hashtag_extractor
  WHERE LOCATE('#', text, hash_pos + 1) > 0
)
SELECT hashtag, COUNT(*) AS frequency
FROM (SELECT SUBSTRING_INDEX(SUBSTRING(text, hash_pos), ' ', 1) AS hashtag FROM hashtag_extractor) AS extracted
WHERE hashtag NOT LIKE '%covid%' AND hashtag NOT LIKE '%corona%' AND hashtag NOT LIKE '%virus%' AND hashtag != '#'
GROUP BY hashtag
ORDER BY frequency DESC
LIMIT 10;

-- sentiment analysis
SELECT 
    SUM(CASE 
            WHEN text LIKE '%getting better%' 
              OR text LIKE '%recovered%' 
              OR text LIKE '%recovering%' 
              OR text LIKE '%improving%' 
              OR text LIKE '%better%' 
              OR text LIKE '%stable%' 
              OR text LIKE '%optimistic%' 
              OR text LIKE '%hopeful%' 
              OR text LIKE '%hope%' 
              OR text LIKE '%hoping%' 
              OR text LIKE '%inspiring%' 
              OR text LIKE '%encouraging%' 
              OR text LIKE '%bright%' 
              OR text LIKE '%optimism%' 
              OR text LIKE '%promising%' 
              OR text LIKE '%upbeat%' 
            THEN 1 
            ELSE 0 
        END) AS positive_tweets,
    
    SUM(CASE 
            WHEN text LIKE '%getting worse%' 
              OR text LIKE '%worse%' 
              OR text LIKE '%death%' 
              OR text LIKE '%deaths%' 
              OR text LIKE '%died%' 
              OR text LIKE '%fatal%' 
              OR text LIKE '%hospitalized%' 
              OR text LIKE '%infection%' 
              OR text LIKE '%outbreak%' 
              OR text LIKE '%lockdown%' 
              OR text LIKE '%quarantine%' 
              OR text LIKE '%suffering%' 
              OR text LIKE '%loss%' 
              OR text LIKE '%grief%' 
            THEN 1 
            ELSE 0 
        END) AS negative_tweets
FROM tweets;


-- most common words (doesn't run)
WITH RECURSIVE word_extractor AS (
  SELECT text, SUBSTRING_INDEX(text, ' ', 1) AS word, SUBSTRING(text, LENGTH(SUBSTRING_INDEX(text, ' ', 1)) + 2) AS remaining_text
  FROM tweets

  UNION ALL

  SELECT remaining_text,SUBSTRING_INDEX(remaining_text, ' ', 1) AS word, SUBSTRING(remaining_text, LENGTH(SUBSTRING_INDEX(remaining_text, ' ', 1)) + 2) AS remaining_text
  FROM word_extractor
  WHERE remaining_text <> ''
)
SELECT word, COUNT(*) AS frequency
FROM word_extractor
WHERE word <> ''
GROUP BY word
ORDER BY frequency DESC
LIMIT 10;
