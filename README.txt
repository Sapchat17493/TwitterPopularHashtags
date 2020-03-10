Name - Saptarshi Chatterjee
Course - CS535 Big Data, Spring 2020
Professor - Dr. Sangmi Pallickara
Computer Science Department, Colorado State University


For the Grader:
My program takes 4 arguments, in the order provided: 1) epsilon, 2) Threshold, 3) Path of final ResultsLog file and; 
4) An optional argument, which signifies parallellism (Enter "Y" for parallel execution)



OVERVIEW:
In this assignment, we design and implement a real-time streaming data analytics
system using Apache Storm. The goal of the system is to detect the most frequently
occurring hash tags from the live Twitter data stream in real-time.
A hashtag is a type of label or metadata tag used in social networks that makes it easier for
users to find messages with a specific theme or content. Users create and use hashtags by
placing the hash character (or number sign) # in front of a word or un-spaced phrase.
Searching for that hashtag will then present each message that has been tagged with it. For
example, #springbreak and #zidane were popular tags for the US on March 11, 2019.


Finding popular and trendy topics (via hashtags and named entities) in real-time marketing
implies that you include something topical in your social media posts to help increase the
overall reach. In this assignment, we will target data from live Twitter message provided by
Twitter developers.
In this assignment, we;
• Implement the Lossy Counting algorithm.
• List the top 100 most popular hashtags every 10 seconds.
• Parallelize the analysis of your system.
To perform above tasks, we were required to use Apache Storm, and Twitter Stream APIs.
