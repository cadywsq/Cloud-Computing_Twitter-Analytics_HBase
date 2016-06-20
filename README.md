# CloudComputing_TwitterAnalytics_HBase

### Overall Objectives:       
	* Build a high performance and reliable web service on the cloud within a specified budget.
	* Design, develop, deploy and optimize functional web-servers that can handle a high load (~ tens of thousands of requests per second).
	* Design and implement Extract Transform and Load (ETL) on a large JSON Twitter dataset of tweets (~ 1 TB) and load into HBase.
	* Design schema as well as configure and optimize HBase databases to deal with scale and improve throughput.
	* Explore methods to identify the potential bottlenecks in a cloud-based web service and methods to improve performance.
              
### Tasks:
	* Query 1: 
		- build the front end system of the web service to accept RESTful requests and send back responses. 
		- Achieve 27000+ QPS for demonstrating encrypted (Phaistos Disc Cipher (PDC)) tweet messages.

	* Query 2: 
		- ETL to clean data: remove illegal data; deal with special characters such as backslash, quote, \n, \t…; replace sensitive words with asterisks; calculate sentiment score of each tweet;
		- Design database schema, deal with emoji, multiple languages when load data.
		- Achieve 10000+QPS for sending out response for requested user ID and hashtag.
		
	* Query3: 
		- Achieve 6000+QPS for user ID + dates range query request for three keywords. Send response of the count of each keyword for tweets within those range.
		- Live test for 5 hours including both MySQL and Hbase as backend.
		
	* Query4:
		- Achieve 10000+QPS for both read and write requests to database, assuring strong consistency.
		- Live test for 3 hours including performance test on Q1-Q4 separately and mixed.
