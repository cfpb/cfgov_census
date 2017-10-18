crawl.db: links.csv results.csv
	sqlite3 crawl.db < load_db.sql
