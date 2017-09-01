# cfgov_census

**Description**:  This is a [scrapy](https://scrapy.org/) crawler that we're using to better understand what-links-to-what on consumerfinance.gov

This has only been tested with Python 2.7

## Setup

Check this repo out, and (perhaps in a virtual environment) `pip install -r requirements.txt`

## Usage

From the directory you've checked out, you should be able to run:

`scrapy crawl cfgov`
  
This will produce two CSV files: one for every link found on every page, and one that shows the HTTP status (and redirect URL, if appropriate) of every URL fetched.

We plan to add additional code here for processing that data and generating various reports.

One thing you might want to do is load this data into sqlite. That can be done pretty easily:

```
sqlite3 crawl.db
SQLite version 3.16.0 2016-11-04 19:09:39
Enter ".help" for usage hints.
sqlite> .mode csv
sqlite> .import links.csv links
sqlite> .import results.csv results
sqlite> select * from results where status = 404 limit 10;
https://www.consumerfinance.gov/ask-cfpb/i-believe-that-my-rights-as-a-servicemember-have-been-violated-by-my-credit-card-bank-what-should-i-do-en-98/,404,""
https://www.consumerfinance.gov/about-us/newsroom/prepared-remarks-cfpb-director-richard-cordray-people-and-places-conference/consumerfinance.gov,404,""
https://www.consumerfinance.gov/ask-cfpb/if-i-am-in-the-united-states-military-what-should-i-do-if-the-house-or-apartment-im-renting-goes-into-foreclosure-en-1547/,404,""
https://www.consumerfinance.gov/about-us/newsroom/press-resources/feed/,404,""
https://www.consumerfinance.gov/ask-cfpb/i-wrote-a-check-and-someone-forged-the-endorsement-and-cashed-the-check-my-bankcredit-union-wont-return-my-money-to-my-account-am-i-responsible-en-995/,404,""
https://www.consumerfinance.gov/ask-cfpb/should-i-use-a-debt-settlement-service-to-help-me-deal-with-my-debt-and-debt-collectors-en-1459/,404,""
https://www.consumerfinance.gov/ask-cfpb/are-all-debt-settlement-services-legitimate-en-1461/,404,""
https://www.consumerfinance.gov/ask-cfpb/i-am-a-servicemember-who-just-applied-for-an-auto-loan-when-i-reviewed-the-paperwork-i-noticed-that-en-877/,404,""
https://www.consumerfinance.gov/ask-cfpb/what-is-credit-counseling-en-1451/askcfpb/1387/6-what-original-creditor-and-what-difference-between-original-creditor-and-debt-collector.html,404,""
https://www.consumerfinance.gov/ask-cfpb/i-am-a-member-of-the-military-or-spousefamily-member-of-a-servicemember-and-i-think-i-have-a-loan-en-895/,404,""
```

----

## Open source licensing info
1. [TERMS](TERMS.md)
2. [LICENSE](LICENSE)
3. [CFPB Source Code Policy](https://github.com/cfpb/source-code-policy/)


----

## Credits and references

1. Projects that inspired you
2. Related projects
3. Books, papers, talks, or other sources that have meaningful impact or influence on this project
