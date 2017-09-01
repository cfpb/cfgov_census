from unicodecsv import DictWriter
from cfgov_census.spiders.cfgov_spider import Link, Result


class SplitCSVPipeline(object):
    def open_spider(self, spider):
        self.links_file = open('links.csv', 'wb')
        self.results_file = open('results.csv', 'wb')

        self.links_writer = DictWriter(self.links_file,
                                       ['source', 'destination'])
        self.results_writer = DictWriter(self.results_file,
                                         ['url', 'status', 'next'])

        self.links_writer.writeheader()
        self.results_writer.writeheader()

    def close_spider(self, spider):
        self.results_file.close()
        self.links_file.close()

    def process_item(self, item, spider):
        if isinstance(item, Link):
            self.links_writer.writerow(item)
        if isinstance(item, Result):
            self.results_writer.writerow(item)

        return item
