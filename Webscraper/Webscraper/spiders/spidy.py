import csv
import scrapy
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher


class SpidySpider(scrapy.Spider):
    name = "spidy"

    def start_requests(self):
        URL = 'https://books.toscrape.com/'
        yield scrapy.Request(url=URL, callback=self.response_parser)

    def response_parser(self, response):
        for selector in response.css('article.product_pod'):
            title = selector.css('h3 > a::attr(title)').extract_first()
            price = selector.css('.price_color::text').extract_first()
            stars = self.convert_star_rating(selector)

            partial_url = selector.css('h3 > a::attr(href)').extract_first()
            url = response.urljoin(partial_url)

            yield response.follow(url, callback=self.parse_details, meta={
                'title': title,
                'price': price,
                'stars': stars
            })


        
        next_page_link = response.css('li.next a::attr(href)').extract_first()
        if next_page_link:
            yield response.follow(next_page_link, callback=self.response_parser)


    def parse_details(self, response):
        upc = response.css('table.table-striped td::text').extract_first()
        yield {
            'title': response.meta['title'],
            'price': response.meta['price'],
            'stars': response.meta['stars'],
            'UPC': upc
        }





    def convert_star_rating(self, selector):
        ratings = {
            "One": 1,
            "Two": 2,
            "Three": 3,
            "Four": 4,
            "Five": 5
        }

        res = selector.css('p.star-rating::attr(class)').extract_first()

        for key, val in ratings.items():
            if key in res:
                return val
        return "Missing"

def book_spider_result():
    books_results = []

    def crawler_results(item):
        books_results.append(item)

    dispatcher.connect(crawler_results, signal=signals.item_scraped)
    crawler_process = CrawlerProcess()
    crawler_process.crawl(SpidySpider)
    crawler_process.start()
    return books_results


if __name__ == '__main__':
    books_data=book_spider_result()

    keys = books_data[0].keys()
    with open('books_data.csv', 'w', newline='') as output_file_name:
        writer = csv.DictWriter(output_file_name, keys)
        writer.writeheader()
        writer.writerows(books_data)