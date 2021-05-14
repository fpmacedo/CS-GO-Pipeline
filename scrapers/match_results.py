import scrapy
import re
import time

class QuotesSpider(scrapy.Spider):
    name = "quotes"

    def start_requests(self):
        url = 'https://www.hltv.org/stats/matches?offset=93500'

        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        self.log('Estou aqui {}'.format(response.url))
        data_unix = response.xpath('//div[@class="time"]/@data-unix').extract()
        links = response.xpath('//td[@class="date-col"]/a/@href').extract()      
        team_link = response.xpath('//td[@class="team-col"]/a/@href').extract()
        team_name = response.xpath('//td[@class="team-col"]/a/text()').extract()
        scores = response.xpath('//span[@class="score"]/text()').extract()
        event_name = response.xpath('//td[@class="event-col"]/a/text()').extract()
        map_match = response.xpath('//div[contains(@class, "dynamic-map-name-full")]/text()').extract()

        count_score = 0

        for i in range(len(links)):

            match_id = re.findall("[0-9]{5,}",links[i])
            offset = re.findall("[0-9]{1,}",response.url)

            yield{
                'match_id' : match_id[0],
                'match_link': links[i],
                'team_1_link': team_link[count_score],
                'team_1_name': team_name[count_score],
                'team_2_link': team_link[count_score+1],
                'team_2_name': team_name[count_score+1],
                'team_1_score' : re.findall("[0-9]{1,}", scores[count_score])[0],
                'team_2_score' : re.findall("[0-9]{1,}", scores[count_score+1])[0],
                'event_name' : event_name[i],
                'map' : map_match[i],
                'data_unix' : data_unix[i],
                'offset' : offset[0]
            }
            
            count_score = count_score + 2


        next_page = response.xpath('//a[@class="pagination-next"]/@href').extract()
        if next_page:
            self.log('Proxima pagina: {}'.format('https://www.hltv.org' + next_page[0]))
            yield scrapy.Request(url='https://www.hltv.org' + next_page[0], callback=self.parse)
