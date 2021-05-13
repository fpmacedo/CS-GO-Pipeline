import scrapy
import re

class QuotesSpider(scrapy.Spider):
    name = "quotes"

    def start_requests(self):
        url = 'https://www.hltv.org/results?offset=59600'

        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        self.log('Estou aqui {}'.format(response.url))
        links = response.xpath('//a[@class="a-reset"]/@href').extract()        
        team1 = response.xpath('//div[@class="line-align team1"]/div/text()').extract()
        team2 = response.xpath('//div[@class="line-align team2"]/div/text()').extract()
        scores = response.xpath('//td[@class="result-score"]/span/text()').extract()
        event_name = response.xpath('//span[@class="event-name"]/text()').extract()
        map_text = response.xpath('//div[contains(@class, "map-text")]/text()').extract()

        count_score = 0

        for i in range(len(team1)):

            match_id = re.findall("[0-9]{5,}",links[i])
            offset = re.findall("[0-9]{1,}",response.url)
            yield{
                'match_id' : match_id[0],
                'link': links[i],
                'team_1': team1[i],
                'team_2': team2[i],
                'score_team_1' : scores[count_score],
                'score_team_2' : scores[count_score+1],
                'event_name' : event_name[i],
                'map' : map_text[i],
                'offset' : offset[0]
            }
            
            count_score = count_score + 2


        next_page = response.xpath('//a[@class="pagination-next"]/@href').extract()
        if next_page:
            self.log('Proxima pagina: {}'.format('https://www.hltv.org' + next_page[0]))
            yield scrapy.Request(url='https://www.hltv.org' + next_page[0], callback=self.parse)