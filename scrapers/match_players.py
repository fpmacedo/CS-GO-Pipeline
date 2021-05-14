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
        links = response.xpath('//td[@class="date-col"]/a/@href').extract()     

        for i in range(len(links)):
            match_id = re.findall("[0-9]{5,}",links[i])
            meta =  {
                        'match_id' : match_id[0],
                        'match_link': links[i]
                    }
            
            yield scrapy.Request(url='https://www.hltv.org'+links[i], callback=self.parse_team, meta=meta,)
            time.sleep(1)

        next_page = response.xpath('//a[@class="pagination-next"]/@href').extract()

        if next_page:
            self.log('Proxima pagina: {}'.format('https://www.hltv.org' + next_page[0]))
            yield scrapy.Request(url='https://www.hltv.org' + next_page[0], callback=self.parse)

    def parse_team(self, response):
        
        self.log('Estou aqui {}'.format(response.url))
        teams = response.xpath('//th[contains(@class,"st-teamname")]/text()').extract()
        players = response.xpath('//td[@class="st-player"]/div/a/text()').extract()
        kills = response.xpath('//td[@class="st-kills"]/text()').extract()
        hs = response.xpath('//td[@class="st-kills"]/span/text()').extract()
        assists = response.xpath('//td[@class="st-assists"]/text()').extract()
        flash_assists = response.xpath('//td[@class="st-kills"]/span/text()').extract()
        deaths = response.xpath('//td[@class="st-deaths"]/text()').extract()
        kdratio = response.xpath('//td[@class="st-kdratio"]/text()').extract()
        adr = response.xpath('//td[@class="st-adr"]/text()').extract()
        fkdiff = response.xpath('//td[contains(@class,"st-fkdiff")]/text()').extract()
        rating = response.xpath('//td[@class="st-rating"]/text()').extract()

        for i in range(len(players)):

            if i <= 4:
                yield   {
                                        'match_id' : response.meta.get('match_id'),
                                        'match_link': response.meta.get('match_link'),
                                        'team' : teams[0],
                                        'player_nick' : players[i],
                                        'kills' : kills[i],
                                        'hs' : re.findall("[0-9]{1,}", hs[i])[0],
                                        'assists' : re.findall("[0-9]{1,}", assists[i])[0],
                                        'flash_assists' : re.findall("[0-9]{1,}", flash_assists[i])[0],
                                        'deaths' : deaths[i],
                                        'kdratio' : kdratio[i],
                                        'adr' : adr[i],
                                        'fkdiff' : fkdiff[i],
                                        'rating' : rating[i]                                                                        
                        }
            else:
                yield   {
                                        'match_id' : response.meta.get('match_id'),
                                        'match_link': response.meta.get('match_link'),
                                        'team' : teams[1],
                                        'player_nick' : players[i],
                                        'kills' : kills[i],
                                        'hs' : re.findall("[0-9]{1,}", hs[i])[0],
                                        'assists' : re.findall("[0-9]{1,}", assists[i])[0],
                                        'flash_assists' : re.findall("[0-9]{1,}", flash_assists[i])[0],
                                        'deaths' : deaths[i],
                                        'kdratio' : kdratio[i],
                                        'adr' : adr[i],
                                        'fkdiff' : fkdiff[i],
                                        'rating' : rating[i]                                                                        
                        }