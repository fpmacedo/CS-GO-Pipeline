import scrapy
import re
import time

class QuotesSpider(scrapy.Spider):
    name = "quotes"
    

    def start_requests(self):
        global player_total
        player_total = []
        url = 'https://www.hltv.org/stats/matches?offset=93600'
        
        yield scrapy.Request(url=url, callback=self.parse, meta=player_total)

    def parse(self, response):
        self.log('Estou aqui {}'.format(response.url))
        links = response.xpath('//td[@class="date-col"]/a/@href').extract()     

        for i in range(len(links)):

            meta =  {
                        'match_link': links[i]
                    }
            
            yield scrapy.Request(url='https://www.hltv.org'+links[i], callback=self.parse_team, meta=meta,)
            time.sleep(1.2)

        next_page = response.xpath('//a[@class="pagination-next"]/@href').extract()

        if next_page:
            self.log('Proxima pagina: {}'.format('https://www.hltv.org' + next_page[0]))
            yield scrapy.Request(url='https://www.hltv.org' + next_page[0], callback=self.parse)

    def parse_team(self, response):
        
        self.log('Estou aqui {}'.format(response.url))
        players = response.xpath('//td[@class="st-player"]/div/a/text()').extract()
        players_links = response.xpath('//td[@class="st-player"]/div/a/@href').extract()

        for i in range(len(players)):

            if players[i] in player_total:
                print("Player already in the list.")
            else:
                player_total.append(players[i])
                meta =  {
                        'player_nick' : players[i],
                        'player_link' : players_links[i]
                        }
                yield scrapy.Request(url='https://www.hltv.org'+players_links[i], callback=self.parse_player_stats, meta=meta,)
                time.sleep(1.1)

    #def parse_player_stats(self, response):


