import scrapy
import re
import time

class QuotesSpider(scrapy.Spider):
    name = "quotes"
    

    def start_requests(self):
      
        global player_total
        player_total = []
        global player_link
        player_link = []
        url = 'https://www.hltv.org/stats/matches?offset=93700'
        
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        self.log('Estou aqui {}'.format(response.url))
        links = response.xpath('//td[@class="date-col"]/a/@href').extract()     

        for i in range(len(links)):

            meta =  {
                        'match_link': links[i]
                    }
            
            yield scrapy.Request(url='https://www.hltv.org'+links[i], callback=self.parse_team, meta=meta,)
            time.sleep(1.1)

        next_page = response.xpath('//a[@class="pagination-next"]/@href').extract()

        if next_page:
            self.log('Proxima pagina: {}'.format('https://www.hltv.org' + next_page[0]))
            yield scrapy.Request(url='https://www.hltv.org' + next_page[0], callback=self.parse)
        else:
            for j in range(len(player_total)): 
              yield scrapy.Request(url='https://www.hltv.org'+player_link[j], callback=self.parse_player_stats, meta={'player_nick': player_total[j], 'player_link': player_link[j] })
              time.sleep(1.1)

    def parse_team(self, response):
        
        self.log('Estou aqui {}'.format(response.url))
        players = response.xpath('//td[@class="st-player"]/div/a/text()').extract()
        players_links = response.xpath('//td[@class="st-player"]/div/a/@href').extract()

        for i in range(len(players)):

            if players[i] in player_total:
                print("Player {} already in the list.".format(players[i]))
            else:
                print("Player {} ADDED in the list.".format(players[i]))
                player_total.append(players[i])
                player_link.append(players_links[i])              

    def parse_player_stats(self, response):

        self.log('Estou aqui {}'.format(response.url))
        player_nick = response.meta.get('player_nick')
        player_link = response.meta.get('player_link')

        if response.xpath('//div[@class="summaryPlayerAge"]/text()').extract():
          player_age = re.findall("[0-9]{1,}", response.xpath('//div[@class="summaryPlayerAge"]/text()').extract()[0])[0],
        else:
          player_age = '-'

        country = response.xpath('//div[@class="summaryRealname text-ellipsis"]/img/@title').extract()
        summary = response.xpath('//div[@class="summaryStatBreakdownDataValue"]/text()').extract()
        stats = response.xpath('//div[@class="stats-row"]/span[2]/text()').extract()

        if len(stats)>10:
          yield {
                  'player_nick' : player_nick,
                  'player_link' : player_link,
                  'player_age' : player_age[0],
                  'player_country' : country[0],
                  'kast' : summary[2],
                  'impact' : summary[3],
                  'total_kills' : stats[0],
                  'hs_percentage' : stats[1],
                  'total_deaths' : stats[2],
                  'kd_ratio' : stats[3],
                  'dmg_round' : stats[4],
                  'gnd_dmg_round' : stats[5],
                  'maps_played' : stats[6],
                  'rounds_played' : stats[7],
                  'kills_round' : stats[8],
                  'assists_round' : stats[9],
                  'deaths_round' : stats[10],
                  'saved_by_round' : stats[11],
                  'save_team_round' : stats[12],
                  'rating_1' : stats[13]
                }
        else:
          yield {
                  'player_nick' : player_nick,
                  'player_link' : player_link,
                  'player_age' : player_age[0],
                  'player_country' : country[0],
                  'kast' : summary[2],
                  'impact' : summary[3],
                  'total_kills' : stats[0],
                  'hs_percentage' : stats[1],
                  'total_deaths' : stats[2],
                  'kd_ratio' : stats[3],
                  'dmg_round' : '-',
                  'gnd_dmg_round' : '-',
                  'maps_played' : stats[4],
                  'rounds_played' : stats[5],
                  'kills_round' : stats[6],
                  'assists_round' : stats[7],
                  'deaths_round' : stats[8],
                  'saved_by_round' : '-',
                  'save_team_round' : '-',
                  'rating_1' : stats[9]
                }


