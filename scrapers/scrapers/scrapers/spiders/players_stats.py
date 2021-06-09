import scrapy
import re
import time
import json

class QuotesSpider(scrapy.Spider):
    name = "stats"
    

    def start_requests(self):

      # Opening JSON file
      f = open('match_players3.json','r')
    
      # returns JSON object as 
      # a dictionary
      data = json.load(f)
      players = []
      for i in range(len(data)):
          player_link = data[i]
          players.append("https://www.hltv.org"+player_link['players_link'])
          
      players_2 = list(set(players))        
      # Closing file
      f.close()

      # Opening JSON file
      g = open('players_stats3.json','r')
                
            # returns JSON object as 
            # a dictionary
      data = json.load(g)
      matches = []
      for i in range(len(data)):
          match_link = data[i]
          matches.append(match_link['player_link'])
            
      players3 = list(set(matches))
        # Closing file
      f.close()

      final_matches=[] 
      count=0
      for i in range(len(players_2)):  
          if players_2[i] not in players3:
              count+=1
              print("Added {} macthes: {}".format(count, players_2[i]))
              final_matches.append(players_2[i])
            
                    
      players2 = list(set(final_matches))        
            # Closing file
      f.close()



      for j in range(len(players2)):
        yield scrapy.Request(url=players2[j], callback=self.parse_player_stats)
                   

    def parse_player_stats(self, response):

        self.log('Estou aqui {}'.format(response.url))
        player_nick = response.xpath('//h1[@class="summaryNickname text-ellipsis"]/text()').extract()[0]
        player_link = response.url
        player_id = re.findall("[0-9]{1,}",response.url)

        if response.xpath('//div[@class="summaryPlayerAge"]/text()').extract():
          player_age = re.findall("[0-9]{1,}", response.xpath('//div[@class="summaryPlayerAge"]/text()').extract()[0])[0],
        else:
          player_age = '-'

        country = response.xpath('//div[@class="summaryRealname text-ellipsis"]/img/@title').extract()
        summary = response.xpath('//div[@class="summaryStatBreakdownDataValue"]/text()').extract()
        stats = response.xpath('//div[@class="stats-row"]/span[2]/text()').extract()

        if len(stats)>10:
          yield {
                  'player_id' : int(player_id[0]),
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
                  'player_id' : int(player_id[0]),
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


