import scrapy
import re
import time
import json

class QuotesSpider(scrapy.Spider):
    name = "players"

    def start_requests(self):
        
        # Opening JSON file
        f = open('match_results3.json','r')
    
        # returns JSON object as 
        # a dictionary
        data = json.load(f)
        matches = []
        for i in range(len(data)):
            match_link = data[i]
            matches.append(match_link['match_link'])
            
        matches2 = list(set(matches))        
        # Closing file
        f.close()

        # Opening JSON file
        f = open('match_players3.json','r')
                
            # returns JSON object as 
            # a dictionary
        data = json.load(f)
        matches = []
        for i in range(len(data)):
            match_link = data[i]
            matches.append(match_link['match_link'])
            
        players3 = list(set(matches))
        # Closing file
        f.close()

        final_matches=[] 
        count=0
        for i in range(len(matches2)):  
            if matches2[i] not in players3:
                count+=1
                print("Added {} macthes: {}".format(count, matches2[i]))
                final_matches.append(matches2[i])
            
                    
        players2 = list(set(final_matches))        
            # Closing file
        f.close()



        for j in range(len(players2)):

            
            match_id = re.findall("[0-9]{5,}",players2[j])
            meta =  {
                    'match_id' : match_id[0],
                    'match_link': players2[j]
                    }
                
            yield scrapy.Request(url='https://www.hltv.org'+players2[j], callback=self.parse_team, meta=meta,)
      

    def parse_team(self, response):
        
        self.log('Estou aqui {}'.format(response.url))
        teams = response.xpath('//th[contains(@class,"st-teamname")]/text()').extract()
        players_link = response.xpath('//td[@class="st-player"]/div/a/@href').extract()
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

            player_id = (re.findall("[0-9]{1,}",players_link[i]) if re.findall("[0-9]{1,}",players_link[i]) else '-')

            if i <= 4:
                yield   {
                                        'match_id' : int(response.meta.get('match_id')),
                                        'match_link': response.meta.get('match_link'),
                                        'team_name' : teams[0],
                                        'players_link': players_link[i],
                                        'player_id': int(player_id[0]),
                                        'player_nick' : players[i],
                                        'kills' : (kills[i]),
                                        'hs' : (re.findall("[0-9]{1,}", hs[i])[0] if re.findall("[0-9]{1,}", hs[i])[0] else '-'),
                                        'assists' : (re.findall("[0-9]{1,}", assists[i])[0] if re.findall("[0-9]{1,}", assists[i])[0] else '-'),
                                        'flash_assists' : (re.findall("[0-9]{1,}", flash_assists[i])[0] if re.findall("[0-9]{1,}", flash_assists[i])[0] else '-'),
                                        'deaths' : deaths[i],
                                        'kdratio' : kdratio[i],
                                        'adr' : adr[i],
                                        'fkdiff' : fkdiff[i],
                                        'rating' : rating[i]                                                                        
                        }
            else:
                yield   {
                                        'match_id' : int(response.meta.get('match_id')),
                                        'match_link': response.meta.get('match_link'),
                                        'team_name' : teams[1],
                                        'players_link': players_link[i],
                                        'player_id': int(player_id[0]),
                                        'player_nick' : players[i],
                                        'kills' : kills[i],
                                        'hs' : (re.findall("[0-9]{1,}", hs[i])[0] if re.findall("[0-9]{1,}", hs[i])[0] else '-'),
                                        'assists' : (re.findall("[0-9]{1,}", assists[i])[0] if re.findall("[0-9]{1,}", assists[i])[0] else '-'),
                                        'flash_assists' : (re.findall("[0-9]{1,}", flash_assists[i])[0] if re.findall("[0-9]{1,}", flash_assists[i])[0] else '-'),
                                        'deaths' : deaths[i],
                                        'kdratio' : kdratio[i],
                                        'adr' : adr[i],
                                        'fkdiff' : fkdiff[i],
                                        'rating' : rating[i]                                                                        
                        }