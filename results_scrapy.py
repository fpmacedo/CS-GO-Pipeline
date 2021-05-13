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

            #yield
            meta = {
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
            
            yield scrapy.Request(url='https://www.hltv.org'+links[i], callback=self.parse_team, meta=meta,)
            time.sleep(1)
            count_score = count_score + 2


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

        yield{
                'match_id' : response.meta.get('match_id'),
                'match_link' : response.meta.get('match_link'),
                'team_1_link': response.meta.get('team_1_link'),
                'team_1_name': response.meta.get('team_1_name'),
                'team_2_link': response.meta.get('team_2_link'),
                'team_2_name': response.meta.get('team_2_name'),
                'team_1_score' : response.meta.get('team_1_score'),
                'team_2_score' : response.meta.get('team_2_score'),
                'event_name' : response.meta.get('event_name'),
                'map' : response.meta.get('map'),
                'data_unix' : response.meta.get('data_unix'),
                'offset' : response.meta.get('offset'),
                'payers' :[
                                     
                                    {   'team' : teams[0],
                                        'player_nick' : players[0],
                                        'kills' : kills[0],
                                        'hs' : re.findall("[0-9]{1,}", hs[0])[0],
                                        'assists' : re.findall("[0-9]{1,}", assists[0])[0],
                                        'flash_assists' : re.findall("[0-9]{1,}", flash_assists[0])[0],
                                        'deaths' : deaths[0],
                                        'kdratio' : kdratio[0],
                                        'adr' : adr[0],
                                        'fkdiff' : fkdiff[0],
                                        'rating' : rating[0]
                                       },

                                       
                                       {'team' : teams[0],
                                        'player_nick' : players[1],
                                        'kills' : kills[1],
                                        'hs' : re.findall("[0-9]{1,}", hs[1])[0],
                                        'assists' : re.findall("[0-9]{1,}", assists[1])[0],
                                        'flash_assists' : re.findall("[0-9]{1,}", flash_assists[1])[0],
                                        'deaths' : deaths[1],
                                        'kdratio' : kdratio[1],
                                        'adr' : adr[1],
                                        'fkdiff' : fkdiff[1],
                                        'rating' : rating[1]
                                       },
                                       {'team' : teams[0],
                                        'player_nick' : players[2],
                                        'kills' : kills[2],
                                        'hs' : re.findall("[0-9]{1,}", hs[2])[0],
                                        'assists' : re.findall("[0-9]{1,}", assists[2])[0],
                                        'flash_assists' : re.findall("[0-9]{1,}", flash_assists[2])[0],
                                        'deaths' : deaths[2],
                                        'kdratio' : kdratio[2],
                                        'adr' : adr[2],
                                        'fkdiff' : fkdiff[2],
                                        'rating' : rating[2]
                                       },
                                       {'team' : teams[0],
                                        'player_nick' : players[3],
                                        'kills' : kills[3],
                                        'hs' : re.findall("[0-9]{1,}", hs[3])[0],
                                        'assists' : re.findall("[0-9]{1,}", assists[3])[0],
                                        'flash_assists' : re.findall("[0-9]{1,}", flash_assists[3])[0],
                                        'deaths' : deaths[3],
                                        'kdratio' : kdratio[3],
                                        'adr' : adr[3],
                                        'fkdiff' : fkdiff[3],
                                        'rating' : rating[3]
                                       },
                                       {'team' : teams[0],
                                        'player_nick' : players[4],
                                        'kills' : kills[4],
                                        'hs' : re.findall("[0-9]{1,}", hs[4])[0],
                                        'assists' : re.findall("[0-9]{1,}", assists[4])[0],
                                        'flash_assists' : re.findall("[0-9]{1,}", flash_assists[4])[0],
                                        'deaths' : deaths[4],
                                        'kdratio' : kdratio[4],
                                        'adr' : adr[4],
                                        'fkdiff' : fkdiff[4],
                                        'rating' : rating[4]
                                       },                                    
                                       {'team' : teams[1],
                                        'player_nick' : players[5],
                                        'kills' : kills[5],
                                        'hs' : re.findall("[0-9]{1,}", hs[5])[0],
                                        'assists' : re.findall("[0-9]{1,}", assists[5])[0],
                                        'flash_assists' : re.findall("[0-9]{1,}", flash_assists[5])[0],
                                        'deaths' : deaths[5],
                                        'kdratio' : kdratio[5],
                                        'adr' : adr[5],
                                        'fkdiff' : fkdiff[5],
                                        'rating' : rating[5]
                                       }, 

                                       
                                       {'team' : teams[1],
                                        'player_nick' : players[6],
                                        'kills' : kills[6],
                                        'hs' : re.findall("[0-9]{1,}", hs[6])[0],
                                        'assists' : re.findall("[0-9]{1,}", assists[6])[0],
                                        'flash_assists' : re.findall("[0-9]{1,}", flash_assists[6])[0],
                                        'deaths' : deaths[6],
                                        'kdratio' : kdratio[6],
                                        'adr' : adr[6],
                                        'fkdiff' : fkdiff[6],
                                        'rating' : rating[6]
                                       },
                                       {'team' : teams[1],
                                        'player_nick' : players[7],
                                        'kills' : kills[7],
                                        'hs' : re.findall("[0-9]{1,}", hs[7])[0],
                                        'assists' : re.findall("[0-9]{1,}", assists[7])[0],
                                        'flash_assists' : re.findall("[0-9]{1,}", flash_assists[7])[0],
                                        'deaths' : deaths[7],
                                        'kdratio' : kdratio[7],
                                        'adr' : adr[7],
                                        'fkdiff' : fkdiff[7],
                                        'rating' : rating[7]
                                       },
                                       {'team' : teams[1],
                                        'player_nick' : players[8],
                                        'kills' : kills[8],
                                        'hs' : re.findall("[0-9]{1,}", hs[8])[0],
                                        'assists' : re.findall("[0-9]{1,}", assists[8])[0],
                                        'flash_assists' : re.findall("[0-9]{1,}", flash_assists[8])[0],
                                        'deaths' : deaths[8],
                                        'kdratio' : kdratio[8],
                                        'adr' : adr[8],
                                        'fkdiff' : fkdiff[8],
                                        'rating' : rating[8]
                                       },
                                       {'team' : teams[1],
                                        'player_nick' : players[9],
                                        'kills' : kills[9],
                                        'hs' : re.findall("[0-9]{1,}", hs[9])[0],
                                        'assists' : re.findall("[0-9]{1,}", assists[9])[0],
                                        'flash_assists' : re.findall("[0-9]{1,}", flash_assists[9])[0],
                                        'deaths' : deaths[9],
                                        'kdratio' : kdratio[9],
                                        'adr' : adr[9],
                                        'fkdiff' : fkdiff[9],
                                        'rating' : rating[9]
                                       },                                     
                                    
                                ]
            }