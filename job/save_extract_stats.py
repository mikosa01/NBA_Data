from extraction import stats_extract


header = {
	"X-RapidAPI-Key": "056503acdcmshde58299aae5c4afp185e60jsn94893d801730",
	"X-RapidAPI-Host": "free-nba.p.rapidapi.com" 
    }
input_url="https://free-nba.p.rapidapi.com/stats"
filename = 'stats'



stats_extract.run(filename, header, input_url)   