# go-sipantau
Crawler for 2024 Election data (Please use it wisely)

# Setup
Put your mongoDB URL on the `.env` file
```
MONGO_DB_URL="YOUR_MONGO_DB_URL_HERE"
```

Adjust your Concurrency capability on 
```
NewLimitedWaitGroup(1)
```
