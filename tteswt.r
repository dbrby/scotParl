require(lubridate)
require(tidyverse)
require(jsonlite)
require(data.table)
require(vroom)
require(stringr)
require(tm)
require(stringi)
require(fuzzyjoin)
require(sqldf)


dates <- seq(as.Date("1999-05-06"), as.Date("2021-02-03"), by = 'day')
dates <- as.data.frame(dates)
dates$dates <- as.POSIXct(dates$dates)
dates$day <- day(dates$dates)
dates$month <- month(dates$dates)
dates$year <- year(dates$dates)

baseURL <- "https://d-contributionserver-wa.azurewebsites.net/?"
day = "day="
month =  "&month="
year = "&year="

dates$urls <- paste0(baseURL, day, dates$day, month, dates$month, 
                     year, dates$year)

dfs <- list()
for(i in 1:length(dates$urls)) {
  dfs[[i]] <- fromJSON(dates$urls[[i]], flatten = TRUE)
  dfs[[i]] <- dfs[[i]]$data
}

sp <- rbindlist(dfs)
sp <- select(sp, -sort)


write.csv(sp, "scot_parl_beta.csv")

sp <- vroom("scot_parl_beta.csv")


sp <- tibble(
  setting = sp$'_source.RecordType',
  committee = sp$'_source.Committee.Name',
  date = as.Date(sp$'_source.Time.Start'),
  item = sp$'_source.ItemOfBusiness.Heading',
  type = sp$'_source.ItemOfBusiness.HeadingType',
  office_held = sp$'_source.Detail.SpeakerOffice',
  speaker = sp$'_source.Detail.SpeakerName',
  display_as = sp$'_source.Detail.SpeakerDisplayName',
  order_no = sp$'_source.Detail.ContributionDisplayOrder',
  speech = sp$'_source.Detail.EditedText'
)

write.csv(sp, "scot_parl_beta2.0.csv")

sp <- vroom("scot_parl_beta2.0.csv")

sp <- sp[!is.na(sp$display_as),]

msps <- fromJSON("https://data.parliament.scot/api/members/json")

msps$LastName <- word(msps$ParliamentaryName, 1, sep = ", ")


msps$speaker <- paste0(msps$PreferredName, " ", msps$LastName)

table(msps$speaker %in% sp$speaker)
table(sp$speaker %in% msps$speaker)

sp$speaker <- trimws(sp$speaker)
msps$speaker <- trimws(msps$speaker)

remove_text <- "1.|2.|3.|4.|5.|6.|7.|8.|9.|\\s*\\([^\\)]+\\)|Sir |Mr |Mrs |Ms |Mr. |Mrs. |Ms. |Dr. |Dr |Rev |Rev.|QC|[0-9]+|[.]|9|MSP|[)]"

sp$speaker  <- gsub(remove_text, "", sp$speaker)

sp$speaker[sp$speaker == "The First Minister" & 
             sp$date %within% interval("2014-11-20", Sys.Date())
] <- "Nicola Sturgeon"
sp$speaker[sp$speaker == "The First Minister" & 
             sp$date %within% interval("2007-05-17", "2014-11-18")
] <- "Alex Salmond"
sp$speaker[sp$speaker == "The First Minister" & 
             sp$date %within% interval("2001-11-27", "2007-05-16")
] <- "Jack McConnell"
sp$speaker[sp$speaker == "The First Minister" & 
             sp$date %within% interval("2000-10-27", "2001-11-08")
] <- "Henry McLeish"
sp$speaker[sp$speaker == "The First Minister" & 
             sp$date %within% interval("1997-05-17", "2000-10-11")
] <- "Donald Dewar"

sp$speaker[sp$speaker == "The Presiding Officer" & 
             sp$date %within% interval("2016-05-12", Sys.Date())
] <- "Ken Macintosh"
sp$speaker[sp$speaker == "The Presiding Officer" & 
             sp$date %within% interval("2011-05-11", "2016-05-11")
] <- "Tricia Marwick"
sp$speaker[sp$speaker == "The Presiding Officer" & 
             sp$date %within% interval("2007-05-14", "2011-05-10")
] <- "Alex Fergusson"
sp$speaker[sp$speaker == "The Presiding Officer" & 
             sp$date %within% interval("2003-05-07", "2007-07-14")
] <- "George Reid"
sp$speaker[sp$speaker == "The Presiding Officer" & 
             sp$date %within% interval("1999-12-05", "2003-05-06")
] <- "David Steel"


sp$speaker[sp$speaker == "The Deputy First Minister" & 
             sp$date %within% interval("1999-12-05", "2005-06-26")
] <- "Jim Wallace"
sp$speaker[sp$speaker == "The Deputy First Minister" & 
             sp$date %within% interval("2005-06-27", "2007-05-16")
] <- "Nicol Stephen"
sp$speaker[sp$speaker == "The Deputy First Minister" & 
             sp$date %within% interval("2007-05-17", "2014-11-20")
] <- "Nicola Sturgeon"
sp$speaker[sp$speaker == "The Deputy First Minister" & 
             sp$date %within% interval("2014-11-21", Sys.Date())
] <- "John Swinney"
sp$speaker <- word(sp$speaker, 1, sep = ":")



write.csv(sp, "scot_parl_beta3.0.csv")

sp <- vroom("scot_parl_beta3.0.csv")

sp_agg <- sp %>% group_by(speaker) %>%
  summarise(n = length(speech))



write.csv(unique(sp$speaker), "speaker_names")
names <- vroom("speaker_names", delim = ",")

msps <- tibble(
  speaker = msps$speaker,
  id = msps$PersonID
)



cons <- fromJSON("https://data.parliament.scot/api/MemberElectionConstituencyStatuses/json")
cons$ValidFromDate <- as.Date(cons$ValidFromDate)
cons$ValidUntilDate <- as.Date(cons$ValidUntilDate)
cons$interval <- interval(cons$ValidFromDate, cons$ValidUntilDate)



sp$speaker <- trimws(sp$speaker)
msps$speaker <- trimws(msps$speaker)

cons <- tibble(
  id = cons$PersonID,
  con_id = cons$ConstituencyID,
  start = cons$ValidFromDate,
  end = cons$ValidUntilDate
)






sp <- full_join(msps, sp, by = "speaker")

write.csv(sp, "scot_parl_beta4.0.csv")

sp <- vroom("scot_parl_beta4.0.csv")

cons$end[is.na(cons$end)] <- Sys.Date()


sp2 <- split(sp, (seq(nrow(sp)-1)) %/% 17362)


for(i in 1:101) {
sp2[[i]] <- fuzzy_left_join(sp2[[i]], cons,  by = c(
  "id" = "id",
  "date" = "start",
  "date" = "end"
),
match_fun = list(`==`, `>=`, `<=`))
}



sp <- rbindlist(sp2)

write.csv(sp, "scot_parl_beta5.0.csv")

sp <- vroom("scot_parl_beta5.0.csv")

sp <- select(sp, -start, -end)

party <- fromJSON("https://data.parliament.scot/api/memberparties/json")

party <- tibble(
  id = party$PersonID,
  party_id = party$PartyID,
  start = as.Date(party$ValidFromDate),
  end = as.Date(party$ValidUntilDate)
)

sp <- sp %>% rename(id = id.x)

sp2 <- split(sp, (seq(nrow(sp)-1)) %/% 17362)

for(i in 1:101) {
  sp2[[i]] <- fuzzy_left_join(sp2[[i]], party,  by = c(
    "id" = "id",
    "date" = "start",
    "date" = "end"
  ),
  match_fun = list(`==`, `>=`, `<=`))
}

sp <- rbindlist(sp2)

write.csv(sp, "scot_parl_beta6.0.csv")



sp <- vroom("scot_parl_beta6.0.csv")

party_names  <- fromJSON("https://data.parliament.scot/api/parties/json")
constituencies <- fromJSON("https://data.parliament.scot/api/constituencies/json")
regions <-  fromJSON("https://data.parliament.scot/api/regions/json")


sp  <- select(sp, -'...2', -start, -end, -id.y...14, -id.y...16)

sp$id <- sp$id.x



party_names <- tibble(
  party_id = party_names$ID,
  party = party_names$ActualName
  
)

sp <- left_join(sp, party_names, by = "party_id")


constituencues <- tibble(
  con_id = constituencies$ID,
  constituency = constituencies$Name
)


sp <- left_join(sp, constituencues, by = "con_id")



reg_elects <- fromJSON("https://data.parliament.scot/api/MemberElectionregionStatuses/json")

reg_elects <- tibble(
  id = reg_elects$PersonID,
  regions = reg_elects$RegionID,
  start = reg_elects$ValidFromDate,
  end = reg_elects$ValidUntilDate
)

sp <- select(sp, -id.x)
sp2 <- split(sp, (seq(nrow(sp)-1)) %/% 17362)



for(i in 1:102) {
  sp2[[i]] <- fuzzy_left_join(sp2[[i]], reg_elects,  by = c(
    "id" = "id",
    "date" = "start",
    "date" = "end"
  ),
  match_fun = list(`==`, `>=`, `<=`))
}



cross_party <- fromJSON("https://data.parliament.scot/api/membercrosspartyroles/json")

committee_memberships <- fromJSON("https://data.parliament.scot/api/personcommitteeroles/json")

save.image("scotParlV1.RData")

