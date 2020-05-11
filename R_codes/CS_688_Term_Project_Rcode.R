#############################################################################################################################################
#############################################################################################################################################
#####################################################FINAL PROJECT###########################################################################
###################################################SPORTS ANALYTICS##########################################################################
#############################################################################################################################################
#############################################################################################################################################


### set working directory to save any file
setwd("C:/Users/diego/OneDrive/Escritorio/BU/Web analytics/Project")

library(SportsAnalytics)
library(XML)
library(xml2)

library(rvest)

######READING THE DATA----
###2019 MLS

#### teams 
MLS_teams<- "https://www.mlssoccer.com/stats/team?year=2019&season_type=REG&op=Search&form_build_id=form-IxmEFKimRsfq1l6cmDgSzfbzR2Vk6eZdsr-K6a-TZuI&form_id=mp7_stats_hub_build_filter_form"
tbls_ls <- read_html(MLS_teams) %>%
  html_nodes("table") %>%
  html_table(fill = TRUE)
teams<- as.data.frame(tbls_ls[[1]])

### standings 


standings<-"https://www.mlssoccer.com/standings/mls/2019/"
tbls_ls <- read_html(standings) %>%
  html_nodes("table") %>%
  html_table(fill = TRUE)



tbls_ls[[1]]<-tbls_ls[[1]][c(2:13),c(2:4,6:12,14,16)]
tbls_ls[[2]]<-tbls_ls[[2]][c(2:13),c(2:4,6:12,14,16)]

colnames(tbls_ls[[1]])<- c("Club","Points","Points_per_game", "Games_Played","Won","Lost","Tie", "Goals_Favor","Goals_Agains","Goal_Difference","Home_W-L-T","Away_W-L-T")
colnames(tbls_ls[[2]])<- c("Club","Points","Points_per_game", "Games_Played","Won","Lost","Tie", "Goals_Favor","Goals_Agains","Goal_Difference","Home_W-L-T","Away_W-L-T")


east_conf_standings <- as.data.frame(tbls_ls[[1]])
west_conf_standings <- as.data.frame(tbls_ls[[2]])
#### table with multiple pages, 26 last one, we can loop over and get tables, then unite them and got one collective table for players statistics
#### first page of the table is not indexed so we add it now and the rest latter
MLS_players<- "https://www.mlssoccer.com/stats/season?franchise=select&year=2019&season_type=REG&group=goals&op=Search&form_build_id=form-ABEFvyRHwcNS9jDvI_EW-x-4FSWKje28dkzA0HakXj0&form_id=mp7_stats_hub_build_filter_form"
tbls_ls <- read_html(MLS_players) %>%
  html_nodes("table") %>%
  html_table(fill = TRUE)
players<- tbls_ls[[1]]


players_list<-list()
for (i in c(1:25)) {
  link= paste("https://www.mlssoccer.com/stats/season?page=",i,"&franchise=select&year=2019&season_type=REG&group=goals&op=Search&form_build_id=form-ABEFvyRHwcNS9jDvI_EW-x-4FSWKje28dkzA0HakXj0&form_id=mp7_stats_hub_build_filter_form",sep="")
  tbls_ls <- read_html(link) %>%
    html_nodes("table") %>%
    html_table(fill = TRUE)
  players_list<-append(players_list, tbls_ls)
  print(i)
}



for (i in c(1:25)){
  players<- rbind(players,players_list[[i]])
}
players <- data.frame(players)

### lets get rid of useless things

remove(players_list)
remove(tbls_ls)

############ lets check the data----

## VARIABLES NAMES FROM THE WEBSITE, APPLY FOR TEAMS AND PLAYERS
#GP: Games Played, GS: Games Started, G: Goals, MIN: Minutes Played, A: Assists, SHT: Shots, SOG: Shots on Goal, FC: Fouls Committed, 
#FS: Fouls Suffered, Y: Yellow Cards, R: Red Cards, GF: Goals For, GA: Goals Against, SO: Shutouts, SV: Saves, CK: Corner Kicks, 
#PKA: Penalty Kick Attempts, PKG: Penalty Kick Goals, PKS: Penalty Kick Saves, OFF: Offsides


#### players----

names(players)
table(duplicated(players$Player)) #### 25 players on the data set with the same name
table(duplicated(players$Player,players$Club)) #### 25 players on the data set with the same name and club
table(duplicated(players)) #### 25 players duplicated, might be because of the way we import the data, 

players<- subset(players, !duplicated(players)) ### take out the duplicates
table(duplicated(players$Player)) ## 603 unique players
table(players$Club)
table(players$POS)

## function to detect NA
check_na<-function(data_input){
  result= c()
  for (i in (1:ncol(data_input))) {
    if (length(table(is.na(data_input[i])))>1){
      result[i]=names(data_input[i])
    } 
  }
  result[!is.na(result)]
}
check_na(players) ### some players have no data

table(is.na(players$GP))
table(is.na(players$GWG))
table(is.na(players$G.90min))

## three players have no data

which(is.na(players$GP))
players[c(626:628),]
### incomplete information for those players

players<- players[c(1:625),]
check_na(players) ### no more columns with incomplete information

names(players)[4:16]<-c("Games_Played","Games_Started","Minutes_Played","Goals","Assists","Shots","Shots_on_Goal","Game_Winning_Goals",
                        "Penalty_Kick_Goals_Assisted","Home_Goals","Road_Goals","G.90min","Score_Percentage")
#GP: Games Played, GS: Games Started, G: Goals, MIN: Minutes Played, A: Assists, SHT: Shots, SOG: Shots on Goal, FC: Fouls Committed, 
#FS: Fouls Suffered, Y: Yellow Cards, R: Red Cards, GF: Goals For, GA: Goals Against, SO: Shutouts, SV: Saves, CK: Corner Kicks, 
#PKA: Penalty Kick Attempts, PKG: Penalty Kick Goals, PKS: Penalty Kick Saves, OFF: Offsides

##### data ready for analysis

####b check


###c ----

## A GALAXY
library(dplyr)
library(googleVis)
LA_Galaxy<- subset(players, players$Club=="LA")

##top scorer
LA_Galaxy$Player[which(LA_Galaxy$Goals==max(LA_Galaxy$Goals))]
##top assist
LA_Galaxy$Player[which(LA_Galaxy$Assists==max(LA_Galaxy$Assists))]
## top winnin scorer
LA_Galaxy$Player[which(LA_Galaxy$Game_Winning_Goals==max(LA_Galaxy$Game_Winning_Goals))]
## most minutes played
LA_Galaxy$Player[which(LA_Galaxy$Minutes_Played==max(LA_Galaxy$Minutes_Played))]


top_scorers=top_n(LA_Galaxy,10,LA_Galaxy$Goals)[,c(1,7)]
top_scorers=top_scorers[order(-top_scorers$Goals),]
top_assisters=top_n(LA_Galaxy,10,LA_Galaxy$Assists)[,c(1,8)]
top_assisters= top_assisters[order(-top_assisters$Assists),]
top_min_played=top_n(LA_Galaxy,10,LA_Galaxy$Minutes_Played)[,c(1,6)]
top_min_played= top_min_played[order(-top_min_played$Minutes_Played),]



plot(gvisTable(top_scorers))
plot(gvisTable(top_assisters))
plot(gvisTable(top_min_played))

###d ----


top_eastern=top_n(east_conf_standings,5, east_conf_standings$Points)[,c(1,2)]
top_western=top_n(west_conf_standings,5, west_conf_standings$Points )[,c(1,2)]

plot(gvisTable(top_eastern))
plot(gvisTable(top_western))

east_conf_standings$Conference="Eastern"
west_conf_standings$Conference="Western"
over_all<- rbind(east_conf_standings,west_conf_standings)

over_all$Points= as.numeric(over_all$Points)
over_all$Won<- as.numeric(over_all$Won)
over_all$Lost<- as.numeric(over_all$Lost)
over_all$Tie<- as.numeric(over_all$Tie)
over_all$Goals_Favor<- as.numeric(over_all$Goals_Favor)
over_all$Goals_Agains<- as.numeric(over_all$Goals_Agains)
over_all$Goal_Difference<- as.numeric(over_all$Goal_Difference)

top_team_MLS= top_n(over_all,5, over_all$Points)[,c(1,2)]
top_team_MLS= top_team_MLS[order(-top_team_MLS$Points),]

plot(gvisTable(top_team_MLS))


###e----

names(LA_Galaxy)
library(googleVis)

### chart1
chart1 <-
     gvisColumnChart(
         LA_Galaxy,
         xvar = "Player",
         yvar = c("Shots", "Shots_on_Goal", "Goals"),
         options=list(
             legend="top",
             height=500, width=850))
plot(chart1)

#### chart 1 LA strikers

LA_Galaxy_forward<- subset(LA_Galaxy, LA_Galaxy$POS=='F')

chart1.1 <-
  gvisBarChart(
    LA_Galaxy_forward,
    xvar = "Player",
    yvar = c("Shots", "Shots_on_Goal", "Goals"),
    options=list(
      legend="top",
      height=500, width=850))
plot(chart1.1)

#### chart 1 MLS strikers


mls_forward<- subset(players, players$POS=='F')

top_mls_forward= top_n(mls_forward,10, mls_forward$Goals)

chart1.2 <-
  gvisBarChart(
    top_mls_forward,
    xvar = "Player",
    yvar = c("Shots", "Shots_on_Goal", "Goals"),
    options=list(
      legend="top",
      height=500, width=850))
plot(chart1.2)

### effectiveness

chart1.3 <-
  gvisColumnChart(
    top_mls_forward,
    xvar = "Player",
    yvar = c( "Goals","Score_Percentage"),
    options=list(
      legend="top",
      height=500, width=850))
plot(chart1.3)


###chart2

chart2 <-
  gvisColumnChart(
    over_all,
    xvar = "Club",
    yvar = c("Won", "Lost", "Tie"),
    options=list(
      legend="top",
      height=500, width=850))
plot(chart2)

###chart3
labels=c(names(over_all)[5:7])
over_all[over_all$Club=="LALA Galaxy",][5:7]
values= c(16,15,3)
data <- data.frame(
  category=labels,
  count=values
)

chart3<- gvisPieChart(data, labelvar = "category", 
                      numvar = "count",
                      options=list(title='Match Results for LA Galaxy',
                                   legend='none',
                                   pieSliceText='label',
                                   pieHole=0.5,height=500, width=850))
plot(chart3)

## chart 4
names(over_all)

conf_summary<- over_all %>% group_by(Conference)


conf_comparision_goals=conf_summary %>% summarise(
  Goal_Favor = sum(Goals_Favor),
  Goal_Agains = sum(Goals_Agains),
  Goal_dif= sum(Goal_Difference)
)

###chart4

chart4 <-
  gvisColumnChart(
    conf_comparision_goals,
    xvar = "Conference",
    yvar = c("Goal_Favor", "Goal_Agains"),
    options=list(
      legend="top",
      height=500, width=850))
plot(chart4)


### chart5

east_conf_standings$Goals_Favor<- as.numeric(east_conf_standings$Goals_Favor)
east_conf_standings$Goals_Agains<- as.numeric(east_conf_standings$Goals_Agains)
east_conf_standings$Goal_Difference<- as.numeric(east_conf_standings$Goal_Difference)

chart5 <-
  gvisColumnChart(
    east_conf_standings,
    xvar = "Club",
    yvar = c("Goals_Favor", "Goals_Agains","Goal_Difference"),
    options=list(
      legend="top",
      height=500, width=850))
plot(chart5)

west_conf_standings$Goals_Favor<- as.numeric(west_conf_standings$Goals_Favor)
west_conf_standings$Goals_Agains<- as.numeric(west_conf_standings$Goals_Agains)
west_conf_standings$Goal_Difference<- as.numeric(west_conf_standings$Goal_Difference)


chart6 <-
  gvisColumnChart(
    west_conf_standings,
    xvar = "Club",
    yvar = c("Goals_Favor", "Goals_Agains","Goal_Difference"),
    options=list(
      legend="top",
      height=500, width=850))
plot(chart6)


###chart7


conf_comparision_goals_withoutC=conf_summary[c(1:11,13:24),] %>% summarise(
  Goal_Favor = sum(Goals_Favor),
  Goal_Agains = sum(Goals_Agains),
  Goal_dif= sum(Goal_Difference)
)

chart7 <-
  gvisBarChart(
    conf_comparision_goals_withoutC,
    xvar = "Conference",
    yvar = c("Goal_Favor", "Goal_Agains"),
    options=list(
      legend="top",
      height=500, width=850))
plot(chart7)

###f----
library(tidyverse)

MLS_teams_cities<- "https://en.wikipedia.org/wiki/Major_League_Soccer"
tbls_ls <- read_html(MLS_teams_cities) %>%
  html_nodes("table") %>%
  html_table(fill = TRUE)
team_cities<- as.data.frame(tbls_ls[[2]])
names(team_cities)
team_cities<- team_cities[,c(1,2)]

team_cities<-separate(team_cities, Location, c("City","State"), sep = ",")
team_cities<-team_cities[c(2:14,16:28),]
team_cities$State<- trimws(team_cities$State)


MLS_finals<- "https://en.wikipedia.org/wiki/List_of_MLS_Cup_finals"
tbls_ls <- read_html(MLS_finals) %>%
  html_nodes("table") %>%
  html_table(fill = TRUE)
tbls_ls

finals<- as.data.frame(tbls_ls[[2]])
winners<- finals[,c(3,1)]
winners$Winner<-gsub("\\*","",winners$Winner)
winners$Winner[1]<- substring(winners$Winner[1],1,11)
winners$Winner[3]<- substring(winners$Winner[3],1,12)
winners$Winner[22]<- substring(winners$Winner[22],1,10)
names(winners)[1]<-"Team"
winners$Team<- as.character(winners$Team)

champs_number<-data.frame(table(winners$Team))
names(champs_number)<- c("Team", "Count")
champs_number$Team<- as.character(champs_number$Team)
champs_number<-champs_number[c(1:6,8,10:15),]
champs_number$Count[7]<- champs_number$Count[7] +1
champs_number$Count[12]<- champs_number$Count[12] +1
champs_number$Team[2]<- team_cities$Team[2]
champs_number$Team[4]<- team_cities$Team[4]
champs_number$Team[7]<- team_cities$Team[18]


champs_number$Team
team_cities$Team

champs_cities_state<- merge(champs_number,team_cities, by ="Team")
champs_cities_state$State[5]<- "Maryland"
champs_cities_state$State<- as.factor(champs_cities_state$State)

state_champs<-champs_cities_state %>% group_by(State)


state_champs_count<-state_champs %>% summarise(
  Count = sum(Count)
)


chart8<- gvisTable(champs_cities_state)
plot(chart8)


chart9<-gvisGeoChart(state_champs_count,locationvar = "State",colorvar = "Count",
             options = list(region="US",displayMode="regions",
                            resolution="provinces",width=600, height=400))
plot(chart9)


plot(gvisTable(champs_cities_state[order(-champs_cities_state$Count),]))
