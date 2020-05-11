library(ggplot2)
library(RColorBrewer)


###PREPARATING THE DATA----
setwd("C:/Users/diego/OneDrive/Escritorio/R foundations/Project")
dir()

T_2000<- read.csv(dir()[1])
T_2001<- read.csv(dir()[2])
T_2002<- read.csv(dir()[3])
T_2003<- read.csv(dir()[4])
T_2004<- read.csv(dir()[5])
T_2005<- read.csv(dir()[6])
T_2006<- read.csv(dir()[7])
T_2007<- read.csv(dir()[8])
T_2008<- read.csv(dir()[9])
T_2009<- read.csv(dir()[10])
T_2010<- read.csv(dir()[11])
T_2011<- read.csv(dir()[12])
T_2012<- read.csv(dir()[13])
T_2013<- read.csv(dir()[14])
T_2014<- read.csv(dir()[15])
T_2015<- read.csv(dir()[16])
T_2016<- read.csv(dir()[17])
T_2017<- read.csv(dir()[18])
T_2018<- read.csv(dir()[19])
T_2019<- read.csv(dir()[20])

data<- rbind(T_2000,T_2001,T_2002,T_2003,T_2004,T_2005,T_2006,T_2007,T_2008,T_2009,T_2010,T_2011,T_2012,
             T_2013,T_2014,T_2015,T_2016,T_2017,T_2018,T_2019)

data$year<- as.numeric(substring(data$tourney_id, first = 1, last = 4))
summary(data$year)
rm(T_2000);rm(T_2001);rm(T_2002);rm(T_2003);rm(T_2004);rm(T_2005);rm(T_2006);rm(T_2007);rm(T_2008);rm(T_2009)
rm(T_2010);rm(T_2011);rm(T_2012);rm(T_2013);rm(T_2014);rm(T_2015);rm(T_2016);rm(T_2017);rm(T_2018);rm(T_2019)

class(data$tourney_id)

for (i in 1:ncol(data)){
  if (class(data[,i])=="factor"){
    data[,i]<- as.character(data[,i])
  }
}


## check for na first  

for (i in 1:ncol(data)){
  print(table(is.na(data[,i])))
  print(i)
}
names(data)
names(data)[10]
## we have some NA on the time they played, maybe there are matches who did not were played, so the amount of sets,
## played should be 0, lets find out
count_sets <- function (a,b){
  str(a)
  count =0
  for (i in 1:nchar(a)){
    if (substring(a, i,i)== b){
      count = count+ 1
    }
  }
  return(count) 
}

for (i in 1:nrow(data)){
  data$sets_played[i]<- count_sets(data$score[i],"-")
}

table(data$sets_played)
### only 291 matches werent played, 
table(is.na(data[,10]))
6999/nrow(data)
### 12% of the data would be missing
### still have a lt of data anyway

data1<- subset(data, is.na(data[,10])==FALSE)
table(data1$surface)
data2<- subset(data1, data1$surface != "None" )
data3<- subset(data2, is.na(data2$w_svpt)==FALSE)
rm(data)
rm(data1)
rm(data2)
#####ANALIZING DATA----
#Do some of the descriptive analyses described in Module3 for at least one
#categorical variable and at least one numerical variable. 
#Show appropriate plots for your data.

##### CATEGORICAL DATA----
table(data3$surface)
Overall_Surface<-data.frame(table(data3$surface))
names(Overall_Surface)[1]<-"Surface"
Overall_Surface_dist<-round((table(data3$surface)/nrow(data3))*100,2)
distribution<-paste(round((table(data3$surface)/nrow(data3))*100,2),"%", sep = "")

G1_Colors <- brewer.pal(4, "Set1")
names(G1_Colors) <- Overall_Surface$Var1

G1<-ggplot(data=Overall_Surface, aes(x=Surface, y=Freq, fill=Surface )) +
  geom_bar(stat="identity")+scale_colour_manual(values= G1_Colors)+
  geom_text(aes(label=Freq), vjust=1.6, color="white", size=3.5)+
 theme_minimal()

G1 + ggtitle("Overall distribution of matches vs surface") +
  theme(plot.title = element_text(hjust = 0.5))+
  xlab("Surface") + ylab("Matches played on surface")
  


##### Numerical data analisys----

table(data3$best_of)/ nrow(data3)
#number of points 

data3$points_played<-(data3$w_svpt + data3$l_svpt)
table(data3$points_played)

summary(data3$points_played)
which(table(data3$points_played)== max(table(data3$points_played)))
range(data3$points_played, na.rm = T)
diff(range(data3$points_played, na.rm = T))
var(data3$points_played, na.rm = T)
fivenum(data3$points_played, na.rm = T)

G2<-ggplot(data3, aes(x=data3$points_played))+
  geom_histogram(color="black", fill="Orange")

G2+ ggtitle("Distribution of Points Played in a ATP match") +
  theme(plot.title = element_text(hjust = 0.5))+
  xlab("Points played") + ylab("Frequency")


G3<-ggplot(data3, aes(x="", y=data3$points_played), fill= '')+
  geom_boxplot(fill='darkolivegreen3', color="darkolivegreen4")  
  
G3+ coord_flip() + ggtitle("BoxPlot") +
  theme(plot.title = element_text(hjust = 0.5))+
  xlab("Points played") + ylab("Frequency")

f<-fivenum(data3$points_played, na.rm = T)
c(f[2]- 1.5*(f[4]-f[2]),
  f[4]+ 1.5*(f[4]-f[2]))
table(data3$points_played>301)
table(data3$points_played<5)


G4<-ggplot(data3, aes(x=data3$year,  group=data3$year, y=data3$points_played), fill= data3$year)+
  geom_boxplot(fill= 'lightblue')

G4+ ggtitle("Points Played by Year") +
  theme(plot.title = element_text(hjust = 0.5))+
  xlab("Year") + ylab("Concentration of Points Played")

G5<-ggplot(data3, aes(x=data3$round,  group=data3$round, y=data3$points_played), fill= data3$round)+
  geom_boxplot(fill= 'lightgreen')

G5+ ggtitle("Points Played by Year") +
  theme(plot.title = element_text(hjust = 0.5))+
  xlab("Round") + ylab("Concentration of Points Played")


library(psych)
describe.by(data3$points_played, data3$year, mat = T)[c(2,4:7,10,11,12)]
describe.by(data3$minutes, data3$year, mat = T)[c(2,4:7,10,11,12)]
describeBy(data3$points_played, data3$round, mat = T)[c(2,4:7,10,11,12)]
describeBy(data3$minutes, data3$round, mat = T)[c(2,4:7,10,11,12)]
######
#pretty similar per year lets see aces per match

data3$aces_p_match<- data3$l_ace + data3$w_ace
mean(data3$aces_p_match)/ mean(data3$points_played)

table(data3$aces_p_match)
which(table(data3$aces_p_match)== max(table(data3$aces_p_match)))
range
summary(data3$aces_p_match)
mean(data3$aces_p_match)
var(data3$aces_p_match)
sd(data3$aces_p_match)



G6<-ggplot(data3, aes(x=data3$aces_p_match))+
  geom_histogram(color="black", fill="brown")

G6+ ggtitle("Distribution of Aces in a ATP match") +
  theme(plot.title = element_text(hjust = 0.5))+
  xlab("Aces") + ylab("Frequency")


G7<-ggplot(data3, aes(x="", y=data3$aces_p_match), fill= '')+
  geom_boxplot(fill='dodgerblue3', color="dodgerblue4")  

G7+ coord_flip() + ggtitle("BoxPlot") +
  theme(plot.title = element_text(hjust = 0.5))+
  xlab("Aces") + ylab("Frequency")




f<-fivenum(data3$aces_p_match, na.rm = T)
c(f[2]- 1.5*(f[4]-f[2]),
  f[4]+ 1.5*(f[4]-f[2]))
table(data3$aces_p_match>31)
1598/nrow(data3)
table(data3$points_played<5)

G8<-ggplot(data3, aes(x=data3$round,  group=data3$round, y=data3$aces_p_match), fill= data3$round)+
  geom_boxplot(fill= 'lightblue')

G8+ ggtitle("Aces by Round") +
  theme(plot.title = element_text(hjust = 0.5))+
  xlab("Round") + ylab("Concentration of Points Played")


G9<-ggplot(data3, aes(x=data3$surface,  group=data3$surface, y=data3$aces_p_match), fill= data3$surface)+
  geom_boxplot(fill= 'darkblue')

G9+ ggtitle("Aces by Surface") +
  theme(plot.title = element_text(hjust = 0.5))+
  xlab("Surface") + ylab("Concentration of Points Played")


describeBy(data3$aces_p_match, data3$surface, mat=T)[c(2,4:7,10,11,12)]
describeBy(data3$aces_p_match, data3$round, mat=T)[c(2,4:7,10,11,12)]

######################### distribution
G2+ ggtitle("Distribution of Points Played in a ATP match") +
  theme(plot.title = element_text(hjust = 0.5))+
  xlab("Points played") + ylab("Frequency")


G6+ ggtitle("Distribution of Aces in a ATP match") +
  theme(plot.title = element_text(hjust = 0.5))+
  xlab("Aces") + ylab("Frequency")


G10<-ggplot(data3, aes(x=data3$minutes))+
  geom_histogram(color="black", fill="coral1")

G10+ ggtitle("Distribution of Minutes played in a ATP match") +
  theme(plot.title = element_text(hjust = 0.5))+
  xlab("Minutes") + ylab("Frequency")

mean(data3$minutes)
sd(data3$minutes)
################# central limit theorem----

library(randomcoloR)
samples <- 10000
xbar <- numeric(samples)

mean(xbar)
sd(xbar)

par(mfrow = c(2,2))
graph_Vector<- c(NA, NA,NA,NA)

for (size in c(10, 20, 30, 40)) {
  for (i in 1:samples) {
    xbar[i] <- mean(rnorm(size, 
                          mean = mean(data3$minutes), sd = sd(data3$minutes)))
  }

  
  hist(xbar, prob = TRUE, 
       breaks = 25, xlim=c(50,150), ylim = c(0, 0.07), xlab="Minutes played",
       main = paste("Sample Size =", size), col = randomColor())
  
  cat("Sample Size = ", size, " Mean = ", mean(xbar),
      " SD = ", sd(xbar), "\n")
}

xbar40<- numeric(samples)
for (i in 1: samples) {
  xbar40[i] <- mean(rnorm(40, 
                        mean = mean(data3$minutes ), sd = sd(data3$minutes)))
}
xbar30<- numeric(samples)

for (i in 1: samples) {
  xbar30[i] <- mean(rnorm(30, 
                        mean = mean(data3$minutes ), sd = sd(data3$minutes)))
}

xbar20<- numeric(samples)

for (i in 1: samples) {
  xbar20[i] <- mean(rnorm(20, 
                        mean = mean(data3$minutes ), sd = sd(data3$minutes)))
}

xbar10<- numeric(samples)
for (i in 1: samples) {
  xbar10[i] <- mean(rnorm(10, 
                        mean = mean(data3$minutes ), sd = sd(data3$minutes)))
}


S10<-data.frame(xbar10)
S20<-data.frame(xbar20)
S30<-data.frame(xbar30)
S40<-data.frame(xbar40)

names(S10)[1]<- "Xbar"
names(S20)[1]<- "Xbar"
names(S30)[1]<- "Xbar"
names(S40)[1]<- "Xbar"

S10$Size= "Size10"
S20$Size= "Size20"
S30$Size= "Size30"
S40$Size= "Size40"


size_4sample <- rbind(S40,S30, S20,S10)


ggplot(size_4sample, aes(Xbar, fill = Size)) + 
  geom_histogram(alpha = 0.5, aes(y = ..density..), position = 'identity', color="black")


#############3sampling----

library(sampling)

set.seed(100)

# SRSWOR
# Equal Probability
s <- srswor(500, 52350)
s

srswor<-data3[s != 0,]

mean(srswor$aces_p_match)
sd(srswor$aces_p_match)

mean(srswor$minutes)
sd(srswor$minutes)

mean(data3$aces_p_match)
sd(data3$aces_p_match)
table(srswor$surface)/nrow(srswor)

distribution


####  Systematic Sampling

set.seed(100)
#

N <- nrow(data3)
n <- 500

k <- ceiling(N / n)
k

r <- sample(k, 1)
r

# select every kth item

s <- seq(r, by = k, length = n)

sys_sample <- data3[s, ]
mean(sys_sample$aces_p_match, na.rm = T)
sd(sys_sample$aces_p_match, na.rm = T)
mean(data3$aces_p_match)
sd(data3$aces_p_match)

mean(sys_sample$minutes, na.rm = T)
sd(sys_sample$minutes, na.rm = T)


mean(data3$minutes)
sd(data3$minutes)

table(sys_sample$surface)/nrow(sys_sample)

#### 3.7. Stratified Sampling

set.seed(100)
# Stratified, unequal sized strata
data4<- data3[order(data3$surface),]

freq <- table(data3$surface)
freq

st.sizes <- round(500 * freq / sum(freq),0)
st.sizes

st.2 <- strata(data4, stratanames = c("surface"),
               size = st.sizes, method = "srswor",
               description = TRUE)

table(st.2$surface)
st.sample2 <- getdata(data4, st.2)


table(st.sample2$surface)/nrow(st.sample2)

mean(st.sample2$minutes)
sd(st.sample2$minutes)

mean(st.sample2$aces_p_match)
sd(st.sample2$aces_p_match)
################################ UnderDogProbability-----

for (i in 1:nrow(data3)){
  ifelse(data3$winner_rank[i]>=data3$loser_rank[i], data3$win_over_rank[i]<-1,data3$win_over_rank[i]<-0 )
  }
table(data3$win_over_rank)/nrow(data3)

library(tidyverse)
matches<-as_tibble(data3)

freq_dat_round<-data.frame(matches %>% group_by(round) %>% summarise(sum_w_o_r = sum(win_over_rank)))
dat_round<-data.frame(table(data3$round))

freq_dat_round$sum_w_o_r/dat_round$Freq

freq_dat_sourface<-data.frame(matches %>% group_by(surface)%>% summarise(sum_w_o_r = sum(win_over_rank)))
dat_surface<-data.frame(table(data3$surface))

freq_dat_sourface$sum_w_o_r/dat_surface$Freq


surface_round<-data.frame(matches %>% group_by(surface,round)%>% summarise(sum_w_o_r = sum(win_over_rank)))

table( data3$round,data3$surface)



write.csv(data3, "Final_data_fproject.csv", row.names = F)