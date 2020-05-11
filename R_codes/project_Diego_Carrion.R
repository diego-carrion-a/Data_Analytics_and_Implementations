
################inital code preprocesing#################
setwd("C:/Users/diego/OneDrive/Escritorio/BU/Data Analysis and Visualization/project")
dir()
data<- read.csv('master.csv')

names(data)[c(1,10,11)]<- c('Country','gdp_for_year','gdp_percapita')


names(data)
table(data$age)
table(data$generation)
table(data$Country)
table(data$year)
table(data$generation)

summary(data$suicides_no)

data2<- data[,c(1:6,10,12)]
names(data2)

unique(data$Country)

data2$gdp_for_year<- gsub(',','', data2$gdp_for_year)
data2$gdp_for_year<- as.numeric(data2$gdp_for_year)
unique(data2$year)
table(data2$sex)
table(data2$age)
summary(data2$suicides_no)
sd(data2$suicides_no)

table(data2$sex,data2$age)
table(data2$sex,data2$generation)
names(data2)
data2$cod<- paste(data2$age, data2$sex,data2$year, sep='_')
library(car)
library(data.table)
library(tidyverse)
library(ggplot2)
library(ggpubr)




data2<- data.table(data2)
data3<- data2[, (suicides_no= sum(suicides_no)), by=cod]
data3<- as.data.frame(data3)
data3<- separate(data3, cod, c('age','sex','year'), sep='_')
data3$year<- as.numeric(data3$year)
names(data3)[4]<-"suicides"


#################### WE CAN UPLOAD THE "data_cleaned.csv" AND KEEP THE SCRIPT FROM HERE
## data3<- read.csv("data_cleaned.csv")
#library(car)



### check assumptions

###linearity
ggscatter(
  data3, x = "year", y = "suicides",
  facet.by  = c("sex", "age"), 
  short.panel.labs = FALSE
)+
  stat_smooth(method = "loess", span = 0.9)

###Homogeneity of regression slopes

data3 %>%
  anova_test(
    suicides ~ age + sex + year +
      sex*year + age*sex + age*year +
      age*sex*year)
### all good

model= lm(suicides~ age + sex + age*sex + year, data=data3 )
summary(model)

## two way anova is not suitable interactions have pvalue is significant

### separete one factor, going to go with sex

data_m= subset(data3, data3$sex=="male")
model_m= lm(suicides~ age + year, data=data_m )
summary(model_m)
Anova(model_m)

#pairwise comparitions
t2<- model_m$coefficients[2]/2375.21 ## al good
t3<- model_m$coefficients[3]/2375.21 ## al good
t4<- model_m$coefficients[4]/2394.59 ## not so good
t5<- model_m$coefficients[5]/2375.21 ## al good
t6<- model_m$coefficients[6]/2375.21 ## al good

df<- 191-6

Critic_value <- 1.653
t1;t2;t3;t4;t5;t6


data_f= subset(data3, data3$sex!="male")
model_f= lm(suicides~ age + year, data=data_f )
summary(model_f)
Anova(model_f)

#pairwise comparitions
t8<- model_f$coefficients[2]/549.78 ## al good
t9<- model_f$coefficients[3]/549.78 ## al good
t10<- model_f$coefficients[4]/554.26 ## not so good
t11<- model_f$coefficients[5]/549.78 ## al good
t12<- model_f$coefficients[6]/549.78 ## al good


write.csv(data3, "data_cleaned.csv") ## use this data and then separate by sex.
