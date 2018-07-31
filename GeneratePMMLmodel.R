## packages:
if(!require(e1071)) { install.packages("e1071"); library(e1071); }
if(!require(caret)) { install.packages("caret"); library(caret); }
if(!require(XML)) { install.packages("XML"); library(XML); }
if(!require(pmml)) { install.packages("pmml"); library(pmml); }
if(!require(randomForest)) { install.packages("randomForest"); library(randomForest); }
if(!require(chron)) { install.packages("chron"); library(chron); }
if(!require(plyr)) { install.packages("plyr"); library(plyr); }
if(!require(dplyr)) { install.packages("dplyr"); library(dplyr); }
if(!require(plotly)) { install.packages("plotly"); library(plotly); }
if(!require(BBmisc )) { install.packages("BBmisc"); library(plotly); }


#mydata <- read.csv("C:/Users/ceder/Flink/BatchStockData/AAPL_batch_big_R.csv")  # read csv file 
#mydata <- read.csv("C:/Users/ceder/Flink/BatchStockData/XOM_big_bigger2.csv")  # read csv file 

## JNJ:
#mydata <- read.csv("C:/Users/ceder/Flink/BatchStockData/JNJ_Big_0.01_2.csv")  # read csv file 
#mydata <- read.csv("C:/Users/ceder/Flink/BatchStockData/JNJ_Big_0.1.csv")  # read csv file 
#mydata <- read.csv("C:/Users/ceder/Flink/BatchStockData/JNJ_Big_0.2.csv")  # read csv file 
#mydata <- read.csv("C:/Users/ceder/Flink/BatchStockData/JNJ_Big_0.5.csv")  # read csv file 
#mydata <- read.csv("C:/Users/ceder/Flink/BatchStockData/batchData/ORCL_big.csv")  # read csv file 
mydata <- read.csv("C:/Users/ceder/Flink/BatchStockData/batchData/MS_big.csv")  # read csv file 
#mydata <- read.csv("C:/Users/ceder/Flink/BatchStockData/batchData/SO_big.csv")  # read csv file 
#mydata <- read.csv("C:/Users/ceder/Flink/BatchStockData/batchData/JNJ_big.csv")  # read csv file 
#mydata <- read.csv("C:/Users/ceder/Flink/BatchStockData/batchData/MS_big_period.csv")  # read csv file
#mydata <- read.csv("C:/Users/ceder/Flink/BatchStockData/batchData/SO_big2.csv")  # read csv file
#mydata <- read.csv("C:/Users/ceder/Flink/BatchStockData/batchData/GOOGL_big.csv")  # read csv file
#mydata <- read.csv("C:/Users/ceder/Flink/BatchStockData/batchData/C_big.CSV")  # read csv file

## delete first 100 rows bc variables are not correctly calculated by then
mydata <- mydata[-c(1:100),]

colnames(mydata) <- c("dateTime", "name", "lastPrice", "lastPriceLag", "SMA10", "SMA100", "SMA_signal", "SMA_direction","BB_lowerbound", "BB_upperbound", "BB_middleband", "BB_signal", "BB_direction","CCI", "CCI_signal", "CCI_direction",
                    "stoch",  "stoch_signal", "stoch_direction","RSI", "RSI_signal", "RSI_direction","MFI", "MFI_signal", "moneyFlowIndex_direction",
                    "chaikin",  "chaikin_signal", "chaikin_direction","willR", "willR_signal", "williamsR_direction")
mydata[,1] <- substr(mydata[,1], 0,19)
str(mydata)


dtparts <- t(as.data.frame(strsplit(mydata[,1],' ')))
row.names(dtparts) = NULL
thetimes <- chron(dates=dtparts[,1],times=dtparts[,2], format=c('y-m-d','h:m:s'))
class(mydata[,1])
mydata[,1] <-  thetimes

mydata <- mydata[do.call(order, mydata), ] 

## overview
table(mydata$SMA_signal)
table(mydata$SMA_direction)
table(mydata$BB_signal)
table(mydata$BB_direction)
table(mydata$CCI_direction)
table(mydata$stoch_signal)
table(mydata$stoch_direction)
table(mydata$RSI_direction)
table(mydata$RSI_signal)
table(mydata$MFI_signal)
table(mydata$moneyFlowIndex_direction)
table(mydata$chaikin_signal)
table(mydata$chaikin_direction)
table(mydata$willR_signal)
table(mydata$williamsR_direction)

mydata$SMA_signal <- factor(mydata$SMA_signal)
mydata$SMA_direction <- factor(mydata$SMA_direction)
mydata$BB_signal <- factor(mydata$BB_signal)
mydata$BB_direction <- factor(mydata$BB_direction)
mydata$CCI_direction <- factor(mydata$CCI_direction)
mydata$CCI_signal <- factor(mydata$CCI_signal)
mydata$stoch_signal <- factor(mydata$stoch_signal)
mydata$stoch_direction <- factor(mydata$stoch_direction)
mydata$RSI_direction <- factor(mydata$RSI_direction)
mydata$RSI_signal <- factor(mydata$RSI_signal)
mydata$MFI_signal <- factor(mydata$MFI_signal)
mydata$moneyFlowIndex_direction <- factor(mydata$moneyFlowIndex_direction)
mydata$chaikin_signal <-factor(mydata$chaikin_signal)
mydata$chaikin_direction <- factor(mydata$chaikin_direction)
mydata$willR_signal <- factor(mydata$willR_signal)
mydata$williamsR_direction <- factor(mydata$williamsR_direction)
str(mydata)


## keep lastPrice per day
mydata[,1] <- factor(substr(mydata[,1],2,10))  
a <- ddply(mydata, "dateTime", tail, 1)[,1:3]
basetable <- merge(x = mydata, y = a, by = 1)

colSums(is.na(basetable))


####################################################
################# return intraday ##################
####################################################

################# optimal return ################# 
duration <- 52512 # six months
deploymentPeriod <- k
durationTest <- duration + deploymentPeriod
threshold <- 0.01


## define response variable based on threshold
basetable$responseVariable <- factor(ifelse(basetable$lastPrice.x - basetable$lastPrice.y >= threshold , 2 , ifelse(basetable$lastPrice.y - basetable$lastPrice.x >= threshold , 1 , 0)))


returnTable <- basetable[1:52512, c("dateTime", "lastPrice.x", "lastPrice.y", "responseVariable") ]


returnTable$prediction <- returnTable$responseVariable
returnTable <- na.omit(returnTable)

## make extra dateTime column for end of day selling
returnTable$dateTime <- factor(returnTable$dateTime )
returnTable$dateTimeHead <- lead(returnTable$dateTime)

returnTable$predictionDay <- as.numeric(ifelse(returnTable$dateTime != returnTable$dateTimeHead, 3, returnTable$prediction))


## remove hold rows except end of day
returnTable <- returnTable[!(returnTable$predictionDay == 1)  ,]

## calculate the lag to define moments of sell/buy
returnTable$lag <- as.numeric(lag(returnTable$predictionDay))
returnTable$returnPrice <- ifelse(returnTable$predictionDay > returnTable$lag, returnTable$lastPrice.x, ifelse(returnTable$predictionDay < returnTable$lag, returnTable$lastPrice.x, 0))
returnTable <- na.omit(returnTable)

countSell <- as.numeric(count(returnTable[returnTable$returnPrice > 0,]))


## make sure the count of buy is same as sell
returnTable[nrow(returnTable),"returnPrice"]  <- ifelse(returnTable[nrow(returnTable),"returnPrice"] == 0, ifelse(count(returnTable[returnTable$returnPrice > 0,]) < count(returnTable[returnTable$returnPrice < 0,]), returnTable[nrow(returnTable),"lastPrice.x"], 0 ),returnTable[nrow(returnTable),"returnPrice"] ) 

returnTable[nrow(returnTable),"returnPrice"]  <- ifelse(returnTable[nrow(returnTable),"returnPrice"] == 0, ifelse(count(returnTable[returnTable$returnPrice > 0,]) > count(returnTable[returnTable$returnPrice < 0,]), returnTable[nrow(returnTable),"lastPrice.x"], 0 ),returnTable[nrow(returnTable),"returnPrice"] ) 

## remove 0 rows, return = postion * (close(t+n) - close)/close
returnTable <- returnTable[!(returnTable$returnPrice == 0)  ,]

returnTable$nextPrice <- lead(returnTable$returnPrice)

returnTable$return <- ifelse(returnTable$predictionDay == 2, (returnTable$nextPrice - returnTable$returnPrice)/abs(returnTable$returnPrice), -(returnTable$nextPrice - returnTable$returnPrice)/abs(returnTable$returnPrice))

returnTable <- na.omit(returnTable)
## total return 
(totalReturn <- sum(returnTable$return,na.rm=TRUE))

sd(returnTable$return)
mean(returnTable$return)

## average daily return
numberOfDays <- nlevels(x = returnTable$dateTime) 
(averageDailyReturn2 <- totalReturn/numberOfDays)

countSell

################# Predicted return ################# 

## remove for next loop
rm(baseTable)
rm(returnTable)
rm(basetableTrain2)
rm(basetableTest2)


resultDF <- data.frame(Period=numeric(), Return = numeric(), 
                       Transactions = numeric(), Period2=numeric(), 
                       Return2 = numeric(), Transactions2 = numeric(),
                       Period3 =numeric(), Return3 = numeric(), Transactions3 = numeric(),
                       Period4 =numeric(), Return4 = numeric(), Transactions4 = numeric())

resultDFweek <- data.frame(Period=numeric(), Return = numeric(), 
                       Transactions = numeric(), Period2=numeric(), 
                       Return2 = numeric(), Transactions2 = numeric(),
                       Period3 =numeric(), Return3 = numeric(), Transactions3 = numeric(),
                       Period4 =numeric(), Return4 = numeric(), Transactions4 = numeric())

resultDFtwoweek <- data.frame(Period=numeric(), Return = numeric(), 
                           Transactions = numeric(), Period2=numeric(), 
                           Return2 = numeric(), Transactions2 = numeric(),
                           Period3 =numeric(), Return3 = numeric(), Transactions3 = numeric(),
                           Period4 =numeric(), Return4 = numeric(), Transactions4 = numeric())

month <- data.frame(Period=numeric(), Return = numeric(), 
                      Transactions = numeric(), Period2=numeric(), 
                      Return2 = numeric(), Transactions2 = numeric(),
                      Period3 =numeric(), Return3 = numeric(), Transactions3 = numeric(),
                      Period4 =numeric(), Return4 = numeric(), Transactions4 = numeric())

twomonth <- data.frame(Period=numeric(), Return = numeric(), 
                    Transactions = numeric(), Period2=numeric(), 
                    Return2 = numeric(), Transactions2 = numeric(),
                    Period3 =numeric(), Return3 = numeric(), Transactions3 = numeric(),
                    Period4 =numeric(), Return4 = numeric(), Transactions4 = numeric())

sixmonthTable <- data.frame(Period=numeric(), Return = numeric(), 
                       Transactions = numeric(), Period2=numeric(), 
                       Return2 = numeric(), Transactions2 = numeric(),
                       Period3 =numeric(), Return3 = numeric(), Transactions3 = numeric(),
                       Period4 =numeric(), Return4 = numeric(), Transactions4 = numeric())


thresholds <- c(0.01, 0.1, 0.2, 0.5)
day <- 389
week <- 5*day
twoWeekn <- 2*week
threeWeekn <- 3*week
fiveWeekn <- 5*week
sixWeekn <- 6*week
sevenWeekn <- 7*week
tenWeekn <- 10*week
monthn <- round(4.5*week,0)
twomonthn <- 2*monthn
threemonthn <- 3*monthn

deploymentPeriods <- c( day, week,twoWeekn,monthn,twomonthn, threemonthn )
trainingPeriods <- c(  week,twoWeekn,threeWeekn, monthn, fiveWeekn, sixWeekn, sevenWeekn,  twomonthn, tenWeekn, threemonthn )

for (k in deploymentPeriods){
  
  for (j in thresholds){
  
    for(i in trainingPeriods){
      
    duration <- 6000
    deploymentPeriod <- 5000
    durationTest <- duration + deploymentPeriod
    threshold <- 0.2
    
    ## define response variable based on threshold
    basetable$responseVariable <- factor(ifelse(basetable$lastPrice.x - basetable$lastPrice.y >= threshold , 2 , ifelse(basetable$lastPrice.y - basetable$lastPrice.x >= threshold , 1 , 0)))
    
    ## only select explaining variables for random forest
    baseTable <- basetable[, c(3,5:31,34)]
    
    basetableTrain2 <-  baseTable[1:duration,]
    basetableTest2 <- basetable[duration:durationTest,]
    returnTable <- basetableTest2[, c("dateTime", "lastPrice.x", "lastPrice.y", "responseVariable") ]
    basetableTest2 <- basetable[duration:durationTest,c(3,5:31,34)]
    
    basetableTrain2$responseVariable <- factor(basetableTrain2$responseVariable)
    
  
    ## make prediction and prediction column
    #RandomForest <-randomForest(responseVariable~.,data=basetableTrain2[, c(2,3,8,11,14,17,20,23,26,29)], ntree=100) 
    rf <-randomForest(responseVariable~.,data=basetableTrain2, ntree=100) 
    varImpPlot(rf,type=2, n.var = 12, main = "Random Forest")
    
    confusionMatrix(predict(rf, newdata=basetableTest2) , returnTable$responseVariable)
    
    returnTable$prediction <- predict(rf,newdata=basetableTest2) 
    (result <- confusionMatrix(returnTable$prediction, returnTable$responseVariable))
    
    # Algorithm Tune (tuneRF)
    # rftuned <- tuneRF(x = basetableTrain2[,1:28], y = basetableTrain2$responseVariable, mtryStart = 100, improve = 0.0000001,  doBest = TRUE )
    # 
    # returnTable$prediction <- predict(rftuned,newdata=basetableTest2)
    # (result <- confusionMatrix(returnTable$prediction, returnTable$responseVariable))
    # returnTable$prediction <- predict(rftuned,newdata=basetableTest2)
    # 
    ## make extra dateTime column for end of day selling
    returnTable$dateTime <- factor(returnTable$dateTime )
    returnTable$dateTimeHead <- lead(returnTable$dateTime)
    
  #  returnTable$predictionDay <- as.numeric(ifelse(returnTable$dateTime != returnTable$dateTimeHead, 3, returnTable$prediction))
    returnTable$predictionDay <- as.numeric(returnTable$prediction)
    
    
    ## remove hold rows except end of day
    returnTable <- returnTable[!(returnTable$predictionDay == 1)  ,]
    
    ## calculate the lag to define moments of sell/buy
    returnTable$lag <- as.numeric(lag(returnTable$predictionDay))
    returnTable$returnPrice <- ifelse(returnTable$predictionDay > returnTable$lag, returnTable$lastPrice.x, ifelse(returnTable$predictionDay < returnTable$lag, returnTable$lastPrice.x, 0))
    returnTable <- na.omit(returnTable)
    
    countSell <- count(returnTable[returnTable$returnPrice > 0,])
    countBuy <- count(returnTable[returnTable$returnPrice < 0,])
    
    ## make sure the count of buy is same as sell
    returnTable[nrow(returnTable),"returnPrice"]  <- ifelse(returnTable[nrow(returnTable),"returnPrice"] == 0, ifelse(count(returnTable[returnTable$returnPrice > 0,]) < count(returnTable[returnTable$returnPrice < 0,]), returnTable[nrow(returnTable),"lastPrice.x"], 0 ),returnTable[nrow(returnTable),"returnPrice"] ) 
    
    returnTable[nrow(returnTable),"returnPrice"]  <- ifelse(returnTable[nrow(returnTable),"returnPrice"] == 0, ifelse(count(returnTable[returnTable$returnPrice > 0,]) > count(returnTable[returnTable$returnPrice < 0,]), returnTable[nrow(returnTable),"lastPrice.x"], 0 ),returnTable[nrow(returnTable),"returnPrice"] ) 
    
    ## remove 0 rows, return = postion * (close(t+n) - close)/close
    returnTable <- returnTable[!(returnTable$returnPrice == 0)  ,]
    
    returnTable$nextPrice <- lead(returnTable$returnPrice)
    
    returnTable$return <- ifelse(returnTable$predictionDay == 2, (returnTable$nextPrice - returnTable$returnPrice)/abs(returnTable$returnPrice), -(returnTable$nextPrice - returnTable$returnPrice)/abs(returnTable$returnPrice))
    
    returnTable <- na.omit(returnTable)
    ## total return 
    (totalReturn <- sum(returnTable$return,na.rm=TRUE))
    
    ## average daily return
    numberOfDays <- nlevels(x = returnTable$dateTime) 
    (averageDailyReturn2 <- totalReturn/numberOfDays)
    
    #i <- i/389 - 4
    i <- as.numeric(i)
  
    if (k == day){
  
        if(j == 0.01){
          ## result for-loop:
          resultDF[i,1] <- i
          resultDF[i,2] <- averageDailyReturn2
          resultDF[i,3] <- countSell
        }
        else if(j == 0.1){
          ## result for-loop:
          resultDF[i,4] <- i
          resultDF[i,5] <- averageDailyReturn2
          resultDF[i,6] <- countSell
        }
        else if(j == 0.2){
        ## result for-loop:
        resultDF[i,7] <- i
        resultDF[i,8] <- averageDailyReturn2
        resultDF[i,9] <- countSell
        }
        else if (j == 0.5){
          ## result for-loop:
          resultDF[i,10] <- i
          resultDF[i,11] <- averageDailyReturn2
          resultDF[i,12] <- countSell
        }
        else if (j == 0.5){
          ## result for-loop:
          resultDF[i,13] <- i
          resultDF[i,14] <- averageDailyReturn2
          resultDF[i,15] <- countSell
        }
      
    } 
    else if (k == week){
      
         if(j == 0.01){
          ## result for-loop:
          resultDFweek[i,1] <- i
          resultDFweek[i,2] <- averageDailyReturn2
          resultDFweek[i,3] <- countSell
        }
        else if(j == 0.1){
          ## result for-loop:
          resultDFweek[i,4] <- i
          resultDFweek[i,5] <- averageDailyReturn2
          resultDFweek[i,6] <- countSell
        }
        else if(j == 0.2){
          ## result for-loop:
          resultDFweek[i,7] <- i
          resultDFweek[i,8] <- averageDailyReturn2
          resultDFweek[i,9] <- countSell
        }
        else if (j == 0.5){
          ## result for-loop:
          resultDFweek[i,10] <- i
          resultDFweek[i,11] <- averageDailyReturn2
          resultDFweek[i,12] <- countSell
        }
        else if (j == 0.5){
          ## result for-loop:
          resultDFweek[i,13] <- i
          resultDFweek[i,14] <- averageDailyReturn2
          resultDFweek[i,15] <- countSell
        }
    }
    
    else if (k == twoWeekn){
      
      if(j == 0.01){
        ## result for-loop:
        resultDFtwoweek[i,1] <- i
        resultDFtwoweek[i,2] <- averageDailyReturn2
        resultDFtwoweek[i,3] <- countSell
      }
      else if(j == 0.1){
        ## result for-loop:
        resultDFtwoweek[i,4] <- i
        resultDFtwoweek[i,5] <- averageDailyReturn2
        resultDFtwoweek[i,6] <- countSell
      }
      else if(j == 0.2){
        ## result for-loop:
        resultDFtwoweek[i,7] <- i
        resultDFtwoweek[i,8] <- averageDailyReturn2
        resultDFtwoweek[i,9] <- countSell
      }
      else if (j == 0.5){
        ## result for-loop:
        resultDFtwoweek[i,10] <- i
        resultDFtwoweek[i,11] <- averageDailyReturn2
        resultDFtwoweek[i,12] <- countSell
      }
      else if (j == 0.5){
        ## result for-loop:
        resultDFtwoweek[i,13] <- i
        resultDFtwoweek[i,14] <- averageDailyReturn2
        resultDFtwoweek[i,15] <- countSell
      }
    }
    
    else if (k == monthn){
      
      if(j == 0.01){
        ## result for-loop:
        month[i,1] <- i
        month[i,2] <- averageDailyReturn2
        month[i,3] <- countSell
      }
      else if(j == 0.1){
        ## result for-loop:
        month[i,4] <- i
        month[i,5] <- averageDailyReturn2
        month[i,6] <- countSell
      }
      else if(j == 0.2){
        ## result for-loop:
        month[i,7] <- i
        month[i,8] <- averageDailyReturn2
        month[i,9] <- countSell
      }
      else if (j == 0.5){
        ## result for-loop:
        month[i,10] <- i
        month[i,11] <- averageDailyReturn2
        month[i,12] <- countSell
      }
      else if (j == 0.5){
        ## result for-loop:
        month[i,13] <- i
        month[i,14] <- averageDailyReturn2
        month[i,15] <- countSell
      }
    }
    
    
    
    else if (k == twomonthn){
      
      if(j == 0.01){
        ## result for-loop:
        twomonth[i,1] <- i
        twomonth[i,2] <- averageDailyReturn2
        twomonth[i,3] <- countSell
      }
      else if(j == 0.1){
        ## result for-loop:
        twomonth[i,4] <- i
        twomonth[i,5] <- averageDailyReturn2
        twomonth[i,6] <- countSell
      }
      else if(j == 0.2){
        ## result for-loop:
        twomonth[i,7] <- i
        twomonth[i,8] <- averageDailyReturn2
        twomonth[i,9] <- countSell
      }
      else if (j == 0.5){
        ## result for-loop:
        twomonth[i,10] <- i
        twomonth[i,11] <- averageDailyReturn2
        twomonth[i,12] <- countSell
      }
      else if (j == 0.5){
        ## result for-loop:
        twomonth[i,13] <- i
        twomonth[i,14] <- averageDailyReturn2
        twomonth[i,15] <- countSell
      }
    }
    
    else if (k == threemonthn){
      
      if(j == 0.01){
        ## result for-loop:
        sixmonthTable[i,1] <- i
        sixmonthTable[i,2] <- averageDailyReturn2
        sixmonthTable[i,3] <- countSell
      }
      else if(j == 0.1){
        ## result for-loop:
        sixmonthTable[i,4] <- i
        sixmonthTable[i,5] <- averageDailyReturn2
        sixmonthTable[i,6] <- countSell
      }
      else if(j == 0.2){
        ## result for-loop:
        sixmonthTable[i,7] <- i
        sixmonthTable[i,8] <- averageDailyReturn2
        sixmonthTable[i,9] <- countSell
      }
      else if (j == 0.5){
        ## result for-loop:
        sixmonthTable[i,10] <- i
        sixmonthTable[i,11] <- averageDailyReturn2
        sixmonthTable[i,12] <- countSell
      }
      else if (j == 0.5){
        ## result for-loop:
        sixmonthTable[i,13] <- i
        sixmonthTable[i,14] <- averageDailyReturn2
        sixmonthTable[i,15] <- countSell
      }
    }
    
    
    
    
    cat("traing period: ", duration ,", threshold: ", threshold,", deployment period: ",k ,"\n")
    cat("averageDailyReturn: ",averageDailyReturn2 , "\n")
    cat("Transactions " , as.numeric(countSell), "\n")
    cat("\n")
  
    ## remove for next loop
    rm(baseTable)
   # rm(returnTable)
    rm(basetableTrain2)
    rm(basetableTest2)
  
  
    }
  }
}



resultDF <- na.omit(resultDF)
resultDF1 <- na.omit(resultDF)
resultDFweek <- na.omit(resultDFweek)
resultDFweek1 <- na.omit(resultDFweek)
resultDFtwoweek <- na.omit(resultDFtwoweek)
resultDFtwoweek1 <- na.omit(resultDFtwoweek)
month <- na.omit(month)
month1 <- na.omit(month)
twomonth <- na.omit(twomonth)
twomonth1 <- na.omit(twomonth)

sum(resultDF$Return)
max(resultDF$Return)
min(resultDF$Return)

sum(resultDF$Return2)
max(resultDF$Return2)
min(resultDF$Return2)

sum(resultDF$Return3)
max(resultDF$Return3)
min(resultDF$Return3)

sum(resultDF$Return4)
max(resultDF$Return4)
min(resultDF$Return4)

colSums(resultDF)
colSums(resultDFtwoweek)
colSums(resultDFweek)
colSums(month)
colSums(twomonth)

## plotly
f <- list(
  
  size = 15,
  color = "#7f7f7f"
)
x <- list(
  title = "Length of the Training Period (in days)",
  titlefont = f
  
)
y <- list(
  title = "Average Daily Return",
  titlefont = f
  , range = c(-0.015,0.015)
)

Period <- resultDF$Period/389

range01 <- function(x){(x-min(x))/(max(x)-min(x))}

resultDFplot <- resultDF[,c(3,6)]/(max(resultDF$Transactions))*30

resultDFweekplot <-resultDFweek[,c(3,6,9)]/(max(resultDFweek$Transactions))*30
resultDFweekplot$Transactions3 <-resultDFweek[,c(9)]/(max(resultDFweek$Transactions))*50

resultDFtwoweekplot <-resultDFtwoweek[,c(3,6)]/(max(resultDFtwoweek$Transactions))*30

monthplot <-month[,c(3,6)]/(max(month$Transactions))*40

twomonthplot<-twomonth[,c(3,6)]/(max(twomonth$Transactions))*30


plot_ly( data = resultDF, x = Period, y = ~Return2, type = "scatter", mode = "markers", marker = list(size = resultDFplot$Transactions), name = 'Treshold 0.01') %>%
  layout(xaxis = x, yaxis = y ,title = 'One day deployment period')%>% 
  add_trace(data = resultDF, x = Period, y = ~Return, type = "scatter", mode = "markers", marker = list(size = resultDFplot$Transactions2),  name = 'Treshold 0.10')%>% 
  add_trace(data = resultDF, x = Period, y = ~Return3, type = "scatter", mode = "markers", marker = list(size = resultDFplot$Transactions3),  name = 'Treshold 0.20')%>% 
  add_trace(data = resultDF, x = Period, y = ~Return4, type = "scatter", mode = "markers", marker = list(size = resultDFplot$Transactions4),  name = 'Treshold 0.50')


plot_ly( data = resultDFweek, x = Period, y = ~Return2, type = "scatter", mode = "markers", marker = list(size = resultDFweekplot$Transactions), name = 'Treshold 0.01') %>%
  layout(xaxis = x, yaxis = y, title = 'One week deployment period')%>% 
  add_trace(data = resultDFweek, x = Period, y = ~Return, type = "scatter", mode = "markers", marker = list(size = resultDFweekplot$Transactions2),  name = 'Treshold 0.10')%>% 
  add_trace(data = resultDFweek, x = Period, y = ~Return3, type = "scatter", mode = "markers", marker = list(size = resultDFweekplot$Transactions3),name = 'Treshold 0.20')%>% 
  add_trace(data = resultDFweek, x = Period, y = ~Return4, type = "scatter", mode = "markers", marker = list(size = resultDFweekplot$Transactions4),  name = 'Treshold 0.50')


plot_ly( data = resultDFtwoweek, x = Period, y = ~Return2, type = "scatter", mode = "markers", marker = list(size = resultDFtwoweekplot$Transactions), name = 'Treshold 0.01') %>%
  layout(xaxis = x, yaxis = y,title = 'two weeks deployment period')%>% 
  add_trace(data = resultDFtwoweek, x = Period, y = ~Return, type = "scatter", mode = "markers", marker = list(size = resultDFtwoweekplot$Transactions2),  name = 'Treshold 0.10')%>% 
  add_trace(data = resultDFtwoweek, x = Period, y = ~Return3, type = "scatter", mode = "markers", marker = list(size = resultDFtwoweekplot$Transactions3),name = 'Treshold 0.20')%>% 
  add_trace(data = resultDFtwoweek, x = Period, y = ~Return4, type = "scatter", mode = "markers", marker = list(size = resultDFtwoweekplot$Transactions4),  name = 'Treshold 0.50')

plot_ly( data = month, x = Period, y = ~Return2, type = "scatter", mode = "markers", marker = list(size =monthplot$Transactions), name = 'Treshold 0.01') %>%
  layout(xaxis = x, yaxis = y,title = 'One month deployment period')%>% 
  add_trace(data = month, x = Period, y = ~Return, type = "scatter", mode = "markers", marker = list(size = monthplot$Transactions2),  name = 'Treshold 0.10')%>% 
  add_trace(data = month, x = Period, y = ~Return3, type = "scatter", mode = "markers", marker = list(size = monthplot$Transactions3),name = 'Treshold 0.20')%>% 
  add_trace(data = month, x = Period, y = ~Return4, type = "scatter", mode = "markers", marker = list(size = monthplot$Transactions4),  name = 'Treshold 0.50')

plot_ly( data = twomonth, x = Period, y = ~Return2, type = "scatter", mode = "markers", name = 'Treshold 0.01', marker = list(size = twomonthplot$Transactions, opacity = 0.8)) %>%
  layout(xaxis = x, yaxis = y,title = 'two months deployment period')%>% 
  add_trace(data = twomonth, x = Period, y = ~Return, type = "scatter", mode = "markers", marker = list(size = twomonthplot$Transactions2),  name = 'Treshold 0.10')%>% 
  add_trace(data = twomonth, x = Period, y = ~Return3, type = "scatter", mode = "markers", marker = list(size = twomonthplot$Transactions3),name = 'Treshold 0.20')%>% 
  add_trace(data = twomonth, x = Period, y = ~Return4, type = "scatter", mode = "markers", marker = list(size = twomonthplot$Transactions4),  name = 'Treshold 0.50')

save(resultDF,file="resultDFms.Rda")
save(resultDFweek,file="resultDFweekms.Rda")
save(resultDFtwoweek,file="resultDFtwoweekms.Rda")
save(month,file="monthms.Rda")
save(twomonth,file="twomonthms.Rda")

load()



#################################################
################# save as pmml ##################
#################################################

# convert model to pmml
rf.pmml <- pmml(rf,name="AAPL Random Forest")
svm.pmml <- pmml(svm_model_after_tune,name="AAPL svm")

# save to file "iris_rf.pmml" in same workspace
saveXML(rf.pmml,"rf_AAPL.pmml")
saveXML(svm.pmml,"svm.pmml")





