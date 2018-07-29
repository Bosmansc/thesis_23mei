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

## delete first 100 rows bc variables are not correctly calculated by then
mydata <- mydata[-c(1:100),]

colnames(mydata) <- c("dateTime", "name", "lastPrice", "lastPriceLag", "SMA10", "SMA100", "SMA_signal", "SMA_direction","BB_lowerbound", "BB_upperbound", "BB_middlebound", "BB_signal", "BB_direction","CCI", "CCI_signal", "CCI_direction",
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

# ## set resonse var
# 
# basetable$responseVariable <- factor(ifelse(basetable$lastPrice.x - basetable$lastPrice.y >= 0.2 , 2 , ifelse(basetable$lastPrice.y - basetable$lastPrice.x >= 0.2 , 1 , 0)))
# check <- basetable[, c("dateTime", "lastPrice.x", "lastPrice.y", "responseVariable") ]
# checkPredictions <- basetable[, c("dateTime", "lastPrice.x", "lastPrice.y", "responseVariable") ]
# 
# ## 389 minuten per dag, 1945 per week
# nrow(basetable)/389/5
# ## 1 week:
# basetableTrain1 <- basetable[1:1945,]
# basetableTest1 <- basetable[1945:2200,]
# table(basetableTrain1$responseVariable)
# 
# ## 2 weken:
# 
# basetableTrain2 <- basetable[1:3890,]
# basetableTest2 <- basetable[3891:5000,]
# table(basetableTrain2$responseVariable)
# 
# ## 80/20:
# 
# t <- round(0.8*(nrow(basetable)),0)
# 
# basetableTrain3 <- basetable[(1:t),]
# basetableTest3 <- basetable[-(1:t),]
# table(basetableTrain3$responseVariable)
# 
# checkPredictionsTest <- basetableTest3[, c("dateTime", "lastPrice.x", "lastPrice.y", "responseVariable") ]
# returnTable <- basetableTest2[, c("dateTime", "lastPrice.x", "lastPrice.y", "responseVariable") ]
# 
# ## only select explaining variables
# baseTable <- basetable[, c(3,5:31,34)]
# basetableTrain2 <- basetable[, c(3,5:31,34)]
# basetableTrain3 <- basetableTrain3[, c(3,5:31,34)]
# basetableTest2 <- basetableTest2[, c(3,5:31,34)]
# basetableTest1 <- basetableTest1[, c(3,5:31,34)]
# str(basetable)
# table(basetable$responseVariable)
# 
# 
# #################################################
# ################# Random Forest #################
# #################################################
# 
# ################# train the rf, and make predictions #################
# rf <-randomForest(responseVariable~.,data=basetableTrain3[,1:29], ntree=100)
# print(rf)
# plot(rf)
# 
# ## evaluation of the model
# (result <- confusionMatrix(predict(rf), basetableTrain3$responseVariable))
# 
# ################# test model on new test data #################
# testPred <- predict(rf,newdata=basetableTest3)
# (resultTest <- confusionMatrix(testPred, basetableTest3$responseVariable))
# varImpPlot(rf,type=2)
# 
# 
# # ################# fine tune the RF model #################
# # #mtry <- tuneRF(x = mydata, y = mydata$ResponseVariable, mtryStart = 5, ntreeTry = 5, stepFactor = 50, improve = 0.0001, plot = TRUE, trace = TRUE)
# # 
# # best.m <- mtry[mtry[, 2] == min(mtry[, 2]), 1]
# # print(mtry)
# # print(best.m)
# # 
# # ################# Variable importance #################
# # (VI_rf=importance(rf))
# # 
# # 
# # ## encourage the model to make buy/sell predictions:
# # Pred.cutoff <- predict(rf,mydata[,1:16], cutoff = c(0.8,0.0000001,0.0000001))
# # (result <- confusionMatrix(Pred.cutoff, mydata[,17]))
# ################# Variable importance #################
# (VI_rf=importance(rf))
# varImpPlot(rf,type=2)
# 
# 
# 
# ######################################################
# ################# return generation ##################
# ######################################################
# 
# 
# ## otpimal return generation:
# check <- check[!(check$responseVariable == 0),]
# na.omit(check)
# check$lag <- as.numeric(lag(check$responseVariable))
# check$responseVariableN <- as.numeric(check$responseVariable)
# 
# check$returnPrice <- ifelse(check$responseVariableN > check$lag, check$lastPrice.x, ifelse(check$responseVariableN < check$lag, -check$lastPrice.x, 0))
# check <- na.omit(check)
# 
# check[nrow(check),"returnPrice"] <- ifelse( check[nrow(check),"responseVariableN"] == 2 , check[nrow(check),"lastPrice.x"] , 0)
# sum(check$returnPrice,na.rm=TRUE)
# 
# ## return based on predictions (full data)
# rf <-randomForest(responseVariable~.,data=baseTable, ntree=100)
# checkPredictions$prediction <- predict(rf)
# 
# checkPredictions <- checkPredictions[!(checkPredictions$prediction == 0),]
# checkPredictions$lag <- as.numeric(lag(checkPredictions$prediction))
# checkPredictions$prediction <- as.numeric(checkPredictions$prediction)
# 
# checkPredictions$returnPrice <- ifelse(checkPredictions$prediction > checkPredictions$lag, checkPredictions$lastPrice.x, ifelse(checkPredictions$prediction < checkPredictions$lag, -checkPredictions$lastPrice.x, 0))
# checkPredictions <- na.omit(checkPredictions)
# 
# checkPredictions[nrow(checkPredictions),"returnPrice"] <- ifelse( checkPredictions[nrow(checkPredictions),"prediction"] == 2 , checkPredictions[nrow(checkPredictions),"lastPrice.x"] , 0)
# sum(checkPredictions$returnPrice,na.rm=TRUE)
# 
# 
# ## return based on predictions on test data 2
# rf <-randomForest(responseVariable~.,data=basetableTrain2, ntree=100)
# returnTable$prediction <- predict(rf,newdata=basetableTest2)
# 
# returnTable <- returnTable[!(returnTable$prediction == 0),]
# returnTable$lag <- as.numeric(lag(returnTable$prediction))
# returnTable$prediction <- as.numeric(returnTable$prediction)
# 
# returnTable$returnPrice <- ifelse(returnTable$prediction > returnTable$lag, returnTable$lastPrice.x, ifelse(returnTable$prediction < returnTable$lag, -returnTable$lastPrice.x, 0))
# returnTable <- na.omit(returnTable)
# 
# #checkPredictionsTest[nrow(checkPredictionsTest),"returnPrice"] <- ifelse( checkPredictionsTest[nrow(checkPredictionsTest),"prediction"] == 2 , checkPredictionsTest[nrow(checkPredictionsTest),"lastPrice.x"] , 0)
# sum(returnTable$returnPrice,na.rm=TRUE)
# 
# 
# returnTable <- returnTable[!(returnTable$returnPrice == 0),]
# 
# 
# 
# ## return based on predictions on test data
# rf <-randomForest(responseVariable~.,data=basetableTrain3, ntree=100)
# checkPredictionsTest$prediction <- predict(rf,newdata=basetableTest3)
# 
# checkPredictionsTest <- checkPredictionsTest[!(checkPredictionsTest$prediction == 0),]
# checkPredictionsTest$lag <- as.numeric(lag(checkPredictionsTest$prediction))
# checkPredictionsTest$prediction <- as.numeric(checkPredictionsTest$prediction)
# 
# checkPredictionsTest$returnPrice <- ifelse(checkPredictionsTest$prediction > checkPredictionsTest$lag, checkPredictionsTest$lastPrice.x, ifelse(checkPredictionsTest$prediction < checkPredictionsTest$lag, -checkPredictionsTest$lastPrice.x, 0))
# checkPredictionsTest <- na.omit(checkPredictionsTest)
# 
# #checkPredictionsTest[nrow(checkPredictionsTest),"returnPrice"] <- ifelse( checkPredictionsTest[nrow(checkPredictionsTest),"prediction"] == 2 , checkPredictionsTest[nrow(checkPredictionsTest),"lastPrice.x"] , 0)
# sum(checkPredictionsTest$returnPrice,na.rm=TRUE)


####################################################
################# return intraday ##################
####################################################

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

thresholds <- c(0.01,0.05,0.1,0.2,0.5,0.75)
test <- c(0.01, 0.1, 0.2, 0.5)

day <- 389
week <- 5*day

for (j in test){
  
  for(i in seq(from=week, 20000, by=389)){
    
  duration <- i
  durationTest <- i *1.2
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
  rf <-randomForest(responseVariable~.,data=basetableTrain2, ntree=100) 
  varImpPlot(rf,type=2)
  
  confusionMatrix(predict(rf,newdata=basetableTest2) , returnTable$responseVariable)
  
  returnTable$prediction <- predict(rf,newdata=basetableTest2) 
  (result <- confusionMatrix(returnTable$prediction, returnTable$responseVariable))
  
  # # Algorithm Tune (tuneRF)
  # rftuned <- tuneRF(x = basetableTrain2[1:28], y = basetableTrain2$responseVariable, mtryStart = 20, improve = 0.0000001,  doBest = TRUE )
  # 
  # returnTable$prediction <- predict(rftuned,newdata=basetableTest2) 
  # (result <- confusionMatrix(returnTable$prediction, returnTable$responseVariable))
  
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
  (averageDailyReturn <- totalReturn/numberOfDays)
  
  
  if(j == 0.01){
    ## result for-loop:  
    resultDF[i,1] <- i
    resultDF[i,2] <- averageDailyReturn
    resultDF[i,3] <- countSell 
  }
  if(j == 0.1){
    ## result for-loop:  
    resultDF[i,4] <- i
    resultDF[i,5] <- averageDailyReturn
    resultDF[i,6] <- countSell 
  }
  if(j == 0.2){
  ## result for-loop:  
  resultDF[i,7] <- i
  resultDF[i,8] <- averageDailyReturn
  resultDF[i,9] <- countSell 
  }
  if (j == 0.5){
    ## result for-loop:  
    resultDF[i,10] <- i
    resultDF[i,11] <- averageDailyReturn
    resultDF[i,12] <- countSell 
  } else {
    
    resultDF[i,13] <- i
    resultDF[i,14] <- averageDailyReturn
    resultDF[i,15] <- countSell 
    
  }
  
  cat(i ," ", j, "\n")
  cat(averageDailyReturn , "\n")
  cat("Transactions " , as.numeric(countSell), "\n")
  cat("\n")
  
  ## remove for next loop
  rm(baseTable)
  rm(returnTable)
  rm(basetableTrain2)
  rm(basetableTest2)
  
    
  }
}

resultDF <- na.omit(resultDF)
sum(resultDF$Return)
max(resultDF$Return)
min(resultDF$Return)

sum(resultDF$Return2)
max(resultDF$Return2)
min(resultDF$Return2)

sum(resultDF$V8)
max(resultDF$V8)
min(resultDF$V8)


## plotly
f <- list(
  
  size = 13,
  color = "#7f7f7f"
)
x <- list(
  title = "Length of the Training Period",
  titlefont = f
)
y <- list(
  title = "Average Daily Return",
  titlefont = f
  #, range = c(-0.002,0.005)
)

resultDF$color <- cut(resultDF$Return, breaks = c(-1000, 0, 100000), labels = c("red", "darkgreen"))

plot_ly( data = resultDF, x = ~Period, y = ~Return2, type = "scatter", mode = "markers") %>%
  layout(xaxis = x, yaxis = y)%>% 
  add_trace(data = resultDF, x = ~Period, y = ~Return, type = "scatter", mode = "markers", colors  = pal)






#######################################
################# save as pmml ##################
#################################################

# convert model to pmml
rf.pmml <- pmml(rf,name="AAPL Random Forest")
svm.pmml <- pmml(svm_model_after_tune,name="AAPL svm")

# save to file "iris_rf.pmml" in same workspace
saveXML(rf.pmml,"rf_AAPL.pmml")
saveXML(svm.pmml,"svm.pmml")





