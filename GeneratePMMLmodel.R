## packages:
if(!require(e1071)) { install.packages("e1071"); library(e1071); }
if(!require(caret)) { install.packages("caret"); library(caret); }
if(!require(XML)) { install.packages("XML"); library(XML); }
if(!require(pmml)) { install.packages("pmml"); library(pmml); }
if(!require(randomForest)) { install.packages("randomForest"); library(randomForest); }
if(!require(chron)) { install.packages("chron"); library(chron); }
if(!require(plyr)) { install.packages("plyr"); library(plyr); }

#mydata <- read.csv("C:/Users/ceder/Flink/BatchStockData/AAPL_batch_big_R.csv")  # read csv file 
#mydata <- read.csv("C:/Users/ceder/Flink/BatchStockData/XOM_big_bigger2.csv")  # read csv file 

## JNJ:
#mydata <- read.csv("C:/Users/ceder/Flink/BatchStockData/JNJ_Big_0.01_2.csv")  # read csv file 
#mydata <- read.csv("C:/Users/ceder/Flink/BatchStockData/JNJ_Big_0.1.csv")  # read csv file 
#mydata <- read.csv("C:/Users/ceder/Flink/BatchStockData/JNJ_Big_0.2.csv")  # read csv file 
mydata <- read.csv("C:/Users/ceder/Flink/BatchStockData/JNJ_Big_0.5.csv")  # read csv file 

colnames(mydata) <- c("dateTime", "name", "lastPrice", "lastPriceLag",  "SMA_signal", "SMA_direction", "BB_signal", "BB_direction", "CCI_signal", "CCI_direction",
                      "stoch_signal", "stoch_direction", "RSI_signal", "RSI_direction", "MFI_signal", "moneyFlowIndex_direction",
                      "chaikin_signal", "chaikin_direction", "willR_signal", "williamsR_direction", "momentum")
mydata[,1] <- substr(mydata[,1], 0,19)
str(mydata)


dtparts <- t(as.data.frame(strsplit(mydata[,1],' ')))
row.names(dtparts) = NULL
thetimes <- chron(dates=dtparts[,1],times=dtparts[,2], format=c('y-m-d','h:m:s'))
class(mydata[,1])
mydata[,1] <-  thetimes

mydata <- mydata[do.call(order, mydata), ] 

## overview
table(mydata$momentum)
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

mydata$momentum <- factor(mydata$momentum)

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

## Aggregate
a <- ddply(mydata, "dateTime", tail, 1)[,1:3]

basetable <- merge(x = mydata, y = a, by = 1)

basetable$responseVariable <- factor(ifelse(basetable$lastPrice.x - basetable$lastPrice.y >= 0.2 , 1 , ifelse(basetable$lastPrice.y - basetable$lastPrice.x >= 0.2 , 2 , 0)))
check <- basetable[, c("lastPrice.x", "lastPrice.y", "responseVariable") ]

basetable <- basetable[, c(3, 5:21,24)]
str(basetable)
table(basetable$responseVariable)

## 389 minuten per dag, 1945 per week

## 1 week:
basetableTrain1 <- basetable[1:1945,]
basetableTest1 <- basetable[1945:2200,]
table(basetableTrain1$responseVariable)

## 2 weken:
basetableTrain2 <- basetable[1:3890,]
basetableTest2 <- basetable[3891:4091,]
table(basetableTrain2$responseVariable)



#################################################
################# Random Forest ################# 
#################################################

################# train the SVM, and make predictions ################# 
rf <-randomForest(responseVariable~.,data=basetableTrain2, ntree=100) 
print(rf)
plot(rf)

## evaluation of the model
(result <- confusionMatrix(predict(rf), basetableTrain2$responseVariable))

################# fine tune the RF model ################# 
mtry <- tuneRF(x = mydata, y = mydata$ResponseVariable, mtryStart = 5, ntreeTry = 5, stepFactor = 50, improve = 0.0001, plot = TRUE, trace = TRUE)

best.m <- mtry[mtry[, 2] == min(mtry[, 2]), 1]
print(mtry)
print(best.m)

################# Variable importance ################# 
(VI_rf=importance(rf))
varImpPlot(rf,type=2)

## encourage the model to make buy/sell predictions:
Pred.cutoff <- predict(rf,mydata[,1:16], cutoff = c(0.8,0.0000001,0.0000001))
(result <- confusionMatrix(Pred.cutoff, mydata[,17]))
################# Variable importance ################# 
(VI_rf=importance(rf))
varImpPlot(rf,type=2)



#################################################
################# save as pmml ################# 
#################################################

# convert model to pmml
rf.pmml <- pmml(rf,name="AAPL Random Forest")
svm.pmml <- pmml(svm_model_after_tune,name="AAPL svm")

# save to file "iris_rf.pmml" in same workspace
saveXML(rf.pmml,"rf_AAPL.pmml")
saveXML(svm.pmml,"svm.pmml")





