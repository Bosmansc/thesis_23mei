## packages:
if(!require(e1071)) { install.packages("e1071"); library(e1071); }
if(!require(caret)) { install.packages("caret"); library(caret); }
if(!require(XML)) { install.packages("XML"); library(XML); }
if(!require(pmml)) { install.packages("pmml"); library(pmml); }
if(!require(randomForest)) { install.packages("randomForest"); library(randomForest); }

#mydata <- read.csv("C:/Users/ceder/Flink/BatchStockData/AAPL_batch_big_R.csv")  # read csv file 
mydata <- read.csv("C:/Users/ceder/Flink/BatchStockData/XOM_big_bigger2.csv")  # read csv file 
mydata <- mydata[,5:21]
mydata[,17] <- factor(mydata[,17])
#mydataSmall <- mydata[1:500,]

colnames(mydata) <- c("SMA_signal", "SMA_direction", "BB_signal", "BB_direction", "CCI_signal", "CCI_direction",
                      "stoch_signal", "stoch_direction", "RSI_signal", "RSI_direction", "MFI_signal", "moneyFlowIndex_direction",
                      "chaikin_signal", "chaikin_direction", "willR_signal", "williamsR_direction", "ResponseVariable")
str(mydata)

table(mydata$ResponseVariable)
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


#######################################
################# SVM ################# 
#######################################

################# train the SVM, and make predictions ################# 
svm_model <- svm(ResponseVariable ~ ., data=mydata)
summary(svm_model)
pred <- predict(svm_model,as.matrix(mydata[1:16]))
table(pred,mydata[,17])

################# fine tune the svm model ################# 
## fine tune the SVM-method
x <- as.matrix(mydata[,1:16])
y <- mydata[,17]

svm_tune <- tune(method = svm, train.x = x,train.y = y, kernel = "linear", ranges = list( gamma=c(.5,1)))

print(svm_tune)

## train model after tuning
svm_model_after_tune <- svm(ResponseVariable ~ ., data=mydata, kernel="radial", cost=1000, gamma=0.5)
summary(svm_model_after_tune)
pred <- predict(svm_model_after_tune,x)

## evaluation of the model
(result <- confusionMatrix(pred, y))


#################################################
################# Random Forest ################# 
#################################################

################# train the SVM, and make predictions ################# 
rf <-randomForest(ResponseVariable~.,data=mydata, ntree=100) 
print(rf)
plot(rf)

## evaluation of the model
(result <- confusionMatrix(predict(rf), mydata[,17]))

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





