# justin.thomson@viome.com
#count basic info for 109.3 and 109.4 studies

library(readxl)
library(aws.s3)

#109.3
q109 = read.csv("/Users/justin/Downloads/1093ValidationStudyData/subject_metadata/Questionnaire_Blood_test_Measurement_ValidationStudy.csv")

# 26 questions, 1199 answers, 50 subjects, 50 of each sample type 

# answers: count number of cells not blank/NA
answerCount = sum(!is.na(q109[2:nrow(q109), 2:ncol(q109)]))
print(answerCount)

#109.4

countAnswersinDF = function(sht, startRow=2, startCol=2) {
  sum(!is.na(sht[startRow:nrow(sht), startCol:ncol(sht)]))
}

# take in excel questionnare and count total number of answers 
countAnswers = function(sheetLoc) {
  sht = read_excel((sheetLoc))
  cnt = countAnswersinDF(sht)
  
  print(paste0(cnt, " answers for sheet ", sheetLoc))
  cnt
}
#28 questions

b1q = "/Users/justin/Downloads/Phase2_Batch1/Questionnaire_Blood test_Measurement_phase2_1st batch (1).xlsx"
b1Count = countAnswers(b1q)
#2525


b2q = "/Users/justin/Downloads/Phase2_Batch2/Questionnaire_Blood test_Measurement_phase2_2nd batch.xlsx"
b2Count = countAnswers(b2q)
#5061

b3q = "/Users/justin/Downloads/Questionnaire_Blood test_Measurement_phase2_3rd batch.xlsx"
b3Count = countAnswers(b3q)

print(paste0("total answers for 109.4: ", b1Count+b2Count+b3Count))
#12662


#v112 has 240 participants, 48 questions with 11219 answers 
q112 = read.csv("/Users/justin/Downloads/v112_data.csv")
answerCount112 = sum(!is.na(q112[2:nrow(q112), 2:ncol(q112)]))
print(answerCount112)

count
#v128.1
handle128p1 = function() {
  bucket = "viome-studies"
  v128b1="/v128.1/OC_Metadata_AUS_1st_batch_100_samples.xlsx"
  v128b2="/v128.1/OC_Metadata_AUS_2nd_batch_30_samples.xlsx"
  v128b3="/v128.1/OC_Metadata_AUS_3rd_batch_50_samples.xlsx"
  v128b4="/v128.1/OC_Metadata_AUS_4th_batch_50_samples.xlsx"
  v128b5="/v128.1/OC_Metadata_AUS_5th_batch_100_OPC_samples.xlsx"
  
  b1=s3read_using(FUN=read_excel, bucket = bucket, object = v128b1)
  b2=s3read_using(FUN=read_excel, bucket = bucket, object = v128b2)
  b3=s3read_using(FUN=read_excel, bucket = bucket, object = v128b3)
  b4=s3read_using(FUN=read_excel, bucket = bucket, object = v128b4)
  b5=s3read_using(FUN=read_excel, bucket = bucket, object = v128b5)
  
  # total number of participants
  total_partcipants=nrow(b1)+nrow(b2)+nrow(b3)+nrow(b4)+nrow(b5)
  print(paste0("total 128.1 participants: ", total_partcipants))
  
  # 12 total questions. Some sheets have smaller amount
  # 
  total_answers = countAnswersinDF(b1, startRow = 1, startCol = 3) + countAnswersinDF(b2, startRow = 1, startCol = 3) + countAnswersinDF(b3, startRow = 1, startCol = 3)  + countAnswersinDF(b4, startRow = 1, startCol = 3) + countAnswersinDF(b5, startRow = 1, startCol = 3) 
  print(paste0("total number of 128.1 questions: 12, answers: ", total_answers))
}


