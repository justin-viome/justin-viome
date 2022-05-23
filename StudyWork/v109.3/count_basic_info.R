# justin.thomson@viome.com
#count basic info for 109.3 and 109.4 studies

library(readxl)

#109.3
q109 = read.csv("/Users/justin/Downloads/1093ValidationStudyData/subject_metadata/Questionnaire_Blood_test_Measurement_ValidationStudy.csv")

# 26 questions, 1199 answers, 50 subjects, 50 of each sample type 

# answers: count number of cells not blank/NA
answerCount = sum(!is.na(q109[2:nrow(q109), 2:ncol(q109)]))
print(answerCount)

#109.4

# take in excel questionnare and count total number of answers 
countAnswers = function(sheetLoc) {
  sht = read_excel((sheetLoc))
  cnt = sum(!is.na(sht[2:nrow(sht), 2:ncol(sht)]))
  
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


