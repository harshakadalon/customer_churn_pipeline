# **Problem Formulation**

## **Business Problem**  
Customer churn, characterized by the voluntary or involuntary departure of clients, directly impacts business sustainability by diminishing revenue streams and escalating the costs associated with acquiring new customers. While customer acquisition is a fundamental business strategy, it is often resource-intensive and less effective than targeted retention efforts. To maintain competitive advantage, financial stability and to sustain long-term profitability, enterprises operating in industries such as finance, telecommunications, and e-commerce must adopt data-driven methodologies to anticipate and mitigate churn.  

## **Business Objectives**  
1. Establish a clear understanding of customer churn and its business implications.
2. Leverage machine learning to forecast customer churn and enable proactive decision-making.
3. Visualize the customer loss and customer lifetime value.  
4. Provide valuable insights to implement targeted retention strategies and reduce churn rates.
5. Automate data pipeline for efficient data processing.  

## **Key Data Sources and Attributes**  
The pipeline will integrate customer data from multiple sources:
1. **Customer Churn Data:**
  - Profile Data (customerID , gender, SeniorCitizen, Partner, Dependents)
  - Usage and Subscription Data (tenure, PhoneService, MultipleLines, InternetService, OnlineSecurity, OnlineBackup)
  - Target Data (Churn)
2. **Activity Logs**
  - Session durations
  - Pages visited
  - Time spent on each page
  - Actions taken  
3. **Data Quality Logs**
  - The Metric Count
  - The missing values
  - The datatype issues
  - The duplicate records   

## **Expected Outputs**  
1.	Clean datasets for exploratory data analysis
2.	Transformed features stored in Microsoft SQL server.
3.	Trained and versioned machine learning models to predict customer churn
4.	Automated workflow with comprehensive logging and monitoring at each step  

## **Evaluation Metrics**  
To assess model performance, the following metrics will be used:  
  - **Accuracy:** Measures how many predictions were correct overall.
  - **Precision:** Indicates how many of the predicted positives are actually correct.
  - **Recall:** Shows how well the model captures actual positives.
  - **F1 Score:** Balances precision and recall, especially useful for imbalanced data.
  - **ROC-AUC Curve:** Evaluates the model's ability to distinguish between classes across different thresholds.
