InsightFlow
AWS-native analytics and operational data platform integrating:

Close CRM
Calendly
Wistia
Architecture
[Architecture Diagram Screenshot]

AWS Services
API Gateway
Lambda
S3
Glue
Athena
SQS
Step Functions
SNS
Streamlit
Analytics Workflow
CRM / Calendly / Wistia ↓ Bronze ↓ Silver ↓ Gold ↓ Athena ↓ Streamlit Dashboard

CRM Operational Workflow
CRM Event ↓ SQS Delay Queue ↓ crm_start_enrichment_workflow ↓ Step Functions ↓ 15 Minute Wait ↓ crm_owner_enrichment ↓ SNS Notification

Dashboard
Executive Overview
CRM Analytics
Wistia Analytics
Calendly Analytics
Marketing Funnel Analysis
Documentation
See:

docs/InsightFlow_System_Design.docx
docs/InsightFlow_system_design.drawio
