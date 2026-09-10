# Building Energy Consumption Prediction and Streaming Analytics

## Overview
This project predicts 6-hour building energy consumption using meter readings, building metadata and weather data. The workflow includes large-scale data processing, feature engineering, machine learning model training and real-time streaming prediction.

## Tools Used
- Python
- PySpark
- Spark SQL
- Spark MLlib
- Kafka
- Spark Structured Streaming
- Parquet
- Matplotlib

## Dataset
The project uses building meter readings, building metadata and weather data. Raw data is not included due to dataset size and licensing restrictions.

## Methodology
1. Loaded meter, building and weather datasets using explicit schemas.
2. Aggregated hourly meter readings into 6-hour energy consumption windows.
3. Cleaned weather data using mean imputation.
4. Engineered features including building age, square footage, seasonal peak/off-peak flag and weather variables.
5. Trained and compared Random Forest and Gradient-Boosted Tree models using RMSLE.
6. Integrated the saved model into a Kafka and Spark Structured Streaming pipeline for real-time prediction.

## Key Results
- Created a feature table with over 1.42M rows.
- Predicted 6-hour building energy consumption using Spark MLlib.
- Compared Random Forest and Gradient-Boosted Tree models.
- Built a streaming prediction workflow using Kafka and Spark Structured Streaming.

## Skills Demonstrated
- Big data processing
- Feature engineering
- Machine learning pipelines
- Real-time streaming analytics
- Model evaluation
- Data engineering

## Repository Structure
notebooks/
├── building_energy_prediction.ipynb              # Batch feature engineering and ML model training
├── streaming_producer.ipynb                      # Kafka producer for simulated weather stream
├── spark_streaming_prediction.ipynb              # Spark Structured Streaming prediction pipeline
└── streaming_consumer_visualisation.ipynb        # Kafka consumer and visualisation workflow

## Notebooks

1. **building_energy_prediction.ipynb**  
   Builds the batch machine leøarning workflow, including data loading, cleaning, feature engineering, model training, evaluation and model saving.

2. **streaming_producer.ipynb**  
   Simulates streaming weather data and sends messages to Kafka topics.

3. **spark_streaming_prediction.ipynb**  
   Consumes streaming data using Spark Structured Streaming, applies the saved model and generates real-time energy predictions.

4. **streaming_consumer_visualisation.ipynb**  
   Reads prediction outputs and creates visual summaries for energy consumption analysis.