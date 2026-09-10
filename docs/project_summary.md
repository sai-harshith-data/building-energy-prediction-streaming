# Project Summary

This project demonstrates an end-to-end big data and machine learning workflow for building energy consumption prediction.

The project includes two main components:

1. **Batch machine learning pipeline**
   - Loads building, meter and weather datasets using PySpark
   - Aggregates hourly meter readings into 6-hour energy consumption windows
   - Cleans and imputes missing weather values
   - Engineers building, weather and seasonal features
   - Trains and evaluates Spark MLlib regression models

2. **Streaming prediction pipeline**
   - Simulates real-time weather data using Kafka producer logic
   - Uses Spark Structured Streaming to consume streaming records
   - Applies the saved model to generate real-time predictions
   - Stores and visualises prediction outputs

The project was adapted from university coursework into a cleaned portfolio version. Assignment instructions, raw datasets, model artifacts and student-identifying files are not included.