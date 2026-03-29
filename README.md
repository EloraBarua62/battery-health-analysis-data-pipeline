# Battery Health Analysis Project

## Introduction
Battery health monitoring is important because changes in voltage, temperature, and operating conditions can indicate potential battery problems over time. In this project, an end-to-end data pipeline was built to process both historical and real-time battery data by combining structured telemetry and unstructured BMS logs. The pipeline uses Azure services, including Azure Event Hub for streaming ingestion, and follows the Medallion architecture with Bronze, Silver, and Gold layers for raw storage, cleaning, and final analytics. After cleaning and transforming the data, the Gold layer produces battery health KPIs that are used for visualization, sharing, and a simple machine learning prediction task. The overall goal of the project is to create a reusable pipeline that can ingest new data with minimal changes and turn raw battery data into meaningful insights.


## Project Overview

### Project Goal
The main goal of this project was to build a complete battery health monitoring pipeline that can handle both old records and live data at the same time. I wanted to create a system that doesn't just store data, but actually processes it to find key performance indicators (KPIs) and uses Machine Learning to flag "High Risk" batteries. The final output is a cleaned, reliable data product that can be used for maintenance alerts.

### Technology Stack
To build this, I used several cloud and engineering tools:
* Azure Data Lake (ADLS Gen2): Used to store the data as it moves through the Bronze, Silver, and Gold layers.
* Azure Event Hub: Used as the entry point for real-time battery data streams.
* Databricks: My main workspace for running notebooks and managing the Spark clusters.
* PySpark: Used for all the heavy lifting, cleaning, and joining of large datasets.
* Delta Lake: This was used to ensure our tables are reliable and support ACID transactions.
* FastAPI: Added as a layer to allow other users or systems to access our battery results through an API.
* GitHub: Used for version control and to document the entire project history.

### Pipeline Summary
The pipeline follows a logical flow from raw data generation to final predictions. First, I used Generator Notebooks to create historical structured telemetry and unstructured BMS logs. For the real-time part, a dedicated notebook sends battery pings to Azure Event Hub, which are then caught by a Receiver Notebook that writes them directly into the Bronze layer. From there, the Silver Notebook takes over to clean the data, fix the alphanumeric errors I found in the logs, and join the different sources together. The Gold Notebook then aggregates this into daily health metrics. Finally, the ML Notebook runs a Logistic Regression model to classify which days are "High Risk." The whole system is supported by quality tests and a data contract to make sure the data stays accurate.

### Final Deliverables
Beyond just the code, this project provides a full set of data products:
* Data Contract: To define exactly what the battery data should look like.
* Quality Tests: Schema and data validation checks to prevent "garbage" data from entering the Gold layer.
* API Interface: A FastAPI file to expose battery health status to the web.
* Data Exports: Support for sharing results in both CSV and JSON formats.
* Visualizations: A set of scatter plots and line graphs showing the correlation between temperature and stability.



## Data Sources
To create a realistic monitoring environment, I used a mix of data types and ingestion methods. The project relies on four distinct streams of information that represent both the historical life of the battery and its current live status. By combining structured sensor readings with unstructured logs, the pipeline is able to provide a more complete picture of battery health than it would with just one source.


### Data Source Summary
| Source Name | Type | Format | Ingestion Mode | Main Fields | Purpose |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Historical Telemetry** | Structured | CSV-like | Batch (Notebook) | timestamp, voltage, temp | Long-term history |
| **Historical BMS Logs** | Unstructured | Text Logs | Batch (Notebook) | timestamp, log_message | Event context |
| **Real-time Telemetry** | Structured | JSON | Streaming (Event Hub) | timestamp, voltage, temp | Live monitoring |
| **Real-time BMS Logs** | Unstructured | Text Logs | Near-real-time | timestamp, log_message | Live events |


### Data Strategy
The backbone of this project is the combination of structured and unstructured data. The telemetry data gives us the hard numbers—like exactly how many volts the battery is pushing—while the BMS logs provide the "story" behind those numbers, such as error codes or state changes. I designed the system to handle both historical batch data, which is used to train the ML model, and real-time streaming data via Azure Event Hubs to simulate a live production environment.


### Data Context & Limitations
It is important to note that the data used in this pipeline is programmatically generated rather than collected from a physical laboratory battery. While this allowed me to simulate specific failure patterns (like high-heat spikes) to test the model, it does come with some limitations. The generated BMS logs are smaller in volume compared to the telemetry pings, and the real-time stream is a simulation of a single battery unit rather than a massive fleet. However, the schema was designed to be identical to real-world industrial BMS outputs to ensure the pipeline logic remains valid for actual hardware in the future.


## Pipeline Architecture


<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/3ba10d2d-d774-44a1-8e37-59a612d22f42" />

### Architecture Overview
The architecture of this project is built on a Medallion Design using Azure Databricks and Azure Data Lake Storage (ADLS Gen2). The goal was to create a reliable flow where raw, "dirty" battery data from multiple sources is gradually refined into a high-quality data product. The pipeline handles structured telemetry and unstructured logs simultaneously, moving them through Bronze, Silver, and Gold layers before they reach final components like the ML model, FastAPI, and automated quality tests.

### Bronze Layer: Raw Ingestion
The Bronze layer acts as our landing zone for all incoming data in its original, unmodified form. This is crucial for data lineage because it allows us to re-process data if the business logic changes later.
* Batch Ingestion: Historical battery telemetry and unstructured BMS logs are generated via notebooks and saved directly here.
* Streaming Ingestion: For the real-time simulation, a receiver notebook captures JSON events from Azure Event Hub and writes them to Bronze as they arrive.
* Integrity: At this stage, no cleaning is performed. The data still contains the "alpha" errors and null values that characterize raw sensor pings.

### Silver Layer: Cleaning & Integration
In the Silver layer, the raw data is transformed into a clean, queryable format using PySpark.
* Standardization: I implemented logic to cast data types (e.g., ensuring voltage is a Double) and standardized all timestamps to ISO 8601.
* Validation & Deduplication: The pipeline filters out corrupted records and removes any duplicate pings caused by sensor glitches.
* Joining: This is where the magic happens—I joined the structured telemetry with the unstructured BMS logs using timestamps. This creates an integrated dataset where sensor readings are enriched with contextual log messages.

### Gold Layer: Analytics & KPIs
The Gold layer is the "business" layer where data is aggregated into high-level metrics. Instead of millions of individual pings, this layer stores daily summaries used for reporting.
* Key Metrics: I calculated specific KPIs like voltage_stability, peak_temp, and thermal_stress_events.
* Reliability: To ensure these KPIs are trustworthy, I implemented a Data Contract (data_contract.json) that defines the expected schema and requirements for the Gold tables.

### Downstream Components & Data Products
Once the data reaches the Gold layer, it powers several supporting components:
* Machine Learning: An ML notebook pulls from the Gold layer to predict high-risk days using Logistic Regression.
* Automated Testing: I created a testing suite (test_schema.py and test_quality.py) that uses PySpark to verify that the Silver and Gold layers meet our quality standards.
* Data Sharing & API: A dedicated export notebook (09_data_sharing_export.ipynb) saves the results as CSV and JSON files for external use, while a FastAPI application (app.py) provides a web interface to query the battery health status.
* Visualization: Final analytics are presented through scatter plots and line graphs to visualize the relationship between temperature and stability.

### Pipeline Flow & Lineage
The lineage of this system is a straight line from source to insight: Raw Sources → Event Hub → Bronze → Silver → Gold → ML/API/Testing. This structured flow ensures that any "High Risk" prediction can be traced back through the cleaned Silver data all the way to the original raw logs in Bronze.



## Medallion Architecture
My pipeline follows the industry-standard Medallion Architecture, moving battery data through three specific layers—Bronze, Silver, and Gold—to transform it from a raw state into a refined, analytics-ready data product.

A. Bronze Layer: The Raw Landing Zone
The Bronze layer acts as the initial landing area where all data is stored in its original, unmodified form. No transformations are performed here to ensure complete traceability back to the source.
Content: This layer contains historical structured telemetry, historical unstructured BMS logs, and the real-time battery data captured from Azure Event Hub.
Purpose: It serves as a permanent record of exactly what the sensors reported, including any "dirty" data or errors, which is essential if we ever need to re-process the pipeline from scratch.

B. Silver Layer: The Cleaned & Standardized Zone
In the Silver layer, the data is cleaned, validated, and standardized to make it usable for engineering tasks. This is the most technical part of the pipeline where the "noise" is removed.
Processing: I implemented PySpark logic to parse timestamps into ISO 8601 format and cast voltage and temperature readings into numeric types (Doubles).
Validation: This layer handles deduplication of records and filters out inconsistent values or "alpha" noise in the sensor columns.
Integration: Most importantly, this is where the cleaned telemetry data is joined with the BMS log information to create a single, unified dataset.

C. Gold Layer: The Analytics & KPI Zone
The Gold layer is the final stage, containing high-level, business-ready outputs. Instead of individual pings, it stores summarized metrics designed for decision-making.
Output Metrics: This layer produces our final KPIs, including avg_voltage, voltage_stability, peak_temp, and counts for thermal_stress_events.
Usage: These tables are the "source of truth" used to power the FastAPI, generate the Visualizations, and provide the features for the ML Model to predict high-risk days.

### Why this architecture is useful
Using a layered design was critical for this project because it separates the "messy" ingestion process from the final analytics. It made debugging much easier—if a calculation in the Gold layer looked wrong, I could simply check the Silver layer to see if the cleaning logic was correct. Additionally, this structure allows the pipeline to handle both batch and real-time inputs seamlessly, ensuring that the ML model always trains on the cleanest possible data.


## Processing and Cleaning
The raw battery telemetry and BMS logs were not directly ready for analytics when they first landed in the Bronze layer. I used the Silver and Gold stages to standardize, clean, and integrate these different sources into a unified structure that could support Machine Learning and API access.

A. Processing in the Silver Layer
The primary goal of the Silver layer was to transform raw, unstructured inputs into a clean, tabular format. My Silver table consists of four standardized columns: timestamp, voltage, temperature, and logs.
* Standardization: I implemented PySpark logic to ensure all timestamps were parsed into a consistent format and cast the sensor readings (voltage and temperature) into numeric types.
* Integration: In this stage, the battery measurements were combined with their related BMS log information. This created an integrated dataset where every sensor reading is enriched with its operational context.
* Initial Cleaning: The final Silver output was processed to ensure that these four core columns contained no missing values, providing a solid foundation for the next stage of aggregation.

B. Cleaning and Aggregation in the Gold Layer
Once the row-level data was cleaned in Silver, it was aggregated into a Gold-level KPI table. This step shifted the project from "raw pings" to "daily health indicators."
* KPI Generation: I grouped the data by the reporting date to calculate daily metrics including average voltage, voltage stability, and peak temperature.
* Event Analysis: I also extracted counts for specific incidents, such as total voltage sags and thermal stress events, along with the average event impact derived from the integrated log data.
* A Note on Data Quality: While the Silver layer successfully cleaned the raw inputs, it is important to note that 12 missing values remained in the derived voltage_stability metric within the Gold layer. Acknowledging these small gaps ensures the report is an accurate reflection of the generated dataset's limits.

C. Final Preparation for Prediction
The final stage of the cleaning pipeline involved reducing the Gold dataset into a refined format specifically for the Machine Learning model.
* Feature Selection: Not all columns were needed for the AI; I selected the most impactful features—voltage stability and peak temperature—as the primary input variables.
* Prediction Output: The resulting prediction table contains the reporting date, these selected features, the actual risk label, and the model's final prediction. This ensures the entire flow—from a raw log in Bronze to a "High Risk" flag in the prediction layer—is fully documented and traceable.



## Data Product
### Final Data Product
The true data product of this project is not the raw sensor pings in Bronze or the cleaned rows in Silver. Instead, it consists of two high-value datasets designed for immediate use:
* Gold Battery KPI Table: A daily summary featuring avg_voltage, voltage_stability, peak_temp, and event counts like thermal_stress_events.
* ML Prediction Table: A refined output containing report_data, the key features used for modeling, and the final prediction (Normal vs. High Risk).
These tables are the "source of truth" for the entire project, optimized for fast querying, clear visualization, and easy interpretation by maintenance teams.

### Data Contract
To ensure these products remain reliable, I implemented a simple Data Contract (data_contract.json). This contract acts as a formal agreement that defines the structure of our Gold and Prediction layers. It specifies:
* Dataset Names: Clearly identifying the gold_layer_notebook and battery_health_prediction sets.
* Schema Requirements: Listing every required column name and its specific data type (e.g., Timestamp, Double, Integer).
* Constraints: Defining which fields are mandatory (True) and which are optional, preventing "broken" data from being served to users.

### API Access
To make this data accessible outside of Databricks, I developed a functional API layer using FastAPI (app.py). This allows external applications to programmatically request the latest battery health status without needing to understand Spark or SQL. The API includes endpoints to fetch the latest KPI summaries and sample predictions, moving the project from a static notebook into a live, interactive service.

### Testing and Validation
A data product is only useful if it is accurate. I created an automated testing suite to validate the product before it is shared:
* Schema Testing (test_schema.py): Automatically verifies that all required columns are present in the Silver and Gold layers.
* Quality Testing (test_quality.py): Runs checks to ensure that critical fields like timestamp are never null and that sensor values (voltage and temperature) fall within realistic physical ranges.

### External Sharing
Finally, I added a dedicated Export Notebook (data_sharing_export.ipynb) to handle the "Data Sharing" requirement. This allows the final data product to be exported as CSV or JSON files. By providing these standard formats, the battery insights can be easily imported into other tools like Excel, Power BI, or external reporting systems, ensuring the data is useful even for non-technical stakeholders.


## Analytics and Results
The analysis was performed on the aggregated Gold dataset, using key features like voltage_stability and peak_temp. The purpose of these visualizations is to validate the health of the battery fleet and to observe how the Logistic Regression model interprets risk.

A. Average Peak Temperature and Voltage Stability Over Time
This line graph tracks the two most critical KPIs from the Gold layer across the project timeline (report_data). It is designed to show the "heartbeat" of the battery system.
#### Observations:
* Steady Baselining: The AVG(peak_temp) remains remarkably consistent, mostly hovering around the 28°C to 29°C mark. This indicates that the battery system is generally operating within safe thermal limits.
* Stability Fluctuations: Unlike temperature, the AVG(voltage_stability) shows significant day-to-day variance. There are visible "dips" where stability drops toward zero, followed by spikes.
* Correlation Check: The graph demonstrates that the Gold layer is successfully producing time-series data, allowing us to see that voltage fluctuations happen even when temperatures are stable.

<img width="738" height="400" alt="image" src="https://github.com/user-attachments/assets/88af02b8-1885-43a1-a2ed-7b00d48ddd6e" />





B. Correlation: Peak Temperature vs. Voltage Stability
This scatter plot is the primary tool for understanding the "physics" of our data. By plotting the two variables against each other, we can see if heat is directly causing instability.
#### Observations:
* Concentrated Cluster: Most of the data points are tightly clustered between 27°C and 30°C. This confirms the battery spends most of its life in a "normal" state.
* Positive Trend: There is a visible, though slight, upward trend. As the temperature moves toward the 30°C mark, we see higher values for voltage stability (meaning more variance).
* Model Insight: The scatter plot shows that the model has to work with a very narrow range of data. The high-risk labels are likely triggered at the far right of this cluster where thermal stress is highest.

<img width="738" height="400" alt="image" src="https://github.com/user-attachments/assets/39d38735-ef75-461c-9d6d-7fa2a015b419" />




C. Risk Distribution and Prediction Analysis
This visualization compares the actual is_high_risk_day labels with the model's prediction results.
#### Observations:
* Binary Classification: The chart clearly separates days into "0" (Normal) and "1" (High Risk).
* Frequency of Risk: According to the Visualization.csv data, the pipeline identifies a significant number of days where the combination of thermal and voltage metrics warrants a risk flag.
* Model Accuracy: With an AUC score of 0.62, the visualization shows that the model is making logical guesses based on the Gold features, though it still misses some edge cases where stability drops without a corresponding heat spike.

<img width="738" height="400" alt="image" src="https://github.com/user-attachments/assets/638432a2-bb99-4925-9ad0-56b33a7df18a" />




### Results Summary
The final Gold layer successfully produced a robust set of daily battery KPIs. By integrating the telemetry and logs, I was able to move beyond raw numbers and create a "Data Product" that reveals real patterns. The visualizations confirm that temperature is a leading indicator of voltage instability, and while the current ML model is simple, the pipeline provides a clean, automated foundation for more advanced predictive maintenance in the future.

### Limitations
While the pipeline produces usable outputs, the current dataset is relatively small. The narrow temperature range in the generated data means the ML model doesn't have many "extreme" failure cases to learn from. In a real-world scenario, further feature engineering—like calculating "cumulative heat exposure"—would likely improve the prediction accuracy. The visualizations confirm that the Gold layer correctly tracks battery health over time, showing a visible link between temperature peaks and voltage instability. However, because the data is programmatically generated, the variety of log messages is limited. The ML model achieved an AUC of 0.62, which proves the pipeline logic works, though more complex feature engineering would be needed for a real-world fleet.



## Discussion and Conclusion
### Overall Outcome
The project successfully delivered an end-to-end battery health monitoring pipeline. By combining historical batch data (telemetry and logs) with real-time JSON streams via Azure Event Hub, I built a system that moves data from a raw, "dirty" state into a refined "Gold" dataset. The final output provides clear daily KPIs and a Machine Learning model that identifies potential high-risk days.

### Technical Achievements
* Medallion Architecture: Successfully implemented Bronze, Silver, and Gold layers to separate raw ingestion from final analytics.
* Real-time Ingestion: Used Azure Event Hub to simulate a live sensor environment.
* Data Quality: Built a production-grade product using a Data Contract (data_contract.json), automated tests (test_schema.py, test_quality.py), and a FastAPI layer (app.py) for external access.

### Future Work
Future versions could scale to monitor multiple battery units, implement more advanced "State of Health" (SoH) metrics in the ML model, and expand the API to support real-time maintenance alerts.
