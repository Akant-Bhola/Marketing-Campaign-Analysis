# Databricks notebook source
# MAGIC %md
# MAGIC # Marketing Campaign Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executive summary
# MAGIC
# MAGIC **Dataset:** ~200K marketing campaign records (2021–2022).
# MAGIC
# MAGIC **Objective:** Evaluate ROI, cost efficiency, conversion behavior, channel performance, and identify underperforming high-spend campaigns.
# MAGIC
# MAGIC **High-level insight:** Most campaigns land in Low ROI, but strong performance pockets exist across Website, Email, and Display; especially for Women 35–44 and Mandarin/Website combinations.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data overview
# MAGIC
# MAGIC **Key columns:** Campaign_ID, Company, Campaign_Type, Channel_Used, Duration (kept as string categories), Conversion_Rate, Acquisition_Cost (int), ROI, Clicks, Impressions, CTR, Cost_per_Conversion, Engagement_Score, ROI_Category.
# MAGIC
# MAGIC **Methods**
# MAGIC
# MAGIC - Loaded Unity Catalog table: workspace.default.marketing_campaign_dataset
# MAGIC
# MAGIC - Cleaned acquisition cost; standardized categorical columns.
# MAGIC  
# MAGIC - Performed SQL analysis using Spark SQL magic.
# MAGIC  
# MAGIC - Created visual analyses: scatterplots, heatmaps, bar charts, and outlier detection.
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

#Data extraction
from pyspark.sql.functions import regexp_replace, regexp_extract, col, round, when, sum as spark_sum

df = spark.table("workspace.default.marketing_campaign_dataset")
display(df)

# COMMAND ----------

# Check schema and row count
df.printSchema()
df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Data Cleaning
# MAGIC
# MAGIC - Removed symbols from Acquisition_Cost → cast to integer.
# MAGIC
# MAGIC - Retained Duration as strings (“15 days”, “30 days”, “45 days”, “60 days”).
# MAGIC
# MAGIC - Created KPIs (CTR, Cost_per_Conversion) and ROI Categories.
# MAGIC
# MAGIC - Confirmed no remaining nulls.
# MAGIC
# MAGIC ---

# COMMAND ----------

# Acquisition_Cost: remove $ and , → cast to double → cast to int
df = df.withColumn(
    "Acquisition_Cost",
    regexp_replace(col("Acquisition_Cost"), "[$,]", "").cast("double").cast("int")
)

# Check schema 
df.printSchema()

# Count nulls in each column
null_counts = df.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])

display(null_counts)

# COMMAND ----------

# Cost per Conversion
df = df.withColumn("Cost_per_Conversion", round(col("Acquisition_Cost") / (col("Conversion_Rate") * col("Impressions")),2))

# CTR in percentage
df = df.withColumn("CTR", round((col("Clicks") / col("Impressions"))*100,2))

# ROI Category
df = df.withColumn(
    "ROI_Category",
    when(col("ROI") >= 7, "High")
    .when(col("ROI") >= 5, "Medium")
    .otherwise("Low")
)

# Verify new columns
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key findings
# MAGIC
# MAGIC #### ROI & Categories
# MAGIC
# MAGIC * **ROI category counts:** High = **1,709**, Medium = **3,357**, Low = **4,934**.
# MAGIC * **Observation:** Although Low ROI is the majority, the High ROI group is large enough to justify focused scaling experiments on those campaigns.
# MAGIC
# MAGIC #### Reach & Engagement
# MAGIC
# MAGIC * **Top impressions by channel:** Social Media (avg ~5,567) > Display > Email > Search (avg ~5,488).
# MAGIC * **Top clicks:** Display campaigns generated the highest average clicks.
# MAGIC * **CTR:** Display and Website-driven campaigns show higher CTR (Display: high CTR; Website for specific audiences is strong).
# MAGIC
# MAGIC #### Conversion & Audience
# MAGIC
# MAGIC * **Best converting audience:** Women 35–44 on Website and Email (Conversion Rate ~0.087 and ~0.086 respectively).
# MAGIC * **Lowest observed:** Women 25–34 on Facebook (~0.076).
# MAGIC * **Insight:** Prioritize creative & personalization for Women 35–44; revisit creative/offer for Women 25–34 on Facebook.
# MAGIC
# MAGIC #### Cost Efficiency
# MAGIC
# MAGIC * **Acquisition cost by Customer Segment:** Foodies segment shows the highest acquisition cost (12,525); Tech enthusiasts lowest (~12,480).
# MAGIC
# MAGIC * **Cost per Conversion by ROI & Campaign Type:**
# MAGIC
# MAGIC   * **High ROI:** Display has highest cost per conversion (~69.63).
# MAGIC   * **Medium ROI:** Display again highest (~62.21).
# MAGIC   * **Low ROI:** Search shows highest cost per conversion (~65.37).
# MAGIC * **Interpretation:** Display often drives scale and ROI but at higher acquisition cost; some Display campaigns are high-value but expensive, test optimizing creative or targeting to lower cost-per-conversion.
# MAGIC
# MAGIC #### Time trends
# MAGIC
# MAGIC * **Monthly ROI:** Peak in April 2021 (5.14), trough in December (~4.98).
# MAGIC * **Engagement Score:** Peak March 2021 (5.65), lowest in May 2021 (~5.27).
# MAGIC * **Action:** Align major spend increases to months with historical performance lift and run A/B tests in lower-performing months.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Recommendations
# MAGIC
# MAGIC 1. **Scale winners, test to reduce cost:** For channels/campaigns with High ROI but high Cost per Conversion (e.g., some Display campaigns), run creative and targeting A/B tests focused on lowering acquisition cost while preserving conversion quality.
# MAGIC 2. **Prioritize audience: Women 35–44** on Website and Email — increase targeted spend, personalized creatives, and dedicated landing pages.
# MAGIC 3. **Optimize Search** for low-ROI / high cost-per-conversion campaigns — refine keywords, landing pages, and bidding strategies.
# MAGIC 4. **Seasonality plan:** Increase tests and budget shifts into April-based windows where ROI historically peaks; prepare mitigation campaigns for low months (Dec/May).
# MAGIC 5. **Segment-level experiments:** For the Foodies segment (highest acquisition cost), run a micro-campaign testing offer variations and measure ROI lift before full reallocation.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced SQL Analysis & Visual Insights
# MAGIC
# MAGIC This section summarizes four advanced SQL analyses conducted on the `marketing_campaigns` dataset to identify outlier behavior, channel–language strengths, segment-level ROI efficiency, and underperforming high-reach campaigns. Each analysis is paired with recommended visualizations and key insights generated from the results.
# MAGIC
# MAGIC ---

# COMMAND ----------

# Create a temporary SQL view for SQL queries
df.createOrReplaceTempView("marketing_campaigns")


# COMMAND ----------

# MAGIC %md
# MAGIC #### High-Cost, Low-ROI Campaigns (Top 50 Outliers)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Campaign_ID, Company, Campaign_Type, Duration, Cost_per_Conversion, ROI, CTR
# MAGIC FROM marketing_campaigns
# MAGIC WHERE Cost_per_Conversion > (SELECT percentile_approx(Cost_per_Conversion, 0.75) FROM marketing_campaigns)
# MAGIC   AND ROI < (SELECT percentile_approx(ROI, 0.25) FROM marketing_campaigns)
# MAGIC ORDER BY Cost_per_Conversion DESC
# MAGIC LIMIT 50;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Key Insights
# MAGIC - Social Media campaigns showed **the lowest ROI (~2.05)** while simultaneously having **the highest Cost per Conversion (~1990)** among the outliers.
# MAGIC - Other low-ROI, high-cost patterns were seen across multiple companies, suggesting cross-channel inefficiencies.
# MAGIC - These campaigns represent priority targets for **budget cuts or creative/targeting optimization**.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC #### Languages × Channel performance (ROI & Conversion)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Language, Channel_Used,
# MAGIC   ROUND(AVG(ROI),2) AS avg_ROI,
# MAGIC   ROUND(AVG(Conversion_Rate),4) AS avg_conversion,
# MAGIC   COUNT(*) AS campaigns
# MAGIC FROM marketing_campaigns
# MAGIC GROUP BY Language, Channel_Used
# MAGIC HAVING COUNT(*) >= 50
# MAGIC ORDER BY avg_ROI DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Key Insights
# MAGIC - **Website + Mandarin** delivered the **highest average ROI (5.05)** across all combinations.
# MAGIC - **Facebook + German** also performed strongly with competitive ROI values.
# MAGIC - **Email + German** showed the **lowest ROI (~4.96)**, indicating potential message–audience mismatch.
# MAGIC - This highlights the importance of **language-tailored creative** and **channel–audience alignment**.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC #### ROI Efficiency by Customer Segment (ROI per Dollar Spent)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Customer_Segment,
# MAGIC   ROUND(AVG(ROI / NULLIF(Acquisition_Cost,0)), 6) AS roi_per_dollar,
# MAGIC   COUNT(*) AS campaigns
# MAGIC FROM marketing_campaigns
# MAGIC GROUP BY Customer_Segment
# MAGIC ORDER BY roi_per_dollar DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Key Insights
# MAGIC - All segments demonstrated **very close ROI-per-dollar efficiency**, with minimal difference between them.
# MAGIC - **Tech Enthusiasts** led marginally as the most cost-efficient audience.
# MAGIC - **Foodies** showed the lowest ROI-per-dollar, indicating potential for offer redesign or targeting refinement.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC #### High-Impression Campaigns Underperforming CTR (Below Channel Median)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH channel_medians AS (
# MAGIC   SELECT Channel_Used, percentile_approx(CTR, 0.5) AS median_ctr
# MAGIC   FROM marketing_campaigns
# MAGIC   GROUP BY Channel_Used
# MAGIC )
# MAGIC SELECT m.Campaign_ID, m.Company, m.Channel_Used, m.Impressions, m.Clicks, m.CTR, m.Duration
# MAGIC FROM marketing_campaigns m
# MAGIC JOIN channel_medians cm ON m.Channel_Used = cm.Channel_Used
# MAGIC WHERE m.Impressions >= 10000 AND m.CTR < cm.median_ctr
# MAGIC ORDER BY m.Impressions DESC
# MAGIC LIMIT 50;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Key Insights
# MAGIC - Large numbers of Google Ads and YouTube campaigns fell below their channel median CTR despite reaching **10,000 impressions**.
# MAGIC - Some campaigns had very low CTR values (e.g., **2.08–2.91** for Google Ads campaigns).
# MAGIC - Duration varied across 15, 30, 45, and 60 days, suggesting that **duration alone is not the cause**.
# MAGIC - These campaigns are ideal for **creative refresh, improved targeting, or landing page optimization**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Overall Insights from SQL Analysis
# MAGIC
# MAGIC - Outlier campaigns (top 50 by cost per conversion) indicate **major budget inefficiencies**, especially on Social Media.
# MAGIC - Website + Mandarin and Facebook + German emerged as **high-performing language–channel pairs**, while Email + German underperformed.
# MAGIC - Customer segments showed **small but meaningful differences** in ROI-per-dollar, with Tech Enthusiasts offering the best value.
# MAGIC - A significant subset of campaigns (especially Google Ads & YouTube) achieved high reach but low CTR — prime candidates for optimization.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Recommendations
# MAGIC These findings support the broader strategy of:
# MAGIC - Reallocating budget away from high-cost/low-ROI outliers  
# MAGIC - Expanding investment in high-performing language–channel combinations  
# MAGIC - Running targeted creative tests on underperforming high-impression campaigns  
# MAGIC - Refining segment-specific messaging where ROI efficiency dips  
# MAGIC
# MAGIC ---