# EDA Structure

1. Data Overview and Summary

• Brief introduction to the dataset
• Summary statistics for numerical variables (e.g., rating, vote_sum, vote_count, review_length)
• Overview of categorical variables (e.g., app_name, category, author)
• Description of the time range covered by the dataset

2. Data Distribution Exploration

• Distribution of ratings across all reviews
• Distribution of review lengths
• Distribution of votes (vote_sum and vote_count)

3. Variable Relationships

• Correlation analysis between numerical variables (e.g., rating, vote_sum, vote_count, review_length)
• Relationship between rating and other variables (e.g., review_length, vote_sum, vote_count)
• Relationship between app_name/category and other variables (e.g., average rating per app, distribution of reviews across categories)

4. Temporal Analysis

• Trends in review volume over time (e.g., monthly, quarterly)
• Seasonal patterns in reviews (if applicable)
• Changes in average rating over time

5. Text Analysis

• Preprocessing steps (e.g., tokenization, stop-word removal, stemming)
• Word frequency analysis for review titles and content
• Distribution of review lengths for titles and contents
• N-gram analysis to identify frequently occurring phrases
• Keyword extraction techniques (e.g., TF-IDF, word embeddings) to identify important terms or phrases in reviews

6. Category Specific Analysis

• Overview of the distribution of reviews across different categories
• Analysis of ratings within each category
• Top reviewed apps within each category
• Comparison of review lengths across different categories
• Text analysis within each category:
o Most frequent words or phrases in review titles and content
o Word cloud visualization for each category
o Analysis of common themes or topics mentioned in reviews


## Univariate Nominal Analysis

For a comprehensive analysis of nominal variable distribution, you can perform the following steps:

- **Frequency Distribution Table:** Create a Top-N table  that shows the frequency, percent, cumulative frequency, and cumulative percent of each unique app name. This helps identify the most common apps and their counts.
- **Bar Plot:** Visualize the frequency distribution using a bar plot. This helps in easily identifying the most and least common apps.
- **Pareto Chart:** Create a Pareto chart, which is a bar plot combined with a line graph showing the cumulative percentage. This is useful for identifying the 'vital few' apps that make up the majority of the distribution.
- **Word Cloud:** Generate a word cloud where the size of each app name is proportional to its frequency. This is a visual representation that highlights the most frequent apps.
- **Mode:** Identify the mode or modes (if multiple) of the distribution. This indicates the app name(s) that appear most frequently.
- **Diversity Index:** Calculate diversity indices such as the Simpson's Diversity Index or Shannon-Wiener Index to quantify the diversity of app names.
- **Lorenz Curve and Gini Coefficient:** Plot a Lorenz curve and calculate the Gini coefficient to measure the inequality in the distribution of app frequencies.
- **Entropy Calculation:** Calculate the entropy of the distribution to measure the uncertainty or randomness. Higher entropy indicates a more uniform distribution.
- **Variance and Standard Deviation:** Although typically used for numerical data, variance and standard deviation can be adapted to measure the spread of frequencies in categorical data.
