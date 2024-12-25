(stats_methods)=
# Statistical Methods

Here, we provide additional brief of the measures of diversity and inequality introduced in the [exploratory data analysis](02_distributions.ipynb).

## Diversity Indices

The Simpson's Diversity Index and Shannon-Wiener Index are both measures used to quantify the diversity or richness of a dataset, typically applied in ecological studies, but they can be relevant in various other fields as well. Here's how to interpret them:

1. **Simpson's Diversity Index (also known as the Simpson's Diversity or Simpson's Index of Diversity)**:

   - **Interpretation**: Simpson's Diversity Index measures the probability that two individuals randomly selected from a sample will belong to different categories (species, in ecological studies).
   - **Formula**: Simpson's Diversity Index (D) is calculated as follows:
     $$\[ D = 1 - \sum(p_i)^2 \]$$
     where $\( p_i \)$ represents the proportional abundance of the $\( i^{th} \)$ category.
   - **Interpretation of Values**:
     - The index ranges between 0 and 1, where:
       - 0 indicates no diversity (all individuals belong to the same category).
       - 1 indicates maximum diversity (all categories are equally represented).
     - Higher values indicate greater diversity or evenness in the dataset.

1. **Shannon-Wiener Index (also known as Shannon's Diversity Index)**:

   - **Interpretation**: Shannon-Wiener Index measures the uncertainty or entropy in a dataset by considering both the richness (number of categories) and the evenness (distribution of individuals among categories).
   - **Formula**: Shannon-Wiener Index (H) is calculated as follows:
     $$\[ H = -\sum(p_i \times \ln(p_i)) \]$$
     where $\( p_i \)$ represents the proportional abundance of the $\( i^{th} \)$ category.
   - **Interpretation of Values**:
     - The index theoretically ranges from 0 to positive infinity.
     - Higher values indicate higher diversity or uncertainty, where diversity increases with both the number of categories and the evenness of their distribution.
     - A value of 0 indicates no diversity (only one category present), while increasing values indicate increasing diversity.

**Interpretation Tips**:
- Comparing Simpson's Diversity Index and Shannon-Wiener Index: While both indices measure diversity, Simpson's Index is more influenced by the presence of dominant categories, whereas Shannon's Index gives equal weight to all categories regardless of their abundance.
- Context: Interpretation should consider the context of the dataset being analyzed and its specific characteristics.

## Lorenz Curve and Gini Coefficient

The Lorenz Curve and Gini Coefficient are tools used to measure income inequality, wealth distribution, or the uneven distribution of any other variable within a population. Here's how to interpret them:

1. **Lorenz Curve**:

   - **Interpretation**: The Lorenz Curve visually represents the cumulative distribution of a variable plotted against the cumulative proportion of the population. It illustrates how the variable (e.g., income, wealth) is distributed across different segments of the population.
   - **Construction**: The Lorenz Curve is typically plotted with the cumulative proportion of the population on the x-axis and the cumulative proportion of the variable (e.g., cumulative income) on the y-axis.
   - **Interpretation**: The greater the curvature of the Lorenz Curve away from the line of equality (the diagonal line representing perfect equality), the greater the degree of inequality in the distribution of the variable.
   - **Example**: If the Lorenz Curve lies close to the line of equality, it indicates a more equal distribution of the variable. Conversely, if the Lorenz Curve deviates significantly from the line of equality, it suggests greater inequality.

2. **Gini Coefficient**:

   - **Interpretation**: The Gini Coefficient provides a numerical measure of income or wealth inequality based on the Lorenz Curve. It quantifies the extent to which the distribution of a variable deviates from perfect equality.
   - **Calculation**: The Gini Coefficient is calculated as the ratio of the area between the Lorenz Curve and the line of equality to the total area under the line of equality.
   - **Interpretation of Values**:
     - The Gini Coefficient ranges from 0 to 1, where:
       - 0 indicates perfect equality (all individuals have the same income or wealth).
       - 1 indicates perfect inequality (one individual has all the income or wealth).
     - Higher values of the Gini Coefficient indicate greater levels of inequality.
   - **Example**: A Gini Coefficient of 0.4 suggests moderate inequality, while a coefficient of 0.7 indicates high inequality.

**Interpretation Tips**:
- Context: The interpretation of the Lorenz Curve and Gini Coefficient should consider the specific context of the variable being analyzed (e.g., income, wealth) and the population under study.
- Comparisons: These measures are useful for comparing inequality across different populations, regions, or time periods. However, interpretation should be cautious, as different populations may have different distributions and thresholds for what constitutes "high" or "low" inequality.
